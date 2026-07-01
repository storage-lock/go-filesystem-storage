package file_system_storage

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	"github.com/stretchr/testify/assert"
)

// 子进程模式的环境变量标记。主测试进程用 exec 启动自身副本，副本看到此标记后
// 执行子进程逻辑（对一个 lockId 并发 CreateWithVersion），借此验证多进程下 CAS 线性一致。
const subprocessEnvKey = "GO_FSTORAGE_SUBPROCESS"

// TestCreateWithVersionCASMultiProcess 验证漏洞 H 的修复：
// 多个【独立进程】同时对同一 lockId 调用 CreateWithVersion，必须【恰好一个成功】，
// 其余全部失败。修复前（Check-Then-Act）多个进程会同时通过"不存在"判断并各自创建成功，
// CAS 完全不成立；修复后（flock 串行化）全局只放行一个。
func TestCreateWithVersionCASMultiProcess(t *testing.T) {
	if mode := os.Getenv(subprocessEnvKey); mode != "" {
		// 子进程：尝试创建锁，输出 OK/FAIL 给主进程解析
		ws := os.Getenv("GO_FSTORAGE_WS")
		lockId := os.Getenv("GO_FSTORAGE_LOCKID")
		ownerId := "proc-" + mode
		s := NewFileSystemStorage(ws)
		_ = s.Init(context.Background())
		info := &storage.LockInformation{
			LockId: lockId, OwnerId: ownerId, Version: 1, LockCount: 1,
		}
		err := s.CreateWithVersion(context.Background(), lockId, 1, info)
		if err == nil {
			fmt.Println("OK")
		} else {
			fmt.Println("FAIL:", err.Error())
		}
		os.Exit(0)
	}

	// 主进程：起 N 个子进程并发抢同一把锁
	ws := t.TempDir()
	lockId := "multi-proc-cas"
	t.Setenv("GO_FSTORAGE_WS", ws)
	t.Setenv("GO_FSTORAGE_LOCKID", lockId)

	const n = 8
	cmds := make([]*exec.Cmd, n)
	for i := 0; i < n; i++ {
		cmd := exec.Command(os.Args[0], "-test.run=TestCreateWithVersionCASMultiProcess")
		cmd.Env = append(os.Environ(), fmt.Sprintf("%s=%s", subprocessEnvKey, strconv.Itoa(i)))
		cmds[i] = cmd
	}

	var wg sync.WaitGroup
	results := make([]string, n)
	for i, cmd := range cmds {
		wg.Add(1)
		go func(idx int, c *exec.Cmd) {
			defer wg.Done()
			out, _ := c.CombinedOutput()
			results[idx] = strings.TrimSpace(string(out))
		}(i, cmd)
	}
	wg.Wait()

	okCount := 0
	for _, r := range results {
		if r == "OK" {
			okCount++
		}
	}
	// 核心断言：多进程并发下，CreateWithVersion 恰好成功一次
	assert.Equal(t, 1, okCount,
		"多进程并发 CreateWithVersion 应恰好一个成功（flock 串行化保证 CAS），实际成功 %d 个。结果: %v", okCount, results)

	// 进一步验证：成功的那个写入的锁信息可被正确读回
	s := NewFileSystemStorage(ws)
	_ = s.Init(context.Background())
	jsonStr, err := s.Get(context.Background(), lockId)
	assert.Nil(t, err)
	info, err := storage.LockInformationFromJsonString(jsonStr)
	assert.Nil(t, err)
	assert.Equal(t, storage.Version(1), info.Version)
	assert.True(t, strings.HasPrefix(info.OwnerId, "proc-"), "owner 应为某个子进程: %s", info.OwnerId)
}

// TestUpdateWithVersionCASMultiProcess 验证多进程并发 UpdateWithVersion 的 CAS：
// 先主进程创建 v=1 的锁，再起多个子进程并发尝试 UpdateWithVersion(v1→v2)，
// 恰好一个成功，其余 ErrVersionMiss。修复前会多个进程同时通过 v=1 检查并都写入成功（覆盖）。
func TestUpdateWithVersionCASMultiProcess(t *testing.T) {
	if mode := os.Getenv(subprocessEnvKey + "_UPD"); mode != "" {
		ws := os.Getenv("GO_FSTORAGE_WS")
		lockId := os.Getenv("GO_FSTORAGE_LOCKID")
		ownerId := "upd-proc-" + mode
		s := NewFileSystemStorage(ws)
		_ = s.Init(context.Background())
		info := &storage.LockInformation{
			LockId: lockId, OwnerId: ownerId, Version: 2, LockCount: 1,
		}
		err := s.UpdateWithVersion(context.Background(), lockId, 1, 2, info)
		if err == nil {
			fmt.Println("OK")
		} else {
			fmt.Println("FAIL:", err.Error())
		}
		os.Exit(0)
	}

	ws := t.TempDir()
	lockId := "multi-proc-update-cas"
	// 预置 v=1 的锁
	s := NewFileSystemStorage(ws)
	assert.Nil(t, s.Init(context.Background()))
	assert.Nil(t, s.CreateWithVersion(context.Background(), lockId, 1, &storage.LockInformation{
		LockId: lockId, OwnerId: "init", Version: 1, LockCount: 1,
	}))

	t.Setenv("GO_FSTORAGE_WS", ws)
	t.Setenv("GO_FSTORAGE_LOCKID", lockId)

	const n = 8
	cmds := make([]*exec.Cmd, n)
	for i := 0; i < n; i++ {
		cmd := exec.Command(os.Args[0], "-test.run=TestUpdateWithVersionCASMultiProcess")
		cmd.Env = append(os.Environ(), fmt.Sprintf("%s_UPD=%d", subprocessEnvKey, i))
		cmds[i] = cmd
	}
	var wg sync.WaitGroup
	results := make([]string, n)
	for i, cmd := range cmds {
		wg.Add(1)
		go func(idx int, c *exec.Cmd) {
			defer wg.Done()
			out, _ := c.CombinedOutput()
			results[idx] = strings.TrimSpace(string(out))
		}(i, cmd)
	}
	wg.Wait()

	okCount := 0
	for _, r := range results {
		if r == "OK" {
			okCount++
		}
	}
	assert.Equal(t, 1, okCount,
		"多进程并发 UpdateWithVersion 应恰好一个成功，实际 %d 个。结果: %v", okCount, results)

	// 验证最终版本为 2
	jsonStr, err := s.Get(context.Background(), lockId)
	assert.Nil(t, err)
	info, _ := storage.LockInformationFromJsonString(jsonStr)
	assert.Equal(t, storage.Version(2), info.Version)
}

// TestSingleProcessBasicCRUD 单进程内的基本 CRUD 与错误码，确保重构没破坏接口契约。
func TestSingleProcessBasicCRUD(t *testing.T) {
	ws := t.TempDir()
	s := NewFileSystemStorage(ws)
	assert.Nil(t, s.Init(context.Background()))
	ctx := context.Background()
	lockId := "basic-crud"

	// Get 不存在的锁 → ErrLockNotFound
	_, err := s.Get(ctx, lockId)
	assert.True(t, errors.Is(err, storage_lock.ErrLockNotFound), "不存在的锁应返回 ErrLockNotFound, got %v", err)

	// Create v=1
	info := &storage.LockInformation{LockId: lockId, OwnerId: "A", Version: 1, LockCount: 1}
	assert.Nil(t, s.CreateWithVersion(ctx, lockId, 1, info))

	// 重复 Create → ErrLockAlreadyExists
	err = s.CreateWithVersion(ctx, lockId, 1, info)
	assert.True(t, errors.Is(err, storage_lock.ErrLockAlreadyExists), "重复创建应 ErrLockAlreadyExists, got %v", err)

	// Update 版本不匹配 → ErrVersionMiss
	err = s.UpdateWithVersion(ctx, lockId, 99, 2, info)
	assert.True(t, errors.Is(err, storage_lock.ErrVersionMiss), "版本不匹配应 ErrVersionMiss, got %v", err)

	// Update 版本匹配 → 成功
	info2 := &storage.LockInformation{LockId: lockId, OwnerId: "A", Version: 2, LockCount: 2}
	assert.Nil(t, s.UpdateWithVersion(ctx, lockId, 1, 2, info2))

	// Delete 版本不匹配 → ErrVersionMiss
	err = s.DeleteWithVersion(ctx, lockId, 1, info)
	assert.True(t, errors.Is(err, storage_lock.ErrVersionMiss))

	// Delete 版本匹配 → 成功
	assert.Nil(t, s.DeleteWithVersion(ctx, lockId, 2, info2))

	// 删后再 Get → ErrLockNotFound
	_, err = s.Get(ctx, lockId)
	assert.True(t, errors.Is(err, storage_lock.ErrLockNotFound))
}

// TestConcurrentGoroutinesSingleProcess 单进程多 goroutine 并发 Create，
// mu + flock 共同保护下恰好一个成功（虽然 flock 在单进程冗余，但验证 mu 路径正确）。
func TestConcurrentGoroutinesSingleProcess(t *testing.T) {
	ws := t.TempDir()
	s := NewFileSystemStorage(ws)
	assert.Nil(t, s.Init(context.Background()))
	ctx := context.Background()
	lockId := "single-proc-concurrent"

	var successCount int64
	var wg sync.WaitGroup
	const n = 50
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			info := &storage.LockInformation{
				LockId: lockId, OwnerId: "g" + strconv.Itoa(i), Version: 1, LockCount: 1,
			}
			err := s.CreateWithVersion(ctx, lockId, 1, info)
			if err == nil {
				atomic.AddInt64(&successCount, 1)
			}
		}(i)
	}
	wg.Wait()
	assert.Equal(t, int64(1), successCount, "单进程并发 Create 应恰好一个成功")
}

// 防止 time 包未使用告警（部分用例可能用不到）
var _ = time.Second
