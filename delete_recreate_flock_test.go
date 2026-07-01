package file_system_storage

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/storage-lock/go-storage"
	"github.com/stretchr/testify/assert"
)

// 子进程模式标记：针对漏洞 H-1（DeleteWithVersion 删 flock.lock 致互斥被绕过）的回归测试。
// 子进程反复"Create→Delete"同一 lockId，每次 Create 成功就自增一个共享文件计数器。
// 若 H-1 未修复（flock 载体被删后重建得新 inode，旧锁失效），会有两个进程同时通过
// "不存在"判断并都 Create 成功，计数器 > 进程数（重复创建）。
const h1SubprocessEnvKey = "GO_FSTORAGE_H1"

// TestDeleteRecreateFlockStability 钉死漏洞 H-1：并发"删-建"循环下 flock 互斥必须始终有效。
//
// 触发场景：多个进程反复对同一 lockId 执行 CreateWithVersion(v=1)→DeleteWithVersion(v=1)。
// 每轮 Delete 会删掉锁目录；若 flock 载体文件也在锁目录内（H-1 未修），下一轮 Create 时
// 另一进程新建的 flock.lock 是新 inode，旧 flock 不保护，两进程可能同时判"不存在"并都
// Create 成功。修复后（flock 载体在 flocks/ 独立目录，永不被删），flock 始终有效，
// 每轮恰好一个进程 Create 成功，计数器 == 总轮数。
func TestDeleteRecreateFlockStability(t *testing.T) {
	if mode := os.Getenv(h1SubprocessEnvKey); mode != "" {
		ws := os.Getenv("GO_FSTORAGE_WS")
		lockId := os.Getenv("GO_FSTORAGE_LOCKID")
		rounds, _ := strconv.Atoi(os.Getenv("GO_FSTORAGE_ROUNDS"))
		ownerId := "h1-proc-" + mode

		s := NewFileSystemStorage(ws)
		_ = s.Init(context.Background())

		for i := 0; i < rounds; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
			info := &storage.LockInformation{
				LockId: lockId, OwnerId: ownerId, Version: 1, LockCount: 1,
			}
			// Create：成功则自增共享计数器（证明"我独占地创建了锁"）
			err := s.CreateWithVersion(ctx, lockId, 1, info)
			if err == nil {
				if e := incrementSharedCounter(ws); e != nil {
					cancel()
					fmt.Println("COUNTER_FAIL:", e)
					os.Exit(1)
				}
			}
			// Create 失败一律合法（已存在/别人正持 flock 都是互斥生效的表现），不视为异常，继续下一轮。
			// 尝试 Delete：版本 1 匹配才删，不匹配(被别人改了/已删)忽略
			_ = s.DeleteWithVersion(ctx, lockId, 1, info)
			cancel()
		}
		fmt.Println("DONE")
		os.Exit(0)
	}

	ws := t.TempDir()
	lockId := "h1-delete-recreate"
	// 预置共享计数器=0
	if err := os.WriteFile(filepath.Join(ws, sharedCounterFile), []byte("0"), 0644); err != nil {
		t.Fatalf("init counter: %v", err)
	}

	const nProc = 6
	const roundsPerProc = 8

	t.Setenv("GO_FSTORAGE_WS", ws)
	t.Setenv("GO_FSTORAGE_LOCKID", lockId)
	t.Setenv("GO_FSTORAGE_ROUNDS", strconv.Itoa(roundsPerProc))

	cmds := make([]*exec.Cmd, nProc)
	for i := 0; i < nProc; i++ {
		cmd := exec.Command(os.Args[0], "-test.run=TestDeleteRecreateFlockStability")
		cmd.Env = append(os.Environ(), fmt.Sprintf("%s=%d", h1SubprocessEnvKey, i))
		cmds[i] = cmd
	}

	var wg sync.WaitGroup
	results := make([]string, nProc)
	start := make(chan struct{})
	for i, cmd := range cmds {
		wg.Add(1)
		go func(idx int, c *exec.Cmd) {
			defer wg.Done()
			<-start
			out, _ := c.CombinedOutput()
			results[idx] = strings.TrimSpace(string(out))
		}(i, cmd)
	}
	close(start)
	wg.Wait()

	allDone := true
	for _, r := range results {
		if r != "DONE" {
			allDone = false
			t.Logf("子进程异常: %q", r)
		}
	}
	assert.True(t, allDone, "所有子进程应正常完成，结果: %v", results)

	// 核心断言：Create 成功总次数（=计数器值）必须 <= 总轮数。
	// 每轮只有一个进程能 Create 成功，所以成功次数 <= 进程数*轮数。
	// 若 H-1 未修，flock 被绕过会导致同一轮多个进程都 Create 成功，计数器 > 总轮数。
	got := readSharedCounterH1(t, ws)
	maxAllowed := int64(nProc * roundsPerProc)
	assert.True(t, got <= maxAllowed,
		"漏洞 H-1 回归：Create 成功次数 %d 超过最大允许 %d（说明删-建循环中 flock 互斥被绕过，有重复创建）",
		got, maxAllowed)
	// 进一步：成功次数应 > 0（否则说明 Create 全失败，测试本身没生效）
	assert.True(t, got > 0, "应至少有一次 Create 成功，实际 %d", got)
}


func readSharedCounterH1(t *testing.T, ws string) int64 {
	b, err := os.ReadFile(filepath.Join(ws, sharedCounterFile))
	assert.Nil(t, err)
	v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	assert.Nil(t, err)
	return v
}
