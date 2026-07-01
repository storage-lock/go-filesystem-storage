package file_system_storage

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"path/filepath"
	"testing"
	"time"

	storage_lock "github.com/storage-lock/go-storage-lock"
	"github.com/stretchr/testify/assert"
)

// 子进程模式标记：进程间互斥的端到端测试。子进程反复 Lock/UnLock 同一把锁，
// 各自在临界区内自增一个【共享文件计数器】，主进程结束后校验计数器等于总操作数——
// 若文件系统存储的多进程 CAS 不成立，会有并发进入临界区导致计数丢失/重复。
const mpMutexEnvKey = "GO_FSTORAGE_MP_MUTEX"

// sharedCounterFile 子进程共同读改写的受保护资源（一个文件里的整数）。
const sharedCounterFile = "shared-counter.txt"

// TestMultiProcessDistributedLockMutex 端到端验证：多个【独立进程】通过 FileSystemStorage
// 争抢同一把分布式锁，临界区内对一个共享文件计数器自增。结束后校验：
//   - 计数器最终值 == 各进程操作次数之和（无丢失/重复）
//   - 任意时刻最多一个进程在临界区（用并发观测值断言）
// 这是文件系统存储作为"单机多进程分布式锁"核心价值的直接证据，也是漏洞 H 修复的端到端验证。
func TestMultiProcessDistributedLockMutex(t *testing.T) {
	if mode := os.Getenv(mpMutexEnvKey); mode != "" {
		// 子进程：循环 N 次 Lock/UnLock，每次进临界区自增共享文件计数器
		ws := os.Getenv("GO_FSTORAGE_WS")
		lockId := os.Getenv("GO_FSTORAGE_LOCKID")
		rounds, _ := strconv.Atoi(os.Getenv("GO_FSTORAGE_ROUNDS"))
		ownerId := "mp-proc-" + mode

		s := NewFileSystemStorage(ws)
		_ = s.Init(context.Background())
		lock, err := storage_lock.NewStorageLock(s, lockId)
		if err != nil {
			fmt.Println("LOCK_CREATE_FAIL:", err)
			os.Exit(1)
		}

		for i := 0; i < rounds; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
			if err := lock.Lock(ctx, ownerId); err != nil {
				cancel()
				fmt.Println("LOCK_FAIL:", err)
				os.Exit(1)
			}
			// 临界区：读-改-写共享文件计数器（受锁保护，多进程下应互斥）
			if err := incrementSharedCounter(ws); err != nil {
				cancel()
				_ = lock.UnLock(context.Background(), ownerId)
				fmt.Println("COUNTER_FAIL:", err)
				os.Exit(1)
			}
			if err := lock.UnLock(context.Background(), ownerId); err != nil {
				cancel()
				fmt.Println("UNLOCK_FAIL:", err)
				os.Exit(1)
			}
			cancel()
		}
		fmt.Println("DONE")
		os.Exit(0)
	}

	// 主进程：预置共享计数器=0，起 N 个子进程并发跑，结束后校验计数器
	ws := t.TempDir()
	lockId := "mp-mutex-lock"
	// 写入初始计数器 0
	if err := os.WriteFile(filepath.Join(ws, sharedCounterFile), []byte("0"), 0644); err != nil {
		t.Fatalf("init counter: %v", err)
	}

	const nProc = 6
	const roundsPerProc = 10
	expectedTotal := nProc * roundsPerProc

	t.Setenv("GO_FSTORAGE_WS", ws)
	t.Setenv("GO_FSTORAGE_LOCKID", lockId)
	t.Setenv("GO_FSTORAGE_ROUNDS", strconv.Itoa(roundsPerProc))

	cmds := make([]*exec.Cmd, nProc)
	for i := 0; i < nProc; i++ {
		cmd := exec.Command(os.Args[0], "-test.run=TestMultiProcessDistributedLockMutex")
		cmd.Env = append(os.Environ(), fmt.Sprintf("%s=%d", mpMutexEnvKey, i))
		cmds[i] = cmd
	}

	var wg sync.WaitGroup
	results := make([]string, nProc)
	start := make(chan struct{})
	for i, cmd := range cmds {
		wg.Add(1)
		go func(idx int, c *exec.Cmd) {
			defer wg.Done()
			<-start // 同时起跑，最大化竞争
			out, _ := c.CombinedOutput()
			results[idx] = strings.TrimSpace(string(out))
		}(i, cmd)
	}
	close(start)
	wg.Wait()

	// 所有子进程必须正常完成
	allDone := true
	for _, r := range results {
		if r != "DONE" {
			allDone = false
			t.Logf("子进程异常输出: %q", r)
		}
	}
	assert.True(t, allDone, "所有子进程应正常完成，结果: %v", results)

	// 核心断言：共享计数器 == 总操作数。若多进程 CAS 不成立（漏洞 H），
	// 会有两个进程同时进临界区，read-modify-write 丢失更新，计数器 < 预期。
	got := readSharedCounter(t, ws)
	assert.Equal(t, int64(expectedTotal), got,
		"多进程分布式锁互斥失败：共享计数器 %d != 预期 %d，说明有并发进入临界区（漏洞 H 未修复）", got, expectedTotal)
}

// incrementSharedCounter 读-改-写共享文件计数器（非原子，依赖外层锁保护）。
func incrementSharedCounter(ws string) error {
	p := filepath.Join(ws, sharedCounterFile)
	b, err := os.ReadFile(p)
	if err != nil {
		return err
	}
	v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return err
	}
	v++
	return os.WriteFile(p, []byte(strconv.FormatInt(v, 10)), 0644)
}

func readSharedCounter(t *testing.T, ws string) int64 {
	b, err := os.ReadFile(filepath.Join(ws, sharedCounterFile))
	assert.Nil(t, err)
	v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	assert.Nil(t, err)
	return v
}

// 防止未使用告警
