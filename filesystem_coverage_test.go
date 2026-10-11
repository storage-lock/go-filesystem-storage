package file_system_storage

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	"github.com/stretchr/testify/assert"
)

func testFSStorage(t *testing.T) (*FileSystemStorage, context.Context) {
	t.Helper()
	ws := t.TempDir()
	s := NewFileSystemStorage(ws)
	assert.Nil(t, s.Init(context.Background()))
	return s, context.Background()
}

func testFSInfo(lockId, owner string, version storage.Version) *storage.LockInformation {
	return &storage.LockInformation{LockId: lockId, OwnerId: owner, Version: version, LockCount: 1}
}

// TestInitBranches 覆盖 Init 的三个分支：不存在则创建、非目录报错、已存在目录成功。
func TestInitBranches(t *testing.T) {
	ctx := context.Background()

	// 不存在的 workspace → 自动创建
	ws := filepath.Join(t.TempDir(), "sub", "dir")
	s := NewFileSystemStorage(ws)
	assert.Nil(t, s.Init(ctx))
	assert.DirExists(t, ws)

	// workspace 是普通文件 → 报错
	f := filepath.Join(t.TempDir(), "file")
	assert.Nil(t, os.WriteFile(f, []byte("x"), 0644))
	s2 := NewFileSystemStorage(f)
	assert.NotNil(t, s2.Init(ctx))

	// 已存在目录 → 成功
	s3 := NewFileSystemStorage(t.TempDir())
	assert.Nil(t, s3.Init(ctx))
}

// TestEnsureDirectoryExistsBranches 覆盖 EnsureDirectoryExists 的 stat 报错与非目录分支。
func TestEnsureDirectoryExistsBranches(t *testing.T) {
	s, _ := testFSStorage(t)

	// 路径上已有普通文件 → MkdirAll 失败 → 报错
	f := filepath.Join(t.TempDir(), "blocker")
	assert.Nil(t, os.WriteFile(f, []byte("x"), 0644))
	sub := filepath.Join(f, "sub")
	assert.NotNil(t, s.EnsureDirectoryExists(sub))

	// 指向普通文件 → 非目录报错
	assert.NotNil(t, s.EnsureDirectoryExists(f))

	// 不存在则创建成功
	assert.Nil(t, s.EnsureDirectoryExists(filepath.Join(t.TempDir(), "newdir")))
}

// TestWithFlockErrorBranches 覆盖 withFlock 的载体目录创建失败与文件打开失败分支。
func TestWithFlockErrorBranches(t *testing.T) {
	// workspace 是普通文件 → flocksDir 创建失败
	f := filepath.Join(t.TempDir(), "file")
	assert.Nil(t, os.WriteFile(f, []byte("x"), 0644))
	s := NewFileSystemStorage(f)
	err := s.withFlock("l", func() error { return nil })
	assert.NotNil(t, err)
}

// TestAtomicWriteFileErrorBranches 覆盖 atomicWriteFile 的写入失败与 rename 失败分支。
func TestAtomicWriteFileErrorBranches(t *testing.T) {
	s, _ := testFSStorage(t)

	// 写到不存在的目录 → WriteFile 失败
	err := s.atomicWriteFile(filepath.Join(t.TempDir(), "no-such-dir", "f.json"), []byte("x"))
	assert.NotNil(t, err)

	// 目标是目录 → Rename 失败
	dir := filepath.Join(t.TempDir(), "adir")
	assert.Nil(t, os.MkdirAll(dir, 0755))
	err = s.atomicWriteFile(dir, []byte("x"))
	assert.NotNil(t, err)
}

// TestCreateErrorBranches 覆盖 Create 的 Stat 其它错误分支（权限/路径异常）。
func TestCreateErrorBranches(t *testing.T) {
	s, ctx := testFSStorage(t)

	// 锁目录被普通文件占住 → EnsureDirectoryExists 失败
	lockId := "blocked"
	assert.Nil(t, os.WriteFile(s.BuildLockDirectory(lockId), []byte("x"), 0644))
	assert.NotNil(t, s.CreateWithVersion(ctx, lockId, 1, testFSInfo(lockId, "A", 1)))
}

// TestUpdateErrorBranches 覆盖 Update 的目录创建失败与读失败分支。
func TestUpdateErrorBranches(t *testing.T) {
	s, ctx := testFSStorage(t)

	// 锁目录被普通文件占住 → EnsureDirectoryExists 失败
	lockId := "blocked-u"
	assert.Nil(t, os.WriteFile(s.BuildLockDirectory(lockId), []byte("x"), 0644))
	assert.NotNil(t, s.UpdateWithVersion(ctx, lockId, 1, 2, testFSInfo(lockId, "A", 2)))
}

// TestDeleteReadError 覆盖 Delete 的 ReadCurrentLockInformation 出错分支（current.json 是目录）。
func TestDeleteReadError(t *testing.T) {
	s, ctx := testFSStorage(t)

	lockId := "broken-del"
	assert.Nil(t, s.CreateWithVersion(ctx, lockId, 1, testFSInfo(lockId, "A", 1)))
	// 把 current.json 替换成目录 → ReadFile 报错（非 NotExist）
	assert.Nil(t, os.Remove(s.BuildCurrentLockFilePath(lockId)))
	assert.Nil(t, os.MkdirAll(s.BuildCurrentLockFilePath(lockId), 0755))
	assert.NotNil(t, s.DeleteWithVersion(ctx, lockId, 1, testFSInfo(lockId, "A", 1)))

	// Get 同样走 ReadCurrentLockInformation 报错分支
	_, err := s.Get(ctx, lockId)
	assert.NotNil(t, err)
}

// TestUpdateReadError 覆盖 Update 的读失败与空文件分支。
func TestUpdateReadError(t *testing.T) {
	s, ctx := testFSStorage(t)

	lockId := "broken-upd"
	assert.Nil(t, s.CreateWithVersion(ctx, lockId, 1, testFSInfo(lockId, "A", 1)))
	assert.Nil(t, os.Remove(s.BuildCurrentLockFilePath(lockId)))
	assert.Nil(t, os.MkdirAll(s.BuildCurrentLockFilePath(lockId), 0755))
	assert.NotNil(t, s.UpdateWithVersion(ctx, lockId, 1, 2, testFSInfo(lockId, "A", 2)))
}

// TestReadCurrentBranches 覆盖 ReadCurrentLockInformation 的空文件与坏 JSON 分支。
func TestReadCurrentBranches(t *testing.T) {
	s, ctx := testFSStorage(t)
	lockId := "read-branches"
	assert.Nil(t, s.CreateWithVersion(ctx, lockId, 1, testFSInfo(lockId, "A", 1)))

	// 空文件 → (nil, nil) → Update 报 ErrVersionMiss
	assert.Nil(t, os.WriteFile(s.BuildCurrentLockFilePath(lockId), []byte{}, 0644))
	got, err := s.ReadCurrentLockInformation(lockId)
	assert.Nil(t, err)
	assert.Nil(t, got)

	// 坏 JSON → 报错
	assert.Nil(t, os.WriteFile(s.BuildCurrentLockFilePath(lockId), []byte("{bad"), 0644))
	_, err = s.ReadCurrentLockInformation(lockId)
	assert.NotNil(t, err)
}

// TestIsLockDirectoryBranches 覆盖 IsLockDirectory 的前后缀/不存在/文件分支。
func TestIsLockDirectoryBranches(t *testing.T) {
	s, _ := testFSStorage(t)

	assert.False(t, s.IsLockDirectory("no-prefix-x.lock"))
	assert.False(t, s.IsLockDirectory(LockDirectoryPrefix+"x-no-suffix"))
	assert.False(t, s.IsLockDirectory(LockDirectoryPrefix+"missing"+LockDirectorySuffix))

	// 真实锁目录 → true
	lockId := "isdir"
	assert.Nil(t, s.EnsureDirectoryExists(s.BuildLockDirectory(lockId)))
	assert.True(t, s.IsLockDirectory(s.BuildLockDirectory(lockId)))

	// 前后缀对但是普通文件 → false
	f := filepath.Join(t.TempDir(), LockDirectoryPrefix+"file"+LockDirectorySuffix)
	assert.Nil(t, os.WriteFile(f, []byte("x"), 0644))
	assert.False(t, s.IsLockDirectory(f))
}

// TestListBranches 覆盖 List 全部分支：workspace 不存在、跳过文件/非前缀/坏名/空记录/坏JSON、排序成功。
func TestListBranches(t *testing.T) {
	ctx := context.Background()

	// workspace 不存在 → ReadDir 报错
	s := NewFileSystemStorage(filepath.Join(t.TempDir(), "no-such-dir"))
	_, err := s.List(ctx)
	assert.NotNil(t, err)

	s2, _ := testFSStorage(t)

	// workspace 下放普通文件 → 跳过
	assert.Nil(t, os.WriteFile(filepath.Join(s2.workspace, "plain.txt"), []byte("x"), 0644))
	// 非前缀目录 → 跳过
	assert.Nil(t, os.MkdirAll(filepath.Join(s2.workspace, "other-dir"), 0755))
	// 前缀对但无后缀 → ExtractLockId 返回空 → 跳过
	assert.Nil(t, os.MkdirAll(filepath.Join(s2.workspace, LockDirectoryPrefix+"nosuffix"), 0755))
	// 前后缀对但 current.json 为空 → ReadCurrent 返回 nil → 跳过
	emptyLockDir := filepath.Join(s2.workspace, LockDirectoryPrefix+"empty"+LockDirectorySuffix)
	assert.Nil(t, os.MkdirAll(emptyLockDir, 0755))
	assert.Nil(t, os.WriteFile(filepath.Join(emptyLockDir, CurrentLockFileName), []byte{}, 0644))

	// 正常建两个锁（乱序创建，验证排序）
	assert.Nil(t, s2.CreateWithVersion(ctx, "b-lock", 1, testFSInfo("b-lock", "A", 1)))
	assert.Nil(t, s2.CreateWithVersion(ctx, "a-lock", 1, testFSInfo("a-lock", "A", 1)))

	it, err := s2.List(ctx)
	assert.Nil(t, err)
	var ids []string
	for it.Next() {
		ids = append(ids, it.Value().LockId)
	}
	assert.Equal(t, []string{"a-lock", "b-lock"}, ids)

	assert.Nil(t, s2.Close(ctx))
}

// TestCompatHelpers 覆盖三个兼容 helper：BuildLockVersionFilePath / ReadLockInformationFromPath / ReadLockLatestVersionInformation。
func TestCompatHelpers(t *testing.T) {
	s, ctx := testFSStorage(t)
	lockId := "compat"

	p := s.BuildLockVersionFilePath(lockId, 3)
	assert.Contains(t, p, lockId)

	// ReadLockInformationFromPath：不存在报错；写入后成功
	_, err := s.ReadLockInformationFromPath(p)
	assert.NotNil(t, err)
	info := testFSInfo(lockId, "A", 1)
	assert.Nil(t, os.MkdirAll(s.BuildLockDirectory(lockId), 0755))
	assert.Nil(t, os.WriteFile(p, []byte(info.ToJsonString()), 0644))
	got, err := s.ReadLockInformationFromPath(p)
	assert.Nil(t, err)
	assert.Equal(t, lockId, got.LockId)

	// ReadLockLatestVersionInformation：current.json 不存在 → (nil, nil)
	latest, err := s.ReadLockLatestVersionInformation("missing")
	assert.Nil(t, err)
	assert.Nil(t, latest)

	// 建锁后 current 存在 → 读到
	assert.Nil(t, s.CreateWithVersion(ctx, lockId, 1, info))
	latest, err = s.ReadLockLatestVersionInformation(lockId)
	assert.Nil(t, err)
	assert.Equal(t, lockId, latest.LockId)
}

// TestExtractLockId 覆盖 ExtractLockId 的非锁目录分支。
func TestExtractLockId(t *testing.T) {
	s, ctx := testFSStorage(t)
	assert.Equal(t, "", s.ExtractLockIdFromLockDirectoryName("random"))
	// 目录名形式：需先建锁使目录存在（IsLockDirectory 要求 stat 为目录）
	assert.Nil(t, s.CreateWithVersion(ctx, "abc", 1, testFSInfo("abc", "A", 1)))
	assert.Equal(t, "abc", s.ExtractLockIdFromLockDirectoryName(LockDirectoryPrefix+"abc"+LockDirectorySuffix))
}

// TestWatchBasics 覆盖 Watch：空 lockId 报错；建锁后能收到变化事件。
func TestWatchBasics(t *testing.T) {
	s, ctx := testFSStorage(t)

	_, err := s.Watch(ctx, "")
	assert.NotNil(t, err)

	lockId := "watch-basic"
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch, err := s.Watch(wctx, lockId)
	assert.Nil(t, err)
	assert.NotNil(t, ch)

	// 建锁后 watcher 应在轮询周期内推送（500ms，留足余量）
	assert.Nil(t, s.CreateWithVersion(ctx, lockId, 1, testFSInfo(lockId, "A", 1)))
	select {
	case info := <-ch:
		assert.Equal(t, lockId, info.LockId)
	case <-time.After(5 * time.Second):
		t.Fatal("watch 未在超时内推送锁信息")
	}
}

// TestGetNameCapabilities 显式覆盖 GetName/Capabilities（防回归）。
func TestGetNameCapabilities(t *testing.T) {
	s, _ := testFSStorage(t)
	assert.Equal(t, StorageName, s.GetName())
	caps := s.Capabilities()
	assert.Contains(t, caps, storage.CapabilityReliableTime)
	assert.Contains(t, caps, storage.CapabilityWatch)
	if flockSupported {
		assert.Contains(t, caps, storage.CapabilityCAS)
		assert.Contains(t, caps, storage.CapabilityAtomicDelete)
	}
}

// TestDeleteMissing 覆盖 Delete 记录不存在 → ErrLockNotFound。
func TestDeleteMissing(t *testing.T) {
	s, ctx := testFSStorage(t)
	err := s.DeleteWithVersion(ctx, "missing", 1, testFSInfo("missing", "A", 1))
	assert.ErrorIs(t, err, storage_lock.ErrLockNotFound)
}

// TestRemainingBranches 覆盖剩余顽固分支：Init 的 Stat 非 NotExist 错误、
// withFlock 的 OpenFile/拿锁失败、Create 的 Stat 其它错误。
func TestRemainingBranches(t *testing.T) {
	ctx := context.Background()

	// 1. Init：workspace 下无权限 → Stat 报 permission denied 并透传（非 root 生效）。
	if runtime.GOOS != "windows" && os.Geteuid() != 0 {
		parent := t.TempDir()
		locked := filepath.Join(parent, "locked")
		assert.Nil(t, os.MkdirAll(locked, 0755))
		assert.Nil(t, os.Chmod(locked, 0000))
		defer func() { _ = os.Chmod(locked, 0755) }()
		s := NewFileSystemStorage(filepath.Join(locked, "ws"))
		err := s.Init(ctx)
		assert.NotNil(t, err)
		assert.False(t, strings.Contains(err.Error(), "is not directory"))
	}

	// 2. withFlock：载体路径被目录占住 → OpenFile 报 is a directory。
	s2, _ := testFSStorage(t)
	flocksDir := filepath.Join(s2.workspace, FlocksDirectoryName)
	assert.Nil(t, os.MkdirAll(flocksDir, 0755))
	occupy := filepath.Join(flocksDir, "x"+FlockFileName)
	assert.Nil(t, os.MkdirAll(occupy, 0755))
	err := s2.withFlock("x", func() error { return nil })
	assert.NotNil(t, err)
	assert.NotErrorIs(t, err, storage_lock.ErrVersionMiss)

	// 3. flock 拿不到锁 → ErrVersionMiss：同进程内先持住排他锁，再调 Update
	//    （Update 内部 withFlock 用 LOCK_NB 拿锁，失败转 ErrVersionMiss）。
	s3, _ := testFSStorage(t)
	lockId := "flock-busy"
	assert.Nil(t, s3.CreateWithVersion(ctx, lockId, 1, testFSInfo(lockId, "A", 1)))
	flockPath := s3.BuildFlockFilePath(lockId)
	holder, err := os.OpenFile(flockPath, os.O_CREATE|os.O_RDWR, 0644)
	assert.Nil(t, err)
	defer func() { _ = holder.Close() }()
	assert.Nil(t, flock(int(holder.Fd())))
	err = s3.UpdateWithVersion(ctx, lockId, 1, 2, testFSInfo(lockId, "A", 2))
	assert.ErrorIs(t, err, storage_lock.ErrVersionMiss)
	assert.Nil(t, funlock(int(holder.Fd())))

	// 4. Create：currentPath 上 Stat 报非 NotExist 错误。
	//    current.json 位置预置循环 symlink → Stat 报 ELOOP（too many links），
	//    errors.Is(err, os.ErrNotExist) 为 false，走 return err 分支。
	s4, _ := testFSStorage(t)
	badLock := "stat-err"
	assert.Nil(t, os.MkdirAll(s4.BuildLockDirectory(badLock), 0755))
	curPath := s4.BuildCurrentLockFilePath(badLock)
	assert.Nil(t, os.Symlink(curPath, curPath))
	err = s4.CreateWithVersion(ctx, badLock, 1, testFSInfo(badLock, "A", 1))
	assert.NotNil(t, err)
	assert.NotErrorIs(t, err, storage_lock.ErrLockAlreadyExists)
}

// TestUpdateMissingRecord 覆盖 Update 的记录不存在分支：
// 锁目录存在但 current.json 缺失 → ReadCurrent 返回 (nil,nil) → ErrVersionMiss（可重试语义）。
func TestUpdateMissingRecord(t *testing.T) {
	s, ctx := testFSStorage(t)
	emptyLock := "empty-dir-upd"
	assert.Nil(t, os.MkdirAll(s.BuildLockDirectory(emptyLock), 0755))
	err := s.UpdateWithVersion(ctx, emptyLock, 1, 2, testFSInfo(emptyLock, "A", 2))
	assert.ErrorIs(t, err, storage_lock.ErrVersionMiss)
}
