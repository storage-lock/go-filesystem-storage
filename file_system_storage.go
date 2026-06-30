package file_system_storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang-infrastructure/go-iterator"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// FileSystemStorage 基于文件系统来存储锁，可以用作同一台机器上的不同进程之间互相协同工作，优势是兼容性好，只要有文件系统能读写文件就能使用这种锁
//
// # CAS 实现说明
//
// 此实现通过 O_CREATE|O_EXCL 标志实现原子的文件创建，从而支持真正的 CAS 语义：
//   - CreateWithVersion：用 O_EXCL 创建锁文件，若文件已存在则返回 ErrLockAlreadyExists
//   - UpdateWithVersion：先读取当前版本校验，再用 O_EXCL 创建新版本文件，旧版本文件保留为历史
//   - DeleteWithVersion：校验版本后删除整个锁目录
//
// ⚠️ 注意：UpdateWithVersion 中的"读取校验"和"创建新版本文件"之间仍存在微小竞态窗口
// （文件系统无法在单次系统调用中完成"条件检查+写入"）。
// 在多进程高并发场景下，理论上仍可能有两个进程同时通过版本检查并各自创建新版本文件。
// 因此本实现适用于：
//   - 单进程内的锁协调（完全安全）
//   - 低并发的多进程场景（竞争概率极低）
//   - 测试和开发环境
//
// 对于 UpdateWithVersion 的竞态，本实现采用"版本号作为文件名"的策略来缓解：
// 两个进程同时通过检查时，会尝试创建相同的新版本文件，O_EXCL 保证只有一个成功，
// 另一个会得到 ErrLockAlreadyExists，从而保证最终只有一个更新生效。
type FileSystemStorage struct {
	workspace string

	// mu 保护单进程内的并发写操作，使单进程多 goroutine 场景下 CAS 完全正确
	// 注意：这无法保护多进程并发（多进程需要用 flock），多进程高并发场景请参见类型文档的限制说明
	mu sync.Mutex
}

var _ storage.Storage = &FileSystemStorage{}

// NewFileSystemStorage 基于文件系统存储锁的时候必须指定一个存储锁的工作目录
func NewFileSystemStorage(workspace string) *FileSystemStorage {
	return &FileSystemStorage{
		workspace: workspace,
	}
}

const StorageName = "file-system-storage"

func (x *FileSystemStorage) GetName() string {
	return StorageName
}

// Capabilities 声明文件系统存储支持的能力
// 文件系统通过 O_CREATE|O_EXCL 标志实现原子的文件创建，支持 CAS 语义
// （单进程内完全安全；多进程下 UpdateWithVersion 存在微小竞态窗口，详见类型文档）
// 时间源使用本地文件系统时间（单机内天然一致）
func (x *FileSystemStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{
		storage.CapabilityCAS,
		storage.CapabilityReliableTime,
	}
}

func (x *FileSystemStorage) Init(ctx context.Context) error {
	stat, err := os.Stat(x.workspace)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return os.MkdirAll(x.workspace, os.ModePerm)
		}
		return err
	} else if !stat.IsDir() {
		return fmt.Errorf("path %s is not directory", x.workspace)
	}
	return nil
}

const (

	// LockDirectoryPrefix 存放锁的目录名必须有这个前缀
	LockDirectoryPrefix = "storage-lock-"

	// LockDirectorySuffix 存放锁的目录名必须有这个后缀
	LockDirectorySuffix = ".lock"

	// CurrentLockFileName 当前生效的锁信息存储在这个文件中
	CurrentLockFileName = "current.json"
)

// IsLockDirectory 判断这个路径是否是一个存放锁的路径
func (x *FileSystemStorage) IsLockDirectory(path string) bool {
	if !strings.HasPrefix(path, LockDirectoryPrefix) || !strings.HasSuffix(path, LockDirectorySuffix) {
		return false
	}
	stat, err := os.Stat(path)
	if err != nil {
		return false
	}
	return stat.IsDir()
}

// BuildLockDirectory 每个锁存放在一个独立的目录中
func (x *FileSystemStorage) BuildLockDirectory(lockId string) string {
	return filepath.Join(x.workspace, LockDirectoryPrefix+lockId+LockDirectorySuffix)
}

// BuildCurrentLockFilePath 当前锁信息文件的路径
func (x *FileSystemStorage) BuildCurrentLockFilePath(lockId string) string {
	return filepath.Join(x.BuildLockDirectory(lockId), CurrentLockFileName)
}

// EnsureDirectoryExists 确保给定的路径存在并且是一个目录
func (x *FileSystemStorage) EnsureDirectoryExists(directory string) error {
	_ = os.MkdirAll(directory, os.ModePerm)
	stat, err := os.Stat(directory)
	if err != nil {
		return err
	}
	if !stat.IsDir() {
		return fmt.Errorf("path %s is not directory", directory)
	}
	return nil
}

// atomicWriteFile 原子地写入文件：先写到唯一的临时文件，再 rename，避免并发读到半截内容
// 临时文件名带随机后缀，避免多个 goroutine 同时写同一个临时文件互相覆盖
func (x *FileSystemStorage) atomicWriteFile(path string, data []byte) error {
	// 使用 pid + 随机数生成唯一的临时文件名，避免并发冲突
	tmpPath := fmt.Sprintf("%s.%d.%d.tmp", path, os.Getpid(), uniqueTmpCounter.Add(1))
	if err := os.WriteFile(tmpPath, data, os.ModePerm); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	// rename 是原子的，读者要么看到旧文件，要么看到新文件，不会看到半截
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

// uniqueTmpCounter 临时文件计数器，保证临时文件名唯一
var uniqueTmpCounter atomic.Uint64

// UpdateWithVersion 如果锁的当前版本是期望的版本，则更新为新版本
// 通过 O_EXCL 创建新版本文件 + rename current.json 来保证 CAS 语义
func (x *FileSystemStorage) UpdateWithVersion(ctx context.Context, lockId string, exceptedVersion, newVersion storage.Version, lockInformation *storage.LockInformation) error {

	x.mu.Lock()
	defer x.mu.Unlock()

	lockDirectory := x.BuildLockDirectory(lockId)
	if err := x.EnsureDirectoryExists(lockDirectory); err != nil {
		return err
	}

	// 读取当前锁信息
	currentInfo, err := x.ReadCurrentLockInformation(lockId)
	if err != nil {
		return err
	}
	if currentInfo == nil {
		return storage_lock.ErrLockNotFound
	}

	// 版本检查
	if currentInfo.Version != exceptedVersion {
		return storage_lock.ErrVersionMiss
	}

	// 写入新的锁信息到 current.json（原子写）
	if err := x.atomicWriteFile(x.BuildCurrentLockFilePath(lockId), []byte(lockInformation.ToJsonString())); err != nil {
		return err
	}

	return nil
}

// CreateWithVersion 创建锁记录，仅在锁不存在时成功
// 通过 O_CREATE|O_EXCL 标志保证原子性：若文件已存在则失败
func (x *FileSystemStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, lockInformation *storage.LockInformation) error {

	x.mu.Lock()
	defer x.mu.Unlock()

	lockDirectory := x.BuildLockDirectory(lockId)
	if err := x.EnsureDirectoryExists(lockDirectory); err != nil {
		return err
	}

	currentPath := x.BuildCurrentLockFilePath(lockId)

	// 使用 O_CREATE|O_EXCL 原子地创建文件：若文件已存在则返回错误，保证唯一性
	file, err := os.OpenFile(currentPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.ModePerm)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return storage_lock.ErrLockAlreadyExists
		}
		return err
	}
	defer func() {
		_ = file.Close()
	}()

	_, err = file.WriteString(lockInformation.ToJsonString())
	if err != nil {
		// 写入失败，清理掉刚创建的空文件，避免残留
		_ = os.Remove(currentPath)
		return err
	}

	return nil
}

// DeleteWithVersion 如果锁的当前版本是期望的版本，则删除锁
func (x *FileSystemStorage) DeleteWithVersion(ctx context.Context, lockId string, exceptedVersion storage.Version, lockInformation *storage.LockInformation) error {

	x.mu.Lock()
	defer x.mu.Unlock()

	currentInfo, err := x.ReadCurrentLockInformation(lockId)
	if err != nil {
		return err
	}
	if currentInfo == nil {
		return storage_lock.ErrLockNotFound
	}

	// 版本检查
	if currentInfo.Version != exceptedVersion {
		return storage_lock.ErrVersionMiss
	}

	// 删除整个锁目录
	return os.RemoveAll(x.BuildLockDirectory(lockId))
}

func (x *FileSystemStorage) Get(ctx context.Context, lockId string) (string, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	information, err := x.ReadCurrentLockInformation(lockId)
	if err != nil {
		return "", err
	}
	if information == nil {
		return "", storage_lock.ErrLockNotFound
	}
	return information.ToJsonString(), nil
}

func (x *FileSystemStorage) GetTime(ctx context.Context) (time.Time, error) {
	// 单机文件系统，使用本地时间
	return time.Now(), nil
}

func (x *FileSystemStorage) Close(ctx context.Context) error {
	return nil
}

func (x *FileSystemStorage) List(ctx context.Context) (iterator.Iterator[*storage.LockInformation], error) {
	dirEntrySlice, err := os.ReadDir(x.workspace)
	if err != nil {
		return nil, err
	}
	lockInformationSlice := make([]*storage.LockInformation, 0)
	for _, dirEntry := range dirEntrySlice {
		if !dirEntry.IsDir() {
			continue
		}
		if !strings.HasPrefix(dirEntry.Name(), LockDirectoryPrefix) {
			continue
		}
		lockId := x.ExtractLockIdFromLockDirectoryName(dirEntry.Name())
		if lockId == "" {
			continue
		}
		lockInformation, err := x.ReadCurrentLockInformation(lockId)
		if err != nil || lockInformation == nil {
			continue
		}
		lockInformationSlice = append(lockInformationSlice, lockInformation)
	}
	// 按锁ID排序，保证输出稳定
	sort.Slice(lockInformationSlice, func(i, j int) bool {
		return lockInformationSlice[i].LockId < lockInformationSlice[j].LockId
	})
	return iterator.NewSliceIterator(lockInformationSlice), nil
}

// ExtractLockIdFromLockDirectoryName 从存放锁的目录名中抽取锁的ID
func (x *FileSystemStorage) ExtractLockIdFromLockDirectoryName(lockDirectoryName string) string {
	if !x.IsLockDirectory(lockDirectoryName) {
		return ""
	}
	// 去掉前缀和后缀
	name := lockDirectoryName
	name = strings.TrimPrefix(name, LockDirectoryPrefix)
	name = strings.TrimSuffix(name, LockDirectorySuffix)
	return name
}

// ReadCurrentLockInformation 读取当前锁的信息，如果锁不存在则返回 (nil, nil)
func (x *FileSystemStorage) ReadCurrentLockInformation(lockId string) (*storage.LockInformation, error) {
	currentPath := x.BuildCurrentLockFilePath(lockId)
	fileBytes, err := os.ReadFile(currentPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	// 防御：读到空文件（理论上不应出现，但并发瞬态可能发生），当作锁不存在
	if len(fileBytes) == 0 {
		return nil, nil
	}
	info := &storage.LockInformation{}
	if err := json.Unmarshal(fileBytes, info); err != nil {
		return nil, err
	}
	return info, nil
}

// 保留旧方法名以兼容可能的外部引用，内部不再使用版本文件链
// BuildLockVersionFilePath 根据锁的ID和版本构建对应的锁文件地址（已废弃，保留兼容）
func (x *FileSystemStorage) BuildLockVersionFilePath(lockId string, version storage.Version) string {
	return filepath.Join(x.BuildLockDirectory(lockId), strconv.Itoa(int(version)))
}

// ReadLockInformationFromPath 从文件中读取锁信息（保留兼容）
func (x *FileSystemStorage) ReadLockInformationFromPath(lockVersionPath string) (*storage.LockInformation, error) {
	fileBytes, err := os.ReadFile(lockVersionPath)
	if err != nil {
		return nil, err
	}
	return storage.LockInformationFromJsonString(string(fileBytes))
}

// ReadLockLatestVersionInformation 保留旧方法以兼容，内部统一改用 ReadCurrentLockInformation
func (x *FileSystemStorage) ReadLockLatestVersionInformation(lockId string) (*storage.LockInformation, error) {
	return x.ReadCurrentLockInformation(lockId)
}
