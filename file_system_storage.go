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

// FileSystemStorage 基于文件系统来存储锁，可以用作同一台机器上的不同进程之间互相协同工作，
// 优势是兼容性好，只要有文件系统能读写文件就能使用这种锁。
//
// # CAS 实现说明（漏洞 H 修复后的正确实现）
//
// 此实现通过【flock 排他锁】把"读版本 + 校验 + 写入"包成一个【进程间互斥的临界区】，
// 从而在多进程下实现真正的线性一致 CAS——满足 go-storage 对 CapabilityCAS 的硬性要求。
//
// 每个锁目录下有一个固定的 flock.lock 文件作为互斥点：
//   - CreateWithVersion：flock → 若 current.json 已存在则 ErrLockAlreadyExists，否则原子写入 → unlock
//   - UpdateWithVersion：flock → 读 current.json 校验版本，不匹配 ErrVersionMiss，匹配则原子写入 → unlock
//   - DeleteWithVersion：flock → 读 current.json 校验版本，不匹配 ErrVersionMiss，匹配则删除目录 → unlock
//
// 关键：flock 是内核级进程互斥，同一 lockId 的并发 CAS 在文件系统层被串行化，
// 任意时刻只有一个进程能进入临界区，因此 Check-Then-Act 不再有竞态窗口，CAS 严格成立。
// atomicWriteFile 的 rename 仍保留（防止读者读到半截内容），但它现在受 flock 保护，不再并发。
//
// # 平台支持
//
//   - Linux / macOS：使用 syscall.Flock，多进程安全（同一台机器内）。
//   - 其它平台（如 Windows）：flock 回退为 no-op，此时仅单进程内安全；Windows 多进程安全需用 LockFileEx 实现。
//
// # 局限（不可逾越的物理边界）
//
// flock 只能保护【同一台机器、同一文件系统】上的进程互斥。跨机器/NFS 场景下 flock 行为依赖
// NFS 服务端实现（NFSv4 通常支持，NFSv3 不可靠），不应直接用于跨机分布式协调——那是
// 关系型数据库/对象存储等真正分布式存储的领域。本实现定位是"单机多进程"分布式锁。
type FileSystemStorage struct {
	workspace string

	// mu 保护单进程内的并发，与 flock 共同构成两层保护：
	// mu 串行化单进程内的 goroutine；flock 串行化不同进程。两者都不可省。
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

// Capabilities 声明文件系统存储支持的能力。
//
// CapabilityCAS：成立——通过 flock 排他锁把"读版本+校验+写"串行化，多进程下线性一致
//   （Linux/macOS；其它平台见类型文档的平台说明）。
// CapabilityReliableTime：成立——单机文件系统，本地时钟在单机内天然一致。
// CapabilityAtomicDelete：成立——DeleteWithVersion 同样在 flock 临界区内"校验版本+删除"。
//
// 平台注意：CapabilityCAS/AtomicDelete 仅在支持 flock 的平台（Linux/macOS）声明。
// 非 Unix 平台 flock 不可用，多进程 CAS 不成立，不声明该能力——NewStorageLockWithOptions
// 的能力校验会拒绝将其用于分布式锁，避免静默不安全。如需 Windows 多进程安全，应实现
// LockFileEx 版本的 flock 并把 flockSupported 改 true。
func (x *FileSystemStorage) Capabilities() []storage.StorageCapability {
	caps := []storage.StorageCapability{
		storage.CapabilityReliableTime, // 单机本地时钟，单机内天然一致
		storage.CapabilityWatch,
	}
	if flockSupported {
		caps = append(caps, storage.CapabilityCAS, storage.CapabilityAtomicDelete)
	}
	return caps
}

func (x *FileSystemStorage) Watch(ctx context.Context, lockId string) (<-chan *storage.LockInformation, error) {
	return storage.NewPollingLockWatcher(x, 500*time.Millisecond).Watch(ctx, lockId)
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

	// FlockFileName 进程间互斥锁文件名。
	// ⚠️ 关键：flock 锁的是【inode】而非路径。锁载体文件必须放在【永不被业务删除】的稳定位置
	// （见 FlocksDirectoryName），否则 DeleteWithVersion 删除锁目录时会一并删除 flock.lock，
	// 此后另一进程新建同名文件得到新 inode，旧 flock 不再保护新文件，互斥被绕过（漏洞 H-1）。
	FlockFileName = "flock.lock"

	// FlocksDirectoryName 存放所有 flock 锁载体文件的目录，位于 workspace 根下、锁目录之外。
	// DeleteWithVersion 只删除锁目录（含 current.json），绝不触碰此目录，保证 flock 载体
	// 文件的 inode 在锁的整个生命周期内稳定不变，flock 互斥始终有效。
	FlocksDirectoryName = "flocks"
)

// IsLockDirectory 判断这个路径是否是一个存放锁的路径。
// 既接受锁目录的全路径，也接受目录名（此时回退到 workspace 下查找）。
// 注意：历史实现直接对传入路径做 HasPrefix 前缀判断，传入全路径时永远返回 false，
// 导致 List 永远为空；现改为对 filepath.Base 做格式判断，stat 用原路径，
// 原路径不存在时再回退到 workspace/Base 查找。
func (x *FileSystemStorage) IsLockDirectory(path string) bool {
	base := filepath.Base(path)
	if !strings.HasPrefix(base, LockDirectoryPrefix) || !strings.HasSuffix(base, LockDirectorySuffix) {
		return false
	}
	if stat, err := os.Stat(path); err == nil {
		return stat.IsDir()
	}
	if stat, err := os.Stat(filepath.Join(x.workspace, base)); err == nil {
		return stat.IsDir()
	}
	return false
}

// BuildLockDirectory 每个锁存放在一个独立的目录中
func (x *FileSystemStorage) BuildLockDirectory(lockId string) string {
	return filepath.Join(x.workspace, LockDirectoryPrefix+lockId+LockDirectorySuffix)
}

// BuildCurrentLockFilePath 当前锁信息文件的路径
func (x *FileSystemStorage) BuildCurrentLockFilePath(lockId string) string {
	return filepath.Join(x.BuildLockDirectory(lockId), CurrentLockFileName)
}

// BuildFlockFilePath 进程间互斥锁文件的路径。
// 放在 workspace 根下的 flocks/ 目录中，【独立于锁目录】——这样 DeleteWithVersion
// 删除锁目录时不会删除 flock 载体文件，保证其 inode 稳定，flock 互斥始终有效（修复漏洞 H-1）。
func (x *FileSystemStorage) BuildFlockFilePath(lockId string) string {
	return filepath.Join(x.workspace, FlocksDirectoryName, lockId+FlockFileName)
}

// withFlock 打开（必要时创建）flock 载体文件并加排他锁，执行 fn 后释放。
// mu 已在调用方持有（串行化单进程内 goroutine），flock 再串行化不同进程，
// 两层共同保证同一 lockId 的 CAS 全局只有一个进程在临界区内。
//
// flock 载体文件位于锁目录之外（见 BuildFlockFilePath），永不被业务删除，inode 稳定。
//
// 返回值：flock 加锁失败或 fn 的错误都会透传；fn 的错误不会阻止 unlock。
func (x *FileSystemStorage) withFlock(lockId string, fn func() error) error {
	// 确保存放 flock 载体文件的目录存在（独立于锁目录，永不被删除）
	flocksDir := filepath.Join(x.workspace, FlocksDirectoryName)
	if err := x.EnsureDirectoryExists(flocksDir); err != nil {
		return err
	}
	flockPath := x.BuildFlockFilePath(lockId)
	// O_CREATE：flock 载体文件不存在则创建（内容无关，只用作锁载体）
	f, err := os.OpenFile(flockPath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	if err := flock(int(f.Fd())); err != nil {
		// 拿不到排他锁（另一进程正持有），转化为版本冲突语义，让上层走重试
		return storage_lock.ErrVersionMiss
	}
	defer func() { _ = funlock(int(f.Fd())) }()

	return fn()
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

// UpdateWithVersion 如果锁的当前版本是期望的版本，则更新为新版本。
// 在 flock 排他锁临界区内完成"读版本+校验+写"，多进程下线性一致。
func (x *FileSystemStorage) UpdateWithVersion(ctx context.Context, lockId string, exceptedVersion, newVersion storage.Version, lockInformation *storage.LockInformation) error {

	x.mu.Lock()
	defer x.mu.Unlock()

	return x.withFlock(lockId, func() error {
		// 锁目录可能不存在（首次创建或被 DeleteWithVersion 删除后），写 current.json 前确保存在
		if err := x.EnsureDirectoryExists(x.BuildLockDirectory(lockId)); err != nil {
			return err
		}
		currentInfo, err := x.ReadCurrentLockInformation(lockId)
		if err != nil {
			return err
		}
		if currentInfo == nil {
			// 注意：映射为 ErrVersionMiss 而不是 ErrLockNotFound——core 的 lockExpired/
			// rlockReclaim 等抢占路径只把 ErrVersionMiss 当作"CAS 失败，可重试"，其它错误
			// 会被当作硬错误直接返回。若真正的持有者恰好在这中间 DeleteWithVersion 真删了
			// 记录（与本次抢占竞态，flock 只在单次调用内持有，两次调用之间仍可能被其它
			// 进程抢先），"记录不存在"和"版本不匹配"对调用方而言是同一件事，应该重试。
			return storage_lock.ErrVersionMiss
		}
		// 版本检查（此时持 flock，无并发进程能改版本，检查有效）
		if currentInfo.Version != exceptedVersion {
			return storage_lock.ErrVersionMiss
		}
		// 原子写入新锁信息（rename 防读者读到半截；flock 保证不会被另一进程的写覆盖）
		return x.atomicWriteFile(x.BuildCurrentLockFilePath(lockId), []byte(lockInformation.ToJsonString()))
	})
}

// CreateWithVersion 创建锁记录，仅在锁不存在时成功。
// 在 flock 排他锁临界区内检查 current.json 是否存在并创建，多进程下只有一个成功。
func (x *FileSystemStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, lockInformation *storage.LockInformation) error {

	x.mu.Lock()
	defer x.mu.Unlock()

	return x.withFlock(lockId, func() error {
		// 锁目录可能不存在，写 current.json 前确保存在
		if err := x.EnsureDirectoryExists(x.BuildLockDirectory(lockId)); err != nil {
			return err
		}
		currentPath := x.BuildCurrentLockFilePath(lockId)
		// 持 flock 后再检查存在性，避免两进程同时判断"不存在"并各自创建
		if _, err := os.Stat(currentPath); err == nil {
			return storage_lock.ErrLockAlreadyExists
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		// 原子写入（O_EXCL 不再必要，因 flock 已串行化；保留 rename 写法保证读到完整内容）
		return x.atomicWriteFile(currentPath, []byte(lockInformation.ToJsonString()))
	})
}

// DeleteWithVersion 如果锁的当前版本是期望的版本，则删除锁。
// 在 flock 排他锁临界区内完成"读版本+校验+删锁目录"，多进程下线性一致。
//
// ⚠️ 关键（修复漏洞 H-1）：只删除锁目录（含 current.json），【绝不删除 flock 载体文件】。
// flock 载体文件位于锁目录之外的 flocks/ 目录，inode 在锁的整个生命周期内稳定不变。
// 若删除 flock 载体文件，另一进程会新建同名文件得到新 inode，旧 flock 不再保护新文件，
// 互斥被绕过——这正是删除路径上的致命竞态。保留 flock 载体文件使该竞态无法发生。
// flock 载体文件的轻微磁盘占用可接受（每个 lockId 一个 0 字节文件）。
func (x *FileSystemStorage) DeleteWithVersion(ctx context.Context, lockId string, exceptedVersion storage.Version, lockInformation *storage.LockInformation) error {

	x.mu.Lock()
	defer x.mu.Unlock()

	return x.withFlock(lockId, func() error {
		currentInfo, err := x.ReadCurrentLockInformation(lockId)
		if err != nil {
			return err
		}
		if currentInfo == nil {
			return storage_lock.ErrLockNotFound
		}
		if currentInfo.Version != exceptedVersion {
			return storage_lock.ErrVersionMiss
		}
		// 删除整个锁目录（含 current.json 与 flock.lock）；flock 已持，本进程内安全。
		// 注意：删除 flock.lock 文件本身不影响已持有的锁（内核锁绑定 fd，文件可被删后仍有锁）。
		return os.RemoveAll(x.BuildLockDirectory(lockId))
	})
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
