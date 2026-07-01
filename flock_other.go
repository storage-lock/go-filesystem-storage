//go:build !linux && !darwin

package file_system_storage

import "errors"

// flockSupported=false：当前平台不支持 flock。Capabilities 据此【不声明 CapabilityCAS】，
// NewStorageLockWithOptions 的能力校验会拒绝将其用于分布式锁，避免静默不安全。
// 如需 Windows 多进程安全，应实现 LockFileEx 版本并把此常量改 true。
const flockSupported = false

// flock 在不支持 flock 的平台返回错误（不再 no-op），使 withFlock 明确失败而非假装成功。
func flock(fd int) error {
	return errors.New("flock unsupported on this platform")
}

func funlock(fd int) error {
	_ = fd
	return nil
}
