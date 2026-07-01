//go:build linux || darwin

package file_system_storage

import (
	"syscall"
)

// flockSupported 标记当前平台支持 flock。Capabilities 据此决定是否声明 CapabilityCAS：
// 非 Unix 平台 flock 退化为 no-op，多进程 CAS 不成立，不应声明该能力，避免误导上层。
const flockSupported = true

// flock 加排他锁（非阻塞：拿不到立即返回错误），用于把"读版本+校验+写"包成进程间互斥的临界区，
// 使 FileSystemStorage 的 CAS 在多进程下线性一致（修复漏洞 H：原实现是 Check-Then-Act，CAS 不成立）。
// 调用方 defer unlock() 释放。
func flock(fd int) error {
	return syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB)
}

// funlock 释放排他锁
func funlock(fd int) error {
	return syscall.Flock(fd, syscall.LOCK_UN)
}
