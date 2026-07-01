//go:build linux || darwin

package file_system_storage

import (
	"syscall"
)

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
