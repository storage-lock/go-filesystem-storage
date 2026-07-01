//go:build !linux && !darwin

package file_system_storage

import "errors"

// flock 在不支持 flock 的平台（如 Windows）回退为 no-op。
// ⚠️ 此时 FileSystemStorage 仅在单进程内安全，多进程并发下不保证 CAS——
// 详见 FileSystemStorage 类型文档的平台限制说明。
// Windows 上若需多进程安全，应改用 LockFileEx，此处暂未实现。
func flock(fd int) error {
	return nil
}

func funlock(fd int) error {
	_ = fd
	return errors.New("flock unsupported on this platform, no-op")
}
