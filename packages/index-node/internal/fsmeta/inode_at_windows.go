//go:build windows

package fsmeta

import "golang.org/x/sys/windows"

// InodeAt returns the stable 64-bit Windows file index. os.FileInfo.Sys on
// Windows exposes Win32FileAttributeData without that index, so the handle
// query is required for move reconciliation.
func InodeAt(path string, system any) *int64 {
	if inode := Inode(system); inode != nil {
		return inode
	}
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil
	}
	handle, err := windows.CreateFile(
		name,
		windows.FILE_READ_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return nil
	}
	defer windows.CloseHandle(handle)
	var information windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &information); err != nil {
		return nil
	}
	inode := int64(uint64(information.FileIndexHigh)<<32 | uint64(information.FileIndexLow))
	return &inode
}
