//go:build !windows

package fsmeta

// InodeAt uses the identity already exposed through FileInfo.Sys on Unix-like
// platforms. path is accepted to keep the call site platform-neutral.
func InodeAt(_ string, system any) *int64 { return Inode(system) }
