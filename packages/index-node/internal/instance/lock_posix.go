//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package instance

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

var errLockUnavailable = errors.New("instance lock unavailable")

func lockFile(file *os.File) error {
	err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return errLockUnavailable
	}
	return err
}

func unlockFile(file *os.File) error {
	if file == nil {
		return nil
	}
	return unix.Flock(int(file.Fd()), unix.LOCK_UN)
}
