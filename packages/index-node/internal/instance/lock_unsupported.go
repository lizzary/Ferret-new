//go:build !windows && !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package instance

import (
	"errors"
	"os"
)

var errLockUnavailable = errors.New("instance lock unavailable")

func lockFile(*os.File) error {
	return errors.New("index-node: OS file locking is unsupported on this platform")
}
func unlockFile(*os.File) error { return nil }
