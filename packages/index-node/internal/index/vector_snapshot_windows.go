//go:build windows

package index

import (
	"fmt"

	"golang.org/x/sys/windows"
)

func replaceSnapshotFile(source, destination string) error {
	from, err := windows.UTF16PtrFromString(source)
	if err != nil {
		return fmt.Errorf("index: encode vector snapshot source: %w", err)
	}
	to, err := windows.UTF16PtrFromString(destination)
	if err != nil {
		return fmt.Errorf("index: encode vector snapshot destination: %w", err)
	}
	if err := windows.MoveFileEx(from, to, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH); err != nil {
		return fmt.Errorf("index: atomically replace vector snapshot: %w", err)
	}
	return nil
}
