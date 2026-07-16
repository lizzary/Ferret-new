//go:build !windows

package index

import (
	"fmt"
	"os"
)

func replaceSnapshotFile(source, destination string) error {
	if err := os.Rename(source, destination); err != nil {
		return fmt.Errorf("index: atomically replace vector snapshot: %w", err)
	}
	return nil
}
