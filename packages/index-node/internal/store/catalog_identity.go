package store

import (
	"context"
	"errors"
	"fmt"
)

var ErrAmbiguousFileIdentity = errors.New("store: file identity is ambiguous")

// FindFileByIdentity returns a unique live catalog row with the observed
// filesystem tuple. Multiple matches can be hard links and must not be
// interpreted as a relocation.
func (s *Store) FindFileByIdentity(ctx context.Context, size, mtimeNS, inode int64) (File, error) {
	rows, err := s.read.QueryContext(ctx, `SELECT `+fileColumns+` FROM files
		WHERE size=? AND mtime_ns=? AND inode=? AND status<>'deleted'
		ORDER BY file_id LIMIT 2`, size, mtimeNS, inode)
	if err != nil {
		return File{}, fmt.Errorf("store: find file identity: %w", err)
	}
	defer rows.Close()
	var matches []File
	for rows.Next() {
		file, scanErr := scanFile(rows)
		if scanErr != nil {
			return File{}, fmt.Errorf("store: scan file identity: %w", scanErr)
		}
		matches = append(matches, file)
	}
	if err := rows.Err(); err != nil {
		return File{}, fmt.Errorf("store: iterate file identity: %w", err)
	}
	switch len(matches) {
	case 0:
		return File{}, ErrNotFound
	case 1:
		return matches[0], nil
	default:
		return File{}, ErrAmbiguousFileIdentity
	}
}
