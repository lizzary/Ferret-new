package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const fileColumns = `file_id,path,size,mtime_ns,inode,sample_hash,kind,generation,status,extractor_version,embed_model_version,indexed_at`

const (
	defaultFilePrefixPageLimit = 1000
	maxFilePrefixPageLimit     = 10000
	fileIDLookupBatchLimit     = 900
)

type rowScanner interface {
	Scan(dest ...any) error
}

func scanFile(row rowScanner) (File, error) {
	var f File
	var inode, indexedAt sql.NullInt64
	var extractorVersion, embedModelVersion sql.NullString
	if err := row.Scan(
		&f.ID, &f.Path, &f.Size, &f.MTimeNS, &inode, &f.SampleHash, &f.Kind,
		&f.Generation, &f.Status, &extractorVersion, &embedModelVersion, &indexedAt,
	); err != nil {
		return File{}, err
	}
	if inode.Valid {
		f.Inode = ptr(inode.Int64)
	}
	if indexedAt.Valid {
		f.IndexedAtMS = ptr(indexedAt.Int64)
	}
	if extractorVersion.Valid {
		f.ExtractorVersion = ptr(extractorVersion.String)
	}
	if embedModelVersion.Valid {
		f.EmbedModelVersion = ptr(embedModelVersion.String)
	}
	return f, nil
}

func ptr[T any](v T) *T { return &v }

func validateFile(f File) error {
	if f.Path == "" {
		return errors.New("store: file path is empty")
	}
	if f.Size < 0 {
		return errors.New("store: file size is negative")
	}
	if f.Generation < 1 {
		return errors.New("store: file generation must be positive")
	}
	switch f.Kind {
	case FileKindText, FileKindImage, FileKindVideo, FileKindOther:
	default:
		return fmt.Errorf("store: invalid file kind %q", f.Kind)
	}
	switch f.Status {
	case FileStatusIndexed, FileStatusPending, FileStatusFailed, FileStatusDeleted:
	default:
		return fmt.Errorf("store: invalid file status %q", f.Status)
	}
	return nil
}

func (s *Store) UpsertFile(ctx context.Context, file File) (File, error) {
	var result File
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		var err error
		result, err = s.UpsertFileTx(ctx, tx, file)
		return err
	})
	return result, err
}

// UpsertFileTx preserves file_id on a path conflict and refuses a stale
// generation. This is the catalog half of the generation commit fence.
func (s *Store) UpsertFileTx(ctx context.Context, tx *sql.Tx, file File) (File, error) {
	if err := validateFile(file); err != nil {
		return File{}, err
	}
	row := tx.QueryRowContext(ctx, `
		INSERT INTO files(path,path_key,size,mtime_ns,inode,sample_hash,kind,generation,status,extractor_version,embed_model_version,indexed_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(path_key) DO UPDATE SET
		 path=excluded.path,
		 size=excluded.size,
		 mtime_ns=excluded.mtime_ns,
		 inode=excluded.inode,
		 sample_hash=excluded.sample_hash,
		 kind=excluded.kind,
		 generation=excluded.generation,
		 status=excluded.status,
		 extractor_version=excluded.extractor_version,
		 embed_model_version=excluded.embed_model_version,
		 indexed_at=excluded.indexed_at
		WHERE excluded.generation >= files.generation
		RETURNING `+fileColumns,
		file.Path, pathKey(file.Path), file.Size, file.MTimeNS, file.Inode, file.SampleHash, file.Kind,
		file.Generation, file.Status, file.ExtractorVersion, file.EmbedModelVersion, file.IndexedAtMS)
	result, err := scanFile(row)
	if errors.Is(err, sql.ErrNoRows) {
		return File{}, ErrStaleGeneration
	}
	if err != nil {
		return File{}, fmt.Errorf("store: upsert file %q: %w", file.Path, err)
	}
	return result, nil
}

// PrepareFileForTask atomically installs the filesystem/extractor snapshot and
// anchors a previously unknown task to its stable catalog file_id. The row is
// kept pending until CommitWriter has durably updated the rebuildable index.
func (s *Store) PrepareFileForTask(ctx context.Context, taskID int64, file File) (File, error) {
	if file.Status == "" {
		file.Status = FileStatusPending
	}
	var prepared File
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		task, err := taskForTransition(ctx, tx, taskID, TaskStateInFlight)
		if err != nil {
			return err
		}
		if pathKey(task.Path) != pathKey(file.Path) || task.Generation != file.Generation {
			return fmt.Errorf("%w: task %d path/generation does not match prepared file", ErrTaskMismatch, taskID)
		}
		if task.FileID != nil {
			prepared, err = prepareAnchoredFileTx(ctx, tx, *task.FileID, file)
		} else {
			prepared, err = s.UpsertFileTx(ctx, tx, file)
		}
		if err != nil {
			return err
		}
		if task.FileID != nil && *task.FileID != prepared.ID {
			return fmt.Errorf("%w: task %d path=%q file_id=%d, path owner=%d",
				ErrPathOwnership, taskID, task.Path, *task.FileID, prepared.ID)
		}
		result, err := tx.ExecContext(ctx, "UPDATE tasks SET file_id=?,updated_at=? WHERE task_id=? AND state='in_flight'", prepared.ID, time.Now().UnixMilli(), taskID)
		if err != nil {
			return fmt.Errorf("store: bind task %d to file %d: %w", taskID, prepared.ID, err)
		}
		return requireChanged(result)
	})
	return prepared, err
}

func prepareAnchoredFileTx(ctx context.Context, tx *sql.Tx, fileID int64, file File) (File, error) {
	var owner int64
	ownerErr := tx.QueryRowContext(ctx, "SELECT file_id FROM files WHERE path_key=?", pathKey(file.Path)).Scan(&owner)
	if ownerErr == nil && owner != fileID {
		return File{}, fmt.Errorf("%w: path %q belongs to file %d, preparing file %d", ErrPathOwnership, file.Path, owner, fileID)
	}
	if ownerErr != nil && !errors.Is(ownerErr, sql.ErrNoRows) {
		return File{}, fmt.Errorf("store: inspect prepared path %q: %w", file.Path, ownerErr)
	}
	prepared, err := scanFile(tx.QueryRowContext(ctx, `
		UPDATE files SET
		 path=?,path_key=?,size=?,mtime_ns=?,inode=?,sample_hash=?,kind=?,status=?,
		 extractor_version=?,embed_model_version=?,indexed_at=?
		WHERE file_id=? AND generation=?
		RETURNING `+fileColumns,
		file.Path, pathKey(file.Path), file.Size, file.MTimeNS, file.Inode, file.SampleHash, file.Kind,
		file.Status, file.ExtractorVersion, file.EmbedModelVersion, file.IndexedAtMS, fileID, file.Generation))
	if errors.Is(err, sql.ErrNoRows) {
		var exists int
		if checkErr := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM files WHERE file_id=?)", fileID).Scan(&exists); checkErr != nil {
			return File{}, fmt.Errorf("store: inspect prepared file %d: %w", fileID, checkErr)
		}
		if exists == 0 {
			return File{}, ErrNotFound
		}
		return File{}, ErrStaleGeneration
	}
	if err != nil {
		return File{}, fmt.Errorf("store: prepare anchored file %d: %w", fileID, err)
	}
	return prepared, nil
}

func (s *Store) GetFileByPath(ctx context.Context, path string) (File, error) {
	file, err := scanFile(s.read.QueryRowContext(ctx, "SELECT "+fileColumns+" FROM files WHERE path_key=?", pathKey(path)))
	if errors.Is(err, sql.ErrNoRows) {
		return File{}, ErrNotFound
	}
	if err != nil {
		return File{}, fmt.Errorf("store: get file by path %q: %w", path, err)
	}
	return file, nil
}

func (s *Store) GetFileByID(ctx context.Context, fileID int64) (File, error) {
	file, err := scanFile(s.read.QueryRowContext(ctx, "SELECT "+fileColumns+" FROM files WHERE file_id=?", fileID))
	if errors.Is(err, sql.ErrNoRows) {
		return File{}, ErrNotFound
	}
	if err != nil {
		return File{}, fmt.Errorf("store: get file %d: %w", fileID, err)
	}
	return file, nil
}

// GetFilesByIDs resolves a search candidate set without issuing one catalog
// query per hit. Missing IDs are deliberately omitted: the search layer treats
// an absent or concurrently deleted catalog row as an ineligible result.
func (s *Store) GetFilesByIDs(ctx context.Context, fileIDs []int64) (map[int64]File, error) {
	if ctx == nil {
		return nil, errors.New("store: context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("store: get files by IDs: %w", err)
	}
	unique := make([]int64, 0, len(fileIDs))
	seen := make(map[int64]struct{}, len(fileIDs))
	for _, fileID := range fileIDs {
		if fileID <= 0 {
			return nil, fmt.Errorf("store: file ID %d must be positive", fileID)
		}
		if _, exists := seen[fileID]; exists {
			continue
		}
		seen[fileID] = struct{}{}
		unique = append(unique, fileID)
	}
	files := make(map[int64]File, len(unique))
	for start := 0; start < len(unique); start += fileIDLookupBatchLimit {
		end := min(start+fileIDLookupBatchLimit, len(unique))
		placeholders := strings.TrimSuffix(strings.Repeat("?,", end-start), ",")
		arguments := make([]any, 0, end-start)
		for _, fileID := range unique[start:end] {
			arguments = append(arguments, fileID)
		}
		rows, err := s.read.QueryContext(ctx,
			"SELECT "+fileColumns+" FROM files WHERE file_id IN ("+placeholders+")", arguments...)
		if err != nil {
			return nil, fmt.Errorf("store: get files by IDs: %w", err)
		}
		for rows.Next() {
			file, scanErr := scanFile(rows)
			if scanErr != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("store: scan file by ID: %w", scanErr)
			}
			files[file.ID] = file
		}
		iterationErr := rows.Err()
		closeErr := rows.Close()
		if iterationErr != nil {
			return nil, fmt.Errorf("store: iterate files by IDs: %w", iterationErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("store: close files by IDs: %w", closeErr)
		}
	}
	return files, nil
}

func (s *Store) ListFilesByPrefix(ctx context.Context, prefix string, limit int) ([]File, error) {
	if limit <= 0 {
		limit = 1000
	}
	filter, args := pathKeyPrefixFilter(prefix)
	query := "SELECT " + fileColumns + " FROM files WHERE 1=1" + filter + " ORDER BY path_key LIMIT ?"
	args = append(args, limit)
	rows, err := s.read.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("store: list files by prefix %q: %w", prefix, err)
	}
	defer rows.Close()
	files := make([]File, 0)
	for rows.Next() {
		file, err := scanFile(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan file by prefix: %w", err)
		}
		files = append(files, file)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate files by prefix: %w", err)
	}
	return files, nil
}

// ListFilesByPrefixPage returns one stable, path-ordered catalog page for a
// directory prefix. Only paths strictly greater than after are returned, so a
// caller can stream the catalog by passing the last path from each page.
//
// Prefix matching is separator-boundary aware: /foo matches /foo itself and
// descendants such as /foo/a, but not /foobar. Both slash forms are accepted
// because a catalog database may be inspected after moving between platforms.
// A prefix ending in a separator denotes descendants, and an empty prefix
// scans the complete catalog. substr is intentional: LIKE would interpret %
// and _ in valid filenames as wildcards.
func (s *Store) ListFilesByPrefixPage(ctx context.Context, prefix, after string, limit int) ([]File, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("store: list files by prefix page %q: %w", prefix, err)
	}
	limit = filePrefixPageLimit(limit)

	afterKey := ""
	if after != "" {
		afterKey = pathKey(after)
	}
	query := "SELECT " + fileColumns + " FROM files WHERE path_key>?"
	args := []any{afterKey}
	filter, filterArgs := pathKeyPrefixFilter(prefix)
	query += filter
	args = append(args, filterArgs...)
	query += " ORDER BY path_key LIMIT ?"
	args = append(args, limit)

	rows, err := s.read.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("store: list files by prefix page %q: %w", prefix, err)
	}
	defer rows.Close()

	files := make([]File, 0, min(limit, 256))
	for rows.Next() {
		file, err := scanFile(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan file by prefix page: %w", err)
		}
		files = append(files, file)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate files by prefix page: %w", err)
	}
	return files, nil
}

func filePrefixPageLimit(limit int) int {
	if limit <= 0 {
		return defaultFilePrefixPageLimit
	}
	return min(limit, maxFilePrefixPageLimit)
}

func isPathSeparator(char byte) bool {
	return char == '/' || char == '\\'
}

// pathKeyPrefixFilter returns a literal, separator-boundary-aware SQL filter.
// substr is intentional: LIKE would interpret valid filename characters as
// wildcards. Both separators remain accepted for databases inspected on a
// platform other than the one that originally produced their path strings.
func pathKeyPrefixFilter(prefix string) (string, []any) {
	if prefix == "" {
		return "", nil
	}
	key := pathKey(prefix)
	trailingSeparator := isPathSeparator(prefix[len(prefix)-1])
	childPrefixes := []string{key}
	if !isPathSeparator(key[len(key)-1]) {
		childPrefixes = []string{key + "/", key + `\`}
	}
	if len(childPrefixes) == 2 && childPrefixes[0] == childPrefixes[1] {
		childPrefixes = childPrefixes[:1]
	}

	clauses := make([]string, 0, len(childPrefixes)+1)
	args := make([]any, 0, len(childPrefixes)*2+1)
	if !trailingSeparator {
		clauses = append(clauses, "path_key=?")
		args = append(args, key)
	}
	for _, childPrefix := range childPrefixes {
		clauses = append(clauses, "substr(path_key,1,length(?))=?")
		args = append(args, childPrefix, childPrefix)
	}
	return " AND (" + strings.Join(clauses, " OR ") + ")", args
}

func (s *Store) ListFilesByStatus(ctx context.Context, status FileStatus, limit int) ([]File, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.read.QueryContext(ctx, "SELECT "+fileColumns+" FROM files WHERE status=? ORDER BY file_id LIMIT ?", status, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list files by status %q: %w", status, err)
	}
	defer rows.Close()
	var files []File
	for rows.Next() {
		file, err := scanFile(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan file by status: %w", err)
		}
		files = append(files, file)
	}
	return files, rows.Err()
}

func (s *Store) BumpGeneration(ctx context.Context, path string) (File, error) {
	var result File
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		var err error
		result, err = s.BumpGenerationTx(ctx, tx, path)
		return err
	})
	return result, err
}

func (s *Store) BumpGenerationTx(ctx context.Context, tx *sql.Tx, path string) (File, error) {
	file, err := scanFile(tx.QueryRowContext(ctx, `
		UPDATE files SET generation=generation+1,status='pending' WHERE path_key=?
		RETURNING `+fileColumns, pathKey(path)))
	if errors.Is(err, sql.ErrNoRows) {
		return File{}, ErrNotFound
	}
	if err != nil {
		return File{}, fmt.Errorf("store: bump generation for %q: %w", path, err)
	}
	return file, nil
}

func (s *Store) IsCurrentGeneration(ctx context.Context, fileID, generation int64) (bool, error) {
	var current int64
	if err := s.read.QueryRowContext(ctx, "SELECT generation FROM files WHERE file_id=?", fileID).Scan(&current); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, ErrNotFound
		}
		return false, fmt.Errorf("store: read generation for file %d: %w", fileID, err)
	}
	return current == generation, nil
}

func (s *Store) CurrentGenerations(ctx context.Context, fileIDs []int64) (map[int64]int64, error) {
	result := make(map[int64]int64, len(fileIDs))
	if len(fileIDs) == 0 {
		return result, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(fileIDs)), ",")
	args := make([]any, len(fileIDs))
	for i, id := range fileIDs {
		args[i] = id
	}
	rows, err := s.read.QueryContext(ctx, "SELECT file_id,generation FROM files WHERE file_id IN ("+placeholders+")", args...)
	if err != nil {
		return nil, fmt.Errorf("store: read current generations: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, generation int64
		if err := rows.Scan(&id, &generation); err != nil {
			return nil, fmt.Errorf("store: scan current generation: %w", err)
		}
		result[id] = generation
	}
	return result, rows.Err()
}

func (s *Store) RelocateFile(ctx context.Context, fileID, generation int64, newPath string) (File, error) {
	if newPath == "" {
		return File{}, errors.New("store: relocate destination is empty")
	}
	var result File
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		var owner int64
		ownerErr := tx.QueryRowContext(ctx, "SELECT file_id FROM files WHERE path_key=?", pathKey(newPath)).Scan(&owner)
		if ownerErr == nil && owner != fileID {
			return fmt.Errorf("%w: path %q belongs to file %d, relocating file %d", ErrPathOwnership, newPath, owner, fileID)
		}
		if ownerErr != nil && !errors.Is(ownerErr, sql.ErrNoRows) {
			return fmt.Errorf("store: inspect relocate destination %q: %w", newPath, ownerErr)
		}
		var err error
		result, err = scanFile(tx.QueryRowContext(ctx, `
			UPDATE files SET path=?,path_key=? WHERE file_id=? AND generation=? RETURNING `+fileColumns,
			newPath, pathKey(newPath), fileID, generation))
		if errors.Is(err, sql.ErrNoRows) {
			var exists int
			if checkErr := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM files WHERE file_id=?)", fileID).Scan(&exists); checkErr != nil {
				return fmt.Errorf("store: check relocated file: %w", checkErr)
			}
			if exists == 0 {
				return ErrNotFound
			}
			return ErrStaleGeneration
		}
		if err != nil {
			return fmt.Errorf("store: relocate file %d: %w", fileID, err)
		}
		return nil
	})
	return result, err
}

func (s *Store) MarkFileDeleted(ctx context.Context, fileID, generation int64) error {
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE files SET status='deleted',indexed_at=NULL WHERE file_id=? AND generation=?`, fileID, generation)
		if err != nil {
			return fmt.Errorf("store: mark file %d deleted: %w", fileID, err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("store: count deleted file update: %w", err)
		}
		if changed == 0 {
			return ErrStaleGeneration
		}
		return nil
	})
}
