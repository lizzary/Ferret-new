// Package fsmeta exposes platform-neutral metadata used by both filesystem
// reconciliation and the IO pipeline.
package fsmeta

import (
	"math"
	"reflect"
)

// Inode extracts the stable identity fields exposed by common os.FileInfo Sys
// values. Unix Stat_t provides Ino; Windows provides FileIndexHigh/Low on
// implementations that make the NTFS file ID available. Nil means the
// platform has no supported identity field, so callers fall back to size and
// mtime as allowed by the catalog schema.
func Inode(system any) *int64 {
	if system == nil {
		return nil
	}
	value := reflect.ValueOf(system)
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return nil
	}
	if inode, ok := integerField(value, "Ino"); ok {
		return &inode
	}
	high, hasHigh := unsignedField(value, "FileIndexHigh")
	low, hasLow := unsignedField(value, "FileIndexLow")
	if hasHigh && hasLow {
		inode := int64((high << 32) | (low & math.MaxUint32))
		return &inode
	}
	return nil
}

func integerField(value reflect.Value, name string) (int64, bool) {
	field := value.FieldByName(name)
	if !field.IsValid() {
		return 0, false
	}
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return field.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return int64(field.Uint()), true
	default:
		return 0, false
	}
}

func unsignedField(value reflect.Value, name string) (uint64, bool) {
	field := value.FieldByName(name)
	if !field.IsValid() {
		return 0, false
	}
	switch field.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return field.Uint(), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return uint64(field.Int()), true
	default:
		return 0, false
	}
}
