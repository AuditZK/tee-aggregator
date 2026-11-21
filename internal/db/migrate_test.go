package db

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseMigrationVersion(t *testing.T) {
	tests := []struct {
		name      string
		filename  string
		want      int
		shouldErr bool
	}{
		{name: "valid", filename: "001_init.sql", want: 1},
		{name: "valid_with_long_prefix", filename: "011_add_snapshot_label.sql", want: 11},
		{name: "invalid_missing_prefix", filename: "init.sql", shouldErr: true},
		{name: "invalid_empty", filename: ".sql", shouldErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseMigrationVersion(tt.filename)
			if tt.shouldErr {
				if err == nil {
					t.Fatalf("expected error for filename %s", tt.filename)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("unexpected version: got %d want %d", got, tt.want)
			}
		})
	}
}

func TestLoadMigrationFiles_SortedByVersion(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "010_ten.sql"), "-- ten")
	mustWriteFile(t, filepath.Join(dir, "002_two.sql"), "-- two")
	mustWriteFile(t, filepath.Join(dir, "001_one.sql"), "-- one")
	mustWriteFile(t, filepath.Join(dir, "README.txt"), "ignore")

	files, err := loadMigrationFiles(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(files) != 3 {
		t.Fatalf("expected 3 sql files, got %d", len(files))
	}
	if files[0].version != 1 || files[1].version != 2 || files[2].version != 10 {
		t.Fatalf("unexpected migration order: %+v", files)
	}
}

func TestLoadMigrationFiles_DuplicateVersionFails(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, filepath.Join(dir, "001_a.sql"), "-- a")
	mustWriteFile(t, filepath.Join(dir, "001_b.sql"), "-- b")

	_, err := loadMigrationFiles(dir)
	if err == nil {
		t.Fatal("expected duplicate version error")
	}
}

func mustWriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write file %s: %v", path, err)
	}
}
