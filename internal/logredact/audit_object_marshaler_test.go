package logredact

// AUDIT — LOG-01 guard.
//
// Finding: doc/audit/findings/LOG-01 (logredact does not scrub
// ObjectMarshaler / ArrayMarshaler).
//
// maskFieldValue only inspects String / Error / Reflect field values. Fields
// built with zap.Object / zap.Inline / zap.Array (and the plural forms)
// implement zapcore.ObjectMarshaler / ArrayMarshaler, which zap encodes
// piecewise straight into the encoder's writer — a path the redactor never
// sees. A credential carried through one of those constructors would render
// in clear, regardless of the field key.
//
// Scrubbing at that layer isn't feasible, so the invariant we enforce instead
// is: production code never uses those constructors — every log field is a
// scalar the redactor can inspect. This test fails if any non-test .go file
// reintroduces one.

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

var objectMarshalerCtors = regexp.MustCompile(`zap\.(Object|Objects|Inline|Array|Arrays)\(`)

func TestAuditNoObjectMarshalerLogFields(t *testing.T) {
	root := repoRoot(t)

	var offenders []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "testdata", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for i, line := range strings.Split(string(src), "\n") {
			if objectMarshalerCtors.MatchString(line) {
				rel, relErr := filepath.Rel(root, path)
				if relErr != nil {
					rel = path
				}
				offenders = append(offenders, rel+":"+strconv.Itoa(i+1))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk source tree: %v", err)
	}
	if len(offenders) > 0 {
		t.Fatalf("AUDIT LOG-01: zap.Object/Objects/Inline/Array bypass logredact "+
			"(only String/Error/Reflect field values are scrubbed). Emit a "+
			"pre-scrubbed string field instead. Offending sites:\n  %s",
			strings.Join(offenders, "\n  "))
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above test working directory")
		}
		dir = parent
	}
}
