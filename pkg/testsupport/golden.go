// Package testsupport holds test-only helpers shared across Go modules.
package testsupport

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// GoldenPath resolves a fixture name to its absolute path in testdata/golden.
// Resolution is relative to this source file, not the caller's working
// directory, so it works from any module in the workspace.
func GoldenPath(name string) string {
	_, self, _, _ := runtime.Caller(0)
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(self))) // pkg/testsupport -> pkg -> root
	return filepath.Join(repoRoot, "testdata", "golden", name)
}

// LoadGolden reads a shared fixture. The same files are parsed by the Dart test
// suite, so an encoding divergence between Go and Dart fails CI on both sides.
func LoadGolden(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(GoldenPath(name))
	if err != nil {
		t.Fatalf("load golden %q: %v", name, err)
	}
	return b
}
