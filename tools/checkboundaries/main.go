// Command checkboundaries enforces the monorepo's three module rules:
//
//  1. services/* may not import each other.
//  2. pkg/* may not import services/*.
//  3. (rule 3 — clients share only gen/ — is a Dart-side concern and is
//     enforced by the Flutter SDK having no path dependency on Go modules.)
//
// It shells out to `go list` over the workspace rather than parsing files, so
// build tags and generated code are handled the same way the compiler does.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

const modulePrefix = "github.com/dhiazfathra/event-tracking/"

type pkgInfo struct {
	ImportPath   string
	Imports      []string
	TestImports  []string
	XTestImports []string
}

func main() {
	pkgs, err := listPackages()
	if err != nil {
		fmt.Fprintf(os.Stderr, "go list failed: %v\n", err)
		os.Exit(2)
	}

	var all []string
	for _, p := range pkgs {
		all = append(all, violations(p.ImportPath, mergedImports(p))...)
	}

	if len(all) > 0 {
		fmt.Fprintln(os.Stderr, "module boundary violations:")
		for _, v := range all {
			fmt.Fprintf(os.Stderr, "  %s\n", v)
		}
		os.Exit(1)
	}
	fmt.Println("module boundaries OK")
}

// mergedImports combines a package's regular, internal-test, and
// external-test imports without mutating any source slice.
func mergedImports(p pkgInfo) []string {
	return append(append(append([]string{}, p.Imports...), p.TestImports...), p.XTestImports...)
}

// listPackages runs `go list -json ./...` inside each workspace module's own
// directory and merges the results. The repo root is not itself a Go module
// (only go.work lives there), so `./...` run from the root cannot resolve —
// it must be run from within each module's directory tree instead.
func listPackages() ([]pkgInfo, error) {
	dirsOut, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}").Output()
	if err != nil {
		return nil, fmt.Errorf("go list -m: %w", err)
	}

	var pkgs []pkgInfo
	for _, dir := range strings.Fields(string(dirsOut)) {
		cmd := exec.Command("go", "list", "-json", "./...")
		cmd.Dir = dir
		out, err := cmd.Output()
		if err != nil {
			return nil, fmt.Errorf("go list ./... in %s: %w", dir, err)
		}
		// `go list -json` emits consecutive top-level objects, not a JSON
		// array. Decoder.More() is only meaningful inside an array or
		// object, so looping on it reads nothing here — and a checker that
		// finds zero packages reports success while checking nothing.
		dec := json.NewDecoder(bytes.NewReader(out))
		for {
			var p pkgInfo
			err := dec.Decode(&p)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, err
			}
			pkgs = append(pkgs, p)
		}
	}
	return pkgs, nil
}

// serviceOf returns the service name for an import path under services/, or "".
func serviceOf(path string) string {
	rest, ok := strings.CutPrefix(path, modulePrefix+"services/")
	if !ok {
		return ""
	}
	return strings.SplitN(rest, "/", 2)[0]
}

func isPkg(path string) bool {
	return strings.HasPrefix(path, modulePrefix+"pkg/")
}

// violations reports rule breaches for one package's import list.
func violations(pkg string, imports []string) []string {
	var out []string
	self := serviceOf(pkg)

	for _, imp := range imports {
		other := serviceOf(imp)

		// Rule 1: cross-service import.
		if self != "" && other != "" && other != self {
			out = append(out, fmt.Sprintf("%s imports %s (services/* may not import each other)", pkg, imp))
		}

		// Rule 2: pkg reaching into a service.
		if isPkg(pkg) && other != "" {
			out = append(out, fmt.Sprintf("%s imports %s (pkg/* may not import services/*)", pkg, imp))
		}
	}
	return out
}
