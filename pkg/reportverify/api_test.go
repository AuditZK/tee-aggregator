package reportverify

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
)

// An exported signature that names an internal type compiles for this
// module's own tests and for no one else, so only a check on the source
// can see it.
func TestExportedAPINamesNoInternalType(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		internal := internalImportNames(f)
		report := func(where token.Pos, owner string, n ast.Node) {
			ast.Inspect(n, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if id, ok := sel.X.(*ast.Ident); ok && internal[id.Name] {
					t.Errorf("%s: exported %s names %s.%s; re-export it with an alias",
						fset.Position(where), owner, id.Name, sel.Sel.Name)
				}
				return true
			})
		}
		for _, decl := range f.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Name.IsExported() {
					report(d.Pos(), d.Name.Name, d.Type)
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					ts, ok := spec.(*ast.TypeSpec)
					if !ok || !ts.Name.IsExported() || ts.Assign.IsValid() {
						continue
					}
					st, ok := ts.Type.(*ast.StructType)
					if !ok {
						report(ts.Pos(), ts.Name.Name, ts.Type)
						continue
					}
					for _, field := range st.Fields.List {
						for _, fn := range field.Names {
							if fn.IsExported() {
								report(fn.Pos(), ts.Name.Name+"."+fn.Name, field.Type)
							}
						}
					}
				}
			}
		}
	}
}

func internalImportNames(f *ast.File) map[string]bool {
	names := map[string]bool{}
	for _, imp := range f.Imports {
		p, err := strconv.Unquote(imp.Path.Value)
		if err != nil || !strings.Contains(p, "/internal/") {
			continue
		}
		name := path.Base(p)
		if imp.Name != nil {
			name = imp.Name.Name
		}
		names[name] = true
	}
	return names
}
