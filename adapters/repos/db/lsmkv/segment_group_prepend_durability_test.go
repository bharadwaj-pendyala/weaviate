//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPrependPublishesEveryRenameDurably pins that every publishing rename's
// directory gets synced: a rename only survives a crash once its directory is
// synced, so a missed sync can drop segments while the caller's "staged
// complete" record survives, promoting an incomplete bucket on the next load.
// An fsync has no assertable effect, so this asserts the call itself —
// syncing the source directory instead would look identical by call count
// while leaving the publish undurable.
func TestPrependPublishesEveryRenameDurably(t *testing.T) {
	const fileName = "segment_group_prepend.go"

	file, err := parser.ParseFile(token.NewFileSet(), fileName, nil, 0)
	require.NoError(t, err)

	renames := 0
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}

		lastRename, target := lastPublishedRename(fn.Body)
		if lastRename == token.NoPos {
			continue
		}
		renames++
		require.NotEmptyf(t, target,
			"%s renames to a path this guard cannot trace to a directory", fn.Name.Name)

		synced, at := syncedDir(fn.Body)
		require.NotEmptyf(t, synced,
			"%s renames a published file but never syncs the directory holding the entry", fn.Name.Name)
		require.Equalf(t, target, synced,
			"%s syncs %q, but the renames it publishes land in %q", fn.Name.Name, synced, target)
		require.Greaterf(t, at, lastRename,
			"%s syncs before its last rename, so that entry is not covered", fn.Name.Name)
	}
	require.NotZero(t, renames, "the guard is watching a file that no longer renames anything")
}

// lastPublishedRename reports the position of the last os.Rename in body and
// the directory its destination is built from.
func lastPublishedRename(body *ast.BlockStmt) (token.Pos, string) {
	assigned := assignmentsIn(body)
	at, dir := token.NoPos, ""
	ast.Inspect(body, func(n ast.Node) bool {
		call, _ := n.(*ast.CallExpr)
		if sel, ok := selectorOfCall(n); !ok || sel != "os.Rename" || len(call.Args) != 2 {
			return true
		}
		at, dir = call.Pos(), dirRootOf(call.Args[1], assigned)
		return true
	})
	return at, dir
}

// syncedDir reports the directory diskio.Fsync is called on, and where.
func syncedDir(body *ast.BlockStmt) (string, token.Pos) {
	dir, at := "", token.NoPos
	ast.Inspect(body, func(n ast.Node) bool {
		call, _ := n.(*ast.CallExpr)
		if sel, ok := selectorOfCall(n); !ok || sel != "diskio.Fsync" || len(call.Args) != 1 {
			return true
		}
		if ident, ok := call.Args[0].(*ast.Ident); ok {
			dir, at = ident.Name, call.Pos()
		}
		return true
	})
	return dir, at
}

// assignmentsIn maps each local name to the expression it was last assigned.
func assignmentsIn(body *ast.BlockStmt) map[string]ast.Expr {
	assigned := map[string]ast.Expr{}
	ast.Inspect(body, func(n ast.Node) bool {
		assign, ok := n.(*ast.AssignStmt)
		if !ok || len(assign.Lhs) != len(assign.Rhs) {
			return true
		}
		for i, lhs := range assign.Lhs {
			if ident, ok := lhs.(*ast.Ident); ok {
				assigned[ident.Name] = assign.Rhs[i]
			}
		}
		return true
	})
	return assigned
}

// dirRootOf resolves a path expression back to the identifier naming the
// directory it is rooted at: a filepath.Join takes its first argument, a
// string transform takes the path it transforms, and a local name is followed
// to what it was assigned.
func dirRootOf(expr ast.Expr, assigned map[string]ast.Expr) string {
	for depth := 0; depth < 8; depth++ {
		switch e := expr.(type) {
		case *ast.Ident:
			next, ok := assigned[e.Name]
			if !ok {
				return e.Name
			}
			delete(assigned, e.Name) // a self-assignment must not loop
			expr = next
		case *ast.CallExpr:
			if len(e.Args) == 0 {
				return ""
			}
			expr = e.Args[0]
		default:
			return ""
		}
	}
	return ""
}

// selectorOfCall returns "pkg.Fn" for a call of that shape.
func selectorOfCall(n ast.Node) (string, bool) {
	call, ok := n.(*ast.CallExpr)
	if !ok {
		return "", false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok {
		return "", false
	}
	return pkg.Name + "." + sel.Sel.Name, true
}
