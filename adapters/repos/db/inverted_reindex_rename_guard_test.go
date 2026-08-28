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

package db

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Pins that migration renames go through diskio.RenameAndSync, not a bare
// os.Rename, so a crash can't publish a name whose bytes never landed.
func TestMigrationRenamesGoThroughTheDurableHelper(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()
	scanned := 0

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		if !matchesAny(name, "inverted_reindex_*.go", "reindex_*.go") {
			continue
		}

		file, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)
		scanned++

		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Rename" {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok || pkg.Name != "os" {
				return true
			}
			require.Failf(t, "os.Rename in the migration machinery",
				"%s: use diskio.RenameAndSync — a rename a durable record vouches for "+
					"must be synced, or a crash publishes a record for a name the "+
					"filesystem never kept", fset.Position(call.Pos()))
			return false
		})
	}

	require.Greater(t, scanned, 20, "the guard has to be reading the migration files")
}

func matchesAny(name string, patterns ...string) bool {
	for _, pattern := range patterns {
		if ok, err := filepath.Match(pattern, name); err == nil && ok {
			return true
		}
	}
	return false
}

// Pins that every payload.mig reader gates on refuseOversizedRecoveryPayload
// with the bound for its call site: the apply path takes the latency bound;
// the startup recovery walk, which precedes any double-write mirror, takes
// the larger memory bound.
func TestEveryPayloadReadIsBounded(t *testing.T) {
	wantBound := map[string]string{"loadReindexRecoveryRecord": "maxRecoveryWalkPayloadBytes"}
	const applyPathBound = "maxRecoveryPayloadBytes"

	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()
	checked, offTheApplyPath := 0, 0

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			names := identsIn(fn.Body)
			if !names["reindexRecoveryPayloadFile"] || !names["ReadFile"] {
				continue
			}
			checked++
			at := fset.Position(fn.Pos())

			gate, found := payloadReadGate(fn.Body)
			require.Truef(t, found,
				"%s: %s reads payload.mig without an `if err := refuseOversizedRecoveryPayload(...); "+
					"err <op> nil` gating the read", at, fn.Name.Name)
			require.Truef(t, gate.admitsTheRead,
				"%s: %s bounds the payload but reads it outside the arm the bound admits, "+
					"so the refusal decides nothing", at, fn.Name.Name)

			want := wantBound[fn.Name.Name]
			if want == "" {
				require.Equalf(t, applyPathBound, gate.bound,
					"%s: %s runs where a RAFT apply reaches it, so it takes %s", at, fn.Name.Name, applyPathBound)
				continue
			}
			offTheApplyPath++
			require.Equalf(t, want, gate.bound,
				"%s: %s takes the apply-path bound, which drops the writes taken since the restart; "+
					"it has to take %s", at, fn.Name.Name, want)
		}
	}

	require.GreaterOrEqual(t, checked, 3, "the guard has to be finding the readers of this file")
	require.Equal(t, len(wantBound), offTheApplyPath, "every reader off the apply path has to still be one")
}

// payloadGate is the bound a reader passes and whether the read it performs
// sits where the refusal can stop it.
type payloadGate struct {
	bound         string
	admitsTheRead bool
}

// payloadReadGate finds the refuseOversizedRecoveryPayload gate a payload
// read is subject to. `err != nil` must return, admitting the read that
// follows; `err == nil` admits only the read inside its own body.
func payloadReadGate(body *ast.BlockStmt) (payloadGate, bool) {
	var gate payloadGate
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		stmt, ok := n.(*ast.IfStmt)
		if !ok {
			return true
		}
		call, errName := refusalIn(stmt.Init)
		if call == nil || len(call.Args) != 2 {
			return true
		}
		bound, ok := call.Args[1].(*ast.Ident)
		if !ok {
			return true
		}
		found = true
		gate.bound = bound.Name
		switch comparisonWithNil(stmt.Cond, errName) {
		case token.NEQ:
			gate.admitsTheRead = endsInReturn(stmt.Body) && readsPayloadAfter(body, stmt.End())
		case token.EQL:
			gate.admitsTheRead = identsIn(stmt.Body)["ReadFile"]
		default:
			// A condition that is not a comparison against nil admits nothing.
		}
		return false
	})
	return gate, found
}

// refusalIn reports the refuseOversizedRecoveryPayload call an if-statement
// takes its condition from, and the name it binds the error to.
func refusalIn(init ast.Stmt) (*ast.CallExpr, string) {
	assign, ok := init.(*ast.AssignStmt)
	if !ok || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
		return nil, ""
	}
	call, ok := assign.Rhs[0].(*ast.CallExpr)
	if !ok {
		return nil, ""
	}
	if fn, ok := call.Fun.(*ast.Ident); !ok || fn.Name != "refuseOversizedRecoveryPayload" {
		return nil, ""
	}
	name, ok := assign.Lhs[0].(*ast.Ident)
	if !ok {
		return nil, ""
	}
	return call, name.Name
}

// comparisonWithNil reports the operator of `<errName> <op> nil`, or
// [token.ILLEGAL] for any other condition.
func comparisonWithNil(cond ast.Expr, errName string) token.Token {
	binary, ok := cond.(*ast.BinaryExpr)
	if !ok {
		return token.ILLEGAL
	}
	lhs, lhsOK := binary.X.(*ast.Ident)
	rhs, rhsOK := binary.Y.(*ast.Ident)
	if !lhsOK || !rhsOK || lhs.Name != errName || rhs.Name != "nil" {
		return token.ILLEGAL
	}
	return binary.Op
}

func endsInReturn(body *ast.BlockStmt) bool {
	if len(body.List) == 0 {
		return false
	}
	_, ok := body.List[len(body.List)-1].(*ast.ReturnStmt)
	return ok
}

func readsPayloadAfter(body *ast.BlockStmt, pos token.Pos) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		ident, ok := n.(*ast.Ident)
		if ok && ident.Name == "ReadFile" && ident.Pos() > pos {
			found = true
		}
		return true
	})
	return found
}

// Pins that writeFileAtomic syncs the temp file before renaming it into
// place, so a crash between the two can't publish a name over content that
// never landed on disk.
func TestRecordWritesReachDiskBeforeTheNameDoes(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "inverted_reindex_record_store.go", nil, 0)
	require.NoError(t, err)

	var fn *ast.FuncDecl
	for _, decl := range file.Decls {
		if decl, ok := decl.(*ast.FuncDecl); ok && decl.Name.Name == "writeFileAtomic" {
			fn = decl
		}
	}
	require.NotNil(t, fn, "the one writer that publishes a record by rename")

	write := firstUse(fn.Body, "Write")
	sync := firstUse(fn.Body, "Sync")
	publish := firstUse(fn.Body, "RenameAndSync")

	require.NotEqual(t, token.NoPos, write, "writeFileAtomic must write the temp file")
	require.NotEqual(t, token.NoPos, publish, "writeFileAtomic must publish by renaming it")
	require.NotEqualf(t, token.NoPos, sync,
		"writeFileAtomic publishes %s without syncing the temp file first",
		fset.Position(publish))
	require.Greater(t, sync, write, "the sync has to follow the write it makes durable")
	require.Greater(t, publish, sync, "the name may only be published once the bytes are on disk")
}

// identsIn collects every identifier named in n, which is all this guard needs:
// it asks whether a function mentions a helper, not where.
func identsIn(n ast.Node) map[string]bool {
	found := map[string]bool{}
	ast.Inspect(n, func(node ast.Node) bool {
		if ident, ok := node.(*ast.Ident); ok {
			found[ident.Name] = true
		}
		return true
	})
	return found
}

// Pins that SealLocalUnit answers from the liveness registry (liveUnits /
// sealedUnits), not the semantic-only re-entry guard (activeWorkers), and
// that every writing span registers unconditionally and checks the seal —
// otherwise reconciliation can remove directories a running worker still uses.
func TestLocalUnitSealIsNotTheReEntryGuard(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "reindex_provider.go", nil, 0)
	require.NoError(t, err)

	bodies := map[string]*ast.FuncDecl{}
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil {
			bodies[fn.Name.Name] = fn
		}
	}

	seal, ok := bodies["SealLocalUnit"]
	require.True(t, ok, "the seal reconciliation gates its removals on")
	names := identsIn(seal.Body)
	require.True(t, names["liveUnits"], "SealLocalUnit must answer from the liveness registry")
	require.True(t, names["sealedUnits"], "SealLocalUnit must record the seal it granted")
	require.False(t, names["activeWorkers"],
		"SealLocalUnit must not answer from the re-entry guard: that map is claimed only "+
			"for semantic migrations and only around the iteration")

	enter, ok := bodies["enterLocalUnit"]
	require.True(t, ok, "the claim every writing span takes")
	require.True(t, identsIn(enter.Body)["sealedUnits"],
		"enterLocalUnit must refuse a unit a teardown holds: the phase decided to run from a "+
			"task snapshot frozen at the start of a tick, so the teardown can have started since")

	// resolvedBy names the shard-resolving call for each iteration entry point.
	resolvedBy := map[string]string{
		"processOneUnit":  "unwrapShard",
		"runPerUnitPhase": "resolveUnitForPhase",
	}
	for _, fn := range []string{"processOneUnit", "runPerUnitPhase"} {
		decl, ok := bodies[fn]
		require.Truef(t, ok, "%s is where a span that writes migration directories lives", fn)
		require.Truef(t, identsIn(decl.Body)["enterLocalUnit"],
			"%s does work through handles taken before it starts, so it must register the unit as live", fn)
		require.Falsef(t, callIsGuardedBySemantic(decl.Body),
			"%s registers the unit only for semantic migrations; the other four types write "+
				"into the same directories", fn)

		// Claim must precede resolve: a hydration runs reconciliation, which
		// seals this unit, so claiming after resolve would race the seal.
		claim := firstUse(decl.Body, "enterLocalUnit")
		resolve := firstUse(decl.Body, resolvedBy[fn])
		require.NotEqualf(t, token.NoPos, resolve, "%s must resolve its shard through %s", fn, resolvedBy[fn])
		require.Greaterf(t, claim, resolve,
			"%s claims the unit before %s hydrates the shard, which refuses the seal reconciliation "+
				"needs to promote this same migration", fn, resolvedBy[fn])
	}
}

// firstUse is the position of the first mention of name in body, or
// [token.NoPos] when it is not mentioned at all.
func firstUse(body *ast.BlockStmt, name string) token.Pos {
	at := token.NoPos
	ast.Inspect(body, func(n ast.Node) bool {
		ident, ok := n.(*ast.Ident)
		if !ok || ident.Name != name || at != token.NoPos {
			return true
		}
		at = ident.Pos()
		return true
	})
	return at
}

// callIsGuardedBySemantic reports an enterLocalUnit call reachable only when a
// migration is semantic, which is the shape the re-entry guard has and the
// liveness registration must not.
func callIsGuardedBySemantic(body *ast.BlockStmt) bool {
	guarded := false
	ast.Inspect(body, func(n ast.Node) bool {
		stmt, ok := n.(*ast.IfStmt)
		if !ok {
			return true
		}
		if !identsIn(stmt.Cond)["semantic"] && !identsIn(stmt.Cond)["IsSemanticMigration"] {
			return true
		}
		if identsIn(stmt.Body)["enterLocalUnit"] {
			guarded = true
		}
		return true
	})
	return guarded
}
