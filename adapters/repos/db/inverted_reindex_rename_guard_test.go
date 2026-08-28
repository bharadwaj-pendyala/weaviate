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
	"testing"

	"github.com/stretchr/testify/require"
)

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
