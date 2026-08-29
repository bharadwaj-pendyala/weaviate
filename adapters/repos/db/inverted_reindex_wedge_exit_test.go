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
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Production-shaped names for one searchable-retokenize migration of "title".
// The sweep gate matches tracker and sidecar directories by name, so a
// synthetic name would make it skip for the wrong reason.
const (
	wedgeTracker   = "searchable_retokenize_title_1"
	wedgeStaged    = "property_title_searchable__retokenize_ingest_1"
	wedgeSidecar   = "property_title_searchable__retokenize_reindex_1"
	wedgeCanonical = "property_title_searchable"
)

func wedgeSubject(version uint64) MigrationSubject {
	subject := testMigrationSubject(version, StrategyCodeSearchableRetokenize, "title")
	subject.TrackerDir = wedgeTracker
	subject.StagedDirs = map[string]string{"title": wedgeStaged}
	subject.SidecarDirs = map[string]string{"title": wedgeSidecar}
	subject.CanonicalDirs = map[string]string{"title": wedgeCanonical}
	return subject
}

// TestALostPromotionStopsWakingItsTenant pins the derived actionability
// predicate. A lost promotion is written when a promoted directory is found
// gone, and nothing anywhere clears it — so the record can never reach
// Promoted and a load reclaims nothing on its account. Preservation is
// unchanged; only the claim that hydrating would reclaim something is.
//
// Two records over identical directories, differing in one byte of state.
func TestALostPromotionStopsWakingItsTenant(t *testing.T) {
	tests := []struct {
		name     string
		lost     bool
		wantSkip bool
	}{
		{name: "no promotion mark: a load would still promote", lost: false, wantSkip: false},
		{name: "a lost promotion: no load can act on it", lost: true, wantSkip: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			class := newTestClassWithProps("WedgeGate_"+uuid.NewString()[:8], []string{"title"})
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			defer shd.Shutdown(ctx)

			const gate = "gate-shard"
			lsm := shardPathLSM(idx.path(), gate)
			require.NoError(t, os.MkdirAll(lsm, 0o755))
			mkTrackerDir(t, lsm, wedgeTracker)
			mkSidecarWithData(t, lsm, wedgeStaged)

			store := NewMigrationRecordStore(lsm, idx.logger)
			rec := NewMigrationRecordSwapped(wedgeSubject(42), []string{"title"},
				map[string]string{"title": wedgeCanonical})
			if tc.lost {
				rec = rec.WithPromotionAt("title", migrationPromotionLost)
			}
			require.NoError(t, store.Put(rec))

			lazy := NewLazyLoadShard(ctx, nil, gate, idx, class, idx.centralJobQueue,
				idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
				false, idx.bitmapBufPool)
			idx.shards.Store(gate, lazy)
			defer func() {
				if lazy.isLoaded() {
					require.NoError(t, lazy.Shutdown(ctx))
				}
			}()

			skip, _ := lazy.canSkipUnloadedSweep("title", "searchable", nil, nil)
			require.Equal(t, tc.wantSkip, skip)

			// Preservation itself must not move: both rows keep the record's
			// directory and its data, whatever the gate answers.
			committed := migrationPreservedStateAt(lsm, idx.logger)
			require.True(t, committed.preservesBucket(wedgeStaged))
			require.True(t, committed.preservesTracker(wedgeTracker))
			require.Equal(t, sidecarDataFor(wedgeStaged), readSidecarData(t, lsm, wedgeStaged))
		})
	}
}

// TestASupersededUnflippedRecordRetires pins the exit that was closed. The
// retirement gate refused any record whose staged data was incomplete, even
// one the supersession predicate says IS superseded — and pre-flip the
// canonical bucket is still the complete primary copy, so there was nothing to
// protect. That refusal is the only reason the cluster-committed wedge had no
// exit at all.
func TestASupersededUnflippedRecordRetires(t *testing.T) {
	for _, state := range []MigrationState{MigrationStateIterating, MigrationStateIterated} {
		t.Run(string(state), func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")

			predecessor := wedgeSubject(41)
			predecessor.TrackerDir = "searchable_retokenize_title_1"
			predecessor.StagedDirs = map[string]string{"title": "property_title_searchable__retokenize_ingest_1"}
			predecessor.SidecarDirs = map[string]string{"title": "property_title_searchable__retokenize_reindex_1"}
			f.mkdirs(predecessor.StagedDirs["title"], predecessor.SidecarDirs["title"], wedgeCanonical)
			if state == MigrationStateIterating {
				f.put(NewMigrationRecordIterating(predecessor, MigrationCheckpoint{}))
			} else {
				f.put(NewMigrationRecordIterated(predecessor))
			}

			// The successor an operator's resubmit produces: same property,
			// same canonical directory, already flipped.
			successor := wedgeSubject(42)
			successor.TrackerDir = "searchable_retokenize_title_2"
			successor.StagedDirs = map[string]string{"title": "property_title_searchable__retokenize_ingest_2"}
			successor.SidecarDirs = map[string]string{"title": "property_title_searchable__retokenize_reindex_2"}
			f.mkdirs(successor.StagedDirs["title"])
			f.put(NewMigrationRecordSwapped(successor, []string{"title"},
				map[string]string{"title": wedgeCanonical}))

			f.reconcile()

			_, stillThere := f.state(predecessor.Key)
			require.False(t, stillThere,
				"a record every property of which a newer migration took over has nothing left to answer for")
			require.False(t, f.exists(predecessor.StagedDirs["title"]),
				"and its staged copy, which nothing reads from pre-flip, is reclaimed with it")
			require.False(t, f.migrationDirExists(predecessor))
			require.True(t, f.logged("took over every property of this one"),
				"the operator who resubmitted has to be able to see what stopped the errors")

			// The successor promoted in the same pass, so the property now
			// serves from the successor's rebuild. That is what makes the
			// reclaim above a reclaim and not a loss.
			require.True(t, f.exists(wedgeCanonical))
			require.Equal(t, successor.StagedDirs["title"], f.contentOf(wedgeCanonical))
		})
	}
}

// TestAPassThatChangedNothingSaysSo pins the settled note: a pass that
// reconciled a record and left it exactly as it found it writes that down, so
// a sweep over the cold shard can answer "would hydrating reclaim anything"
// without hydrating. Any record write invalidates it, because a record write
// is the only thing that changes what a load would do.
func TestAPassThatChangedNothingSaysSo(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")

	subject := wedgeSubject(42)
	f.mkdirs(wedgeStaged, wedgeSidecar, wedgeCanonical)
	// A promotion whose target is gone: the record stands and no later load
	// changes it, which is exactly the shape the note is for.
	f.put(NewMigrationRecordSwapped(subject, []string{"title"},
		map[string]string{"title": wedgeCanonical}).WithPromotionAt("title", migrationPromotionLost))

	f.reconcile()

	note := migrationReadSettledNote(f.lsmPath)
	require.True(t, note[wedgeTracker], "the pass reconciled this tracker and changed nothing")
	require.True(t, note[wedgeStaged])
	require.Equal(t, 1, f.wedgeCount(), "and it counted the record for the shard's gauge")
	require.True(t, f.logged(migrationWedgeRemedy),
		"an operator reading the error has to be told what clears it")

	// The gate reads the note rather than hydrating.
	committed := migrationPreservedStateAt(f.lsmPath, f.logger)
	require.True(t, committed.preservesTracker(wedgeTracker))
	require.False(t, committed.trackerNeedsLoad(wedgeTracker))

	// And any record write takes the note away again.
	require.NoError(t, f.store.Put(NewMigrationRecordSwapped(subject, []string{"title"},
		map[string]string{"title": wedgeCanonical})))
	require.Empty(t, migrationReadSettledNote(f.lsmPath),
		"a note that outlived the record it describes would suppress a hydration that is due")
	require.NoFileExists(t, filepath.Join(f.lsmPath, migrationsDir, migrationSettledNoteFile))
}
