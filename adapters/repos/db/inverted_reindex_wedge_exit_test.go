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

// TestAPassThatChangedNothingSaysSo pins the settled note: a pass writes down
// a directory only when it reached an answer for that directory's record which
// no later load revisits, so a sweep over the cold shard can answer "would
// hydrating reclaim anything" without hydrating.
//
// An unchanged record is not that answer. The commonest reason a pass leaves a
// record alone is a verdict it could not take, which turns on this node's
// applied task map — an input that changes with no record write at all. Both
// rows plant identical directories and differ only in whether the pass reached
// a terminal answer.
func TestAPassThatChangedNothingSaysSo(t *testing.T) {
	tests := []struct {
		name string
		// tasksReadable is this node's applied task map. False is the startup
		// window migrationLocalTasks documents: an eagerly loaded shard
		// reconciles before the index has its database handle.
		tasksReadable bool
		record        func(MigrationSubject) MigrationRecord
		wantNoted     bool
		wantWedged    int
	}{
		{
			name:          "a promotion whose target is gone: no later load changes it",
			tasksReadable: true,
			record: func(subject MigrationSubject) MigrationRecord {
				return NewMigrationRecordSwapped(subject, []string{"title"},
					map[string]string{"title": wedgeCanonical}).WithPromotionAt("title", migrationPromotionLost)
			},
			wantNoted:  true,
			wantWedged: 1,
		},
		{
			name:          "a flip this node's task map cannot decide yet: the next load re-asks",
			tasksReadable: false,
			record: func(subject MigrationSubject) MigrationRecord {
				return NewMigrationRecordMerged(subject)
			},
			wantNoted:  false,
			wantWedged: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")
			f.tasksReadable = tt.tasksReadable

			subject := wedgeSubject(42)
			f.mkdirs(wedgeStaged, wedgeSidecar, wedgeCanonical)
			f.put(tt.record(subject))

			require.Equal(t, tt.wantWedged, f.reconcile().WedgedCount(),
				"the note and the shard's wedge counter read the same answer")

			note := migrationReadSettledNote(f.lsmPath)
			committed := migrationPreservedStateAt(f.lsmPath, f.logger)
			_, finalizable := hasStalePartialReindexState(f.lsmPath, "title", "searchable", nil, nil, f.logger)

			if !tt.wantNoted {
				require.Empty(t, note,
					"a pass that could not decide has settled nothing, and saying otherwise stops the sweep waking the tenant that needs it")
				require.True(t, committed.trackerNeedsLoad(wedgeTracker))
				require.True(t, finalizable,
					"the cold-tenant gate must answer as it would with no note at all")
				return
			}

			require.True(t, note[wedgeTracker], "the pass reached an answer no later load revisits")
			require.True(t, note[wedgeStaged])
			require.True(t, f.logged(migrationWedgeRemedy),
				"an operator reading the error has to be told what clears it")

			// The gate reads the note rather than hydrating.
			require.True(t, committed.preservesTracker(wedgeTracker))
			require.False(t, committed.trackerNeedsLoad(wedgeTracker))
			require.False(t, finalizable)

			// And any record write takes the note away again.
			require.NoError(t, f.store.Put(NewMigrationRecordSwapped(subject, []string{"title"},
				map[string]string{"title": wedgeCanonical})))
			require.Empty(t, migrationReadSettledNote(f.lsmPath),
				"a note that outlived the record it describes would suppress a hydration that is due")
			require.NoFileExists(t, filepath.Join(f.lsmPath, migrationsDir, migrationSettledNoteFile))
		})
	}
}

// TestAMixedRecordWakesOnlyTheHalfALoadCanMove pins that actionability is a
// per-property fact. Promotion promotes every property whose promotion is not
// lost and skips the ones that are, so a record can hold one directory nothing
// will ever move next to one the very next load renames. Folding them together
// answers wrongly for whichever half loses the fold.
func TestAMixedRecordWakesOnlyTheHalfALoadCanMove(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title", "body")

	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "body", "title")
	lostStaged := subject.StagedDirs["title"]
	lostSidecar := subject.SidecarDirs["title"]
	pendingStaged := subject.StagedDirs["body"]
	pendingCanonical := subject.CanonicalDirs["body"]

	f.mkdirs(lostStaged, lostSidecar, pendingStaged, subject.SidecarDirs["body"],
		subject.CanonicalDirs["title"], pendingCanonical)
	f.put(NewMigrationRecordSwapped(subject, []string{"body", "title"},
		map[string]string{"body": pendingCanonical, "title": subject.CanonicalDirs["title"]}).
		WithPromotionAt("title", migrationPromotionLost))

	state := migrationPreservedStateAt(f.lsmPath, f.logger)

	// Preservation does not move: every directory is kept either way.
	require.True(t, state.preservesBucket(lostStaged))
	require.True(t, state.preservesBucket(pendingStaged))
	require.True(t, state.preservesTracker(subject.TrackerDir))

	require.False(t, state.bucketNeedsLoad(lostStaged),
		"nothing anywhere clears a lost promotion, so hydrating reclaims nothing on its account")
	require.False(t, state.bucketNeedsLoad(lostSidecar))
	require.True(t, state.bucketNeedsLoad(pendingStaged),
		"the sibling's loss must not claim this directory is settled")
	require.True(t, state.trackerNeedsLoad(subject.TrackerDir),
		"one property that can still act keeps the whole record's tracker claimed")

	// And the load the gate promised really does move it.
	f.reconcile()

	require.Equal(t, pendingStaged, f.contentOf(pendingCanonical),
		"the pending property's staged data was renamed onto its canonical name")
	require.False(t, f.exists(pendingStaged))
	require.True(t, f.exists(lostStaged), "the lost property's directories are untouched")
	require.True(t, f.exists(lostSidecar))
}
