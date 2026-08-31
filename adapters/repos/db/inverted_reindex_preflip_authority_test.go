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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func sidecarDataFor(dir string) string {
	return "segment-of-" + dir
}

func mkSidecarWithData(t *testing.T, lsmPath, name string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, name), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(lsmPath, name, "segment-0.db"),
		[]byte(sidecarDataFor(name)), 0o644))
}

func readSidecarData(t *testing.T, lsmPath, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(lsmPath, name, "segment-0.db"))
	if err != nil {
		return "<gone: " + err.Error() + ">"
	}
	return string(data)
}

// Production-shaped names for one searchable-retokenize migration of "title",
// shared by the fixtures below.
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

// preflipTerms is how many distinct terms the canonical bucket serves. Enough
// that a bucket serving a subset, or an empty one, is unmistakable.
const preflipTerms = 25

// preflipLoads is how many times the shard is loaded after retirement. One load
// would not distinguish a canonical bucket that survives from one a later pass
// takes away.
const preflipLoads = 6

// TestThePreFlipCanonicalBucketIsAuthoritative measures the precondition
// retirement rests on: reclaiming the staged directory of a record that never
// flipped is safe only because the canonical bucket is still the complete
// primary copy, which is also what the cancel edge relies on.
//
// So: a real property serving 25 terms, a record staging a rebuild for it, the
// record retired by a resubmit, and the canonical bucket asked again at every
// load. Posting lists are compared byte for byte, not counted.
func TestThePreFlipCanonicalBucketIsAuthoritative(t *testing.T) {
	ctx := testCtx()
	className := "PreFlip" + uuid.NewString()[:8]
	class := buildTwoTokenizationClass(className, "alpha", "title")
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)

	for i := 0; i < preflipTerms; i++ {
		require.NoError(t, shard.PutObject(ctx, &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:         strfmt.UUID(uuid.NewString()),
				Class:      className,
				Properties: map[string]interface{}{"title": fmt.Sprintf("term%03d", i)},
			},
		}))
	}
	canonicalName := helpers.BucketSearchableFromPropNameLSM("title")
	before := fingerprintInvertedBucket(t, shard.store.Bucket(canonicalName))
	require.Len(t, before, preflipTerms, "fixture: the property has to serve every term")

	lsm := shard.pathLSM()
	shardName := shard.Name()
	require.NoError(t, shard.Shutdown(ctx))

	// A rebuild that never flipped, and the resubmit that supersedes it. The
	// successor's staged directory is deliberately absent, so its own promotion
	// does nothing and the canonical bucket below is the one that was there
	// before any of this.
	predecessor := wedgeSubject(41)
	mkSidecarWithData(t, lsm, predecessor.StagedDirs["title"])
	mkTrackerDir(t, lsm, predecessor.TrackerDir)
	successor := wedgeSubject(42)
	successor.TrackerDir = "searchable_retokenize_title_2"
	successor.StagedDirs = map[string]string{"title": "property_title_searchable__retokenize_ingest_2"}
	store := NewMigrationRecordStore(lsm, idx.logger)
	require.NoError(t, store.Put(NewMigrationRecordIterated(predecessor)))
	require.NoError(t, store.Put(NewMigrationRecordSwapped(successor, []string{"title"},
		map[string]string{"title": wedgeCanonical})))

	for load := 1; load <= preflipLoads; load++ {
		reloaded, err := idx.initShard(ctx, shardName, class, nil, true, true)
		require.NoError(t, err)
		got := fingerprintInvertedBucket(t, reloaded.(*Shard).store.Bucket(canonicalName))
		require.Equalf(t, before, got,
			"load %d: the canonical bucket must still serve every term it served before the retirement", load)
		require.NoError(t, reloaded.Shutdown(ctx))

		if load == 1 {
			// The retirement really ran, so the equality above is about a
			// reclaim that happened rather than one that was skipped.
			require.NoError(t, store.Load())
			_, stillThere := store.Get(predecessor.Key)
			require.False(t, stillThere, "the superseded pre-flip record is retired on the first load")
		}
	}
}
