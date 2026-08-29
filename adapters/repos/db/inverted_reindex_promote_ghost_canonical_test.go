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
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A shard load re-creates the canonical bucket directory, empty, for every
// property in the schema. These tests pin what promotion may conclude from
// finding one: nothing on its own, and everything once the record says a
// rename of that property started.

const (
	ghostProp    = "title"
	ghostTracker = "rebuild_searchable_title_1"
	ghostStaged  = "property_title_searchable__rebuild_searchable_ingest_1"
)

func ghostCanonicalDir() string { return helpers.BucketSearchableFromPropNameLSM(ghostProp) }

// ghostObjects gives a bucket terms to lose, so an assertion can tell an
// emptied bucket from a populated one, and one shard's data from another's.
func ghostObjects(t *testing.T, className, vocabulary string, n int) []*storobj.Object {
	t.Helper()
	objs := make([]*storobj.Object, n)
	for i := range objs {
		objs[i] = &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:         strfmt.UUID(uuid.NewString()),
				Class:      className,
				Properties: map[string]interface{}{ghostProp: vocabulary + string(rune('a'+i%20))},
			},
			Vector: []float32{float32(i)},
		}
	}
	return objs
}

// aGhostShard is a loaded shard whose canonical searchable bucket holds terms
// drawn from vocabulary.
func aGhostShard(t *testing.T, ctx context.Context, vocabulary string) (*Shard, *Index, *models.Class) {
	t.Helper()
	className := "PromoteGhost" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{ghostProp})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	for _, obj := range ghostObjects(t, className, vocabulary, 20) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	return shard, idx, class
}

// copyDirTree copies a shut-down bucket directory under a new name, which is
// how a test gets a staged directory holding data the canonical one does not.
func copyDirTree(t *testing.T, from, to string) {
	t.Helper()
	require.NoError(t, filepath.WalkDir(from, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(from, path)
		if err != nil {
			return err
		}
		target := filepath.Join(to, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		src, err := os.Open(path)
		if err != nil {
			return err
		}
		defer src.Close()
		dst, err := os.Create(target)
		if err != nil {
			return err
		}
		defer dst.Close()
		_, err = io.Copy(dst, src)
		return err
	}))
}

// reloadGhostShard is one process restart: reconciliation runs first, then the
// bucket init that re-creates a canonical directory for every schema property.
// Only a real load puts those two in that order.
func reloadGhostShard(t *testing.T, ctx context.Context, idx *Index, shard *Shard,
	class *models.Class,
) *Shard {
	t.Helper()
	name := shard.Name()
	require.NoError(t, shard.Shutdown(ctx))
	simulateProcessRestartBucketCleanup(t, shard.pathLSM())
	next, err := idx.initShard(ctx, name, class, nil, true, true)
	require.NoError(t, err)
	idx.shards.Store(name, next)
	return next.(*Shard)
}

// ghostRecordState reads the record off disk, which is what a later load, the
// closure sweep, and an operator all read.
func ghostRecordState(t *testing.T, lsmPath string) MigrationState {
	t.Helper()
	logger, _ := test.NewNullLogger()
	store := NewMigrationRecordStore(lsmPath, logger)
	require.NoError(t, store.Load())
	records := store.Records()
	require.Len(t, records, 1, "the fixture plants exactly one record")
	return records[0].State()
}

// TestPromoteReadsTheRecordNotTheCanonicalDirectory drives real shard loads
// over a flipped migration and pins both answers promotion has to give about
// a canonical directory it finds.
//
// The defect this covers: an index DELETE removes a flipped migration's
// staged AND canonical directories at once. The next load re-creates the
// canonical bucket empty because the property is still in the schema, and the
// load after that reads that empty directory as proof the promotion already
// ran — writing Promoted over a bucket with none of the migration's data in
// it, which is the state every later reader trusts.
func TestPromoteReadsTheRecordNotTheCanonicalDirectory(t *testing.T) {
	tests := []struct {
		name string
		// stage runs on the shut-down shard, between the flip and the loads.
		stage func(t *testing.T, lsmPath, stagedPath, canonicalPath string)
		// loads is how many restarts run after staging.
		loads     int
		wantState MigrationState
		// wantTerms is the vocabulary the canonical bucket must hold after
		// the loads, or "" when it must hold nothing.
		wantTerms string
		reason    string
	}{
		{
			name: "an index DELETE took both directories",
			stage: func(t *testing.T, _, stagedPath, canonicalPath string) {
				require.NoError(t, os.RemoveAll(canonicalPath))
				require.NoError(t, os.RemoveAll(stagedPath))
			},
			// Load 1 finds neither directory and re-creates the canonical one
			// empty; load 2 is the one that used to read it as a promotion.
			loads:     2,
			wantState: MigrationStateSwapped,
			wantTerms: "",
			reason: "the record must not read Promoted over a canonical directory the shard load re-created: " +
				"no promotion of this property ever started",
		},
		{
			name:  "the staged directory is there to promote",
			stage: func(t *testing.T, _, _, _ string) {},
			loads: 1,
			// The staged copy carries a vocabulary the canonical bucket never
			// had, so only a rename that really ran can put it there.
			wantState: MigrationStatePromoted,
			wantTerms: "donor",
			reason:    "a promotion whose staged directory is present must still run and move its data",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			shard, idx, class := aGhostShard(t, ctx, "canonical")
			lsmPath := shard.pathLSM()
			canonical := ghostCanonicalDir()

			before := fingerprintInvertedBucket(t, shard.store.Bucket(canonical))
			require.NotEmpty(t, before, "fixture: the canonical bucket holds terms before anything touches it")

			// A staged directory holding data the canonical name does not, so
			// the two are told apart by what is in them, not by which exists.
			donor, _, _ := aGhostShard(t, ctx, "donor")
			donorPath := donor.pathLSM()
			donorFP := fingerprintInvertedBucket(t, donor.store.Bucket(canonical))
			require.NotEmpty(t, donorFP, "fixture: the donor bucket holds terms")
			require.NotEqual(t, before, donorFP, "fixture: the two vocabularies must differ")
			require.NoError(t, donor.Shutdown(ctx))

			require.NoError(t, shard.Shutdown(ctx))
			simulateProcessRestartBucketCleanup(t, lsmPath)
			copyDirTree(t, filepath.Join(donorPath, canonical), filepath.Join(lsmPath, ghostStaged))

			// From the flip on, the staged directory holds the live data and
			// only a promotion may put it under the canonical name.
			mkTrackerDir(t, lsmPath, ghostTracker)
			mkFlippedMigrationRecord(t, lsmPath, ghostTracker, ghostProp, ghostStaged, canonical)
			require.Equal(t, MigrationStateSwapped, ghostRecordState(t, lsmPath), "fixture")

			tc.stage(t, lsmPath, filepath.Join(lsmPath, ghostStaged), filepath.Join(lsmPath, canonical))

			loaded, err := idx.initShard(ctx, shard.Name(), class, nil, true, true)
			require.NoError(t, err)
			idx.shards.Store(shard.Name(), loaded)
			current := loaded.(*Shard)
			for i := 1; i < tc.loads; i++ {
				current = reloadGhostShard(t, ctx, idx, current, class)
			}
			defer current.Shutdown(ctx)

			got := fingerprintInvertedBucket(t, current.store.Bucket(canonical))
			if tc.wantTerms == "" {
				require.Empty(t, got,
					"fixture: the canonical bucket must hold nothing, or promoting it would lose nothing")
			} else {
				assert.Equal(t, donorFP, got,
					"the canonical bucket must hold the data the promotion renamed onto it")
			}
			assert.Equal(t, tc.wantState, ghostRecordState(t, lsmPath), tc.reason)
		})
	}
}
