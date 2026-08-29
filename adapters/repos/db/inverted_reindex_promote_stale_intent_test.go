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
	"path/filepath"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A promotion that renames one property's staged directory but cannot finish
// its record — a sibling property of the same migration is not promotable —
// leaves a record that still says Swapped while the rename already ran. These
// tests pin what the loads after that may conclude about a canonical
// directory they find, once an index DELETE has taken the promoted one and a
// shard load has re-created it empty.

const (
	// staleRenamedProp is the property whose rename really runs.
	staleRenamedProp = "title"
	// staleBlockedProp keeps the pass from settling, which is what leaves the
	// record at Swapped with the rename already behind it.
	staleBlockedProp = "body"

	staleTracker = "rebuild_searchable_pair_1"
)

// canonicalSearchableDir is where a property's searchable index lives, and
// where a promotion of it renames its staged directory.
func canonicalSearchableDir(prop string) string {
	return helpers.BucketSearchableFromPropNameLSM(prop)
}

func staleStagedDir(prop string) string {
	return "property_" + prop + "_searchable__rebuild_searchable_ingest_1"
}

// aStaleIntentShard is a loaded two-property shard whose canonical searchable
// buckets hold terms drawn from vocabulary.
func aStaleIntentShard(t *testing.T, ctx context.Context, vocabulary string) (*Shard, *Index, *models.Class) {
	t.Helper()
	className := "PromoteStaleIntent" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{staleRenamedProp, staleBlockedProp})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	for _, obj := range staleIntentObjects(className, vocabulary, 20) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	return shard, idx, class
}

// staleIntentObjects gives both properties terms to lose, so an assertion can
// tell an emptied bucket from a populated one and one shard's data from
// another's.
func staleIntentObjects(className, vocabulary string, n int) []*storobj.Object {
	objs := make([]*storobj.Object, n)
	for i := range objs {
		term := vocabulary + string(rune('a'+i%20))
		objs[i] = &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(uuid.NewString()),
				Class: className,
				Properties: map[string]interface{}{
					staleRenamedProp: term,
					staleBlockedProp: term,
				},
			},
			Vector: []float32{float32(i)},
		}
	}
	return objs
}

// TestPromoteAfterTheCanonicalDirectoryWasTakenAndRecreated drives real shard
// loads over a migration whose rename ran in a pass that could not settle.
//
// The defect this covers: the pass records its intent to rename before
// renaming, and takes that intent back only when the rename fails. A rename
// that SUCCEEDED without the record reaching Promoted keeps its intent
// forever. An index DELETE then removes the canonical directory the rename
// produced, the next load re-creates it empty because the property is still
// in the schema, and the load after that reads intent-plus-directory as proof
// its own rename put the data there — writing Promoted over a bucket holding
// none of it.
func TestPromoteAfterTheCanonicalDirectoryWasTakenAndRecreated(t *testing.T) {
	tests := []struct {
		name string
		// deleteCanonical runs the index DELETE that takes the directory the
		// rename produced, between the first load and the second.
		deleteCanonical bool
		// loads counts every shard load the row runs, the fixture's two
		// included.
		loads     int
		wantState MigrationState
		// wantRecordSwept says the closure sweep removed the record, which
		// only a migration that promoted every property reaches.
		wantRecordSwept bool
		// wantRenamedTerms is the vocabulary staleRenamedProp's canonical
		// bucket must hold at the end, or "" when it must hold nothing.
		wantRenamedTerms string
		reason           string
	}{
		{
			name:            "an index DELETE took the directory the rename produced",
			deleteCanonical: true,
			// Load 2 finds neither directory and re-creates the canonical one
			// empty; load 3 is the one that reads it as its own rename's work.
			loads:            3,
			wantState:        MigrationStateSwapped,
			wantRenamedTerms: "",
			reason: "the record must not read Promoted over a canonical directory the shard load re-created: " +
				"the data this migration renamed onto that name is gone",
		},
		{
			name:            "nothing takes the directory the rename produced",
			deleteCanonical: false,
			// Load 2 promotes the sibling and settles the record; load 3 is
			// the closure sweep that removes it.
			loads:           3,
			wantRecordSwept: true,
			// The staged copies carry a vocabulary the canonical buckets never
			// had, so only a rename that really ran can put it there.
			wantRenamedTerms: "donor",
			reason:           "a promotion whose renamed data is still under the canonical name must complete and close",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			shard, idx, class := aStaleIntentShard(t, ctx, "canonical")
			lsmPath := shard.pathLSM()

			donor, _, _ := aStaleIntentShard(t, ctx, "donor")
			donorPath := donor.pathLSM()
			donorTerms := map[string]map[string][]uint64{}
			for _, prop := range []string{staleRenamedProp, staleBlockedProp} {
				donorTerms[prop] = fingerprintInvertedBucket(t, donor.store.Bucket(canonicalSearchableDir(prop)))
				require.NotEmpty(t, donorTerms[prop], "fixture: the donor bucket holds terms")
			}
			require.NoError(t, donor.Shutdown(ctx))

			require.NoError(t, shard.Shutdown(ctx))
			simulateProcessRestartBucketCleanup(t, lsmPath)

			// Only the renamed property gets a staged directory now. Its
			// sibling has none, which is what keeps the first pass from
			// settling the record onto Promoted.
			copyDirTree(t,
				filepath.Join(donorPath, canonicalSearchableDir(staleRenamedProp)),
				filepath.Join(lsmPath, staleStagedDir(staleRenamedProp)))

			mkTrackerDir(t, lsmPath, staleTracker)
			mkMigrationRecordAt(t, lsmPath, staleTracker,
				map[string]string{
					staleRenamedProp: staleStagedDir(staleRenamedProp),
					staleBlockedProp: staleStagedDir(staleBlockedProp),
				},
				map[string]string{
					staleRenamedProp: canonicalSearchableDir(staleRenamedProp),
					staleBlockedProp: canonicalSearchableDir(staleBlockedProp),
				},
				MigrationStateSwapped)
			require.Equal(t, MigrationStateSwapped, soleMigrationRecordState(t, lsmPath), "fixture")

			current := openShardFromDisk(t, ctx, idx, class, shard.Name())

			// Receipt that the rename really ran and the record could not
			// settle: this is the state the rest of the test acts on.
			require.Equal(t, donorTerms[staleRenamedProp],
				fingerprintInvertedBucket(t, current.store.Bucket(canonicalSearchableDir(staleRenamedProp))),
				"fixture: the first load renames the staged directory onto the canonical name")
			require.Equal(t, MigrationStateSwapped, soleMigrationRecordState(t, lsmPath),
				"fixture: a sibling property with no staged directory keeps the pass from settling")

			if tc.deleteCanonical {
				// What a RAFT apply of an index DELETE does: shut the bucket
				// down and remove its directory.
				require.NoError(t, current.removeBucket(ctx, canonicalSearchableDir(staleRenamedProp)))
			}

			require.NoError(t, current.Shutdown(ctx))
			simulateProcessRestartBucketCleanup(t, lsmPath)
			// The sibling's staged directory arrives, so the record can settle
			// on a later pass and the promotion of the other property is the
			// only thing left deciding what that record says.
			copyDirTree(t,
				filepath.Join(donorPath, canonicalSearchableDir(staleBlockedProp)),
				filepath.Join(lsmPath, staleStagedDir(staleBlockedProp)))

			current = openShardFromDisk(t, ctx, idx, class, shard.Name())
			for i := 3; i <= tc.loads; i++ {
				current = reloadShardFromDisk(t, ctx, idx, current, class)
			}
			defer current.Shutdown(ctx)

			got := fingerprintInvertedBucket(t,
				current.store.Bucket(canonicalSearchableDir(staleRenamedProp)))
			if tc.wantRenamedTerms == "" {
				require.Empty(t, got,
					"fixture: the canonical bucket must hold nothing, or promoting it would lose nothing")
			} else {
				assert.Equal(t, donorTerms[staleRenamedProp], got,
					"the canonical bucket must hold the data the promotion renamed onto it")
			}
			assert.Equal(t, donorTerms[staleBlockedProp],
				fingerprintInvertedBucket(t, current.store.Bucket(canonicalSearchableDir(staleBlockedProp))),
				"the sibling's own promotion must still run and move its data")
			if tc.wantRecordSwept {
				assert.Empty(t, migrationRecordStates(t, lsmPath), tc.reason)
			} else {
				assert.Equal(t, tc.wantState, soleMigrationRecordState(t, lsmPath), tc.reason)
			}
		})
	}
}
