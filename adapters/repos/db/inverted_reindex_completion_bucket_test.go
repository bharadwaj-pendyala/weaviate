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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestPromotedCompletionServesTheIndexItAdvertises pins the completion gate
// against the bucket, not against the record. Once the flip is durable the
// completion gate can be re-entered — by a retry, or by a restart that acked
// its own unit before the last one acked — and by then the canonical name
// denotes the promoted bucket, which nothing has necessarily opened. Committing
// the schema effect over a closed bucket advertises an index nothing serves.
//
// The assertion is what the property serves afterwards, not whether some
// directory exists.
func TestPromotedCompletionServesTheIndexItAdvertises(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	tests := []struct {
		name string
		// class builds the fixture, and newTask the migration that promotes it.
		class   func(className string) *models.Class
		newTask func(t *testing.T, idx *Index, className string) *ShardReindexTaskGeneric
		// enable is the schema effect the completed migration commits, which
		// the next load reads before it opens the property's buckets.
		enable func(prop *models.Property)
		// canonical names the bucket the completed migration advertises.
		canonical func(propName string) string
		// serves reports the terms the canonical bucket answers with.
		serves func(t *testing.T, b *lsmkv.Bucket) map[string][]uint64
	}{
		{
			name:  "enable-filterable",
			class: func(cn string) *models.Class { return newEnableFilterableTestClass(cn, propName) },
			newTask: func(t *testing.T, idx *Index, cn string) *ShardReindexTaskGeneric {
				task, _ := newEnableFilterableTask(t, idx, cn, propName)
				return task
			},
			enable:    func(p *models.Property) { p.IndexFilterable = boolPtr(true) },
			canonical: helpers.BucketFromPropNameLSM,
			serves:    fingerprintRoaringSetBucket,
		},
		{
			name:  "enable-searchable",
			class: func(cn string) *models.Class { return newEnableSearchableTestClass(cn, []string{propName}) },
			newTask: func(t *testing.T, idx *Index, cn string) *ShardReindexTaskGeneric {
				task, _ := newEnableSearchableTask(t, idx, cn, propName, models.PropertyTokenizationWord)
				return task
			},
			enable:    func(p *models.Property) { p.IndexSearchable = boolPtr(true) },
			canonical: helpers.BucketSearchableFromPropNameLSM,
			serves:    fingerprintInvertedBucket,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "CompletionBucket_" + uuid.NewString()[:8]
			shd, idx := testShardWithSettings(t, ctx, tc.class(className),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)

			for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			task := tc.newTask(t, idx, className)
			require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			require.NoError(t, task.RunSwapOnShard(ctx, shard))

			// The promotion is a load's work, so the record only reaches
			// Promoted on the next one. That is also the state the completion
			// gate is re-entered in.
			shardName := shard.Name()
			require.NoError(t, shard.Shutdown(ctx))
			postClass := tc.class(className)
			for _, prop := range postClass.Properties {
				if prop.Name == propName {
					tc.enable(prop)
				}
			}
			reloaded, err := idx.initShard(ctx, shardName, postClass, nil, true, true)
			require.NoError(t, err)
			promoted := reloaded.(*Shard)
			defer promoted.Shutdown(ctx)

			canonical := tc.canonical(propName)
			before := tc.serves(t, promoted.store.Bucket(canonical))
			require.NotEmpty(t, before, "fixture: the migration has to have produced an index")

			// A retry, or a restart that acked its own unit before the last one
			// acked, re-enters the gate with the canonical bucket closed.
			require.NoError(t, promoted.store.ShutdownBucket(ctx, canonical))
			require.Nil(t, promoted.store.Bucket(canonical), "fixture: the bucket has to be closed")

			require.NoError(t, task.RunSwapOnShard(ctx, promoted),
				"the completion retry must not report success over a closed bucket")

			require.Equal(t, before, tc.serves(t, promoted.store.Bucket(canonical)),
				"after the completion the property must serve exactly what the migration built")
		})
	}
}
