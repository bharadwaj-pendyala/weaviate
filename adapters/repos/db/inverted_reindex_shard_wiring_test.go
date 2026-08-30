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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestShutdownStagedBucketsClosesOnlyTheNamedProperty pins the scope of the
// shutdown. Supersession retires a predecessor one property at a time, and a
// successor's property set can overlap only part of it, so the properties the
// predecessor still owns keep serving while one of them is retired. Closing
// their buckets takes them down for no reason.
func TestShutdownStagedBucketsClosesOnlyTheNamedProperty(t *testing.T) {
	const propA, propB = "title", "author"

	tests := []struct {
		name  string
		prop  string
		other string
	}{
		{name: "retiring the first property", prop: propA, other: propB},
		{name: "retiring the second property", prop: propB, other: propA},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			className := "WiringStagedScope_" + uuid.NewString()[:8]
			shd, _ := testShardWithSettings(t, ctx, newTestClassWithProps(className, []string{propA, propB}),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())

			subject := testMigrationSubject(42, StrategyCodeEnableFilterable, propA, propB)

			for _, dir := range migrationOwnedDirs(subject) {
				require.NoError(t, shard.store.CreateOrLoadBucket(ctx, dir,
					lsmkv.WithStrategy(lsmkv.StrategyRoaringSet)))
				require.NotNil(t, shard.store.Bucket(dir), "the fixture has to open %q", dir)
			}
			require.NoError(t, shard.migrationRecords.Put(
				NewMigrationRecordIterating(subject, MigrationCheckpoint{})))

			require.NoError(t, shard.ShutdownStagedBuckets(ctx, subject.Key, tt.prop))

			assert.Nil(t, shard.store.Bucket(subject.StagedDirs[tt.prop]),
				"the staged bucket of the retired property")
			assert.Nil(t, shard.store.Bucket(subject.SidecarDirs[tt.prop]),
				"the sidecar bucket of the retired property")
			assert.NotNil(t, shard.store.Bucket(subject.StagedDirs[tt.other]),
				"the staged bucket of the property still in use")
			assert.NotNil(t, shard.store.Bucket(subject.SidecarDirs[tt.other]),
				"the sidecar bucket of the property still in use")
		})
	}
}
