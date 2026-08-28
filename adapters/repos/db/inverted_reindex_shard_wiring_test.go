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
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// installTestMigrationTaskSources installs the pair reconciliation reads, both
// answering from the same list: the ordinary case where this node has caught
// up with the leader. A non-nil leaderErr makes the leader unreachable
// instead.
func installTestMigrationTaskSources(ctx context.Context, database *DB, leaderErr error,
	tasks ...*distributedtask.Task,
) {
	database.SetMigrationTaskSources(ctx,
		func() ([]*distributedtask.Task, bool) { return tasks, true },
		func(context.Context) ([]*distributedtask.Task, error) {
			if leaderErr != nil {
				return nil, leaderErr
			}
			return tasks, nil
		})
}

// TestReconcileWithClusterWithholdsWhereItCannotAct covers the two things the
// off-load walk refuses to decide. It holds a shard pointer across a pass
// that removes directories, and a concurrent tenant/collection teardown can
// pull that shard out from under it, failing a flush into a directory this
// pass removed — a failure that latches and fails every later activation.
// The whole pass also rests on the leader's list, so an unreachable leader
// leaves every record where it was; the next pass asks again.
func TestReconcileWithClusterWithholdsWhereItCannotAct(t *testing.T) {
	const propName = "title"

	tests := []struct {
		name         string
		shuttingDown bool
		leaderErr    error
		wantSurvives bool
	}{
		{
			name: "a shard that is staying decides the migration the cluster abandoned",
		},
		{
			name:         "a shard on its way out is left to its next activation",
			shuttingDown: true,
			wantSurvives: true,
		},
		{
			name:         "an unreachable leader decides nothing at all",
			leaderErr:    errors.New("leader unreachable"),
			wantSurvives: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			className := "WiringShutdownGuard_" + uuid.NewString()[:8]
			shd, idx := testShardWithSettings(t, ctx, newTestClassWithProps(className, []string{propName}),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, propName)
			require.NoError(t, shard.migrationRecords.Put(NewMigrationRecordMerged(subject)))
			for _, dir := range migrationOwnedDirs(subject) {
				require.NoError(t, os.MkdirAll(filepath.Join(shard.pathLSM(), dir), 0o777))
			}
			staged := filepath.Join(shard.pathLSM(), subject.StagedDirs[propName])

			if tt.shuttingDown {
				shard.shutdownRequested.Store(true)
				// Before the deferred Shutdown, which the flag would refuse.
				defer shard.shutdownRequested.Store(false)
			}

			// The pass walks db.indices, which the shard fixture does not
			// populate.
			require.NotNil(t, idx.db, "the test shard fixture has to wire idx.db")
			idx.db.indices[indexID(idx.Config.ClassName)] = idx

			installTestMigrationTaskSources(ctx, idx.db, tt.leaderErr, &distributedtask.Task{
				Namespace: ReindexNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{
					ID: subject.TaskID, Version: subject.Key.TaskVersion,
				},
				Status: distributedtask.TaskStatusCancelled,
			})

			assert.Equal(t, tt.wantSurvives, dirIsThere(t, staged), "the staged directory")
			_, present := shard.migrationRecords.Get(subject.Key)
			assert.Equal(t, tt.wantSurvives, present, "the migration record")
		})
	}
}

// TestReconcileWithoutADatabaseHandle pins the startup window: an index gets
// its database handle only after its constructor returns, so an eagerly
// loaded shard reconciles with no handle to read the task map from. The
// constructor's recover swallows the resulting panic, the index never
// registers, and every later submit fails against the whole collection.
func TestReconcileWithoutADatabaseHandle(t *testing.T) {
	const propName = "title"

	// Merged is the state whose disposition the task map decides; the others
	// reach the same withhold through the same guard, so one row pins it.
	tests := []struct {
		name string
		rec  func(MigrationSubject) MigrationRecord
	}{
		{
			name: "merged",
			rec:  func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			className := "WiringNoDBHandle_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})
			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, propName)
			require.NoError(t, shard.migrationRecords.Put(tt.rec(subject)))
			for _, dir := range migrationOwnedDirs(subject) {
				require.NoError(t, os.MkdirAll(filepath.Join(shard.pathLSM(), dir), 0o777))
			}
			staged := filepath.Join(shard.pathLSM(), subject.StagedDirs[propName])

			handle := idx.db
			idx.db = nil
			// Restored before the deferred Shutdown, which needs the handle.
			defer func() { idx.db = handle }()

			require.NotPanics(t, func() { shard.reconcileMigrationRecords(ctx, class) })

			assert.True(t, dirIsThere(t, staged), "the staged directory")
			_, present := shard.migrationRecords.Get(subject.Key)
			assert.True(t, present, "the migration record")
		})
	}
}

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
