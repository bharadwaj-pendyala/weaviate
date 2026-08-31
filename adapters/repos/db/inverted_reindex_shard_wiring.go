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

	"github.com/weaviate/weaviate/cluster/distributedtask"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// reconcileMigrationRecords runs the load-time reconciliation pass. It must
// stay ahead of bucket loading: it renames directories, and a bucket opened
// at a name it is about to move would serve the wrong data.
func (s *Shard) reconcileMigrationRecords(ctx context.Context, class *models.Class) {
	s.migrationRecords = NewMigrationRecordStore(s.pathLSM(), s.index.logger)
	// A store that does not own this directory must not sweep it: deleting a
	// scratch file there breaks the owner's rename. No such store exists yet.
	s.migrationRecords.SweepTempFiles()

	reconciler := s.migrationReconciler(func() *models.Class { return class })
	if err := reconciler.Reconcile(ctx); err != nil {
		// A shard whose records cannot be read still has to load; every
		// individual disposition already fails toward doing nothing.
		s.index.logger.WithField("shard", s.ID()).Errorf("reconcile migration records: %v", err)
	}
	// Node-wide and unlabelled: which shard is in the log line above, since a
	// label carrying a class or tenant name is one series per tenant.
	monitoring.GetMetrics().AddMigrationRecordsWedged(
		reconciler.WedgedCount(), len(s.migrationRecords.Unreadable()))
}

func (s *Shard) migrationReconciler(class func() *models.Class) *migrationReconciler {
	// Shard-scoped, because the wedge metric is node-wide and unlabelled: the
	// line an operator is sent to has to say which shard it is about.
	return newMigrationReconciler(s.migrationRecords, s.pathLSM(),
		s.index.logger.WithField("shard", s.ID()),
		migrationReconcileDeps{
			Class:   class,
			Buckets: s,
			// Granted explicitly: no task writes a record on this build, so
			// no worker can hold a unit a record-driven teardown touches.
			// The nil default refuses, which keeps every other construction
			// (tests included) fail-safe; this one production construction
			// opts in, and the cutover PR must replace this grant with the
			// real worker registry — nothing stalls if it forgets, so the
			// replacement is a review obligation, not an enforced one.
			SealUnit: func(distributedtask.TaskDescriptor, string) (func(), bool) {
				return func() {}, true
			},
		})
}

// ShutdownStagedBuckets closes a record's open buckets for one property so
// their directories can be removed — a no-op at shard load, but needed when
// a successor retires a predecessor within one process.
func (s *Shard) ShutdownStagedBuckets(ctx context.Context, key MigrationRecordKey, prop string) error {
	if s.store == nil || s.migrationRecords == nil {
		return nil
	}
	rec, ok := s.migrationRecords.Get(key)
	if !ok {
		return nil
	}

	subject := rec.Subject()
	for _, dir := range migrationOwnCopyDirs(subject, prop) {
		if s.store.Bucket(dir) == nil {
			continue
		}
		if err := s.store.ShutdownBucket(ctx, dir); err != nil {
			return err
		}
	}
	return nil
}
