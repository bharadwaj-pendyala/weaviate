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
	"fmt"
	"os"
)

// migrationDisplacer is the part of a flipped record that names what its flip
// pushed aside. Only Swapped and Promoted have one.
type migrationDisplacer interface {
	// displacedFor names the property whose flip pushed dir aside — the
	// property, not just the fact, since a claim lapses per property.
	displacedFor(dir string) (string, bool)
}

func (f migrationFlipBlock) displacedFor(dir string) (string, bool) {
	if dir == "" {
		return "", false
	}
	for prop, displaced := range f.displacedDirs {
		if displaced == dir {
			return prop, true
		}
	}
	return "", false
}

// migrationPropertySuperseded is the supersession predicate, ordered by task
// version alone (already a total order), so it's closed under any retirement
// order or crash.
//
// The bar is Swapped, not Merged: a successor that's staged but undecided may
// still be cancelled, so treating it as settled would withhold promotion of a
// record whose own flip already retired the old data. "Same property"
// compares canonical directories, not index types — searchable and
// filterable migrations on one property stage into different buckets.
func migrationPropertySuperseded(all []MigrationRecord, subject MigrationSubject, prop string) bool {
	canonical := subject.CanonicalDirs[prop]
	if canonical == "" {
		return false
	}
	for _, other := range all {
		if migrationSupersedes(other, subject) && other.Subject().CanonicalDirs[prop] == canonical {
			return true
		}
	}
	return false
}

func migrationSupersedes(candidate MigrationRecord, subject MigrationSubject) bool {
	key := candidate.Subject().Key
	return key != subject.Key && key.TaskVersion > subject.Key.TaskVersion && candidate.FlipDecided()
}

// migrationDirRole is how one record holds a directory, for the log line that
// refuses to take it away.
type migrationDirRole string

const (
	migrationRoleCanonical migrationDirRole = "canonical directory"
	migrationRoleStaged    migrationDirRole = "staged directory"
	migrationRoleSidecar   migrationDirRole = "sidecar directory"
)

// migrationLiveDataRoles are the roles in which a directory holds a record's
// own copy of a property's index. A record that replaces a directory another
// record holds in one of these takes that copy with it.
var migrationLiveDataRoles = []migrationDirRole{migrationRoleStaged, migrationRoleSidecar}

// migrationOwnedRoles adds the canonical role, which no record may reclaim
// because it is where the holder's property serves from.
var migrationOwnedRoles = []migrationDirRole{
	migrationRoleCanonical, migrationRoleStaged, migrationRoleSidecar,
}

// migrationDirHeldByAnotherRecord reports whether any other record on the
// shard names dir in one of roles.
//
// [validateOneOwnerPerDirectory] cannot see this: encode and decode each see
// one record. Two individually valid records still collide, and the harm is
// the same as within one — an os.RemoveAll of live data — with the sweeping
// record none the wiser.
//
// The displaced role is deliberately absent, and stays the business of
// [migrationDirClaimedAsDisplaced]: that claim lapses when the claimer's own
// property is itself superseded, precisely so the directory is not stranded,
// and folding it in here would refuse the reclaim the lapse exists to permit.
func migrationDirHeldByAnotherRecord(all []MigrationRecord, subject MigrationSubject,
	dir string, roles []migrationDirRole,
) (MigrationRecordKey, migrationDirRole, bool) {
	for _, other := range all {
		key := other.Subject().Key
		if key == subject.Key {
			continue
		}
		for _, role := range roles {
			for _, held := range migrationDirsInRole(other.Subject(), role) {
				if held == dir {
					return key, role, true
				}
			}
		}
	}
	return MigrationRecordKey{}, "", false
}

func migrationDirsInRole(subject MigrationSubject, role migrationDirRole) map[string]string {
	switch role {
	case migrationRoleCanonical:
		return subject.CanonicalDirs
	case migrationRoleStaged:
		return subject.StagedDirs
	case migrationRoleSidecar:
		return subject.SidecarDirs
	}
	return nil
}

// migrationTrackerHeldByAnotherRecord reports whether another record names the
// same tracker directory. The tracker holds that record's payload.mig, so
// removing it leaves the survivor naming a path that no longer exists.
func migrationTrackerHeldByAnotherRecord(all []MigrationRecord, subject MigrationSubject) (MigrationRecordKey, bool) {
	if subject.TrackerDir == "" {
		return MigrationRecordKey{}, false
	}
	for _, other := range all {
		key := other.Subject().Key
		if key == subject.Key {
			continue
		}
		if other.Subject().TrackerDir == subject.TrackerDir {
			return key, true
		}
	}
	return MigrationRecordKey{}, false
}

// migrationDirClaimedAsDisplaced reports whether a surviving later-versioned
// record claims dir as what its flip displaced. A predecessor that flipped
// but never promoted still holds live data at that staged name — exactly
// what a successor displaces, making that directory the property's only copy.
func migrationDirClaimedAsDisplaced(all []MigrationRecord, subject MigrationSubject, dir string) bool {
	for _, other := range all {
		if !migrationSupersedes(other, subject) {
			continue
		}
		displacer, ok := other.(migrationDisplacer)
		if !ok {
			continue
		}
		claimedFor, ok := displacer.displacedFor(dir)
		if !ok {
			continue
		}
		// The claim lapses with the claimer's own property, not its whole
		// record, since a lapsed claim would strand the directory unreclaimed.
		if migrationPropertySuperseded(all, other.Subject(), claimedFor) {
			continue
		}
		return true
	}
	return false
}

// RetireSuperseded runs supersession in the process that flipped. An
// unreadable record never retires anything and never supersedes anything, so
// it withholds this pass exactly as it withholds reconciliation's.
func (r *migrationReconciler) RetireSuperseded(ctx context.Context) {
	if len(r.store.Unreadable()) > 0 {
		return
	}
	r.retireSuperseded(ctx, r.store.Records())
}

// retireSuperseded runs supersession over every record whose staged data is
// complete. Order is not needed for correctness, but ascending task version
// leaves the fewest dangling links after a crash.
func (r *migrationReconciler) retireSuperseded(ctx context.Context, all []MigrationRecord) {
	for _, rec := range all {
		subject := rec.Subject()
		if len(subject.Properties) == 0 {
			continue
		}

		// Asked before the seal: the common record here is the swap that just
		// wrote, which sealing first would refuse against.
		superseded := supersededProperties(all, subject)
		if len(superseded) == 0 {
			continue
		}
		if !migrationRetirable(rec, superseded) {
			continue
		}

		// Retirement removes directories a worker may still write into; a
		// live one declines the seal and the next pass retires it.
		release, sealed := r.sealUnit(subject)
		if !sealed {
			r.logger.WithField("record", subject.Key.String()).Info(
				"a local unit of this migration is still running, so its retirement waits for the next pass")
			continue
		}
		func() {
			// Deferred, not called after: a leaked seal would refuse this
			// unit for the life of the process.
			defer release()
			r.retireOneSealed(ctx, all, subject, superseded)
		}()
	}
}

// migrationRetirable reports whether retirement may act on this record at all.
//
// A record with complete staged data holds a copy of its own, and a successor
// taking one of its properties over is what makes reclaiming that property's
// copy safe.
//
// A record that has not flipped holds no copy anything reads from: the
// canonical bucket is still the complete primary copy, which is the same
// precondition the cancel edge relies on and states at
// [migrationReconciler.reconcileMerged]. So it may retire too — but only once
// EVERY one of its properties is superseded, since a partly superseded record
// still owns the rest and its staged directories are still the rebuild's only
// output.
//
// Without this a record the supersession predicate says IS superseded is never
// offered to retirement, purely because it has not flipped. That is the one
// wedge with no exit anywhere: the record stands forever, its tracker keeps
// dragging cold tenants into hydration, and the submit-time sweep rewrites it
// to Iterating with a full horizon on every load with no task left to resume it.
func migrationRetirable(rec MigrationRecord, superseded []string) bool {
	if rec.StagedDataComplete() {
		return true
	}
	return !rec.PointerSwapped() && len(superseded) == len(rec.Subject().Properties)
}

// supersededProperties names the properties of subject a later-versioned
// record has taken over, which is the whole of what retirement acts on.
func supersededProperties(all []MigrationRecord, subject MigrationSubject) []string {
	var out []string
	for _, prop := range subject.Properties {
		if migrationPropertySuperseded(all, subject, prop) {
			out = append(out, prop)
		}
	}
	return out
}

// retireOneSealed retires one superseded record, under its unit's seal.
func (r *migrationReconciler) retireOneSealed(ctx context.Context, all []MigrationRecord,
	subject MigrationSubject, superseded []string,
) {
	retired := true
	for _, prop := range superseded {
		if err := r.retireProperty(ctx, all, subject, prop); err != nil {
			r.logger.WithField("record", subject.Key.String()).Errorf(
				"retire a superseded property: %v", err)
			retired = false
		}
	}
	// A directory whose removal failed must keep the record naming it, or
	// nothing can attribute it; the next load retries.
	if !retired || len(superseded) != len(subject.Properties) {
		return
	}

	// Every property is gone or has become a successor's responsibility,
	// so the record has nothing left to answer for.
	for _, dir := range subject.SidecarDirs {
		if !r.mayReclaim(all, subject, dir) {
			continue
		}
		if err := os.RemoveAll(r.path(dir)); err != nil {
			r.logger.WithField("dir", dir).Errorf("remove sidecar directory of a superseded migration: %v", err)
		}
	}
	r.removeTrackerDir(all, subject)
	if err := r.store.Remove(subject.Key); err != nil {
		r.logger.WithField("record", subject.Key.String()).Errorf("remove superseded migration record: %v", err)
		return
	}
	// The one line that says what stopped the errors. Without it an operator
	// who resubmitted sees a wedge repeat every restart, then sees it stop,
	// with nothing saying which of the two things they tried did it.
	r.logger.WithField("record", subject.Key.String()).
		WithField("properties", superseded).
		Info("a newer migration took over every property of this one, so its record and directories are reclaimed")
}

// retireProperty disarms before it removes: without that order the directory
// removed is exactly where the superseded record's still-armed mirror sends
// its next copy, and a failed mirror copy fails the user's write with it.
func (r *migrationReconciler) retireProperty(ctx context.Context, all []MigrationRecord,
	subject MigrationSubject, prop string,
) error {
	if err := r.disarmAndClose(ctx, subject.Key, prop); err != nil {
		return err
	}

	dir := subject.StagedDirs[prop]
	if dir == "" || migrationDirClaimedAsDisplaced(all, subject, dir) {
		return nil
	}
	if !r.mayReclaim(all, subject, dir) {
		return nil
	}
	if err := os.RemoveAll(r.path(dir)); err != nil {
		return fmt.Errorf("remove staged directory %q of a superseded migration: %w", dir, err)
	}
	return nil
}
