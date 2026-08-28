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
	return key != subject.Key && key.TaskVersion > subject.Key.TaskVersion && candidate.PointerSwapped()
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
		if !rec.StagedDataComplete() {
			continue
		}
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
			r.retireOneSealed(ctx, all, rec, subject, superseded)
		}()
	}
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
	rec MigrationRecord, subject MigrationSubject, superseded []string,
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
	// Fully superseded is the condition for removing the record itself rather
	// than just some of its directories, and the caller already computed which
	// properties a successor took over.
	if !retired || len(subject.Properties) == 0 ||
		len(superseded) != len(subject.Properties) || !rec.StagedDataComplete() {
		return
	}

	// Every property is gone or has become a successor's responsibility,
	// so the record has nothing left to answer for.
	for _, dir := range subject.SidecarDirs {
		if err := os.RemoveAll(r.path(dir)); err != nil {
			r.logger.WithField("dir", dir).Errorf("remove sidecar directory of a superseded migration: %v", err)
		}
	}
	r.removeTrackerDir(subject)
	if err := r.store.Remove(subject.Key); err != nil {
		r.logger.WithField("record", subject.Key.String()).Errorf("remove superseded migration record: %v", err)
	}
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
	if err := os.RemoveAll(r.path(dir)); err != nil {
		return fmt.Errorf("remove staged directory %q of a superseded migration: %w", dir, err)
	}
	return nil
}
