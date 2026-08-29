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
	"path/filepath"
	"slices"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/models"
)

// migrationMirrorDisarmer disarms one record's double-write mirror for one
// property. The disarming actor is never the arming one, so the handle
// cannot live on the arming task; disarming an unarmed pair is a no-op.
type migrationMirrorDisarmer interface {
	DisarmMigrationMirror(key MigrationRecordKey, prop string)
}

// migrationStagedBucketCloser shuts a record's staged buckets down before
// their directory is removed, so mmaps, in-flight compactions, and the
// registry entry don't leak.
type migrationStagedBucketCloser interface {
	ShutdownStagedBuckets(ctx context.Context, key MigrationRecordKey, prop string) error
}

// migrationReconcileDeps are the facts and collaborators reconciliation needs
// from outside itself, wired in when the shard loads.
type migrationReconcileDeps struct {
	// LocalTasks is this node's applied view of the reindex namespace, kept
	// synchronous so a shard load never blocks on a round trip to the leader.
	// The bool distinguishes "not readable yet" from "read and empty" — only
	// the latter licenses a discard.
	LocalTasks func() ([]*distributedtask.Task, bool)

	// SealUnit reserves a (task, unit) for teardown, or refuses because a
	// worker is running for it — writing through handles taken before its
	// phase began, even after the cluster status goes terminal.
	SealUnit func(distributedtask.TaskDescriptor, string) (func(), bool)

	// Class is the locally applied schema for the migrated collection, or nil
	// when the collection is not in it.
	Class func() *models.Class

	Mirror  migrationMirrorDisarmer
	Buckets migrationStagedBucketCloser
}

// migrationReconciler runs one reconciliation pass per shard load, before any
// bucket opens (it renames directories) and before any question is answered.
// It is the only place external facts enter and disk may override the record.
type migrationReconciler struct {
	store   *MigrationRecordStore
	lsmPath string
	logger  logrus.FieldLogger
	deps    migrationReconcileDeps
}

func newMigrationReconciler(store *MigrationRecordStore, lsmPath string,
	logger logrus.FieldLogger, deps migrationReconcileDeps,
) *migrationReconciler {
	return &migrationReconciler{store: store, lsmPath: lsmPath, logger: logger, deps: deps}
}

// migrationVerdict is what the cluster says should become of a migration
// whose disposition is not decidable from disk alone.
type migrationVerdict uint8

const (
	// migrationVerdictLeave means a live task still owns this migration, or the
	// evidence is too thin to act on; doing nothing is always safe.
	migrationVerdictLeave migrationVerdict = iota
	migrationVerdictCommit
	migrationVerdictDiscard
)

// migrationPassRecords is the one read of the record set every arm of a pass
// decides against: the records themselves, plus whether any record on the
// shard could not be read. An unreadable record's properties are exactly what
// could not be read, so that withholding covers the whole shard rather than
// some properties, and every arm has to consult it.
type migrationPassRecords struct {
	all            []MigrationRecord
	someUnreadable bool
}

// Reconcile runs one pass over this shard's records.
func (r *migrationReconciler) Reconcile(ctx context.Context) error {
	if err := r.store.Load(); err != nil {
		return fmt.Errorf("load migration records: %w", err)
	}

	// An unreadable record's properties are exactly what could not be read, so
	// the withholding cannot be scoped to them; it must cover the whole shard.
	someRecordsUnreadable := len(r.store.Unreadable()) > 0
	if someRecordsUnreadable {
		r.logger.WithField("path", r.store.Dir()).Warnf(
			"%d migration record(s) not understood: withholding every destructive and promoting action on this shard",
			len(r.store.Unreadable()))
	}

	records := r.store.Records()
	if !someRecordsUnreadable {
		r.retireSuperseded(ctx, records)
		records = r.store.Records()
	}
	pass := migrationPassRecords{all: records, someUnreadable: someRecordsUnreadable}

	for _, rec := range records {
		if err := r.reconcileOne(ctx, rec, pass); err != nil {
			// One migration must not be able to keep a shard from loading.
			r.logger.WithField("record", rec.Subject().Key.String()).Errorf("reconcile migration record: %v", err)
		}
	}
	return nil
}

func (r *migrationReconciler) reconcileOne(ctx context.Context, rec MigrationRecord,
	pass migrationPassRecords,
) error {
	switch typed := rec.(type) {
	case MigrationRecordIterating, MigrationRecordIterated:
		return r.reconcileUncommitted(ctx, rec, pass)
	case MigrationRecordMerged:
		return r.reconcileMerged(ctx, typed, pass)
	case MigrationRecordSwapped:
		return r.reconcileSwapped(ctx, typed, pass)
	case MigrationRecordPromoted:
		return r.reconcilePromoted(ctx, typed, pass)
	default:
		return fmt.Errorf("no reconciliation for record variant %T", rec)
	}
}

// reconcileUncommitted handles Iterating and Iterated together, since both
// take the same three decisions in the same order. Discard runs before the
// reverse edge: restarting a migration whose directories are gone would arm
// a live mirror that nothing will ever recreate directories for.
func (r *migrationReconciler) reconcileUncommitted(ctx context.Context, rec MigrationRecord,
	pass migrationPassRecords,
) error {
	subject := rec.Subject()

	// Withholding covers destructive and promoting action only; the reverse
	// edge takes back work nothing else covers, so it still runs.
	if pass.someUnreadable {
		_, err := r.restartIfRebuiltDataGone(subject)
		return err
	}

	verdict, why := r.localVerdict(subject)
	if verdict == migrationVerdictDiscard {
		return r.discard(ctx, pass.all, subject, why)
	}

	restarted, err := r.restartIfRebuiltDataGone(subject)
	if err != nil || restarted {
		return err
	}

	switch verdict {
	case migrationVerdictCommit:
		// The cluster reports the task done, but this shard never finished its
		// rebuild; nothing here may complete it or delete on a guess.
		r.logger.WithField("record", subject.Key.String()).Warnf(
			"migration is %s locally but the cluster reports it committed (%s)", rec.State(), why)
		return nil
	case migrationVerdictLeave:
		return nil
	default:
		return fmt.Errorf("unhandled verdict %d", verdict)
	}
}

// restartIfRebuiltDataGone is reconciliation's only reverse edge, gated on
// directory presence: a record's horizon delegates everything since to the
// mirror, so a missing owned directory means the rebuild never reached disk
// (or was reclaimed), and resuming would swap in an incomplete bucket.
func (r *migrationReconciler) restartIfRebuiltDataGone(subject MigrationSubject) (bool, error) {
	for _, dir := range migrationOwnedDirs(subject) {
		there, err := r.dirExists(dir)
		if err != nil {
			return false, err
		}
		if there {
			continue
		}

		// The mirror's own directory is missing too, so this rebuild must take
		// back everything delegated to it. Raising the cutoff (not clearing it)
		// still converges, since re-indexing what the mirror also covers is
		// per-key idempotent.
		restarted := subject
		restarted.IterationCutoff = migrationHorizonEverything

		r.logger.WithField("record", subject.Key.String()).Warnf(
			"rebuilt data at %q is gone; restarting the rebuild from the beginning and re-covering the horizon the mirror held", dir)
		return true, r.store.Put(NewMigrationRecordIterating(restarted, MigrationCheckpoint{}))
	}
	return false, nil
}

// reconcileMerged owns reconciliation's one external-fact edge. Staged data
// is complete; whether it should go live is a cluster fact the record
// deliberately does not hold.
func (r *migrationReconciler) reconcileMerged(ctx context.Context, rec MigrationRecordMerged,
	pass migrationPassRecords,
) error {
	if pass.someUnreadable {
		return nil
	}
	subject := rec.Subject()
	verdict, why := r.localVerdict(subject)

	switch verdict {
	case migrationVerdictLeave:
		return nil
	case migrationVerdictDiscard:
		// Safe: the flip record is written before the first flip, so a
		// Merged record proves the canonical bucket still has every write.
		return r.discard(ctx, pass.all, subject, why)
	case migrationVerdictCommit:
	default:
		return fmt.Errorf("unhandled verdict %d", verdict)
	}

	swapped, err := r.commitMerged(subject, why)
	if err != nil {
		return err
	}
	return r.reconcileSwapped(ctx, swapped, pass)
}

// commitMerged writes the flip decision and nothing else, before acting on it,
// so a crash between the two resumes from Swapped and re-runs idempotent
// directory work instead of re-deciding on inputs that may have changed.
func (r *migrationReconciler) commitMerged(subject MigrationSubject, why string) (MigrationRecordSwapped, error) {
	// No flip has happened yet, so every staged directory this promotes must
	// still exist, or promotion would mistake an absent one for an
	// already-run rename.
	for _, prop := range subject.Properties {
		staged := subject.StagedDirs[prop]
		there, err := r.dirExists(staged)
		if err != nil {
			return MigrationRecordSwapped{}, err
		}
		if !there {
			return MigrationRecordSwapped{}, fmt.Errorf(
				"refusing to commit the flip: property %q names no staged directory %q to promote", prop, staged)
		}
	}

	displaced := make(map[string]string, len(subject.Properties))
	for _, prop := range subject.Properties {
		displaced[prop] = subject.CanonicalDirs[prop]
	}
	swapped := NewMigrationRecordSwapped(subject, slices.Clone(subject.Properties), displaced)
	if err := r.store.Put(swapped); err != nil {
		return swapped, fmt.Errorf("record the flip decision: %w", err)
	}
	r.logger.WithField("record", subject.Key.String()).Infof("committing merged migration: %s", why)
	return swapped, nil
}

// ReconcileWithClusterTasks settles dispositions the load path withheld
// (task and effect were both invisible), using the leader's task list as an
// argument so a shard load never blocks on it; it reads in-memory records
// rather than reloading disk.
//
// Commit only records the decision (promotion waits for the next load).
// Discard runs immediately, pre-flip only, under the unit's seal. The
// reverse edge is left to a load, since it would reset live iteration.
func (r *migrationReconciler) ReconcileWithClusterTasks(ctx context.Context, tasks []*distributedtask.Task) {
	if len(r.store.Unreadable()) > 0 {
		return
	}
	records := r.store.Records()
	for _, rec := range records {
		if rec.PointerSwapped() {
			continue
		}
		subject := rec.Subject()
		verdict, why := r.clusterVerdict(subject, tasks)
		switch {
		case verdict == migrationVerdictDiscard:
			if err := r.discard(ctx, records, subject, why); err != nil {
				r.logger.WithField("record", subject.Key.String()).Errorf(
					"discard a migration the leader's task list settled: %v", err)
			}
		case verdict == migrationVerdictCommit && rec.State() == MigrationStateMerged:
			if _, err := r.commitMerged(subject, why); err != nil {
				r.logger.WithField("record", subject.Key.String()).Errorf(
					"commit a merged migration the leader's task list settled: %v", err)
				continue
			}
			r.logger.WithField("record", subject.Key.String()).Info(
				"the staged data is the data; the next shard load promotes it onto the canonical name")
		}
	}
}

// reconcileSwapped promotes. Every arm is decided by probing the handles the
// record already carries, never by parsing a directory name. Promotion
// removes the displaced and canonical directories before renaming, so it is
// as destructive as discard and is sealed the same way.
func (r *migrationReconciler) reconcileSwapped(ctx context.Context, rec MigrationRecordSwapped,
	pass migrationPassRecords,
) error {
	if pass.someUnreadable {
		return nil
	}
	return r.withSealedUnit(rec.Subject(), "its promotion", func() error {
		return r.promoteSealed(rec, pass.all)
	})
}

func (r *migrationReconciler) promoteSealed(rec MigrationRecordSwapped,
	all []MigrationRecord,
) error {
	subject := rec.Subject()
	settled := true

	for _, prop := range subject.Properties {
		// A superseded property is retired by supersession; probing it here
		// would read a successor's removal as a promotion that already ran.
		if migrationPropertySuperseded(all, subject, prop) {
			continue
		}

		staged, canonical := subject.StagedDirs[prop], subject.CanonicalDirs[prop]
		if staged == "" || canonical == "" {
			settled = false
			r.logger.WithField("record", subject.Key.String()).Errorf(
				"cannot promote property %q: the record names no staged or canonical directory for it", prop)
			continue
		}

		displaced, _ := rec.DisplacedDir(prop)
		updated, promoted, err := r.promoteProperty(rec, prop,
			promotionDirs{staged: staged, canonical: canonical, displaced: displaced})
		// The record carries every promotion intent written so far, so the
		// next property acts on the same one this property just widened.
		rec = updated
		if err != nil {
			settled = false
			r.logger.WithField("record", subject.Key.String()).Errorf("promote property %q: %v", prop, err)
			continue
		}
		if !promoted {
			settled = false
		}
	}

	if !settled {
		return nil
	}
	return r.store.Put(NewMigrationRecordPromoted(subject, rec.Flipped(), rec.displacedDirs))
}

// promoteProperty renames one property's staged directory onto its canonical
// name, and is idempotent across a crash in the middle of that rename.
//
// Which of the two it is doing is not readable from the directories. A shard
// load re-creates the canonical directory, empty, for every property in the
// schema, and a strategy pre-creates it when arming, so its presence is no
// evidence of anything — least of all that a rename put the migration's data
// there. Nor are the files inside it: the store renames, compacts and rewrites
// its own segments, so a name that was there when the rename ran is not there
// a restart later.
//
// So the record carries the answer instead. The rename is bracketed by two
// writes, a start and a finish, with nothing between them, and every later
// pass reads the finish rather than the disk. Only a process that stopped
// between those two statements leaves a start standing, and
// [migrationReconciler.settleInterruptedPromotion] resolves that in the very
// next pass — before any bucket on the shard opens — so it is never carried
// forward.
func (r *migrationReconciler) promoteProperty(rec MigrationRecordSwapped, prop string,
	dirs promotionDirs,
) (MigrationRecordSwapped, bool, error) {
	subject := rec.Subject()
	staged, canonical, displaced := dirs.staged, dirs.canonical, dirs.displaced
	switch rec.PromotionOf(prop) {
	case migrationPromotionFinished:
		return r.confirmPromotionSurvives(rec, prop, canonical)
	case migrationPromotionLost:
		r.logger.WithField("record", subject.Key.String()).Errorf(
			"property %q was promoted onto %q and that directory is gone; preserving the record and promoting nothing",
			prop, canonical)
		return rec, false, nil
	}
	stagedThere, err := r.dirExists(staged)
	if err != nil {
		return rec, false, err
	}
	if !stagedThere {
		return r.settleInterruptedPromotion(rec, prop, staged, canonical)
	}

	// Displaced directories have exactly one owner: the record that displaced
	// them. Usually the canonical path, but a predecessor that flipped and
	// never promoted still holds live data at a staged name instead.
	if displaced != "" && displaced != canonical {
		displacedThere, err := r.dirExists(displaced)
		if err != nil {
			return rec, false, err
		}
		if displacedThere {
			if err := os.RemoveAll(r.path(displaced)); err != nil {
				return rec, false, fmt.Errorf("remove displaced directory %q: %w", displaced, err)
			}
		}
	}
	canonicalThere, err := r.dirExists(canonical)
	if err != nil {
		return rec, false, err
	}
	if canonicalThere {
		if err := os.RemoveAll(r.path(canonical)); err != nil {
			return rec, false, fmt.Errorf("remove canonical directory %q the promotion replaces: %w", canonical, err)
		}
	}

	started := rec.WithPromotionAt(prop, migrationPromotionStarted)
	if err := r.store.Put(started); err != nil {
		return rec, false, fmt.Errorf(
			"record the promotion of property %q before renaming %q onto %q: %w", prop, staged, canonical, err)
	}
	rec = started

	if err := r.rename(staged, canonical); err != nil {
		return r.abandonPromotion(rec, prop, staged), false, err
	}

	finished := rec.WithPromotionAt(prop, migrationPromotionFinished)
	if err := r.store.Put(finished); err != nil {
		// The rename ran, so the property is promoted whatever this write did.
		// The next pass reaches the same answer from the start still standing.
		r.logger.WithField("record", subject.Key.String()).Errorf(
			"record the finished promotion of property %q: %v", prop, err)
		return rec, true, nil
	}
	return finished, true, nil
}

// confirmPromotionSurvives re-reads the one thing a record cannot carry: that
// the directory the rename produced is still there. Reconciliation runs before
// any bucket on the shard opens, so a directory an index DELETE removed is
// still absent here — no load has re-created it yet — and the answer is
// written down as lost the first time it is no.
func (r *migrationReconciler) confirmPromotionSurvives(rec MigrationRecordSwapped,
	prop, canonical string,
) (MigrationRecordSwapped, bool, error) {
	there, err := r.dirExists(canonical)
	if err != nil {
		return rec, false, err
	}
	if there {
		return rec, true, nil
	}
	r.logger.WithField("record", rec.Subject().Key.String()).Errorf(
		"property %q was promoted onto %q and that directory is gone, so the data this migration renamed onto it is gone; "+
			"preserving the record and promoting nothing", prop, canonical)
	lost := rec.WithPromotionAt(prop, migrationPromotionLost)
	if err := r.store.Put(lost); err != nil {
		r.logger.WithField("record", rec.Subject().Key.String()).Errorf(
			"record that the promoted directory of property %q is gone: %v", prop, err)
		return rec, false, nil
	}
	return lost, false, nil
}

// settleInterruptedPromotion decides a promotion that recorded its start and
// never recorded its finish, which is the one state no completed shard load
// can produce: the two writes bracket the rename with nothing between them.
//
// Reconciliation runs before any bucket on the shard opens, so the directory
// under the canonical name is still exactly what the rename left it — or the
// rename never ran, and the canonical directory the promotion removed before
// recording its start is still absent. Both answers are written down here, so
// no later pass asks the disk again and no load's empty re-creation can be
// read as this rename's output.
func (r *migrationReconciler) settleInterruptedPromotion(rec MigrationRecordSwapped,
	prop, staged, canonical string,
) (MigrationRecordSwapped, bool, error) {
	subject := rec.Subject()
	if rec.PromotionOf(prop) != migrationPromotionStarted {
		r.logger.WithField("record", subject.Key.String()).Errorf(
			"property %q lost its staged directory %q to something that is not its promotion, which never started; "+
				"preserving the record and promoting nothing", prop, staged)
		return rec, false, nil
	}
	canonicalThere, err := r.dirExists(canonical)
	if err != nil {
		return rec, false, err
	}
	if !canonicalThere {
		r.logger.WithField("record", subject.Key.String()).Errorf(
			"property %q has neither its staged directory %q nor its canonical directory %q, so its rename never ran; "+
				"preserving the record and promoting nothing", prop, staged, canonical)
		abandoned := rec.WithPromotionAbandoned(prop)
		if err := r.store.Put(abandoned); err != nil {
			r.logger.WithField("record", subject.Key.String()).Errorf(
				"take back the started promotion of property %q whose rename never ran: %v", prop, err)
			return rec, false, nil
		}
		return abandoned, false, nil
	}
	finished := rec.WithPromotionAt(prop, migrationPromotionFinished)
	if err := r.store.Put(finished); err != nil {
		return rec, false, fmt.Errorf("record the finished promotion of property %q: %w", prop, err)
	}
	return finished, true, nil
}

// abandonPromotion takes back the start a rename that returned instead of
// running recorded — but only while the staged directory it was to move is
// still there. [diskio.RenameAndSync] moves the directory first and syncs
// after, so an error can come from the sync of a rename that already ran;
// taking the start back there would leave every later pass reading a promoted
// canonical directory as one no rename of this property produced.
//
// A write that fails here is logged and left: the record is then merely less
// precise than it should be, which is not worth failing a shard load over,
// and the next pass re-runs the rename anyway because the staged directory is
// still there.
func (r *migrationReconciler) abandonPromotion(rec MigrationRecordSwapped, prop, staged string) MigrationRecordSwapped {
	stagedThere, err := r.dirExists(staged)
	if err != nil || !stagedThere {
		return rec
	}
	abandoned := rec.WithPromotionAbandoned(prop)
	if err := r.store.Put(abandoned); err != nil {
		r.logger.WithField("record", rec.Subject().Key.String()).Errorf(
			"take back the started promotion of property %q after its rename failed: %v", prop, err)
		return rec
	}
	return abandoned
}

// promotionDirs are the three directories a promotion of one property acts on.
// They are one value rather than three adjacent strings because two of them
// reach os.RemoveAll and the third holds the only copy, so swapping any two at
// a call site deletes live data.
type promotionDirs struct {
	staged    string
	canonical string
	displaced string
}

// reconcilePromoted is the closure sweep. The record outlives its data: its
// owned-dirs list is what attributes a leftover from a partly failed
// retirement step back to this record.
func (r *migrationReconciler) reconcilePromoted(ctx context.Context, rec MigrationRecordPromoted,
	pass migrationPassRecords,
) error {
	if pass.someUnreadable {
		return nil
	}
	return r.withSealedUnit(rec.Subject(), "its closure sweep", func() error {
		return r.reconcilePromotedSealed(rec, pass.all)
	})
}

func (r *migrationReconciler) reconcilePromotedSealed(rec MigrationRecordPromoted,
	all []MigrationRecord,
) error {
	subject := rec.Subject()

	// The record is durable before the rename it records reaches disk;
	// repairing first keeps the reclaim from deleting the only copy.
	if err := r.repromoteWhatTheRecordOutran(all, subject); err != nil {
		return err
	}

	remaining := r.reclaimOwnedDirs(all, subject)
	if len(remaining) > 0 {
		r.logger.WithField("record", subject.Key.String()).Warnf(
			"%d directory/directories of a promoted migration could not be reclaimed yet", len(remaining))
		return nil
	}

	class := r.deps.Class()
	if class == nil {
		r.logger.WithField("record", subject.Key.String()).Warn(
			"a promoted migration's collection is not in the schema, so nothing here can confirm its effect; keeping the record")
		return nil
	}
	// Sweeping while the effect is pending would delete the answer a promoted
	// record exists to give; an unobservable effect is never pending.
	if effect, missing := migrationEffectStatus(class, subject); effect == migrationEffectPending {
		// The one state that persists across loads: promoted here, but the
		// schema change never landed. No load will fix that.
		r.logger.WithField("record", subject.Key.String()).WithField("properties", missing).Warn(
			"this shard promoted a migration whose effect is not in the schema; keeping the record, " +
				"which is what answers for the property until the effect lands")
		return nil
	}
	r.removeTrackerDir(subject)
	return r.store.Remove(subject.Key)
}

// repromoteWhatTheRecordOutran re-runs a promotion the record already claims.
// If a surviving successor claims staged as displaced, the property was
// superseded, not promoted, and staged is that successor's only copy.
//
// A property holding a directory under both names stops the sweep. Which of
// the two holds the promoted data is not readable from here — a shard load
// re-creates the canonical one, empty, for every property in the schema — and
// the sweep this returns to reclaims staged directories, so reading the
// canonical name as the answer would delete the only copy.
//
// Stopping the sweep is not stopping the loop: every later property still gets
// its repair, because a property whose data is still at its staged name serves
// an empty canonical bucket until that rename runs, and it would otherwise wait
// on a sibling no load can resolve.
func (r *migrationReconciler) repromoteWhatTheRecordOutran(all []MigrationRecord, subject MigrationSubject) error {
	var ambiguous []string
	for _, prop := range subject.Properties {
		staged, canonical := subject.StagedDirs[prop], subject.CanonicalDirs[prop]
		if staged == "" || canonical == "" {
			continue
		}
		if migrationDirClaimedAsDisplaced(all, subject, staged) {
			continue
		}
		stagedThere, err := r.dirExists(staged)
		if err != nil {
			return err
		}
		if !stagedThere {
			continue
		}
		canonicalThere, err := r.dirExists(canonical)
		if err != nil {
			return err
		}
		if canonicalThere {
			r.logger.WithField("record", subject.Key.String()).Errorf(
				"property %q is recorded as promoted but holds a directory at both its staged name %q and its canonical name %q; "+
					"nothing here can tell which one the promotion produced, so the record and both directories are preserved",
				prop, staged, canonical)
			ambiguous = append(ambiguous, prop)
			continue
		}

		r.logger.WithField("record", subject.Key.String()).Errorf(
			"property %q is recorded as promoted but its data is still at the staged name %q "+
				"and %q does not exist; re-running the promotion", prop, staged, canonical)
		if err := r.rename(staged, canonical); err != nil {
			return err
		}
	}
	if len(ambiguous) > 0 {
		return fmt.Errorf(
			"%d property/properties hold a directory at both their staged and canonical names: %s; "+
				"nothing here can tell which one the promotion produced, so the record and both directories are preserved",
			len(ambiguous), strings.Join(ambiguous, ", "))
	}
	return nil
}

// localVerdict is what this node can decide alone: a task in its own applied
// map, or the effect in its own applied schema — both positive evidence that
// can't be undone. Two absences at once can't be told apart from "not
// applied yet", so it withholds instead of guessing; see
// [migrationReconciler.ReconcileWithClusterTasks] for how those get settled.
func (r *migrationReconciler) localVerdict(subject MigrationSubject) (migrationVerdict, string) {
	if r.deps.LocalTasks == nil {
		return migrationVerdictLeave, "this node's task map cannot be read yet"
	}
	tasks, readable := r.deps.LocalTasks()
	if !readable {
		return migrationVerdictLeave, "this node's task map cannot be read yet"
	}
	return r.verdictFrom(subject, tasks, taskListMayLag)
}

// clusterVerdict decides what the load path withheld. It checks this node's
// own applied map first (positive evidence no snapshot age can spoil, since a
// unit only starts from that map), then the leader's list, which is fetched
// once per walk and can go stale. A map that can't be read yet withholds
// outright — falling through would read an absent task as gone.
func (r *migrationReconciler) clusterVerdict(subject MigrationSubject, tasks []*distributedtask.Task) (migrationVerdict, string) {
	if r.deps.LocalTasks == nil {
		return migrationVerdictLeave, "this node's task map cannot be read yet"
	}
	local, readable := r.deps.LocalTasks()
	if !readable {
		return migrationVerdictLeave, "this node's task map cannot be read yet"
	}
	if task := findMigrationTask(subject, local); task != nil {
		return migrationVerdictForTask(task)
	}
	return r.verdictFrom(subject, tasks, taskListIsComplete)
}

// sealUnit holds this migration's unit for the length of a teardown, or
// refuses because a worker is still running here. No registry installed
// always succeeds.
func (r *migrationReconciler) sealUnit(subject MigrationSubject) (func(), bool) {
	if r.deps.SealUnit == nil {
		return func() {}, true
	}
	return r.deps.SealUnit(
		distributedtask.TaskDescriptor{ID: subject.TaskID, Version: subject.Key.TaskVersion},
		subject.Key.UnitID)
}

// withSealedUnit runs a teardown under the unit's seal, declining (and
// retrying next pass) if a worker is live — every arm here writes through
// pointers taken before the worker's phase began. The per-unit orphan audit
// takes the same seal separately.
func (r *migrationReconciler) withSealedUnit(subject MigrationSubject, what string, run func() error) error {
	release, sealed := r.sealUnit(subject)
	if !sealed {
		r.logger.WithField("record", subject.Key.String()).Infof(
			"a local unit of this migration is still running, so %s waits for the next pass", what)
		return nil
	}
	defer release()
	return run()
}

// taskListCompleteness says what a task's absence from a list means: with a
// list this node built alone it can only mean "not seen yet", while the
// leader's list is the cluster's, so absence there means gone.
type taskListCompleteness bool

const (
	taskListMayLag     taskListCompleteness = false
	taskListIsComplete taskListCompleteness = true
)

// verdictFrom consults the two external facts, in an order that skips the
// second whenever the first is conclusive.
func (r *migrationReconciler) verdictFrom(subject MigrationSubject, tasks []*distributedtask.Task,
	completeness taskListCompleteness,
) (migrationVerdict, string) {
	if task := findMigrationTask(subject, tasks); task != nil {
		return migrationVerdictForTask(task)
	}

	// A task absent from a complete map is one no unit will resume, whether it
	// went terminal or its class/task-map entry was removed outright (delete,
	// re-submit, snapshot restore). Either way, the schema effect below
	// decides what the run left behind.
	class := r.deps.Class()
	if class == nil {
		return migrationVerdictLeave, "collection is not in the locally applied schema"
	}
	if migrationEffectConfirmsCommit(class, subject) {
		// Needs no complete list: schema and task changes travel one replicated
		// log, so if this node applied the effect it applied the task-creating
		// entry too — an absent task here means removed, not lagging. This
		// doesn't prove THIS shard swapped (the rangeable family sets its flag
		// from the first shard's swap, while the task runs elsewhere too), but
		// reading an absent/unobservable effect as a commit would diverge
		// replicas that saw the same cancel differently.
		return migrationVerdictCommit, "owning task is gone and the schema shows its effect"
	}
	if completeness == taskListMayLag {
		return migrationVerdictLeave, "this node can see neither the owning task nor its effect, which a node still applying its log cannot tell from a task that is gone"
	}
	return migrationVerdictDiscard, "owning task is gone and the schema does not show its effect"
}

// migrationVerdictForTask reads a task's status as positive evidence wherever
// found, by either list: terminal stays terminal, and an active task resumes
// the migration itself.
func migrationVerdictForTask(task *distributedtask.Task) (migrationVerdict, string) {
	switch task.Status {
	case distributedtask.TaskStatusFinished:
		return migrationVerdictCommit, "owning task finished"
	case distributedtask.TaskStatusCancelled, distributedtask.TaskStatusFailed:
		return migrationVerdictDiscard, fmt.Sprintf("owning task %s", task.Status)
	default:
		// Active, or a status this build does not recognize.
		return migrationVerdictLeave, fmt.Sprintf("owning task is %s", task.Status)
	}
}

// findMigrationTask matches ID and version together: a re-submitted task ID
// is a different run whose outcome says nothing about this record's.
func findMigrationTask(subject MigrationSubject, tasks []*distributedtask.Task) *distributedtask.Task {
	for _, task := range tasks {
		if task != nil && task.ID == subject.TaskID && task.Version == subject.Key.TaskVersion {
			return task
		}
	}
	return nil
}

// discard is the cancel edge: seals the unit first (declining and retrying
// if a worker is live) before removing directories it might still write
// through. Can't block like other teardown paths do — this walk holds each
// index's drop lock, and waiting would stall the RAFT apply loop.
func (r *migrationReconciler) discard(ctx context.Context, all []MigrationRecord,
	subject MigrationSubject, why string,
) error {
	return r.withSealedUnit(subject, "the discard of its staged data", func() error {
		return r.discardSealed(ctx, all, subject, why)
	})
}

func (r *migrationReconciler) discardSealed(ctx context.Context, all []MigrationRecord,
	subject MigrationSubject, why string,
) error {
	r.logger.WithField("record", subject.Key.String()).Infof("discarding staged migration data: %s", why)

	ec := errorcompounder.New()
	for _, prop := range subject.Properties {
		ec.Add(r.disarmAndClose(ctx, subject.Key, prop))
	}
	if err := ec.ToError(); err != nil {
		return err
	}
	if remaining := r.reclaimOwnedDirs(all, subject); len(remaining) > 0 {
		return fmt.Errorf("%d owned directory/directories survived the discard", len(remaining))
	}
	r.removeTrackerDir(subject)
	return r.store.Remove(subject.Key)
}

// disarmAndClose reports shutdown failure rather than logging it away: every
// caller removes the property's directory next, and an open bucket's
// directory leaves mmaps, in-flight compactions, and a registry entry behind.
func (r *migrationReconciler) disarmAndClose(ctx context.Context, key MigrationRecordKey, prop string) error {
	if r.deps.Mirror != nil {
		r.deps.Mirror.DisarmMigrationMirror(key, prop)
	}
	if r.deps.Buckets == nil {
		return nil
	}
	if err := r.deps.Buckets.ShutdownStagedBuckets(ctx, key, prop); err != nil {
		return fmt.Errorf("shut down staged buckets of %s for property %q: %w", key.String(), prop, err)
	}
	return nil
}

// removeTrackerDir removes the migration's own directory under .migrations,
// holding the recovery payload. Deliberately not an owned dir: those are
// bucket directories the Iterated probe reads as proof the rebuild reached
// disk.
func (r *migrationReconciler) removeTrackerDir(subject MigrationSubject) {
	if subject.TrackerDir == "" {
		return
	}
	path := filepath.Join(r.lsmPath, migrationsDir, subject.TrackerDir)
	if err := os.RemoveAll(path); err != nil {
		r.logger.WithField("dir", path).Errorf("remove migration directory: %v", err)
	}
}

// reclaimOwnedDirs removes every directory the record created and reports the
// ones that survived; the canonical directory is never among them. A
// directory a later-versioned record claims as displaced belongs to that
// claimer, not here — removing it would take the successor's only copy.
func (r *migrationReconciler) reclaimOwnedDirs(all []MigrationRecord, subject MigrationSubject) []string {
	var remaining []string
	for _, dir := range migrationOwnedDirs(subject) {
		if migrationDirClaimedAsDisplaced(all, subject, dir) {
			continue
		}
		if !r.mayReclaim(all, subject, dir) {
			continue
		}
		if err := os.RemoveAll(r.path(dir)); err != nil {
			r.logger.WithField("dir", dir).Errorf("remove migration directory: %v", err)
		}
		// A directory we cannot stat counts as surviving: the caller aborts on
		// a non-empty list, which is the safe reading of "could not tell".
		there, err := r.dirExists(dir)
		if err != nil {
			r.logger.WithField("dir", dir).Errorf("confirm migration directory is gone: %v", err)
		}
		if there || err != nil {
			remaining = append(remaining, dir)
		}
	}
	return remaining
}

// mayReclaim refuses to hand another record's canonical directory to
// os.RemoveAll. Skip and log rather than refuse the load: refusing freezes
// every migration on the shard, while skipping leaks one directory and keeps
// the data, which is the same trade [migrationDirClaimedAsDisplaced] already
// makes.
func (r *migrationReconciler) mayReclaim(all []MigrationRecord, subject MigrationSubject, dir string) bool {
	holder, taken := migrationDirIsAnotherRecordsCanonical(all, subject, dir)
	if !taken {
		return true
	}
	r.logger.WithField("record", subject.Key.String()).WithField("dir", dir).Errorf(
		"refusing to reclaim %q: record %s serves a property from it; leaving it for that record to answer for",
		dir, holder)
	return false
}

// migrationOwnedDirs lists what the migration created and a sweep may
// reclaim. Canonical and displaced directories are deliberately absent: the
// canonical name predates the migration and, after promotion, holds live data.
func migrationOwnedDirs(subject MigrationSubject) []string {
	dirs := make([]string, 0, len(subject.StagedDirs)+len(subject.SidecarDirs))
	for _, prop := range subject.Properties {
		if dir := subject.StagedDirs[prop]; dir != "" {
			dirs = append(dirs, dir)
		}
		if dir := subject.SidecarDirs[prop]; dir != "" {
			dirs = append(dirs, dir)
		}
	}
	return dirs
}

func (r *migrationReconciler) path(dir string) string { return filepath.Join(r.lsmPath, dir) }

// migrationDirExists separates "not there" from "could not tell". Destructive
// arms guard on absence, so any stat failure other than ENOENT must stop the
// decision — otherwise the promotion probe would take "cannot see it" as
// proof the rename already happened.
func migrationDirExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("stat migration directory %q: %w", path, err)
	}
	return info.IsDir(), nil
}

func (r *migrationReconciler) dirExists(dir string) (bool, error) {
	if dir == "" {
		return false, nil
	}
	return migrationDirExists(r.path(dir))
}

// rename is the reconciler's only promoting filesystem step. The Promoted
// record written on its strength is durable, so the rename must be durable
// too, or a crash leaves a record naming a path the filesystem never created.
func (r *migrationReconciler) rename(from, to string) error {
	if err := diskio.RenameAndSync(r.path(from), r.path(to)); err != nil {
		return fmt.Errorf("promote %q to %q: %w", from, to, err)
	}
	return nil
}
