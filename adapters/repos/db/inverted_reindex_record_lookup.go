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
	"os"
	"path/filepath"
	"slices"

	"github.com/sirupsen/logrus"
)

// migrationRecordsAt reads one shard's records straight from disk, for sweeps
// and gates deciding about a cold tenant. someRecordsUnreadable scopes
// withholding to the whole shard; recordSetUnreadable is the stronger fault
// (nothing could be read), so a caller that would report clean must fail open.
func migrationRecordsAt(lsmPath string, logger logrus.FieldLogger) (records []MigrationRecord, someRecordsUnreadable, recordSetUnreadable bool) {
	store := NewMigrationRecordStore(lsmPath, logger)
	if err := store.Load(); err != nil {
		logger.WithField("path", store.Dir()).Errorf("read migration records: %v", err)
		return nil, true, true
	}
	// One line per read, even on the healthy path — a per-tuple read
	// regression would otherwise be invisible until startup latency shows it.
	logger.WithField("path", store.Dir()).WithField("records", len(store.Records())).
		Debug("read migration records")
	return store.Records(), len(store.Unreadable()) > 0, false
}

// migrationPreservedState names what a sweep of one shard must leave alone: a
// record with complete staged data holds a copy nobody else holds, and
// removing a directory it names loses that copy silently.
type migrationPreservedState struct {
	// records is the whole understood set, not just the preserved part:
	// sweeps also ask an extant record which properties it belongs to.
	records []MigrationRecord
	// buckets maps each preserved bucket directory to whether a load would
	// still do disk work for it, the same question trackers answers.
	buckets map[string]bool
	// trackers maps each preserved migration directory to whether a load
	// would still do disk work for it.
	trackers map[string]bool
	// settled is what the last reconciliation pass on this shard left exactly
	// as it found it. See [migrationSettledNoteFile]: a load would reconcile
	// these to the same answer, so it would reclaim nothing on their account.
	settled map[string]bool

	// withholdEverything preserves the whole shard: a record this build
	// cannot read, or a tracker no record names whose payload could not be
	// read, names directories nothing else accounts for.
	withholdEverything bool
	// recordSetUnreadable means no record here could be read at all, so no
	// caller may report the shard clean. It implies withholdEverything.
	recordSetUnreadable bool
	// migrationsDirUnlistable is the same fault one level up: the tracker
	// directory itself could not be enumerated. It sets withholdEverything
	// too, and is carried separately because a shard that was never read has
	// to report differently from one that was read and withheld.
	migrationsDirUnlistable bool
	// withheld names why withholdEverything is set. Meaningless while it is
	// not.
	withheld migrationWithholdReasons
}

// migrationWithholdReasons names the populations that withhold every removal
// on one shard. They are kept apart because a caller deciding whether to
// hydrate has to tell them apart: hydrating runs
// [FinalizeCompletedMigrations], which removes a completed tracker whether or
// not it managed to promote the staged data behind it, so for one population
// the load is the repair and for another it is the loss.
//
// Every producer of migrationPreservedState.withholdEverything sets one, so a
// caller can be exhaustive over them.
type migrationWithholdReasons struct {
	// undecided: whether a completed migration is here could not be read at
	// all — an unstattable completion marker, or a .migrations that could not
	// be listed. A caller that would report the shard clean guesses instead.
	undecided bool
	// completedActionable: a completed tracker a load's finalize promotes —
	// swapped.mig beside tidied.mig (or merged.mig alone, whose missing
	// sentinels finalize's recovery path writes itself), plus a non-empty
	// properties.mig to promote from.
	completedActionable bool
	// completedStuck: a completed tracker finalize cannot promote — no
	// properties.mig, tidied.mig without the swapped.mig that precedes it, or
	// a dir name no strategy claims. Finalize removes the tracker all the
	// same, and the sweep that follows then finds nothing preserving the
	// staged directory and frees the property's only copy.
	completedStuck bool
	// unreadableRecord: a migration record this build cannot read. It may name
	// any directory here, so nothing is removable until it can be read, and a
	// load reclaims nothing on its account.
	unreadableRecord bool
}

// migrationPreservedStateAt is the shard-wide preserve state: every tracker on
// the shard answers, whatever property a caller goes on to ask about. Callers
// sweeping one property want [migrationPreservedStateFor], which costs less.
func migrationPreservedStateAt(lsmPath string, logger logrus.FieldLogger) migrationPreservedState {
	return migrationPreservedStateFor(lsmPath, "", nil, logger)
}

// migrationPreservedStateFor is the only way to build a populated
// migrationPreservedState, so no sweep can learn about records but not about
// the trackers no record names. The zero value preserves nothing.
//
// An empty propName reads every tracker's payload; a non-empty one narrows
// the parse (never the listing) to trackers that could hold something of
// that property's ([migrationTrackerMayOwnProperty]) — parsing the rest costs
// megabytes each inside the RAFT apply a property DELETE holds cluster-wide.
//
// A skipped tracker's payload is never parsed, so it no longer withholds:
// sound because withholding exists only to stop a sweep removing something a
// tracker owns, and a name proving the tracker stages nothing of this
// property owns nothing this sweep can name. The record set itself is
// unaffected, since it comes from the listing.
//
// props memoizes the payloads across the index types of one property's
// sweep; a nil memo reads every payload again and counts nothing.
func migrationPreservedStateFor(lsmPath, propName string, props *taskPropsCache,
	logger logrus.FieldLogger,
) migrationPreservedState {
	records, someRecordsUnreadable, recordSetUnreadable := migrationRecordsAt(lsmPath, logger)
	state := migrationPreservedStateFromRecords(records, someRecordsUnreadable, recordSetUnreadable)
	state.settled = migrationReadSettledNote(lsmPath)
	legacyTrackers, listed, listErr := migrationLegacyMarkerTrackersAt(lsmPath, records, propName, props)
	if someRecordsUnreadable && listed {
		// The whole shard is already withheld, so the tracker names below
		// change no answer. The listing above still runs, because an
		// unlistable directory is a different fault.
		return state
	}
	if !listed {
		// Nothing here could be enumerated, so the preserve set is missing
		// names sweeps could read as permission to delete. Debug here since a
		// DELETE asks this per (property, index type) per shard inside the
		// RAFT apply; the shard load warns once.
		logger.WithField("path", filepath.Join(lsmPath, migrationsDir)).
			Debugf("the migration directory could not be listed; withholding every removal on this shard: %v", listErr)
		state.withholdEverything = true
		state.migrationsDirUnlistable = true
		state.withheld.undecided = true
		return state
	}
	for _, legacy := range legacyTrackers {
		if legacy.unreadable {
			// Its payload names the directories holding this marker's data;
			// preserving only the tracker would strand them from the reclaimers.
			state.withholdEverything = true
			switch {
			case legacy.marker == "":
				state.withheld.undecided = true
			case legacy.finalizable:
				state.withheld.completedActionable = true
			default:
				state.withheld.completedStuck = true
			}
			continue
		}
		// True: a load's finalize reclaims the tracker directory whether or
		// not its sidecars survive, so the tracker asks for the load on its
		// own account. Its sidecars ask through state.buckets below, but a
		// property-index DELETE can remove them first, and a tracker that
		// only asked through them would then be skipped forever.
		state.trackers[legacy.dirName] = true
		for _, dir := range legacy.sidecars {
			// A marker-era tracker has no record to have written a promotion
			// off, so the load's finalize really does act on these.
			state.buckets[dir] = true
		}
	}
	return state
}

func migrationPreservedStateFromRecords(records []MigrationRecord, someRecordsUnreadable, recordSetUnreadable bool) migrationPreservedState {
	state := migrationPreservedState{
		records:             records,
		buckets:             map[string]bool{},
		trackers:            map[string]bool{},
		withholdEverything:  someRecordsUnreadable,
		recordSetUnreadable: recordSetUnreadable,
		withheld:            migrationWithholdReasons{unreadableRecord: someRecordsUnreadable},
	}
	for _, rec := range records {
		if !rec.StagedDataComplete() {
			continue
		}
		subject := rec.Subject()
		anyCanAct := false
		for _, prop := range subject.Properties {
			// Per property, because promotion is: a load promotes every
			// property whose promotion is not lost and skips the ones that
			// are. Folding the properties together claims a load would act on
			// a lost property's directories, or that it would act on none
			// because a sibling's is lost.
			canAct := migrationPropertyLoadCanStillAct(rec, prop)
			anyCanAct = anyCanAct || canAct
			for _, dir := range migrationOwnCopyDirs(subject, prop) {
				// Union, never overwrite: where two records name one
				// directory, a later "no load claim" must not erase an
				// earlier record's claim.
				state.buckets[dir] = state.buckets[dir] || canAct
			}
		}
		// The directory this record's flip displaced is preserved on the
		// displacer's account: until the claim lapses it can be the
		// property's only copy, and the preserve side answers wider than any
		// reclaimer — [migrationOwnCopyDirs] stays reclaim-only. It never
		// justifies a load by itself.
		if displacer, ok := rec.(migrationDisplacer); ok {
			for _, prop := range subject.Properties {
				dir, claimed := displacer.DisplacedDir(prop)
				if !claimed || dir == "" {
					continue
				}
				if _, seen := state.buckets[dir]; !seen {
					state.buckets[dir] = false
				}
			}
		}
		if subject.TrackerDir != "" {
			// The tracker directory goes when the whole record retires, so one
			// property that can still act keeps it claimed. A promoted
			// record's own directory waits on the schema effect, which no load
			// can force, so it never justifies hydration alone; its owned
			// directories still do, counted from buckets above.
			state.trackers[subject.TrackerDir] = rec.State() != MigrationStatePromoted && anyCanAct
		}
	}
	return state
}

func (s migrationPreservedState) preservesBucket(dir string) bool {
	if s.withholdEverything {
		return true
	}
	_, ok := s.buckets[dir]
	return ok
}

func (s migrationPreservedState) preservesTracker(dir string) bool {
	if s.withholdEverything {
		return true
	}
	_, ok := s.trackers[dir]
	return ok
}

// trackerNeedsLoad reports whether hydrating this shard would reclaim dir.
func (s migrationPreservedState) trackerNeedsLoad(dir string) bool {
	return s.trackers[dir] && !s.settled[dir]
}

// bucketNeedsLoad is [migrationPreservedState.trackerNeedsLoad] for a bucket
// directory: preserved, and a load would still act on it.
func (s migrationPreservedState) bucketNeedsLoad(dir string) bool {
	return s.buckets[dir] && !s.settled[dir]
}

// migrationPropertyLoadCanStillAct reports whether a shard load could change
// what this record holds for one property. A lost promotion has no exit: the
// mark is written once a promoted directory is found gone and nothing clears
// it, so that property can never be promoted and a load reclaims nothing on
// its account — asked per property because promoteSealed skips a lost one and
// promotes the rest.
//
// Preservation is unaffected either way; only the claim that hydrating would
// reclaim something changes, which is what would otherwise drag a cold tenant
// into a load on every schema operation against its collection, forever. The
// record remains the exit's own witness: a resubmit supersedes it, and
// retirement removes it from the record set before this is ever asked.
func migrationPropertyLoadCanStillAct(rec MigrationRecord, prop string) bool {
	sw, ok := rec.(MigrationRecordSwapped)
	if !ok {
		return true
	}
	return sw.PromotionOf(prop) != migrationPromotionLost
}

// bucketsOf names the preserved sidecars of one main bucket, sorted, for a
// log line that would otherwise report every migration on the shard.
func (s migrationPreservedState) bucketsOf(mainBucketName string) []string {
	out := make([]string, 0, len(s.buckets))
	for dir := range s.buckets {
		if isSidecarDirOf(dir, mainBucketName) {
			out = append(out, dir)
		}
	}
	slices.Sort(out)
	return out
}

// migrationRecordForTracker finds the record owning one tracker directory,
// for a reader holding just the directory name.
func migrationRecordForTracker(records []MigrationRecord, trackerDir string) (MigrationRecord, bool) {
	for _, rec := range records {
		if rec.Subject().TrackerDir == trackerDir {
			return rec, true
		}
	}
	return nil, false
}

// migrationLegacyMarkerTracker is a tracker directory no record names, whose
// completion marker names the property's live data at the staged name. Every
// completed migration leaves one today, since no task writes a record yet;
// so did every pre-migration-records release.
type migrationLegacyMarkerTracker struct {
	dirName string
	marker  string
	prefix  string
	gen     int
	// unreadable means the property list could not be learned — the payload
	// was unreadable, absent, or named nothing — so props and sidecars are
	// empty because nothing could be learned, not because there is none.
	unreadable bool
	props      []string
	sidecars   []string
	// finalizable is whether a shard load would promote this tracker's staged
	// data rather than remove the tracker having promoted nothing. It restates
	// [finalizeMigrationDir]'s own precondition: swapped.mig beside tidied.mig
	// — or merged.mig alone, whose missing sentinels finalize's recovery path
	// writes itself — plus a non-empty properties.mig to promote from and a
	// dir name [migrationSuffixes] claims. Missing any of those, finalize
	// returns, its caller removes the tracker anyway, and nothing on disk then
	// records that the staged directory holds the property's only copy. Asked
	// only of a tracker nothing could be learned about (unreadable).
	finalizable bool
}

// migrationLegacyMarkerTrackersAt finds the completed trackers no record
// names on one shard. listed=false is distinct from finding none: a fault
// hiding every one of them (fd exhaustion on a many-tenant node) would
// otherwise free a property's only copy.
func migrationLegacyMarkerTrackersAt(lsmPath string, records []MigrationRecord,
	propName string, props *taskPropsCache,
) (trackers []migrationLegacyMarkerTracker, listed bool, listErr error) {
	migsDir := filepath.Join(lsmPath, migrationsDir)
	entries, err := os.ReadDir(migsDir)
	if err != nil {
		// Absent is the ordinary case — most shards never ran a migration —
		// and it really does mean there is nothing marker-era here. Anything
		// else is carried out so the one operator-facing line can name it.
		if os.IsNotExist(err) {
			return nil, true, nil
		}
		return nil, false, err
	}
	var out []migrationLegacyMarkerTracker
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dirName := entry.Name()
		prefix, gen, ok := parseMigrationDirName(dirName)
		if !ok {
			continue
		}
		if _, named := migrationRecordForTracker(records, dirName); named {
			continue
		}
		if propName != "" && !migrationTrackerMayOwnProperty(dirName, propName) {
			// Its own name proves it stages nothing of this property's, so no
			// sweep of this property removes anything it owns and its payload
			// need not be parsed.
			continue
		}
		marker, found, unreadable := migrationCompletionMarker(filepath.Join(migsDir, dirName))
		if unreadable {
			// Whether this tracker completed could not be read, and a completed
			// one holds the property's only copy. Carry it as a tracker nothing
			// could be learned about, which is what withholds the shard.
			out = append(out, migrationLegacyMarkerTracker{
				dirName: dirName, prefix: prefix, gen: gen, unreadable: true,
			})
			continue
		}
		if !found {
			continue
		}
		migDir := filepath.Join(migsDir, dirName)
		answer := props.lookup(migDir)
		// The list finalize itself promotes from; unverified, and an
		// unreadable one reads as empty, exactly as finalize treats it.
		finalizeProps, _ := readMigrationProps(migDir)
		out = append(out, migrationLegacyMarkerTracker{
			dirName: dirName,
			marker:  marker,
			prefix:  prefix,
			gen:     gen,
			// The marker says this tracker's staged data became the property's
			// data, and the directories holding it are composed from the
			// property list. A list that could not be learned is therefore the
			// same fault as one that could not be read, and a tracker written
			// before payload.mig shipped (v1.37.x wrote tidied.mig without one)
			// reaches here with nothing to compose from. Withhold rather than
			// preserve nothing.
			unreadable: answer.unreadable || len(answer.props) == 0,
			props:      append([]string(nil), answer.props...),
			sidecars:   migrationPreservedSidecarDirsFor(dirName, prefix, gen, answer.props),
			// [finalizeMigrationDir]'s own precondition, asked here so a
			// caller can tell a load that would promote this tracker's data
			// from one that would remove the tracker and strand it. The
			// marker is merged.mig only while tidied.mig is absent — the
			// population finalize's recovery path completes and promotes.
			finalizable: (marker == "merged.mig" ||
				(marker == "tidied.mig" && fileExists(filepath.Join(migDir, "swapped.mig")))) &&
				len(finalizeProps) > 0 && migrationSuffixes(dirName) != nil,
		})
	}
	return out, true, nil
}

// servesEmpty reports properties whose data is still under this tracker's
// staged name while the canonical directory is gone: the schema flip already
// committed cluster-wide, and nothing has renamed the staged directory back.
func (t migrationLegacyMarkerTracker) servesEmpty(lsmPath string) []string {
	suffixes := migrationSuffixes(t.dirName)
	if suffixes == nil {
		return nil
	}
	genTail := genSuffix(t.gen)
	var out []string
	for _, prop := range t.props {
		canonical := suffixes.sourceBucketName(prop)
		if fileExists(filepath.Join(lsmPath, canonical)) {
			continue
		}
		if !fileExists(filepath.Join(lsmPath, canonical+suffixes.ingestSuffix+genTail)) {
			continue
		}
		out = append(out, prop)
	}
	return out
}
