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
	// One line per read, even on the healthy path. One read per shard is the
	// cost every caller is written for, and a probe that reads once per tuple
	// or once per property instead is otherwise invisible until it shows up as
	// startup latency on a many-tenant node.
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
// An empty propName reads every tracker's payload. A propName narrows the
// parse — never the listing — to the trackers that could hold something of
// that property's ([migrationTrackerMayOwnProperty]): the rest are settled by
// their own directory name, and parsing them costs megabytes each inside the
// RAFT apply that a property DELETE holds cluster-wide.
//
// The record set is unaffected, since it comes from the listing. The shard-wide
// withhold is: a skipped tracker's marker is never stat'd and its payload never
// parsed, so an unreadable one no longer withholds. That is sound because the
// withhold exists to stop a sweep removing something a tracker owns, and a
// tracker whose own name proves it stages nothing of this property's owns
// nothing this sweep can name.
//
// props memoizes the payloads across the index types of one property's sweep;
// a nil memo reads every payload again and counts nothing.
func migrationPreservedStateFor(lsmPath, propName string, props *taskPropsCache,
	logger logrus.FieldLogger,
) migrationPreservedState {
	records, someRecordsUnreadable, recordSetUnreadable := migrationRecordsAt(lsmPath, logger)
	state := migrationPreservedStateFromRecords(records, someRecordsUnreadable, recordSetUnreadable)
	state.settled = migrationReadSettledNote(lsmPath)
	legacyTrackers, listed := migrationLegacyMarkerTrackersAt(lsmPath, propName, props)
	if someRecordsUnreadable && listed {
		// The shard is already preserved whole, so what the trackers say
		// changes nothing. The walk above still had to run: an unlistable
		// directory is the one answer this arm may not swallow.
		return state
	}
	if !listed {
		// Nothing here could be enumerated, so the preserve set is missing
		// names sweeps could read as permission to delete. Debug here since a
		// DELETE asks this per (property, index type) per shard inside the
		// RAFT apply; the shard load warns once.
		logger.WithField("path", filepath.Join(lsmPath, migrationsDir)).
			Debug("the migration directory could not be listed; withholding every removal on this shard")
		state.withholdEverything = true
		state.migrationsDirUnlistable = true
		return state
	}
	for _, legacy := range legacyTrackers {
		if legacy.unreadable {
			// Its payload names the directories holding this marker's data;
			// preserving only the tracker would strand them from the reclaimers.
			state.withholdEverything = true
			continue
		}
		// false: the tracker's own sidecars go into state.buckets below, and
		// the gate already reports those as reclaimable-by-load, so nothing
		// more is gained by asking for the load on the tracker's account. A
		// record that already asked for one keeps its answer.
		if _, named := state.trackers[legacy.dirName]; !named {
			state.trackers[legacy.dirName] = false
		}
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
			if dir := subject.StagedDirs[prop]; dir != "" {
				state.buckets[dir] = canAct
			}
			if dir := subject.SidecarDirs[prop]; dir != "" {
				state.buckets[dir] = canAct
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

// mirrorFor names the (record, property) whose staged directory is dir. Every
// readable record answers, not just committed ones.
func (s migrationPreservedState) mirrorFor(dir string) (MigrationRecordKey, string, bool) {
	for _, rec := range s.records {
		subject := rec.Subject()
		for _, prop := range subject.Properties {
			if subject.StagedDirs[prop] == dir {
				return subject.Key, prop, true
			}
		}
	}
	return MigrationRecordKey{}, "", false
}

// migrationPreservingOnly is a preserve state naming exactly these
// directories, for a caller that computed the set itself rather than reading it
// off the shard's records.
func migrationPreservingOnly(dirs map[string]bool) migrationPreservedState {
	return migrationPreservedState{buckets: dirs}
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
// what this record holds for one property. A lost promotion has no exit
// anywhere in the system: the mark is written when a promoted directory is
// found gone and nothing clears it, so that property can never be promoted and
// a load reclaims nothing on its account.
//
// It is asked per property because promotion is per property: promoteSealed
// skips a lost one and promotes the rest, so a record can hold one property
// nothing will ever move next to one the very next load renames.
//
// Preservation is unaffected — the record and its directories are kept either
// way. Only the claim that hydrating the shard would reclaim them changes, and
// that claim is what drags a cold tenant into a load on every schema operation
// against its collection, forever.
//
// The record is still the exit's own witness: a resubmit supersedes it, and
// retirement removes it from the record set entirely before this is asked.
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

// migrationLegacyMarkerTracker is a tracker directory whose completion marker
// names the property's live data at the staged name. Every completed migration
// leaves one today, since no task writes a record yet; so did every
// pre-migration-records release.
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
}

// migrationLegacyMarkerTrackersAt finds every tracker on one shard carrying a
// completion marker. listed=false is distinct from finding none: a fault
// hiding every one of them (fd exhaustion on a many-tenant node) would
// otherwise free a property's only copy.
//
// The marker is asked before anything else, exactly as the orphan audit asks
// it. A record naming the tracker is not evidence against the marker: after an
// upgrade, rehydrate adopts the marker-era generation and writes a record for
// it, and that record is Iterating — which no preserve set covers, so asking
// the record first would hand the marker's staged data to the reclaimers.
func migrationLegacyMarkerTrackersAt(lsmPath, propName string, props *taskPropsCache,
) (trackers []migrationLegacyMarkerTracker, listed bool) {
	migsDir := filepath.Join(lsmPath, migrationsDir)
	entries, err := os.ReadDir(migsDir)
	if err != nil {
		// Absent is the ordinary case — most shards never ran a migration —
		// and it really does mean there is nothing marker-era here.
		return nil, os.IsNotExist(err)
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
		if propName != "" && !migrationTrackerMayOwnProperty(dirName, propName) {
			// Its own name proves it stages nothing of this property's, so no
			// sweep of this property removes anything it owns and its payload
			// need not be parsed. Asked after the marker, so a tracker whose
			// completion could not be read still withholds the shard.
			continue
		}
		migDir := filepath.Join(migsDir, dirName)
		answer := props.lookup(migDir)
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
		})
	}
	return out, true
}

// migrationLegacyMarkerDirsAt is the same answer as a name set, for removal
// loops keeping their own record check. complete=false means names are
// missing (unreadable payload, or unlistable directory), so callers must stop.
func migrationLegacyMarkerDirsAt(lsmPath string) (dirs map[string]struct{}, complete bool) {
	dirs = map[string]struct{}{}
	trackers, listed := migrationLegacyMarkerTrackersAt(lsmPath, "", nil)
	if !listed {
		return dirs, false
	}
	complete = true
	for _, legacy := range trackers {
		if legacy.unreadable {
			complete = false
			continue
		}
		dirs[legacy.dirName] = struct{}{}
		for _, sidecar := range legacy.sidecars {
			dirs[sidecar] = struct{}{}
		}
	}
	return dirs, complete
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

// migrationRecordStagingIncomplete is the want-predicate for readers asking
// the opposite of StagedDataComplete.
func migrationRecordStagingIncomplete(rec MigrationRecord) bool { return !rec.StagedDataComplete() }

// migrationRecordFor reports whether any record on the shard belongs to the
// named migration and satisfies want. Matching on type and property list,
// not directory names, covers both strategies a change-tokenization fans into.
func migrationRecordFor(records []MigrationRecord, migrationType ReindexMigrationType,
	properties []string, want func(MigrationRecord) bool,
) bool {
	for _, rec := range records {
		subject := rec.Subject()
		if subject.MigrationType != migrationType || !want(rec) {
			continue
		}
		for _, prop := range properties {
			if slices.Contains(subject.Properties, prop) {
				return true
			}
		}
	}
	return false
}
