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
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

// MigrationState is the durable state of one shard-local reindex migration.
// Absent — no record on disk — is the null, and has no constant.
type MigrationState string

const (
	MigrationStateIterating MigrationState = "iterating"
	MigrationStateIterated  MigrationState = "iterated"
	MigrationStateMerged    MigrationState = "merged"
	MigrationStateSwapped   MigrationState = "swapped"
	MigrationStatePromoted  MigrationState = "promoted"
)

// MigrationStrategyCode names the strategy a record belongs to. The values
// land in record file names, so they are a durable on-disk format: never
// rename one, and never reuse a retired one.
type MigrationStrategyCode string

const (
	StrategyCodeSearchableMapToBlockmax     MigrationStrategyCode = "searchable_map_to_blockmax"
	StrategyCodeFilterableRoaringsetRefresh MigrationStrategyCode = "filterable_roaringset_refresh"
	StrategyCodeFilterableToRangeable       MigrationStrategyCode = "filterable_to_rangeable"
	StrategyCodeSearchableRetokenize        MigrationStrategyCode = "searchable_retokenize"
	StrategyCodeFilterableRetokenize        MigrationStrategyCode = "filterable_retokenize"
	StrategyCodeEnableFilterable            MigrationStrategyCode = "enable_filterable"
	StrategyCodeEnableSearchable            MigrationStrategyCode = "enable_searchable"
	StrategyCodeRebuildSearchable           MigrationStrategyCode = "rebuild_searchable"
)

func (c MigrationStrategyCode) valid() bool {
	switch c {
	case StrategyCodeSearchableMapToBlockmax, StrategyCodeFilterableRoaringsetRefresh,
		StrategyCodeFilterableToRangeable, StrategyCodeSearchableRetokenize,
		StrategyCodeFilterableRetokenize, StrategyCodeEnableFilterable,
		StrategyCodeEnableSearchable, StrategyCodeRebuildSearchable:
		return true
	default:
		return false
	}
}

func migrationTypeKnown(t ReindexMigrationType) bool {
	switch t {
	case ReindexTypeChangeAlgorithm, ReindexTypeRebuildSearchable,
		ReindexTypeRepairFilterable, ReindexTypeEnableRangeable,
		ReindexTypeRepairRangeable, ReindexTypeEnableFilterable,
		ReindexTypeEnableSearchable, ReindexTypeChangeTokenization,
		ReindexTypeChangeTokenizationFilterable:
		return true
	default:
		return false
	}
}

// MigrationRecordKey identifies one migration on one shard. TaskVersion is the
// RAFT log index of the task's creation — a total order allocated by
// consensus and identical on every node, so two records on one property can
// be compared without chasing links. It is not the generation: that's a
// separate, per-node counter (visible in directory names and operator docs)
// that two nodes running the same migration routinely disagree on.
type MigrationRecordKey struct {
	TaskVersion  uint64                `json:"taskVersion"`
	StrategyCode MigrationStrategyCode `json:"strategyCode"`
	UnitID       string                `json:"unitID"`
}

// fileName carries the whole key, unit included. A backup walks the migrations
// directory recursively and shard copy ships the files under it, so a foreign
// unit's record does land here; a name that left the unit out collided with the
// local record's, and the next local write destroyed it.
func (k MigrationRecordKey) fileName() string {
	return fmt.Sprintf("%d_%s_%s.json", k.TaskVersion, k.StrategyCode, k.UnitID)
}

func (k MigrationRecordKey) String() string {
	return fmt.Sprintf("%d/%s/%s", k.TaskVersion, k.StrategyCode, k.UnitID)
}

// valid also rejects a unit the file name could not carry, since the name is
// now built from it.
func (k MigrationRecordKey) valid() bool {
	return k.TaskVersion > 0 && k.StrategyCode.valid() && migrationHandleIsOneElement(k.UnitID)
}

// MigrationCheckpoint is the iteration resume point.
type MigrationCheckpoint struct {
	LastProcessedKey []byte    `json:"lastProcessedKey,omitempty"`
	ProcessedCount   int       `json:"processedCount"`
	IndexedCount     int       `json:"indexedCount"`
	UpdatedAt        time.Time `json:"updatedAt"`
}

// MigrationSubject is what every variant carries: enough to identify the
// migration and to name every directory it touches, so that no reader ever
// re-derives a directory from a property name or a generation number.
type MigrationSubject struct {
	Key                  MigrationRecordKey   `json:"key"`
	TaskID               string               `json:"taskID"`
	MigrationType        ReindexMigrationType `json:"migrationType"`
	Properties           []string             `json:"properties,omitempty"`
	TargetTokenization   string               `json:"targetTokenization,omitempty"`
	OriginalTokenization string               `json:"originalTokenization,omitempty"`

	// IterationCutoff is the horizon the rebuild iterates up to: an object
	// updated at or after it is left to the double-write mirror. Fixed at the
	// record's first write and carried unchanged after, so a resume never
	// re-derives it from a moved clock. The reverse edge raises it to
	// migrationHorizonEverything once the mirror's own directory is lost.
	IterationCutoff time.Time `json:"iterationCutoff"`

	// TrackerDir is the migration's directory under .migrations, relative to
	// it. Recorded rather than re-derived because the sweeps have to decide
	// whether they may remove it, and the number in its name is the node's own
	// generation counter, which no record key can be compared against.
	TrackerDir string `json:"trackerDir,omitempty"`

	// StagedDirs is the re-derivation's "live-data dir": per property, the
	// directory holding this migration's own data. The flip makes it live and
	// promotion renames it onto CanonicalDirs.
	StagedDirs    map[string]string `json:"stagedDirs,omitempty"`
	CanonicalDirs map[string]string `json:"canonicalDirs,omitempty"`

	// SidecarDirs is keyed by property like its two siblings, so a caller
	// acting on one property can name that property's sidecar without
	// touching the ones the record's other properties are still using.
	SidecarDirs map[string]string `json:"sidecarDirs,omitempty"`
}

// migrationHorizonEverything is the horizon of a rebuild that skips nothing.
// The skip predicate processes an object only while it is older than the
// horizon, so "cover everything" is a horizon nothing can reach rather than a
// zero one.
var migrationHorizonEverything = time.Date(9999, time.January, 1, 0, 0, 0, 0, time.UTC)

// MigrationRecord is the sealed set of five variants. Only this package can
// implement it, and a record is a value: whoever holds one must not mutate it
// or anything reachable from it, because the store hands the same value to
// every concurrent reader.
type MigrationRecord interface {
	State() MigrationState
	Subject() MigrationSubject
	migrationRecordQuestions

	// toEnvelope both serializes and seals: an unexported method keeps the
	// variant set closed to this package.
	toEnvelope() migrationRecordEnvelope
}

type migrationRecordBase struct {
	subject MigrationSubject
}

func (b migrationRecordBase) Subject() MigrationSubject { return b.subject }

// migrationFlipBlock is the flip decision: which properties the record's flip
// covers, and per property the directory that flip displaced. Both are
// resolvable only at the moment of the flip, which is why they are written
// then rather than re-derived later.
type migrationFlipBlock struct {
	flipped       []string
	displacedDirs map[string]string
}

func (f migrationFlipBlock) Flipped() []string { return f.flipped }

func (f migrationFlipBlock) DisplacedDir(prop string) (string, bool) {
	dir, ok := f.displacedDirs[prop]
	return dir, ok
}

type MigrationRecordIterating struct {
	migrationRecordBase
	checkpoint MigrationCheckpoint
}

type MigrationRecordIterated struct {
	migrationRecordBase
}

type MigrationRecordMerged struct {
	migrationRecordBase
}

type MigrationRecordSwapped struct {
	migrationRecordBase
	migrationFlipBlock
	// promotion maps a property to how far its own promotion got. A finished
	// mark is what every later pass reads, so the question "did my rename
	// run" is answered by the record rather than by the contents of a
	// directory the store keeps rewriting. See
	// [migrationReconciler.promoteProperty].
	promotion map[string]migrationPromotionMark
}

// migrationPromotionMark is how far one property's promotion got. Started is
// written immediately before the rename and finished immediately after, with
// nothing in between, so only a process that stopped between those two
// statements leaves a start standing.
type migrationPromotionMark string

const (
	migrationPromotionStarted  migrationPromotionMark = "started"
	migrationPromotionFinished migrationPromotionMark = "finished"
	// migrationPromotionLost is terminal: the directory this rename produced
	// was found gone, and a shard load re-creates that name empty. Writing the
	// answer down is what keeps the re-creation from reading as the rename's
	// output on the pass after.
	migrationPromotionLost migrationPromotionMark = "lost"
)

func migrationPromotionMarkKnown(m migrationPromotionMark) bool {
	switch m {
	case migrationPromotionStarted, migrationPromotionFinished, migrationPromotionLost:
		return true
	}
	return false
}

type MigrationRecordPromoted struct {
	migrationRecordBase
	migrationFlipBlock
}

func NewMigrationRecordIterating(subject MigrationSubject, checkpoint MigrationCheckpoint) MigrationRecordIterating {
	return MigrationRecordIterating{migrationRecordBase{subject}, checkpoint}
}

func NewMigrationRecordIterated(subject MigrationSubject) MigrationRecordIterated {
	return MigrationRecordIterated{migrationRecordBase{subject}}
}

func NewMigrationRecordMerged(subject MigrationSubject) MigrationRecordMerged {
	return MigrationRecordMerged{migrationRecordBase{subject}}
}

func NewMigrationRecordSwapped(subject MigrationSubject, flipped []string, displacedDirs map[string]string) MigrationRecordSwapped {
	return MigrationRecordSwapped{migrationRecordBase{subject}, migrationFlipBlock{flipped, displacedDirs}, nil}
}

// PromotionOf returns how far prop's own promotion got, or the empty mark
// when none ever started. Without a start, a missing staged directory is not
// this migration's doing.
func (r MigrationRecordSwapped) PromotionOf(prop string) migrationPromotionMark {
	return r.promotion[prop]
}

// WithPromotionAt records how far prop's promotion got.
func (r MigrationRecordSwapped) WithPromotionAt(prop string, mark migrationPromotionMark) MigrationRecordSwapped {
	next := make(map[string]migrationPromotionMark, len(r.promotion)+1)
	maps.Copy(next, r.promotion)
	next[prop] = mark
	r.promotion = next
	return r
}

// WithPromotionAbandoned drops what prop recorded before a rename that
// returned instead of running, so a started promotion never outlives the pass
// that observes it.
func (r MigrationRecordSwapped) WithPromotionAbandoned(prop string) MigrationRecordSwapped {
	if _, marked := r.promotion[prop]; !marked {
		return r
	}
	next := make(map[string]migrationPromotionMark, len(r.promotion))
	maps.Copy(next, r.promotion)
	delete(next, prop)
	r.promotion = next
	return r
}

func NewMigrationRecordPromoted(subject MigrationSubject, flipped []string, displacedDirs map[string]string) MigrationRecordPromoted {
	return MigrationRecordPromoted{migrationRecordBase{subject}, migrationFlipBlock{flipped, displacedDirs}}
}

func (r MigrationRecordIterating) Checkpoint() MigrationCheckpoint { return r.checkpoint }

func (r MigrationRecordIterating) State() MigrationState { return MigrationStateIterating }
func (r MigrationRecordIterated) State() MigrationState  { return MigrationStateIterated }
func (r MigrationRecordMerged) State() MigrationState    { return MigrationStateMerged }
func (r MigrationRecordSwapped) State() MigrationState   { return MigrationStateSwapped }
func (r MigrationRecordPromoted) State() MigrationState  { return MigrationStatePromoted }

// migrationRecordFormatVersion is bumped only for changes a previous release
// can't read. The gate is exact equality both ways, so a bump makes every
// record from either version read as NotUnderstood on the other — freezing
// all writes and removals on that shard, not just the record. Adding a
// second version is a rolling-upgrade decision, not an additive one.
const migrationRecordFormatVersion = 1

type migrationFlipEnvelope struct {
	Flipped       []string          `json:"flipped,omitempty"`
	DisplacedDirs map[string]string `json:"displacedDirs,omitempty"`
	// Promotion carries its own key rather than reusing one an earlier shape
	// wrote, so a build that reads only the other key promotes nothing rather
	// than promoting on a claim it cannot check.
	Promotion map[string]migrationPromotionMark `json:"promotion,omitempty"`
}

type migrationRecordEnvelope struct {
	FormatVersion int                    `json:"formatVersion"`
	State         MigrationState         `json:"state"`
	Subject       MigrationSubject       `json:"subject"`
	Checkpoint    *MigrationCheckpoint   `json:"checkpoint,omitempty"`
	Flip          *migrationFlipEnvelope `json:"flip,omitempty"`
}

func newMigrationRecordEnvelope(state MigrationState, subject MigrationSubject) migrationRecordEnvelope {
	return migrationRecordEnvelope{FormatVersion: migrationRecordFormatVersion, State: state, Subject: subject}
}

func (f migrationFlipBlock) toEnvelope() *migrationFlipEnvelope {
	return &migrationFlipEnvelope{Flipped: f.flipped, DisplacedDirs: f.displacedDirs}
}

func (r MigrationRecordIterating) toEnvelope() migrationRecordEnvelope {
	env := newMigrationRecordEnvelope(MigrationStateIterating, r.subject)
	cp := r.checkpoint
	env.Checkpoint = &cp
	return env
}

func (r MigrationRecordIterated) toEnvelope() migrationRecordEnvelope {
	return newMigrationRecordEnvelope(MigrationStateIterated, r.subject)
}

func (r MigrationRecordMerged) toEnvelope() migrationRecordEnvelope {
	return newMigrationRecordEnvelope(MigrationStateMerged, r.subject)
}

func (r MigrationRecordSwapped) toEnvelope() migrationRecordEnvelope {
	env := newMigrationRecordEnvelope(MigrationStateSwapped, r.subject)
	env.Flip = r.migrationFlipBlock.toEnvelope()
	env.Flip.Promotion = r.promotion
	return env
}

func (r MigrationRecordPromoted) toEnvelope() migrationRecordEnvelope {
	env := newMigrationRecordEnvelope(MigrationStatePromoted, r.subject)
	env.Flip = r.migrationFlipBlock.toEnvelope()
	return env
}

// encodeMigrationRecord indents so an operator can read a record with cat;
// records are written once per transition, never on a hot path. It applies
// the decoder's own handle check, so a rejected handle fails the transition
// loudly instead of persisting a record that reads as not-understood (and
// freezes removal) at the next load.
func encodeMigrationRecord(rec MigrationRecord) ([]byte, error) {
	env := rec.toEnvelope()
	if err := validateMigrationEnvelope(env); err != nil {
		return nil, err
	}
	data, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		return nil, err
	}
	// The loader refuses at this same bound, and a record only the writer
	// accepts is the worst outcome of the two: it lands under a name the next
	// load will neither read, write over, nor remove. Refusing the encode turns
	// a permanently wedged key into a transition the caller can report.
	if len(data) > maxMigrationRecordBytes {
		return nil, fmt.Errorf("record %q holds %d bytes, bound is %d",
			env.Subject.Key, len(data), maxMigrationRecordBytes)
	}
	return data, nil
}

// validateMigrationEnvelope holds every state-independent reason a record is
// refused. Both directions ask it: a record only the writer accepts is worse
// than one neither does — it lands under a name that reads back as refused,
// freezing that name's writes and removals forever.
func validateMigrationEnvelope(e migrationRecordEnvelope) error {
	if e.FormatVersion != migrationRecordFormatVersion {
		return fmt.Errorf("unknown record format version %d", e.FormatVersion)
	}
	if !e.Subject.Key.valid() {
		return fmt.Errorf("record key %q is incomplete or names an unknown strategy", e.Subject.Key)
	}
	if e.Subject.TaskID == "" {
		return fmt.Errorf("record %q has no task ID", e.Subject.Key)
	}
	if !migrationTypeKnown(e.Subject.MigrationType) {
		return fmt.Errorf("record %q names unknown migration type %q", e.Subject.Key, e.Subject.MigrationType)
	}
	if err := validateMigrationHandles(e); err != nil {
		return err
	}
	if err := validatePromotion(e); err != nil {
		return err
	}
	return validateOneOwnerPerDirectory(e)
}

// validatePromotion refuses a promotion mark on a property the record does not
// carry, a mark this build cannot read, and any mark at all on a state that
// has no promotion to be part way through. Promotion reads the mark to decide
// that a missing staged directory is its own rename's doing, so it may not be
// taken on trust.
func validatePromotion(e migrationRecordEnvelope) error {
	if e.Flip == nil {
		return nil
	}
	if len(e.Flip.Promotion) > 0 && e.State != MigrationStateSwapped {
		return fmt.Errorf("record %q in state %q carries promotion marks, which only a swapped record does",
			e.Subject.Key, e.State)
	}
	// Sorted, so a record with two bad entries names the same one every time.
	for _, prop := range slices.Sorted(maps.Keys(e.Flip.Promotion)) {
		if !slices.Contains(e.Subject.Properties, prop) {
			return fmt.Errorf("record %q records a promotion of property %q, which it does not name",
				e.Subject.Key, prop)
		}
		if !migrationPromotionMarkKnown(e.Flip.Promotion[prop]) {
			return fmt.Errorf("record %q records unknown promotion mark %q for property %q",
				e.Subject.Key, e.Flip.Promotion[prop], prop)
		}
	}
	return nil
}

// validateOneOwnerPerDirectory refuses a record that names one directory
// twice. Every actor is handed a single property and acts on the handles that
// property names — ShutdownStagedBuckets closes its staged and sidecar
// buckets, retirement removes its staged directory, promotion removes its
// canonical and displaced ones — so a name a second property also carries is
// closed or deleted while that property is still serving from it. (A restored
// archive may carry any handle.)
func validateOneOwnerPerDirectory(e migrationRecordEnvelope) error {
	type claim struct{ role, prop string }

	var displaced map[string]string
	if e.Flip != nil {
		displaced = e.Flip.DisplacedDirs
	}
	owner := make(map[string]claim, len(e.Subject.StagedDirs)+
		len(e.Subject.CanonicalDirs)+len(e.Subject.SidecarDirs)+len(displaced))

	for _, group := range []struct {
		role string
		dirs map[string]string
		// isDisplaced marks the one repeat a record legitimately carries: what
		// a flip displaced is the canonical name that flip replaced.
		isDisplaced bool
	}{
		{role: "staged directory", dirs: e.Subject.StagedDirs},
		{role: "canonical directory", dirs: e.Subject.CanonicalDirs},
		{role: "sidecar directory", dirs: e.Subject.SidecarDirs},
		{role: "displaced directory", dirs: displaced, isDisplaced: true},
	} {
		// Sorted, so a record with two collisions names the same one every
		// time. Ranging the map would make the error text a coin flip.
		for _, prop := range slices.Sorted(maps.Keys(group.dirs)) {
			dir := group.dirs[prop]
			if dir == "" || (group.isDisplaced && dir == e.Subject.CanonicalDirs[prop]) {
				continue
			}
			if held, taken := owner[dir]; taken {
				return fmt.Errorf(
					"record %q names directory %q as both the %s of property %q and the %s of property %q",
					e.Subject.Key, dir, held.role, held.prop, group.role, prop)
			}
			owner[dir] = claim{group.role, prop}
		}
	}
	return nil
}

// decodeMigrationRecord rejects anything it cannot place exactly, including a
// state-specific block on a state that has none. Every rejection becomes
// NotUnderstood, which preserves rather than deletes.
func decodeMigrationRecord(data []byte) (MigrationRecord, error) {
	var env migrationRecordEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("decode record: %w", err)
	}
	if err := validateMigrationEnvelope(env); err != nil {
		return nil, err
	}

	switch env.State {
	case MigrationStateIterating:
		if err := env.requireBlocks(migrationBlocks{checkpoint: true}); err != nil {
			return nil, err
		}
		return NewMigrationRecordIterating(env.Subject, *env.Checkpoint), nil
	case MigrationStateIterated:
		if err := env.requireBlocks(migrationBlocks{}); err != nil {
			return nil, err
		}
		return NewMigrationRecordIterated(env.Subject), nil
	case MigrationStateMerged:
		if err := env.requireBlocks(migrationBlocks{}); err != nil {
			return nil, err
		}
		return NewMigrationRecordMerged(env.Subject), nil
	case MigrationStateSwapped:
		if err := env.requireBlocks(migrationBlocks{flip: true}); err != nil {
			return nil, err
		}
		swapped := NewMigrationRecordSwapped(env.Subject, env.Flip.Flipped, env.Flip.DisplacedDirs)
		swapped.promotion = env.Flip.Promotion
		return swapped, nil
	case MigrationStatePromoted:
		if err := env.requireBlocks(migrationBlocks{flip: true}); err != nil {
			return nil, err
		}
		return NewMigrationRecordPromoted(env.Subject, env.Flip.Flipped, env.Flip.DisplacedDirs), nil
	default:
		return nil, fmt.Errorf("record %q names unknown state %q", env.Subject.Key, env.State)
	}
}

// migrationHandleIsOneElement reports whether h names a single entry under
// the directory it's joined onto. [filepath.IsLocal] isn't that test: it
// accepts ".", "x/.." and "a/b/../..", each of which Join resolves back to
// the shard's LSM root — which then reaches os.RemoveAll.
func migrationHandleIsOneElement(h string) bool {
	if h == "" || h == "." || h == ".." || filepath.IsAbs(h) {
		return false
	}
	if h != filepath.Clean(h) {
		return false
	}
	return !strings.ContainsRune(h, '/') && !strings.ContainsRune(h, os.PathSeparator)
}

// validateMigrationHandles rejects any recorded string that doesn't name a
// single entry under the shard root: directory handles (removed via
// os.RemoveAll) and property names (used to compose bucket/sidecar names to
// remove). Nothing legitimate is refused — writer-emitted handles are always
// a strategy prefix plus sorted property names; a restored backup archive is
// the only reachable producer of anything else. Both directions call this.
func validateMigrationHandles(e migrationRecordEnvelope) error {
	// Sorted, so a record with two bad handles names the same one every time.
	// Ranging the maps directly would make the error text a coin flip.
	byProperty := func(dirs map[string]string) (props, handles []string) {
		props = slices.Sorted(maps.Keys(dirs))
		for _, prop := range props {
			handles = append(handles, dirs[prop])
		}
		return props, handles
	}
	stagedProps, staged := byProperty(e.Subject.StagedDirs)
	canonicalProps, canonical := byProperty(e.Subject.CanonicalDirs)
	sidecarProps, sidecars := byProperty(e.Subject.SidecarDirs)
	var flipped, displacedProps, displaced []string
	if e.Flip != nil {
		flipped = e.Flip.Flipped
		displacedProps, displaced = byProperty(e.Flip.DisplacedDirs)
	}

	for _, group := range []struct {
		field string
		// namesDirectory separates the handles a sweep hands to os.RemoveAll
		// from the property names it composes bucket names out of. Only the
		// former may be checked against the reserved set: property names are
		// user-chosen, and a collection may legitimately have one called
		// "records".
		namesDirectory bool
		// stagesData marks the roles that hold a migration's own copy of a
		// property's index. Those are reclaimed on every teardown path, so
		// they may not name a bucket the shard serves from.
		stagesData bool
		handles    []string
	}{
		{field: "tracker directory", namesDirectory: true, handles: []string{e.Subject.TrackerDir}},
		{field: "sidecar directory", namesDirectory: true, stagesData: true, handles: sidecars},
		{field: "property", handles: slices.Concat(
			e.Subject.Properties, stagedProps, canonicalProps, sidecarProps, displacedProps, flipped)},
		{field: "staged directory", namesDirectory: true, stagesData: true, handles: staged},
		{field: "canonical directory", namesDirectory: true, handles: canonical},
		{field: "displaced directory", namesDirectory: true, handles: displaced},
	} {
		for _, handle := range group.handles {
			// An empty handle is the ordinary "this record names none", and
			// every reader already guards on it. An empty property name is
			// not: nothing legitimate emits one, and it composes into a
			// bucket name that names another property's sidecar.
			if handle == "" && group.namesDirectory {
				continue
			}
			if !migrationHandleIsOneElement(handle) {
				return fmt.Errorf("record %q names %s %q, which is not a single directory inside the shard",
					e.Subject.Key, group.field, handle)
			}
			if group.namesDirectory && migrationReservedDirName(handle) {
				return fmt.Errorf("record %q names %s %q, which is a store the shard serves from",
					e.Subject.Key, group.field, handle)
			}
			if group.stagesData && migrationHandleIsLiveBucket(handle) {
				return fmt.Errorf("record %q names %s %q, which is a property's own bucket rather than a sidecar of one",
					e.Subject.Key, group.field, handle)
			}
		}
	}
	return nil
}

// migrationReservedDirName reports whether h names a store the shard reads
// from rather than a directory a migration may own. A record naming one as a
// directory it owns points every teardown path at it: reclaiming a
// ".migrations" handle removes every tracker and the record store with it, a
// tracker directory of "records" removes the store on its own, and "objects"
// is the shard's whole object store.
func migrationReservedDirName(h string) bool {
	return h == migrationsDir || h == migrationRecordsDirName || h == helpers.ObjectsBucketLSM
}

// migrationHandleIsLiveBucket reports whether h names a property's own bucket
// rather than a sidecar of one.
//
// Staged and sidecar directories hold a migration's private copy and are
// reclaimed on every teardown path, so a record naming a live bucket in either
// role hands that bucket to os.RemoveAll. Every such directory a writer emits
// is <property bucket> + "__" + a strategy tail ending in one of
// [sidecarRoleWords]; a bare property bucket has no such tail.
//
// A handle that is no property bucket at all is not this check's business: it
// names nothing the shard serves from.
func migrationHandleIsLiveBucket(h string) bool {
	tail, ok := strings.CutPrefix(h, migrationPropertyBucketPrefix)
	if !ok {
		return false
	}
	i := strings.Index(tail, "__")
	if i < 0 {
		return true
	}
	return !slices.Contains(sidecarRoleWords, sidecarRoleWord(tail[i+2:]))
}

// migrationPropertyBucketPrefix is what every property bucket directory name
// starts with. TestEveryPropertyBucketCarriesTheMigrationPrefix pins it
// against the helpers that build those names.
const migrationPropertyBucketPrefix = "property_"

// migrationBlocks names which optional blocks a state carries, so the call
// sites read as the states they decode rather than as two bare booleans.
type migrationBlocks struct {
	checkpoint bool
	flip       bool
}

func (e migrationRecordEnvelope) requireBlocks(want migrationBlocks) error {
	if (e.Checkpoint != nil) != want.checkpoint {
		return fmt.Errorf("record %q in state %q: checkpoint block present=%v, wanted=%v",
			e.Subject.Key, e.State, e.Checkpoint != nil, want.checkpoint)
	}
	if (e.Flip != nil) != want.flip {
		return fmt.Errorf("record %q in state %q: flip block present=%v, wanted=%v",
			e.Subject.Key, e.State, e.Flip != nil, want.flip)
	}
	return nil
}
