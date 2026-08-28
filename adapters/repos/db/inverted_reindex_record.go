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
	return MigrationRecordSwapped{migrationRecordBase{subject}, migrationFlipBlock{flipped, displacedDirs}}
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
	return json.MarshalIndent(env, "", "  ")
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
	return validateDisplacedAreNotStaged(e)
}

// validateDisplacedAreNotStaged refuses a record whose flip displaced a
// directory this record itself staged: promotion removes the displaced
// directory then renames staged onto canonical, so a collision would destroy
// the property's only copy. (A restored archive may carry any handle.)
func validateDisplacedAreNotStaged(e migrationRecordEnvelope) error {
	if e.Flip == nil {
		return nil
	}
	stagedBy := make(map[string]string, len(e.Subject.StagedDirs))
	for prop, staged := range e.Subject.StagedDirs {
		if staged != "" {
			stagedBy[staged] = prop
		}
	}
	for prop, displaced := range e.Flip.DisplacedDirs {
		if displaced == "" {
			continue
		}
		if owner, ok := stagedBy[displaced]; ok {
			return fmt.Errorf("record %q says property %q displaced %q, the directory staged for property %q",
				e.Subject.Key, prop, displaced, owner)
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
		return NewMigrationRecordSwapped(env.Subject, env.Flip.Flipped, env.Flip.DisplacedDirs), nil
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
		field   string
		handles []string
	}{
		{"tracker directory", []string{e.Subject.TrackerDir}},
		{"sidecar directory", sidecars},
		{"property", slices.Concat(
			e.Subject.Properties, stagedProps, canonicalProps, sidecarProps, displacedProps, flipped)},
		{"staged directory", staged},
		{"canonical directory", canonical},
		{"displaced directory", displaced},
	} {
		for _, handle := range group.handles {
			// An empty handle is the ordinary "this record names none", and
			// every reader already guards on it. An empty property name is
			// not: nothing legitimate emits one, and it composes into a
			// bucket name that names another property's sidecar.
			if handle == "" && group.field != "property" {
				continue
			}
			if !migrationHandleIsOneElement(handle) {
				return fmt.Errorf("record %q names %s %q, which is not a single directory inside the shard",
					e.Subject.Key, group.field, handle)
			}
		}
	}
	return nil
}

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
