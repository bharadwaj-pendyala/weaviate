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
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// Migration directory names live under <shard>/lsm/.migrations/<name>/ and
// uniquely identify a per-strategy in-progress migration on a shard.
//
// Each name is its strategy's [MigrationStrategyCode], so a record file and
// the directory it describes cannot drift apart. These constants exist so
// the writer, the orphan audit's bucket-name recipes (migrationSuffixes),
// and the debug endpoints all reach the name through one symbol.
//
// Some strategies pin a single directory (e.g. searchable_map_to_blockmax),
// others suffix per-property names onto a common prefix (e.g.
// enable_filterable_<prop1>_<prop2>) — for those, the constant is the
// prefix and callers append.
const (
	// MigrationDirSearchableMapToBlockmax is the directory name for the
	// MapCollection → Inverted (blockmax WAND) migration of searchable
	// properties.
	MigrationDirSearchableMapToBlockmax = string(StrategyCodeSearchableMapToBlockmax)

	// MigrationDirFilterableRoaringsetRefresh is the directory name for the
	// same-strategy rebuild of an existing filterable (RoaringSet) index.
	MigrationDirFilterableRoaringsetRefresh = string(StrategyCodeFilterableRoaringsetRefresh)

	// MigrationDirPrefixFilterableToRangeable is the directory-name prefix
	// for the filterable → rangeable migration. The actual directory is
	// either this prefix on its own (no specific properties) or this prefix
	// + "_<prop1>_<prop2>...". Use as both equality check (no propnames) and
	// HasPrefix check.
	MigrationDirPrefixFilterableToRangeable = string(StrategyCodeFilterableToRangeable)

	// MigrationDirPrefixSearchableRetokenize is the directory-name prefix
	// for the per-property retokenize migration on the searchable index.
	// Actual dir: "<prefix>_<propName>".
	MigrationDirPrefixSearchableRetokenize = string(StrategyCodeSearchableRetokenize)

	// MigrationDirPrefixFilterableRetokenize is the directory-name prefix
	// for the per-property retokenize migration on the filterable index.
	// Actual dir: "<prefix>_<propName>".
	MigrationDirPrefixFilterableRetokenize = string(StrategyCodeFilterableRetokenize)

	// MigrationDirPrefixEnableFilterable is the directory-name prefix for
	// the enable-filterable migration. The actual directory is either this
	// prefix on its own (no specific properties) or this prefix +
	// "_<prop1>_<prop2>...".
	MigrationDirPrefixEnableFilterable = string(StrategyCodeEnableFilterable)

	// MigrationDirPrefixEnableSearchable is the directory-name prefix for
	// the enable-searchable migration. The actual directory is either this
	// prefix on its own (no specific properties) or this prefix +
	// "_<prop1>_<prop2>...".
	MigrationDirPrefixEnableSearchable = string(StrategyCodeEnableSearchable)

	// MigrationDirPrefixRebuildSearchable is the directory-name prefix for
	// the per-property rebuild-searchable migration (rebuild a BlockMax
	// searchable bucket from the objects store). Actual dir:
	// "<prefix>_<prop1>_<prop2>...".
	MigrationDirPrefixRebuildSearchable = string(StrategyCodeRebuildSearchable)
)

// migrationDirWithProps assembles a migration directory name from a
// prefix and an optional set of property names. Empty propNames returns
// the prefix on its own; otherwise the prefix is joined with the
// property names by underscores. Three strategies (enable-filterable,
// enable-searchable, filterable-to-rangeable) share this naming pattern.
//
// Property names are sorted before joining so that the directory name
// is a function of the *set* of properties, not the caller's slice
// order. This keeps restart-recovery deterministic: a task built from
// payload.Properties=["b","a"] and one built from ["a","b"] both
// resolve to the same on-disk directory.
func migrationDirWithProps(prefix string, propNames []string) string {
	if len(propNames) == 0 {
		return prefix
	}
	sorted := make([]string, len(propNames))
	copy(sorted, propNames)
	sort.Strings(sorted)
	return prefix + "_" + strings.Join(sorted, "_")
}

// genSuffix returns the per-migration generation suffix, e.g. "_2".
// Every concrete strategy's MigrationDirName / ReindexSuffix / IngestSuffix /
// BackupSuffix appends this so back-to-back in-process migrations on the
// same (prop, indexType) tuple don't collide on dir paths. Generation is
// computed per-node at task start by [nextMigrationGeneration]; the
// previous live main bucket lives at `…_ingest_<N-1>` (the in-memory
// pointer was already swapped to it; the on-disk rename onto the canonical
// name is deferred to the next load's finalize), and the new migration
// writes to `…_ingest_<N>`.
//
// Generation 0 is reserved for the canonical (post-promotion) bucket at
// `property_<prop>_<index>`, which has no suffix. Live migrations always
// use generation ≥ 1.
func genSuffix(generation int) string {
	return "_" + strconv.Itoa(generation)
}

// parseMigrationDirName splits a migration dir name (e.g.
// "searchable_retokenize_text_2", "enable_filterable_p1_p2_3",
// "searchable_map_to_blockmax_1") into its (prefix, generation) parts.
// The "prefix" returned is everything up to and excluding the trailing
// "_<N>" — for per-property strategies it includes the property name(s).
//
// Returns ok=false if the input does not end with "_<positive-int>". The
// finalize / recovery paths use this to enumerate generations on disk and
// pick the right strategy instance + gen.
func parseMigrationDirName(name string) (prefix string, generation int, ok bool) {
	idx := strings.LastIndex(name, "_")
	if idx <= 0 || idx == len(name)-1 {
		return "", 0, false
	}
	gen, err := strconv.Atoi(name[idx+1:])
	if err != nil || gen < 1 {
		return "", 0, false
	}
	return name[:idx], gen, true
}

// migrationDirPrefixesForIndexType returns the per-property migration
// strategy prefixes for a "filterable"/"searchable"/"rangeable" indexType,
// whose tracker dirs would lie (report "previous run completed") after the
// bucket has been removed on a property DELETE.
//
// Class-level migration dirs are deliberately omitted: they aggregate state
// across every property, so deleting one on a single property's DELETE would
// corrupt migrations for the rest of the class.
func migrationDirPrefixesForIndexType(indexType string) []string {
	switch indexType {
	case "filterable":
		return []string{
			MigrationDirPrefixEnableFilterable,
			MigrationDirPrefixFilterableRetokenize,
			MigrationDirPrefixFilterableToRangeable,
		}
	case "searchable":
		return []string{
			MigrationDirPrefixEnableSearchable,
			MigrationDirPrefixRebuildSearchable,
			MigrationDirPrefixSearchableRetokenize,
		}
	case "rangeable":
		return []string{
			MigrationDirPrefixFilterableToRangeable,
		}
	}
	// Not "this index type owns no tracker dirs" — every one this build knows
	// owns some. Callers refuse an index type they cannot map before they
	// build a scope, so a nil here never reaches a deletion decision.
	return nil
}

// migrationPerPropertyDirPrefixes are the strategy prefixes whose tracker dir
// carries its own property list in its name. Class-level prefixes are absent
// for the same reason [migrationDirPrefixesForIndexType] omits them: their
// directory names name no property, so only their payload can say what they
// own.
func migrationPerPropertyDirPrefixes() []string {
	return []string{
		MigrationDirPrefixEnableFilterable,
		MigrationDirPrefixEnableSearchable,
		MigrationDirPrefixFilterableRetokenize,
		MigrationDirPrefixFilterableToRangeable,
		MigrationDirPrefixRebuildSearchable,
		MigrationDirPrefixSearchableRetokenize,
	}
}

// migrationTrackerMayOwnProperty reports whether a tracker directory could own
// a bucket of propName, deciding from the directory name alone.
//
// [migrationDirWithProps] builds a per-property tracker's name from its sorted
// property list, so a name that does not carry propName proves the tracker
// stages nothing for it and its payload need never be parsed. A class-level
// tracker names no property at all, so the answer for one is always yes and the
// payload decides — which is what keeps a completed class-level migration's
// sidecars in the preserve set.
func migrationTrackerMayOwnProperty(name, propName string) bool {
	base := migrationDirBase(name)
	for _, prefix := range migrationPerPropertyDirPrefixes() {
		if tail, ok := strings.CutPrefix(base, prefix+"_"); !ok || tail == "" {
			continue
		}
		return base == migrationDirWithProps(prefix, []string{propName}) ||
			namesPropertyToken(base, prefix, propName)
	}
	return true
}

// migrationDirScope names the migration tracker dirs one (property, index
// type) cleanup owns on one shard, and decides whether a dir on disk is one
// of them.
//
// A dir name alone can be ambiguous (e.g. "enable_filterable_a_b_1" is both
// a two-property tracker for "a"+"b" and a one-property tracker for "a_b"),
// so an ambiguous name falls back to the task's recorded property list
// ([readTaskProps]). That list is trusted only where it rebuilds the dir's
// own name, and with no list only an exact one-property name, since guessing
// wider could remove another property's tracker.
type migrationDirScope struct {
	lsmPath  string
	dirs     *dirNamesCache
	propName string
	// prefixes are the per-property strategy prefixes this cleanup deletes.
	prefixes []string
	// props memoizes payloads across the passes of one sweep; nil reads every
	// time. Set by [migrationDirScope.cachingProps].
	props *taskPropsCache
	// records answers for every directory a record names, which is what keeps
	// payload.mig off this path. Set by [migrationDirScope.knownFrom].
	records []MigrationRecord
	// classDir is a whole tracker dir name matched as it is. Only
	// [classLevelMigrationDirsOf] sets it, and it goes away with that
	// function's last caller.
	classDir string
}

// cachingProps scopes a payload memo to this scope and every scope derived from
// it. See [taskPropsCache] for why it must not outlive one sweep.
func (s migrationDirScope) cachingProps(c *taskPropsCache) migrationDirScope {
	s.props = c
	return s
}

// knownFrom hands the scope the shard's records, which the sweep has already
// read to decide what it must preserve.
func (s migrationDirScope) knownFrom(state migrationPreservedState) migrationDirScope {
	s.records = state.records
	return s
}

// cachingDirs scopes a directory-listing memo to this scope, for the scopes
// [classLevelMigrationDirsOf] builds without one.
func (s migrationDirScope) cachingDirs(c *dirNamesCache) migrationDirScope {
	s.dirs = c
	return s
}

// classLevelMigrationDirsOf returns the scope of a single class-level tracker
// dir, which every property of the collection shares. The per-property scope
// omits it, so only a caller that names it explicitly ever sees it.
func classLevelMigrationDirsOf(lsmPath, classDir string) migrationDirScope {
	return migrationDirScope{lsmPath: lsmPath, classDir: classDir}
}

// migrationDirsOf returns the tracker dirs a (propName, indexType) cleanup
// deletes on the shard at lsmPath. A nil cache reads the filesystem every time.
func migrationDirsOf(lsmPath string, dirs *dirNamesCache, propName, indexType string) migrationDirScope {
	return migrationDirScope{
		lsmPath:  lsmPath,
		dirs:     dirs,
		propName: propName,
		prefixes: migrationDirPrefixesForIndexType(indexType),
	}
}

// inScope reports whether the tracker dir called name is in this scope. See
// [migrationDirScope] for why the payload decides what the name leaves open,
// and the name only fills in when there is no readable payload.
//
// Retokenize strategies name their dir with a bare property name; that's safe
// only because [ReindexProvider.createReindexTasks] rejects such a payload
// unless it carries exactly one property.
func (s migrationDirScope) inScope(name string) bool {
	if s.classDir != "" && migrationDirBase(name) == s.classDir {
		return true
	}
	if matched, decided := s.matchByName(name); decided {
		return matched
	}
	matched, _ := s.inScopeFailingOpen(name)
	return matched
}

// matchByName decides the dirs whose name settles the question on its own;
// decided=false means ask the payload.
//
// A dir's property segment is [migrationDirWithProps]'s sorted "_"-join, so
// an exact match is unforgeable (see [isProvablySingleProperty]), and a name
// that doesn't equal or carry propName as a token can't come from any list
// holding it — both decide without the payload, which is safe because name
// and payload always come from the same sorted list
// ([TestMatchByNameOverridesAContradictingPayload] pins this).
//
// [migrationDirScope.inScopeFailingOpen] takes only the negative arms, so the
// unloaded-shard gate still fails open on a payload it cannot parse for a dir
// the name does leave in scope.
func (s migrationDirScope) matchByName(name string) (matched, decided bool) {
	base := migrationDirBase(name)
	if !s.hasStrategyPrefix(base) {
		return false, true
	}
	exact, token := s.nameArms(base)
	switch {
	case exact && isProvablySingleProperty(s.propName):
		return true, true
	case !exact && !token:
		return false, true
	default:
		return false, false
	}
}

// nameArms reports how a tracker dir's base relates to propName under any of
// this scope's strategy prefixes: exact means the base is the one-property
// name, token means its property list carries propName as a whole segment.
func (s migrationDirScope) nameArms(base string) (exact, token bool) {
	for _, prefix := range s.prefixes {
		exact = exact || base == migrationDirWithProps(prefix, []string{s.propName})
		token = token || namesPropertyToken(base, prefix, s.propName)
	}
	return exact, token
}

// maxProvablySinglePropertyTokens bounds the split enumeration below. A longer
// property name is reported ambiguous and pays for its payload, which is the
// direction that cannot delete a dir it should have kept.
const maxProvablySinglePropertyTokens = 8

// isProvablySingleProperty reports whether propName can only have come
// from a one-property list, i.e. whether an equal property segment is
// unforgeable.
//
// [migrationDirWithProps] joins a sorted, non-decreasing list with "_", so a
// name with no such split names one property regardless of its own
// underscores — e.g. "price_cents" can't come from ["price","cents"], which
// sorts the other way. Non-decreasing (not strictly increasing) because the
// join doesn't dedup, so "a_a" is a legal two-property name.
func isProvablySingleProperty(propName string) bool {
	gaps := strings.Count(propName, "_")
	if gaps == 0 {
		return true
	}
	if gaps >= maxProvablySinglePropertyTokens {
		return false
	}
	tokens := strings.Split(propName, "_")
	for cuts := 1; cuts < 1<<gaps; cuts++ {
		if isNonDecreasingSplit(tokens, cuts) {
			return false
		}
	}
	return true
}

// isNonDecreasingSplit reports whether splitting tokens at the gaps set in cuts
// yields parts in non-decreasing order. Bit i of cuts is the gap after
// tokens[i].
func isNonDecreasingSplit(tokens []string, cuts int) bool {
	prev, start := "", 0
	for i := range tokens {
		if i < len(tokens)-1 && cuts&(1<<i) == 0 {
			continue
		}
		part := strings.Join(tokens[start:i+1], "_")
		if start > 0 && part < prev {
			return false
		}
		prev, start = part, i+1
	}
	return true
}

// migrationDirBase strips a tracker dir's generation suffix, returning any
// name without one unchanged — a pre-[genSuffix] tracker, but also any
// directory that is no tracker at all, which the caller's prefix check
// rejects.
func migrationDirBase(name string) string {
	if base, _, ok := parseMigrationDirName(name); ok {
		return base
	}
	return name
}

// inScopeFailingOpen additionally reports a payload that exists but could not
// be read or parsed. Deletion ([migrationDirScope.inScope]) ignores it and
// keeps the no-payload fallback (guessing wider could remove another
// property's tracker); the unloaded-shard gate and recovery probe fail open on
// it instead, since the narrowed fallback could wrongly report
// clean/recovered.
//
// That fail-open only covers a dir whose name leaves this property possible
// and no record names the dir. Where one does,
// [migrationDirScope.taskProperties] answers from it, unreadablePayload stays
// false, and both probes decide from that list instead.
//
// An intact payload still requires the exact sorted-name reconstruction —
// unreachable from real writers, which always derive the name and payload
// from the same sorted list.
func (s migrationDirScope) inScopeFailingOpen(name string) (matched, unreadablePayload bool) {
	base := migrationDirBase(name)
	if s.classDir != "" && base == s.classDir {
		return true, false
	}
	if !s.hasStrategyPrefix(base) {
		// Not this cleanup's dir; skip reading its payload.
		return false, false
	}
	if exact, token := s.nameArms(base); !exact && !token {
		// The name is the older half of one sorted list, so no payload of this
		// dir can name a property the name left out.
		return false, false
	}
	props, ok, unreadable := s.taskProperties(name)
	if ok {
		if !slices.Contains(props, s.propName) {
			return false, false
		}
		for _, prefix := range s.prefixes {
			if base == migrationDirWithProps(prefix, props) {
				return true, false
			}
		}
		return false, false
	}
	for _, prefix := range s.prefixes {
		if base == migrationDirWithProps(prefix, []string{s.propName}) {
			return true, unreadable
		}
	}
	return false, unreadable
}

// namesPropertyToken reports whether base is prefix + "_" + a property list
// that carries propName as one whole "_"-delimited token, e.g.
// "enable_filterable_a_b" for propName "a".
//
// This also matches a property whose own name extends propName across "_"
// (e.g. "a_b" produces the same dir as a list containing "a"+"b") — see
// [migrationDirScope] for why over-matching is the safe direction here. The
// single-token shape (props == propName) is handled earlier, by the exact
// match in [migrationDirScope.inScopeFailingOpen].
func namesPropertyToken(base, prefix, propName string) bool {
	props, ok := strings.CutPrefix(base, prefix+"_")
	if !ok {
		return false
	}
	return strings.HasPrefix(props, propName+"_") ||
		strings.HasSuffix(props, "_"+propName) ||
		strings.Contains(props, "_"+propName+"_")
}

// hasStrategyPrefix reports whether a tracker dir's base could belong to one of
// this scope's strategies, whatever properties it names. A bare prefix naming
// no properties does not qualify, in either scope.
func (s migrationDirScope) hasStrategyPrefix(base string) bool {
	for _, prefix := range s.prefixes {
		if strings.HasPrefix(base, prefix+"_") {
			return true
		}
	}
	return false
}

// taskProperties returns the property list the task recorded in its
// tracker dir. ok=false, unreadable=false means the task recorded nothing
// (no payload file, or one naming no property); unreadable=true means a
// payload exists but couldn't be read, so "recorded nothing" isn't a safe
// conclusion.
func (s migrationDirScope) taskProperties(name string) (props []string, ok, unreadable bool) {
	// The record is authoritative and costs nothing: it was already read to
	// build the preserve set. payload.mig is the fallback for a directory no
	// record names, and parsing it costs megabytes per tracker inside the
	// RAFT apply that holds the FSM loop cluster-wide.
	if rec, found := migrationRecordForTracker(s.records, name); found {
		return rec.Subject().Properties, len(rec.Subject().Properties) > 0, false
	}
	migDir := filepath.Join(s.lsmPath, ".migrations", name)
	answer := s.props.lookup(migDir)
	return answer.props, answer.ok, answer.unreadable
}

// taskPropsCache memoizes parsed tracker payloads; a nil cache reads every
// time. Not safe for concurrent use.
//
// Lives no longer than one cleanup pass, or one gate run over a tuple grid
// ([dirNamesCache.trackerProps]): a hydrated shard stops consulting the memo,
// so it never drives a sweep decision off a stale snapshot
// ([DB.NewStalePartialReindexSweep]).
type taskPropsCache struct {
	byDir map[string]taskProps
	reads int
}

// taskProps is one [migrationDirScope.taskProperties] answer. migrationType
// travels with the property list because the orphan audit needs both to name
// the sidecar buckets a tracker owns, and both come from the same read.
type taskProps struct {
	props         []string
	migrationType ReindexMigrationType
	ok            bool
	unreadable    bool
	// taskID, taskVersion and unitID are the migration's identity, which the
	// orphan audit needs to ask whether the task owning a record-less tracker
	// is still live before it reclaims one.
	taskID      string
	taskVersion uint64
	unitID      string
}

// lookup answers for one tracker dir. The memo is keyed by dir alone — safe
// because the answer is a pure function of the directory: no strategy prefix
// is a prefix of another, so at most one can ever satisfy a given dir name.
//
// properties.mig is consulted here rather than by the caller so a memo hit
// skips it too: on a shard with many completed trackers the stat-and-read it
// costs is paid once per index type of the same sweep otherwise.
func (c *taskPropsCache) lookup(migDir string) taskProps {
	if c == nil {
		answer, _ := readTrackerProps(migDir)
		return answer
	}
	if answer, hit := c.byDir[migDir]; hit {
		return answer
	}
	answer, readPayload := readTrackerProps(migDir)
	if c.byDir == nil {
		c.byDir = map[string]taskProps{}
	}
	c.byDir[migDir] = answer
	if readPayload {
		c.reads++
	}
	return answer
}

// readTrackerProps is one tracker directory's property list, taken from
// properties.mig where that small sidecar reconstructs the directory's own
// name and from payload.mig otherwise. readPayload reports whether the
// megabyte-scale payload had to be opened, which is the cost the caller counts.
func readTrackerProps(migDir string) (answer taskProps, readPayload bool) {
	if props, ok := propsFromSidecar(migDir, migrationPerPropertyDirPrefixes()); ok {
		return taskProps{props: props, ok: true}, false
	}
	return readTaskProps(migDir)
}

// count is how many payloads this cache had to read; a refusal opens none.
func (c *taskPropsCache) count() int {
	if c == nil {
		return 0
	}
	return c.reads
}

// readTaskProps answers from payload.mig, which costs megabytes per tracker
// on a large migration inside a RAFT apply holding the FSM loop cluster-wide.
// A payload over [maxRecoveryPayloadBytes] is refused rather than parsed, and
// reads the same as an unparseable one — fail-open, never fail-wrong:
// deletion falls back to the dir's own name, preservation and the
// unloaded-shard gate err toward keeping more.
//
// A property name that isn't a single directory entry makes the whole payload
// unreadable: sweeps and the orphan audit compose bucket/sidecar names from it
// and remove those, and unlike a record's names these never passed
// [validateMigrationHandles] (a restored archive can carry anything here).
//
// readPayload reports whether payload.mig was opened, so the caller's read
// counter keeps meaning what it says. A refusal opens nothing.
func readTaskProps(migDir string) (answer taskProps, readPayload bool) {
	facts, err := readRecoveryPayloadFacts(migDir)
	if err != nil {
		if os.IsNotExist(err) {
			return taskProps{}, false
		}
		return taskProps{unreadable: true}, !errors.Is(err, errRecoveryPayloadTooLarge)
	}
	answer = taskProps{
		migrationType: facts.migrationType,
		taskID:        facts.taskID,
		taskVersion:   facts.taskVersion,
		unitID:        facts.unitID,
	}
	for _, prop := range facts.properties {
		if !migrationHandleIsOneElement(prop) {
			return taskProps{unreadable: true}, true
		}
	}
	if len(facts.properties) == 0 {
		return answer, true
	}
	answer.props = facts.properties
	answer.ok = true
	return answer, true
}

// propsFromSidecar accepts properties.mig's list only if it reconstructs
// the tracker dir's own name — an independent witness that catches a
// truncated, deduped, or contradicting list for free.
//
// This is load-bearing: a wrong list accepted here decides the scope of a
// deletion sweep ([migrationDirScope.inScopeFailingOpen]) and names the
// sidecar dirs of a tracker no record names ([migrationLegacyMarkerTrackersAt])
// — for a completed migration those dirs hold the property's only copy.
func propsFromSidecar(migDir string, prefixes []string) ([]string, bool) {
	if _, err := os.Stat(filepath.Join(migDir, reindexRecoveryPayloadFile)); err != nil {
		return nil, false
	}
	props, err := readMigrationProps(migDir)
	if err != nil || len(props) == 0 {
		return nil, false
	}
	base := migrationDirBase(filepath.Base(migDir))
	for _, prefix := range prefixes {
		if base == migrationDirWithProps(prefix, props) {
			return props, true
		}
	}
	return nil, false
}
