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
	"hash/fnv"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func fixtureStrategyOf(t *testing.T, trackerName string) (MigrationStrategyCode, ReindexMigrationType) {
	t.Helper()
	for _, known := range []struct {
		prefix string
		code   MigrationStrategyCode
		mType  ReindexMigrationType
	}{
		{MigrationDirSearchableMapToBlockmax, StrategyCodeSearchableMapToBlockmax, ReindexTypeChangeAlgorithm},
		{MigrationDirFilterableRoaringsetRefresh, StrategyCodeFilterableRoaringsetRefresh, ReindexTypeRepairFilterable},
		{MigrationDirPrefixFilterableToRangeable, StrategyCodeFilterableToRangeable, ReindexTypeEnableRangeable},
		{MigrationDirPrefixSearchableRetokenize, StrategyCodeSearchableRetokenize, ReindexTypeChangeTokenization},
		{MigrationDirPrefixFilterableRetokenize, StrategyCodeFilterableRetokenize, ReindexTypeChangeTokenizationFilterable},
		{MigrationDirPrefixEnableFilterable, StrategyCodeEnableFilterable, ReindexTypeEnableFilterable},
		{MigrationDirPrefixEnableSearchable, StrategyCodeEnableSearchable, ReindexTypeEnableSearchable},
		{MigrationDirPrefixRebuildSearchable, StrategyCodeRebuildSearchable, ReindexTypeRebuildSearchable},
	} {
		if strings.HasPrefix(trackerName, known.prefix) {
			return known.code, known.mType
		}
	}
	require.FailNowf(t, "no strategy owns this tracker dir name", "%q", trackerName)
	return "", ""
}

func fixtureRecordVersion(trackerName string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(trackerName))
	// Zero is not a valid generation, and the record loader rejects it.
	if v := h.Sum64(); v != 0 {
		return v
	}
	return 1
}

func fixtureSidecarFor(staged string) string {
	if reindex := strings.Replace(staged, "_ingest_", "_reindex_", 1); reindex != staged {
		return reindex
	}
	return staged + "__reindex"
}

func mkMigrationRecordAt(t *testing.T, lsmPath, trackerName string,
	staged, canonical map[string]string, state MigrationState,
) {
	t.Helper()
	code, migrationType := fixtureStrategyOf(t, trackerName)
	subject := MigrationSubject{
		Key: MigrationRecordKey{
			TaskVersion:  fixtureRecordVersion(trackerName),
			StrategyCode: code,
			UnitID:       "shard-1__node-0",
		},
		TaskID:        "fixture:" + trackerName,
		MigrationType: migrationType,
		TrackerDir:    trackerName,
		StagedDirs:    staged,
		CanonicalDirs: canonical,
		SidecarDirs:   map[string]string{},
	}
	for prop, dir := range staged {
		subject.Properties = append(subject.Properties, prop)
		subject.SidecarDirs[prop] = fixtureSidecarFor(dir)
	}
	sort.Strings(subject.Properties)

	var rec MigrationRecord
	switch state {
	case MigrationStateIterating:
		rec = NewMigrationRecordIterating(subject, MigrationCheckpoint{})
	case MigrationStateSwapped:
		rec = NewMigrationRecordSwapped(subject, subject.Properties, canonical)
	case MigrationStatePromoted:
		rec = NewMigrationRecordPromoted(subject, subject.Properties, canonical)
	default:
		require.FailNowf(t, "unsupported fixture state", "%q", state)
	}
	logger, _ := test.NewNullLogger()
	require.NoError(t, NewMigrationRecordStore(lsmPath, logger).Put(rec))
}

// A shard load re-creates the canonical bucket directory, empty, for every
// property in the schema, and the store rewrites its own segment names under
// the promoted one. So neither the presence of a canonical directory nor
// anything inside it says whether a promotion's rename put the migration's
// data there. These tests drive real shard loads and pin both answers
// promotion has to give.

const (
	// promoteRenamedProp is the property whose rename really runs.
	promoteRenamedProp = "title"
	// promoteBlockedProp keeps the pass from settling, which is what leaves the
	// record at Swapped with the rename already behind it.
	promoteBlockedProp = "body"

	promoteTracker = "rebuild_searchable_pair_1"
)

func promoteStagedDir(prop string) string {
	return "property_" + prop + "_searchable__rebuild_searchable_ingest_1"
}

// aPromoteShard is a loaded shard whose canonical searchable buckets hold
// terms drawn from vocabulary, one per property in props.
func aPromoteShard(t *testing.T, ctx context.Context, props []string, vocabulary string) (*Shard, *Index, *models.Class) {
	t.Helper()
	className := "Promote" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, props)
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	for _, obj := range promoteObjects(className, props, vocabulary, 20) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	return shard, idx, class
}

// promoteObjects gives every property terms to lose, so an assertion can tell
// an emptied bucket from a populated one and one shard's data from another's.
func promoteObjects(className string, props []string, vocabulary string, n int) []*storobj.Object {
	objs := make([]*storobj.Object, n)
	for i := range objs {
		term := vocabulary + string(rune('a'+i%20))
		properties := map[string]interface{}{}
		for _, prop := range props {
			properties[prop] = term
		}
		objs[i] = &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:         strfmt.UUID(uuid.NewString()),
				Class:      className,
				Properties: properties,
			},
			Vector: []float32{float32(i)},
		}
	}
	return objs
}

// copyDirTree copies a bucket directory under a new name, which is how a test
// gets a staged directory holding data the canonical one does not.
func copyDirTree(t *testing.T, from, to string) {
	t.Helper()
	require.NoError(t, filepath.WalkDir(from, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(from, path)
		if err != nil {
			return err
		}
		target := filepath.Join(to, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		src, err := os.Open(path)
		if err != nil {
			return err
		}
		defer src.Close()
		dst, err := os.Create(target)
		if err != nil {
			return err
		}
		defer dst.Close()
		_, err = io.Copy(dst, src)
		return err
	}))
}

// reloadShardFromDisk is one process restart: reconciliation runs first, then
// the bucket init that re-creates a canonical directory for every schema
// property. Only a real load puts those two in that order.
func reloadShardFromDisk(t *testing.T, ctx context.Context, idx *Index, shard *Shard,
	class *models.Class,
) *Shard {
	t.Helper()
	name := shard.Name()
	require.NoError(t, shard.Shutdown(ctx))
	simulateProcessRestartBucketCleanup(t, shard.pathLSM())
	return openShardFromDisk(t, ctx, idx, class, name)
}

// openShardFromDisk loads a shut-down shard back off disk and registers it,
// which is what runs reconciliation and then the bucket init after it.
func openShardFromDisk(t *testing.T, ctx context.Context, idx *Index,
	class *models.Class, name string,
) *Shard {
	t.Helper()
	loaded, err := idx.initShard(ctx, name, class, nil, true, true)
	require.NoError(t, err)
	idx.shards.Store(name, loaded)
	return loaded.(*Shard)
}

// migrationRecordStates reads the records off disk, which is what a later
// load, the closure sweep, and an operator all read.
func migrationRecordStates(t *testing.T, lsmPath string) []MigrationState {
	t.Helper()
	logger, _ := test.NewNullLogger()
	store := NewMigrationRecordStore(lsmPath, logger)
	require.NoError(t, store.Load())
	states := make([]MigrationState, 0, len(store.Records()))
	for _, rec := range store.Records() {
		states = append(states, rec.State())
	}
	return states
}

func soleMigrationRecordState(t *testing.T, lsmPath string) MigrationState {
	t.Helper()
	states := migrationRecordStates(t, lsmPath)
	require.Len(t, states, 1, "the fixture plants exactly one record")
	return states[0]
}

// deleteBeforeAnyLoad is the index DELETE that lands before reconciliation has
// ever run on the record. A positive deleteAfterLoad is the load it follows,
// and zero is no DELETE at all.
const deleteBeforeAnyLoad = -1

// TestPromoteDecidesFromTheRecordNotFromTheDirectory pins two defects: a
// canonical directory an index DELETE emptied and a later load re-created
// must never read as an already-run promotion, and a rename that really ran
// must still count even once its only on-disk evidence (the bucket's file
// names) is gone to a WAL flush or an ordinary write.
func TestPromoteDecidesFromTheRecordNotFromTheDirectory(t *testing.T) {
	tests := []struct {
		name string
		// props are the record's properties. The first one's staged directory
		// is planted before any load; the rest arrive only after the first
		// load, which is what keeps the first pass from settling the record
		// while the first property's rename has already run.
		props []string
		// liveDonor takes the staged copy from a donor that was never shut
		// down, so it holds a write-ahead log rather than a flushed segment
		// and the first clean restart renames the file underneath it.
		liveDonor bool
		// emptyStaged plants the first property's staged directory holding
		// nothing, which is what a promotion of a property with no values
		// renames onto the canonical name.
		emptyStaged bool
		// writeAfterLoad1, when set, is the vocabulary an ordinary write puts
		// into the shard once the first load has promoted.
		writeAfterLoad1 string
		// deleteAfterLoad says when the index DELETE that removes the promoted
		// property's directories runs.
		deleteAfterLoad int
		// loads counts every shard load the row runs.
		loads int
		// wantRecordSwept says the closure sweep removed the record, which only
		// a migration that promoted every property reaches.
		wantRecordSwept bool
		wantState       MigrationState
		// wantRenamedTerms is what the promoted property's canonical bucket
		// must hold at the end: "donor" for the staged copy's vocabulary,
		// "written" for what an ordinary write put there afterwards, or "" for
		// nothing.
		wantRenamedTerms string
		reason           string
	}{
		{
			name:            "an index DELETE took both directories before the rename could run",
			props:           []string{promoteRenamedProp},
			deleteAfterLoad: deleteBeforeAnyLoad,
			// Load 1 finds neither directory and re-creates the canonical one
			// empty; load 2 is the pass that would read the re-created
			// directory as the rename's output.
			loads:            2,
			wantState:        MigrationStateSwapped,
			wantRenamedTerms: "",
			reason: "the record must not read Promoted over a canonical directory the shard load re-created: " +
				"no promotion of this property ever started",
		},
		{
			name:  "the staged directory is there to promote",
			props: []string{promoteRenamedProp},
			loads: 1,
			// The staged copy carries a vocabulary the canonical bucket never
			// had, so only a rename that really ran can put it there. The
			// closure sweep is a later load's work, so the record is still
			// here, saying Promoted.
			wantState:        MigrationStatePromoted,
			wantRenamedTerms: "donor",
			reason:           "a promotion whose staged directory is present must still run and move its data",
		},
		{
			name:            "an index DELETE took the directory the rename produced",
			props:           []string{promoteRenamedProp, promoteBlockedProp},
			deleteAfterLoad: 1,
			// Load 2 finds neither directory and re-creates the canonical one
			// empty; load 3 is the one that would read it as its own work.
			loads:            3,
			wantState:        MigrationStateSwapped,
			wantRenamedTerms: "",
			reason: "the record must not read Promoted over a canonical directory the shard load re-created: " +
				"the data this migration renamed onto that name is gone",
		},
		{
			name:  "nothing takes the directory the rename produced",
			props: []string{promoteRenamedProp, promoteBlockedProp},
			// Load 2 promotes the sibling and settles the record; load 3 is the
			// closure sweep that removes it.
			loads:            3,
			wantRecordSwept:  true,
			wantRenamedTerms: "donor",
			reason:           "a promotion whose renamed data is still under the canonical name must complete and close",
		},
		{
			// The promoted bucket's segment is still a write-ahead log when the
			// rename runs, and the first clean shutdown rewrites it under a
			// different name. Nothing about the data changes.
			name:             "the promoted data is renamed underneath the promotion by an ordinary restart",
			props:            []string{promoteRenamedProp, promoteBlockedProp},
			liveDonor:        true,
			loads:            3,
			wantRecordSwept:  true,
			wantRenamedTerms: "donor",
			reason: "a promotion whose renamed data is under the canonical name must complete and close, " +
				"whatever the store has since called the files holding it",
		},
		{
			// A property with no values promotes an empty bucket. An ordinary
			// write into it afterwards is the shard doing its job, and must not
			// make the promotion unfinishable.
			name:             "an ordinary write lands in the empty bucket the promotion produced",
			props:            []string{promoteRenamedProp, promoteBlockedProp},
			emptyStaged:      true,
			writeAfterLoad1:  "written",
			loads:            3,
			wantRecordSwept:  true,
			wantRenamedTerms: "written",
			reason:           "a promotion that moved no data must still complete and close",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			shard, idx, class := aPromoteShard(t, ctx, tc.props, "canonical")
			lsmPath := shard.pathLSM()
			renamedCanonical := helpers.BucketSearchableFromPropNameLSM(promoteRenamedProp)

			before := fingerprintInvertedBucket(t, shard.store.Bucket(renamedCanonical))
			require.NotEmpty(t, before, "fixture: the canonical bucket holds terms before anything touches it")

			// A donor shard holding a vocabulary this one never had, so the two
			// are told apart by what is in them, not by which exists.
			donor, _, _ := aPromoteShard(t, ctx, tc.props, "donor")
			donorPath := donor.pathLSM()
			donorTerms := map[string]map[string][]uint64{}
			for _, prop := range tc.props {
				donorTerms[prop] = fingerprintInvertedBucket(t,
					donor.store.Bucket(helpers.BucketSearchableFromPropNameLSM(prop)))
				require.NotEmpty(t, donorTerms[prop], "fixture: the donor bucket holds terms")
			}
			require.NotEqual(t, before, donorTerms[promoteRenamedProp], "fixture: the two vocabularies must differ")

			renamedStagedSource := filepath.Join(donorPath, renamedCanonical)
			if !tc.liveDonor {
				require.NoError(t, donor.Shutdown(ctx))
			}

			require.NoError(t, shard.Shutdown(ctx))
			simulateProcessRestartBucketCleanup(t, lsmPath)

			// Only the first property gets a staged directory now. Any sibling
			// gets its own only after the first load, which is what keeps the
			// first pass from settling the record onto Promoted.
			renamedStaged := filepath.Join(lsmPath, promoteStagedDir(promoteRenamedProp))
			if tc.emptyStaged {
				require.NoError(t, os.MkdirAll(renamedStaged, 0o755))
			} else {
				copyDirTree(t, renamedStagedSource, renamedStaged)
			}
			if tc.liveDonor {
				require.NoError(t, donor.Shutdown(ctx))
			}

			staged := map[string]string{}
			canonical := map[string]string{}
			for _, prop := range tc.props {
				staged[prop] = promoteStagedDir(prop)
				canonical[prop] = helpers.BucketSearchableFromPropNameLSM(prop)
			}
			mkTrackerDir(t, lsmPath, promoteTracker)
			mkMigrationRecordAt(t, lsmPath, promoteTracker, staged, canonical, MigrationStateSwapped)
			require.Equal(t, MigrationStateSwapped, soleMigrationRecordState(t, lsmPath), "fixture")

			if tc.deleteAfterLoad == deleteBeforeAnyLoad {
				require.NoError(t, os.RemoveAll(filepath.Join(lsmPath, renamedCanonical)))
				require.NoError(t, os.RemoveAll(renamedStaged))
			}

			current := openShardFromDisk(t, ctx, idx, class, shard.Name())
			afterFirstLoad := fingerprintInvertedBucket(t, current.store.Bucket(renamedCanonical))
			if tc.deleteAfterLoad != deleteBeforeAnyLoad && !tc.emptyStaged {
				// Receipt that the rename really ran: it is the state the rest
				// of the row acts on.
				require.Equal(t, donorTerms[promoteRenamedProp], afterFirstLoad,
					"fixture: the first load renames the staged directory onto the canonical name")
			}
			if tc.emptyStaged {
				require.Empty(t, afterFirstLoad,
					"fixture: a promotion of an empty staged directory leaves an empty bucket")
			}

			writtenTerms := map[string][]uint64{}
			if tc.writeAfterLoad1 != "" {
				for _, obj := range promoteObjects(class.Class, tc.props, tc.writeAfterLoad1, 20) {
					require.NoError(t, current.PutObject(ctx, obj))
				}
				writtenTerms = fingerprintInvertedBucket(t, current.store.Bucket(renamedCanonical))
				require.NotEmpty(t, writtenTerms, "fixture: the write reaches the promoted bucket")
			}
			if tc.deleteAfterLoad == 1 {
				// What a RAFT apply of an index DELETE does: shut the bucket
				// down and remove its directory.
				require.NoError(t, current.removeBucket(ctx, renamedCanonical))
			}

			// Every sibling's staged directory arrives, so the promotion of the
			// first property is the only thing left deciding the record.
			for _, prop := range tc.props[1:] {
				copyDirTree(t,
					filepath.Join(donorPath, helpers.BucketSearchableFromPropNameLSM(prop)),
					filepath.Join(lsmPath, promoteStagedDir(prop)))
			}

			for load := 2; load <= tc.loads; load++ {
				current = reloadShardFromDisk(t, ctx, idx, current, class)
			}
			defer current.Shutdown(ctx)

			got := fingerprintInvertedBucket(t, current.store.Bucket(renamedCanonical))
			switch tc.wantRenamedTerms {
			case "":
				require.Empty(t, got,
					"fixture: the canonical bucket must hold nothing, or promoting it would lose nothing")
			case "written":
				assert.Equal(t, writtenTerms, got,
					"the canonical bucket must still hold what was written into it after the promotion")
			default:
				assert.Equal(t, donorTerms[promoteRenamedProp], got,
					"the canonical bucket must hold the data the promotion renamed onto it")
			}
			for _, prop := range tc.props[1:] {
				assert.Equal(t, donorTerms[prop],
					fingerprintInvertedBucket(t, current.store.Bucket(helpers.BucketSearchableFromPropNameLSM(prop))),
					"the sibling's own promotion must still run and move its data")
			}

			if tc.wantRecordSwept {
				assert.Empty(t, migrationRecordStates(t, lsmPath), tc.reason)
				assert.NoDirExists(t, filepath.Join(lsmPath, migrationsDir, promoteTracker),
					"a record that closes takes its tracker directory with it")
			} else {
				assert.Equal(t, tc.wantState, soleMigrationRecordState(t, lsmPath), tc.reason)
			}
		})
	}
}
