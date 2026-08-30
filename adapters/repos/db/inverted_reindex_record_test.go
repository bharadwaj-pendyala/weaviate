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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
)

func testMigrationSubject(version uint64, code MigrationStrategyCode, props ...string) MigrationSubject {
	subject := MigrationSubject{
		Key:                  MigrationRecordKey{TaskVersion: version, StrategyCode: code, UnitID: "shard-1__node-0"},
		TaskID:               "Books:change-tokenization:title:ab12",
		MigrationType:        ReindexTypeChangeTokenization,
		Properties:           props,
		TargetTokenization:   models.PropertyTokenizationLowercase,
		OriginalTokenization: models.PropertyTokenizationWord,
		TrackerDir:           fmt.Sprintf("m_%d_tracker", version),
		// A real past horizon: with a zero one, an assertion that a horizon
		// was kept or raised compares zero against zero and cannot fail.
		IterationCutoff: time.Date(2026, 8, 21, 9, 0, 0, 0, time.UTC),
	}
	if len(props) == 0 {
		return subject
	}

	subject.StagedDirs = map[string]string{}
	subject.CanonicalDirs = map[string]string{}
	subject.SidecarDirs = map[string]string{}
	for _, prop := range props {
		subject.StagedDirs[prop] = fmt.Sprintf("property_%s__g%d_ingest", prop, version)
		subject.CanonicalDirs[prop] = "property_" + prop
		subject.SidecarDirs[prop] = fmt.Sprintf("property_%s__s%d_reindex", prop, version)
	}
	return subject
}

func TestMigrationRecordRoundTrip(t *testing.T) {
	checkpoint := MigrationCheckpoint{
		LastProcessedKey: []byte{0xDE, 0xAD, 0xBE, 0xEF},
		ProcessedCount:   1200,
		IndexedCount:     980,
		UpdatedAt:        time.Date(2026, 8, 21, 10, 0, 0, 123456789, time.UTC),
	}
	displaced := map[string]string{"title": "property_title"}

	tests := []struct {
		name      string
		record    MigrationRecord
		wantState MigrationState
	}{
		{
			name:      "iterating carries the checkpoint",
			record:    NewMigrationRecordIterating(testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title"), checkpoint),
			wantState: MigrationStateIterating,
		},
		{
			name:      "iterated",
			record:    NewMigrationRecordIterated(testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")),
			wantState: MigrationStateIterated,
		},
		{
			name:      "merged",
			record:    NewMigrationRecordMerged(testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title", "body")),
			wantState: MigrationStateMerged,
		},
		{
			name:      "swapped carries the flip set and the displaced handles",
			record:    NewMigrationRecordSwapped(testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title"), []string{"title"}, displaced),
			wantState: MigrationStateSwapped,
		},
		{
			name: "swapped carries the promotion it started, which is the only thing that makes a missing staged dir readable",
			record: NewMigrationRecordSwapped(testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title"),
				[]string{"title"}, displaced).WithPromotionAt("title", migrationPromotionStarted),
			wantState: MigrationStateSwapped,
		},
		{
			name:      "promoted keeps the flip block so a partly failed retirement is still attributable",
			record:    NewMigrationRecordPromoted(testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title"), []string{"title"}, displaced),
			wantState: MigrationStatePromoted,
		},
		{
			name:      "class-level migration with no properties keeps its nil maps nil",
			record:    NewMigrationRecordMerged(testMigrationSubject(7, StrategyCodeSearchableMapToBlockmax)),
			wantState: MigrationStateMerged,
		},
		{
			// An empty handle is the ordinary "this record names none", and
			// two properties naming none are not two properties naming one
			// directory. Without that reading the one-owner check refuses a
			// record nothing is wrong with.
			name: "two properties that displaced nothing still name one owner each",
			record: NewMigrationRecordSwapped(testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title", "body"),
				[]string{"title", "body"}, map[string]string{"title": "", "body": ""}),
			wantState: MigrationStateSwapped,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := encodeMigrationRecord(tt.record)
			require.NoError(t, err)
			require.True(t, bytes.Contains(encoded, []byte("\n  ")), "records are indented so an operator can read one with cat")

			decoded, err := decodeMigrationRecord(encoded)
			require.NoError(t, err)
			require.Equal(t, tt.wantState, decoded.State())
			require.Equal(t, tt.record, decoded)
		})
	}
}

// TestAFlipKeyThisBuildDoesNotKnowStillDecodes is the other half of the
// promotion contract. A mark this build cannot read is refused, because acting
// on it would mean promoting on a claim it cannot check. A whole key it does
// not know is not: json.Unmarshal drops it, the record decodes with no
// promotion recorded, and the shard promotes nothing rather than freezing every
// removal on it.
func TestAFlipKeyThisBuildDoesNotKnowStillDecodes(t *testing.T) {
	subject := testMigrationSubject(42, StrategyCodeEnableFilterable, "title")
	swapped := NewMigrationRecordSwapped(subject, []string{"title"},
		map[string]string{"title": "property_title"})

	encoded, err := encodeMigrationRecord(swapped)
	require.NoError(t, err)
	env := map[string]any{}
	require.NoError(t, json.Unmarshal(encoded, &env))
	env["flip"].(map[string]any)["someLaterBuildsBlock"] = map[string]any{"title": "whatever"}
	data, err := json.Marshal(env)
	require.NoError(t, err)

	decoded, err := decodeMigrationRecord(data)
	require.NoError(t, err)
	require.Equal(t, swapped, decoded)
	require.Empty(t, decoded.(MigrationRecordSwapped).PromotionOf("title"),
		"a key this build cannot read licenses no promotion")
}

func TestMigrationRecordNotUnderstood(t *testing.T) {
	valid := func(mutate func(env map[string]any)) []byte {
		encoded, err := encodeMigrationRecord(NewMigrationRecordMerged(testMigrationSubject(42, StrategyCodeEnableFilterable, "title")))
		require.NoError(t, err)
		env := map[string]any{}
		require.NoError(t, json.Unmarshal(encoded, &env))
		mutate(env)
		out, err := json.Marshal(env)
		require.NoError(t, err)
		return out
	}

	// twoProperties gives the fixture a second property with its own three
	// directories, so a row can collide exactly one pair of them.
	twoProperties := func(mutate func(subject, env map[string]any)) []byte {
		return valid(func(env map[string]any) {
			subject := env["subject"].(map[string]any)
			subject["properties"] = []string{"title", "body"}
			subject["stagedDirs"].(map[string]any)["body"] = "property_body__g42_ingest"
			subject["canonicalDirs"].(map[string]any)["body"] = "property_body"
			subject["sidecarDirs"].(map[string]any)["body"] = "property_body__s42_reindex"
			mutate(subject, env)
		})
	}

	// wantErr is what each row's refusal has to say. Without it a build that
	// collapsed every validation into one error would keep all of these green.
	tests := []struct {
		name    string
		data    []byte
		wantErr string
	}{
		{
			name: "not json at all", data: []byte("this is not a record"),
			wantErr: "decode record:",
		},
		{
			name: "truncated mid-write", data: []byte(`{"formatVersion":1,"state":"mer`),
			wantErr: "unexpected end of JSON input",
		},
		{name: "empty file", data: nil, wantErr: "unexpected end of JSON input"},
		{
			name:    "format version from a future build",
			data:    valid(func(env map[string]any) { env["formatVersion"] = 99 }),
			wantErr: "unknown record format version 99",
		},
		{
			name:    "state this build does not know",
			data:    valid(func(env map[string]any) { env["state"] = "tidied" }),
			wantErr: "names unknown state \"tidied\"",
		},
		{
			name: "migration type this build does not know",
			data: valid(func(env map[string]any) {
				env["subject"].(map[string]any)["migrationType"] = "reticulate-splines"
			}),
			wantErr: "names unknown migration type \"reticulate-splines\"",
		},
		{
			name:    "task ID missing",
			data:    valid(func(env map[string]any) { env["subject"].(map[string]any)["taskID"] = "" }),
			wantErr: "has no task ID",
		},
		{
			name: "checkpoint on a state that has none",
			data: valid(func(env map[string]any) {
				env["checkpoint"] = map[string]any{"processedCount": 1}
			}),
			wantErr: "in state \"merged\": checkpoint block present=true, wanted=false",
		},
		{
			name: "flip block on a state that has none",
			data: valid(func(env map[string]any) {
				env["flip"] = map[string]any{"flipped": []string{"title"}}
			}),
			wantErr: "in state \"merged\": flip block present=true, wanted=false",
		},
		{
			name:    "iterating without its checkpoint",
			data:    valid(func(env map[string]any) { env["state"] = string(MigrationStateIterating) }),
			wantErr: "in state \"iterating\": checkpoint block present=false, wanted=true",
		},
		{
			name:    "swapped without its flip block",
			data:    valid(func(env map[string]any) { env["state"] = string(MigrationStateSwapped) }),
			wantErr: "in state \"swapped\": flip block present=false, wanted=true",
		},
		{
			name:    "promoted without its flip block",
			data:    valid(func(env map[string]any) { env["state"] = string(MigrationStatePromoted) }),
			wantErr: "in state \"promoted\": flip block present=false, wanted=true",
		},
		{
			// Promotion removes the displaced directory and then renames the
			// staged one onto the canonical name. With the two equal it
			// removes the only copy and the rename finds nothing.
			name: "a flip that displaced the directory it staged",
			data: valid(func(env map[string]any) {
				subject := env["subject"].(map[string]any)
				env["state"] = string(MigrationStateSwapped)
				env["flip"] = map[string]any{
					"flipped":       []string{"title"},
					"displacedDirs": map[string]any{"title": subject["stagedDirs"].(map[string]any)["title"]},
				}
			}),
			wantErr: `names directory "property_title__g42_ingest" as both the staged directory of property "title" and the displaced directory of property "title"`,
		},
		{
			// Promoting title removes body's only staged copy, and body then
			// reads staged-absent plus canonical-present as an already-run
			// promotion, settling on the empty canonical arming pre-created.
			name: "a flip that displaced another property's staged directory",
			data: valid(func(env map[string]any) {
				subject := env["subject"].(map[string]any)
				subject["properties"] = []string{"title", "body"}
				staged := subject["stagedDirs"].(map[string]any)
				staged["body"] = "property_body__g42_ingest"
				subject["canonicalDirs"].(map[string]any)["body"] = "property_body"
				env["state"] = string(MigrationStateSwapped)
				env["flip"] = map[string]any{
					"flipped":       []string{"title", "body"},
					"displacedDirs": map[string]any{"title": staged["body"]},
				}
			}),
			wantErr: `names directory "property_body__g42_ingest" as both the staged directory of property "body" and the displaced directory of property "title"`,
		},
		{
			// ShutdownStagedBuckets closes the directory the property it is
			// handed names, so a shared name takes a bucket down under a
			// property that is still serving from it.
			name: "two properties naming the same sidecar directory",
			data: valid(func(env map[string]any) {
				subject := env["subject"].(map[string]any)
				subject["properties"] = []string{"title", "body"}
				sidecars := subject["sidecarDirs"].(map[string]any)
				sidecars["body"] = sidecars["title"]
			}),
			wantErr: `names directory "property_title__s42_reindex" as both the sidecar directory of property "body" and the sidecar directory of property "title"`,
		},
		{
			// A started promotion is what lets a missing staged directory read
			// as this migration's own rename, so one recorded for a property
			// the record does not carry is the claim that must not be trusted.
			name: "a promotion recorded for a property the record does not name",
			data: valid(func(env map[string]any) {
				env["state"] = string(MigrationStateSwapped)
				env["flip"] = map[string]any{
					"flipped":       []string{"title"},
					"displacedDirs": map[string]any{"title": "property_title"},
					"promotion":     map[string]any{"body": "started"},
				}
			}),
			wantErr: `records a promotion of property "body", which it does not name`,
		},
		{
			// Only a swapped record is part way through a promotion. On any
			// other state the mark describes a step that state has already
			// left behind, and re-encoding the record drops it silently.
			name: "a promotion mark on a state that has no promotion to be part way through",
			data: valid(func(env map[string]any) {
				env["state"] = string(MigrationStatePromoted)
				env["flip"] = map[string]any{
					"flipped":       []string{"title"},
					"displacedDirs": map[string]any{"title": "property_title"},
					"promotion":     map[string]any{"title": "finished"},
				}
			}),
			wantErr: `in state "promoted" carries promotion marks, which only a swapped record does`,
		},
		{
			// A mark this build cannot read is a claim about how far a
			// promotion got that it has no way to act on.
			name: "a promotion mark this build does not know",
			data: valid(func(env map[string]any) {
				env["state"] = string(MigrationStateSwapped)
				env["flip"] = map[string]any{
					"flipped":       []string{"title"},
					"displacedDirs": map[string]any{"title": "property_title"},
					"promotion":     map[string]any{"title": "bogus"},
				}
			}),
			wantErr: `records unknown promotion mark "bogus" for property "title"`,
		},
		// The same harm in every remaining pairing of the four roles a record
		// hands out: one property's teardown closes or deletes the directory
		// another property is still serving from.
		{
			name: "two properties naming the same staged directory",
			data: twoProperties(func(subject, _ map[string]any) {
				staged := subject["stagedDirs"].(map[string]any)
				staged["body"] = staged["title"]
			}),
			wantErr: `names directory "property_title__g42_ingest" as both the staged directory of property "body" and the staged directory of property "title"`,
		},
		{
			name: "a property staged into another property's sidecar directory",
			data: twoProperties(func(subject, _ map[string]any) {
				subject["stagedDirs"].(map[string]any)["body"] = subject["sidecarDirs"].(map[string]any)["title"]
			}),
			wantErr: `names directory "property_title__s42_reindex" as both the staged directory of property "body" and the sidecar directory of property "title"`,
		},
		{
			name: "a property staged into its own sidecar directory",
			data: twoProperties(func(subject, _ map[string]any) {
				subject["stagedDirs"].(map[string]any)["title"] = subject["sidecarDirs"].(map[string]any)["title"]
			}),
			wantErr: `names directory "property_title__s42_reindex" as both the staged directory of property "title" and the sidecar directory of property "title"`,
		},
		{
			name: "two properties naming the same canonical directory",
			data: twoProperties(func(subject, _ map[string]any) {
				canonical := subject["canonicalDirs"].(map[string]any)
				canonical["body"] = canonical["title"]
			}),
			wantErr: `names directory "property_title" as both the canonical directory of property "body" and the canonical directory of property "title"`,
		},
		{
			// The canonical directory here is a previous migration's staged
			// name, which is the shape a property serves from between a flip
			// and its promotion. The per-role shapes allow it, so this row
			// reaches the one-owner rule rather than being refused earlier.
			name: "a property staged into another property's canonical directory",
			data: twoProperties(func(subject, _ map[string]any) {
				canonical := subject["canonicalDirs"].(map[string]any)
				canonical["title"] = "property_title__retokenize_ingest_1"
				subject["stagedDirs"].(map[string]any)["body"] = canonical["title"]
			}),
			wantErr: `names directory "property_title__retokenize_ingest_1" as both the staged directory of property "body" and the canonical directory of property "title"`,
		},
		{
			name: "a sidecar that is another property's canonical directory",
			data: twoProperties(func(subject, _ map[string]any) {
				canonical := subject["canonicalDirs"].(map[string]any)
				canonical["title"] = "property_title__retokenize_ingest_1"
				subject["sidecarDirs"].(map[string]any)["body"] = canonical["title"]
			}),
			wantErr: `names directory "property_title__retokenize_ingest_1" as both the canonical directory of property "title" and the sidecar directory of property "body"`,
		},
		{
			// And the shape rule that would otherwise have answered those two
			// first, on the role the one-owner rule cannot see: a live bucket
			// no record covers.
			name: "a sidecar directory that is a live property bucket",
			data: valid(func(env map[string]any) {
				env["subject"].(map[string]any)["sidecarDirs"].(map[string]any)["title"] = "property_body_searchable"
			}),
			wantErr: `names sidecar directory "property_body_searchable", which is not shaped like a sidecar of a property bucket`,
		},
		{
			name: "a flip that displaced a sidecar directory",
			data: twoProperties(func(subject, env map[string]any) {
				env["state"] = string(MigrationStateSwapped)
				env["flip"] = map[string]any{
					"flipped":       []string{"title", "body"},
					"displacedDirs": map[string]any{"title": subject["sidecarDirs"].(map[string]any)["body"]},
				}
			}),
			wantErr: `names directory "property_body__s42_reindex" as both the sidecar directory of property "body" and the displaced directory of property "title"`,
		},
		{
			name: "a flip that displaced another property's canonical directory",
			data: twoProperties(func(subject, env map[string]any) {
				env["state"] = string(MigrationStateSwapped)
				env["flip"] = map[string]any{
					"flipped":       []string{"title", "body"},
					"displacedDirs": map[string]any{"title": subject["canonicalDirs"].(map[string]any)["body"]},
				}
			}),
			wantErr: `names directory "property_body" as both the canonical directory of property "body" and the displaced directory of property "title"`,
		},
		{
			name: "two properties displacing the same directory",
			data: twoProperties(func(_, env map[string]any) {
				env["state"] = string(MigrationStateSwapped)
				env["flip"] = map[string]any{
					"flipped":       []string{"title", "body"},
					"displacedDirs": map[string]any{"title": "property_shared__g41_ingest", "body": "property_shared__g41_ingest"},
				}
			}),
			wantErr: `names directory "property_shared__g41_ingest" as both the displaced directory of property "body" and the displaced directory of property "title"`,
		},
		{
			// Reclaiming this handle hands the shard's whole migration tree
			// to os.RemoveAll: every tracker and the record store with it.
			name: "a staged directory that is the shard's migrations tree",
			data: valid(func(env map[string]any) {
				env["subject"].(map[string]any)["stagedDirs"].(map[string]any)["title"] = migrationsDir
			}),
			wantErr: `names staged directory ".migrations", which is a directory no migration may own`,
		},
		{
			// The object store is the shard's whole object store, and a
			// Cancelled verdict hands every staged directory to os.RemoveAll.
			name: "a staged directory that is the object store",
			data: valid(func(env map[string]any) {
				env["subject"].(map[string]any)["stagedDirs"].(map[string]any)["title"] = "objects"
			}),
			wantErr: `names staged directory "objects", which is a directory no migration may own`,
		},
		{
			// A live bucket no record covers passes every cross-record check,
			// so only its shape distinguishes it from a migration's own copy.
			name: "a staged directory that is a live property bucket",
			data: valid(func(env map[string]any) {
				env["subject"].(map[string]any)["stagedDirs"].(map[string]any)["title"] = "property_body_searchable"
			}),
			wantErr: `names staged directory "property_body_searchable", which is not shaped like a sidecar of a property bucket`,
		},
		{
			// removeTrackerDir joins this onto .migrations, so the tracker
			// sweep of one record deletes every record on the shard.
			name: "a tracker directory that is the record store",
			data: valid(func(env map[string]any) {
				env["subject"].(map[string]any)["trackerDir"] = migrationRecordsDirName
			}),
			wantErr: `names tracker directory "records", which is a directory no migration may own`,
		},
		{
			name: "a unit the record file name could not carry",
			data: valid(func(env map[string]any) {
				env["subject"].(map[string]any)["key"].(map[string]any)["unitID"] = "../shard-2__node-0"
			}),
			wantErr: "record key \"42/enable_filterable/../shard-2__node-0\" is incomplete",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec, err := decodeMigrationRecord(tt.data)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
			require.Nil(t, rec)
		})
	}
}

func TestMigrationRecordKey(t *testing.T) {
	tests := []struct {
		name      string
		key       MigrationRecordKey
		wantFile  string
		wantValid bool
	}{
		{
			name:      "searchable half of a change-tokenization fan-out",
			key:       MigrationRecordKey{TaskVersion: 42, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-0"},
			wantFile:  "42_searchable_retokenize_shard-1__node-0.json",
			wantValid: true,
		},
		{
			name:      "filterable half of the same fan-out, same task and unit",
			key:       MigrationRecordKey{TaskVersion: 42, StrategyCode: StrategyCodeFilterableRetokenize, UnitID: "shard-1__node-0"},
			wantFile:  "42_filterable_retokenize_shard-1__node-0.json",
			wantValid: true,
		},
		{
			name:      "a later generation on the same strategy",
			key:       MigrationRecordKey{TaskVersion: 43, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-0"},
			wantFile:  "43_searchable_retokenize_shard-1__node-0.json",
			wantValid: true,
		},
		{
			name:      "generation zero is never allocated by raft",
			key:       MigrationRecordKey{TaskVersion: 0, StrategyCode: StrategyCodeEnableSearchable, UnitID: "shard-1__node-0"},
			wantFile:  "0_enable_searchable_shard-1__node-0.json",
			wantValid: false,
		},
		{
			name:      "unknown strategy code",
			key:       MigrationRecordKey{TaskVersion: 42, StrategyCode: "quantum_reindex", UnitID: "shard-1__node-0"},
			wantFile:  "42_quantum_reindex_shard-1__node-0.json",
			wantValid: false,
		},
		{
			name:      "no unit",
			key:       MigrationRecordKey{TaskVersion: 42, StrategyCode: StrategyCodeRebuildSearchable},
			wantFile:  "42_rebuild_searchable_.json",
			wantValid: false,
		},
		{
			// The same migration on the node next door. A backup or a shard
			// copy brings its record here, and it must not name our file.
			name:      "the same task and strategy on another unit",
			key:       MigrationRecordKey{TaskVersion: 42, StrategyCode: StrategyCodeSearchableRetokenize, UnitID: "shard-1__node-1"},
			wantFile:  "42_searchable_retokenize_shard-1__node-1.json",
			wantValid: true,
		},
		{
			name:      "a unit that would escape the records directory",
			key:       MigrationRecordKey{TaskVersion: 42, StrategyCode: StrategyCodeEnableFilterable, UnitID: "../shard-2__node-0"},
			wantFile:  "42_enable_filterable_../shard-2__node-0.json",
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantFile, tt.key.fileName())
			require.Equal(t, tt.wantValid, tt.key.valid())
		})
	}
}

func TestMigrationRecordStore(t *testing.T) {
	merged := func(version uint64, code MigrationStrategyCode) MigrationRecord {
		return NewMigrationRecordMerged(testMigrationSubject(version, code, "title"))
	}

	tests := []struct {
		name        string
		arrange     func(t *testing.T, s *MigrationRecordStore)
		assert      func(t *testing.T, s *MigrationRecordStore)
		wantLoadErr bool
	}{
		{
			name:    "a shard that never ran a migration loads empty",
			arrange: func(t *testing.T, s *MigrationRecordStore) {},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				require.Empty(t, s.Records())
				require.Empty(t, s.Unreadable())
			},
		},
		{
			name: "a written record survives a reload",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				require.NoError(t, s.Put(merged(42, StrategyCodeEnableFilterable)))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				got, ok := s.Get(MigrationRecordKey{TaskVersion: 42, StrategyCode: StrategyCodeEnableFilterable, UnitID: "shard-1__node-0"})
				require.True(t, ok)
				require.Equal(t, merged(42, StrategyCodeEnableFilterable), got)
			},
		},
		{
			name: "a file this build cannot place is surfaced, not deleted",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				require.NoError(t, os.MkdirAll(s.Dir(), 0o777))
				require.NoError(t, os.WriteFile(filepath.Join(s.Dir(), "99_enable_searchable.json"), []byte("{"), 0o600))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				require.Len(t, s.Unreadable(), 1)
				require.Equal(t, "99_enable_searchable.json", s.Unreadable()[0].FileName)
				_, err := os.Stat(filepath.Join(s.Dir(), "99_enable_searchable.json"))
				require.NoError(t, err, "an unreadable record must survive the load that could not read it")
			},
		},
		{
			name: "one unreadable file does not hide the readable ones",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				require.NoError(t, s.Put(merged(42, StrategyCodeEnableFilterable)))
				require.NoError(t, os.WriteFile(filepath.Join(s.Dir(), "99_enable_searchable.json"), []byte("{"), 0o600))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				require.Len(t, s.Records(), 1)
				require.Len(t, s.Unreadable(), 1)

				// The unreadable one may name any directory on this shard, so
				// the sweeps have to keep all of them and not just the ones
				// the readable record happens to name.
				logger, _ := test.NewNullLogger()
				committed := migrationPreservedStateAt(filepath.Dir(filepath.Dir(s.Dir())), logger)
				require.True(t, committed.preservesBucket("a directory no readable record names"))
				require.True(t, committed.preservesTracker("a directory no readable record names"))
			},
		},
		{
			name: "a record whose content names a different file is not trusted",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				require.NoError(t, s.Put(merged(42, StrategyCodeEnableFilterable)))
				require.NoError(t, os.Rename(
					filepath.Join(s.Dir(), "42_enable_filterable_shard-1__node-0.json"),
					filepath.Join(s.Dir(), "43_enable_filterable_shard-1__node-0.json")))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				require.Empty(t, s.Records())
				require.Len(t, s.Unreadable(), 1)
			},
		},
		{
			name: "removing a record twice is not an error",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				key := MigrationRecordKey{TaskVersion: 42, StrategyCode: StrategyCodeEnableFilterable, UnitID: "shard-1__node-0"}
				require.NoError(t, s.Put(merged(42, StrategyCodeEnableFilterable)))
				require.NoError(t, s.Remove(key))
				require.NoError(t, s.Remove(key))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				require.Empty(t, s.Records())
			},
		},
		{
			name: "a write racing a collection DELETE fails instead of re-creating the tree",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				// A DELETE renames the class directory away, and the shard's
				// LSM directory the store paths off goes with it.
				require.NoError(t, os.RemoveAll(filepath.Dir(filepath.Dir(s.Dir()))))
				require.Error(t, s.Put(merged(42, StrategyCodeEnableFilterable)))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				_, err := os.Stat(filepath.Dir(filepath.Dir(s.Dir())))
				require.True(t, os.IsNotExist(err),
					"the deleted collection's directory tree must stay deleted")
			},
		},
		{
			name: "a scratch file another writer owns survives a load and only the owner sweeps it",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				require.NoError(t, os.MkdirAll(s.Dir(), 0o777))
				require.NoError(t, os.WriteFile(filepath.Join(s.Dir(), "42_enable_filterable.json.1234"+tmpExt), []byte("half"), 0o600))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				scratch := filepath.Join(s.Dir(), "42_enable_filterable.json.1234"+tmpExt)
				require.Empty(t, s.Records())
				require.Empty(t, s.Unreadable(), "a scratch file is not a record this build failed to read")

				// A reader over someone else's directory removing this file is
				// what makes that writer's rename fail.
				logger, _ := test.NewNullLogger()
				_, _, recordSetUnreadable := migrationRecordsAt(filepath.Dir(filepath.Dir(s.Dir())), logger)
				require.False(t, recordSetUnreadable)
				_, err := os.Stat(scratch)
				require.NoError(t, err, "a foreign reader must not delete a scratch file it does not own")

				s.SweepTempFiles()
				left, err := os.ReadDir(s.Dir())
				require.NoError(t, err)
				require.Empty(t, left, "the owning store sweeps what a crash left behind")
			},
		},
		{
			// Overwriting it destroys the one artifact the freeze exists to
			// preserve, and what replaces it is a guess about the very
			// migration nobody could read.
			name: "a record this build cannot read is not overwritten by a fresh one",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				require.NoError(t, os.MkdirAll(s.Dir(), 0o777))
				require.NoError(t, os.WriteFile(
					filepath.Join(s.Dir(), "42_enable_filterable_shard-1__node-0.json"), []byte("{torn"), 0o600))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				require.Len(t, s.Unreadable(), 1)
				require.Error(t, s.Put(merged(42, StrategyCodeEnableFilterable)))

				kept, err := os.ReadFile(filepath.Join(s.Dir(), "42_enable_filterable_shard-1__node-0.json"))
				require.NoError(t, err)
				require.Equal(t, "{torn", string(kept))

				// A different migration on the same shard is not this file.
				require.NoError(t, s.Put(merged(43, StrategyCodeEnableFilterable)))
			},
		},
		{
			// A caller logs the error and carries on with the shard load, so
			// an empty store here reads as "no migration on this shard" and
			// licenses every sweep to reclaim.
			name: "a records directory that cannot be read leaves the whole set unreadable, not clean",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				require.NoError(t, os.MkdirAll(filepath.Dir(s.Dir()), 0o777))
				require.NoError(t, os.WriteFile(s.Dir(), []byte("not a directory"), 0o600))
			},
			wantLoadErr: true,
			assert: func(t *testing.T, s *MigrationRecordStore) {
				require.Empty(t, s.Records())
				require.NotEmpty(t, s.Unreadable(),
					"a directory nobody could read must withhold, not report a clean shard")
			},
		},
		{
			// The directory fault is keyed by the directory's own name, which
			// no record key can ever render to, so a guard that matches file
			// names alone is dead on this arm. The fault is cleared before the
			// write so the write reaches disk and can do the damage.
			name: "a records directory that could not be read freezes every write, not one file name",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				require.NoError(t, s.Put(merged(42, StrategyCodeEnableFilterable)))
				require.NoError(t, os.Rename(s.Dir(), s.Dir()+".aside"))
				require.NoError(t, os.WriteFile(s.Dir(), []byte("not a directory"), 0o600))
			},
			wantLoadErr: true,
			assert: func(t *testing.T, s *MigrationRecordStore) {
				require.NoError(t, os.Remove(s.Dir()))
				require.NoError(t, os.Rename(s.Dir()+".aside", s.Dir()))

				before, err := os.ReadFile(filepath.Join(s.Dir(), "42_enable_filterable_shard-1__node-0.json"))
				require.NoError(t, err)

				require.Error(t, s.Put(NewMigrationRecordIterating(
					testMigrationSubject(42, StrategyCodeEnableFilterable, "title"), MigrationCheckpoint{})),
					"a build that cannot tell what is recorded here must not demote a flip record")
				require.Error(t, s.Remove(MigrationRecordKey{
					TaskVersion: 42, StrategyCode: StrategyCodeEnableFilterable, UnitID: "shard-1__node-0",
				}), "the freeze that preserves a record must not be undone by removing it")

				after, err := os.ReadFile(filepath.Join(s.Dir(), "42_enable_filterable_shard-1__node-0.json"))
				require.NoError(t, err)
				require.Equal(t, before, after)
			},
		},
		{
			// A file fault is one file, and the shard's other migrations have
			// to keep making progress under it.
			name: "a file this build cannot read freezes that key and no other",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				require.NoError(t, os.MkdirAll(s.Dir(), 0o777))
				require.NoError(t, os.WriteFile(
					filepath.Join(s.Dir(), "42_enable_filterable_shard-1__node-0.json"), []byte("{torn"), 0o600))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				frozen := MigrationRecordKey{TaskVersion: 42, StrategyCode: StrategyCodeEnableFilterable, UnitID: "shard-1__node-0"}
				require.Error(t, s.Remove(frozen))
				require.NoError(t, s.Put(merged(43, StrategyCodeEnableFilterable)))
				require.NoError(t, s.Remove(MigrationRecordKey{
					TaskVersion: 43, StrategyCode: StrategyCodeEnableFilterable, UnitID: "shard-1__node-0",
				}))

				kept, err := os.ReadFile(filepath.Join(s.Dir(), "42_enable_filterable_shard-1__node-0.json"))
				require.NoError(t, err)
				require.Equal(t, "{torn", string(kept))
			},
		},
		{
			// A backup walks the migrations directory recursively and shard
			// copy ships the files under it, so another unit's record does
			// land here. It has to survive this unit's own write of the same
			// task and strategy.
			name: "a foreign unit's record is neither answered for nor overwritten",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				foreign := testMigrationSubject(42, StrategyCodeEnableFilterable, "title")
				foreign.Key.UnitID = "shard-1__node-9"
				require.NoError(t, s.Put(NewMigrationRecordMerged(foreign)))
				require.NoError(t, s.Put(merged(42, StrategyCodeEnableFilterable)))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				require.Len(t, s.Records(), 2)
				for _, unit := range []string{"shard-1__node-0", "shard-1__node-9"} {
					_, ok := s.Get(MigrationRecordKey{
						TaskVersion: 42, StrategyCode: StrategyCodeEnableFilterable, UnitID: unit,
					})
					require.True(t, ok, "unit %q lost its record", unit)
				}
			},
		},
		{
			// A teardown seals the unit its record names, and a foreign unit
			// is one no local worker claims — so that seal is always granted
			// and the teardown would remove directories a live local worker
			// is writing into.
			name: "two units on one shard freeze it rather than being acted on",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				foreign := testMigrationSubject(42, StrategyCodeEnableFilterable, "title")
				foreign.Key.UnitID = "shard-1__node-9"
				require.NoError(t, s.Put(NewMigrationRecordMerged(foreign)))
				require.NoError(t, s.Put(merged(42, StrategyCodeEnableFilterable)))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				require.Len(t, s.Unreadable(), 1)
				require.Equal(t, MigrationRecordFaultStore, s.Unreadable()[0].Scope)
				require.Contains(t, s.Unreadable()[0].Reason, "shard-1__node-9")

				logger, _ := test.NewNullLogger()
				committed := migrationPreservedStateAt(filepath.Dir(filepath.Dir(s.Dir())), logger)
				require.True(t, committed.preservesBucket("a directory no record names"))
				require.Error(t, s.Put(merged(43, StrategyCodeEnableFilterable)),
					"a frozen store must not take a write it cannot place among the records it could not attribute")
			},
		},
		{
			name: "records come back in ascending task-version order, then strategy code",
			arrange: func(t *testing.T, s *MigrationRecordStore) {
				require.NoError(t, s.Put(merged(43, StrategyCodeEnableFilterable)))
				require.NoError(t, s.Put(merged(7, StrategyCodeEnableFilterable)))
				require.NoError(t, s.Put(merged(42, StrategyCodeFilterableRetokenize)))
				require.NoError(t, s.Put(merged(42, StrategyCodeEnableFilterable)))
			},
			assert: func(t *testing.T, s *MigrationRecordStore) {
				var got []string
				for _, rec := range s.Records() {
					got = append(got, rec.Subject().Key.fileName())
				}
				require.Equal(t, []string{
					"7_enable_filterable_shard-1__node-0.json",
					"42_enable_filterable_shard-1__node-0.json",
					"42_filterable_retokenize_shard-1__node-0.json",
					"43_enable_filterable_shard-1__node-0.json",
				}, got)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			store := NewMigrationRecordStore(t.TempDir(), logger)
			tt.arrange(t, store)

			// Every assertion runs against a store that re-read the directory,
			// so nothing passes on in-memory state a restart would lose.
			err := store.Load()
			if tt.wantLoadErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			tt.assert(t, store)
		})
	}
}

func TestMigrationRecordStoreConcurrentAccess(t *testing.T) {
	logger, _ := test.NewNullLogger()
	lsmPath := t.TempDir()
	store := NewMigrationRecordStore(lsmPath, logger)

	const writers, readers = 8, 8
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for gen := 1; gen <= 8; gen++ {
				subject := testMigrationSubject(uint64(i*8+gen), StrategyCodeEnableFilterable, "title")
				if err := store.Put(NewMigrationRecordMerged(subject)); err != nil {
					t.Errorf("put record: %v", err)
					return
				}
			}
		}()
	}
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 64 {
				for _, rec := range store.Records() {
					_, _ = store.Get(rec.Subject().Key)
				}
				_ = store.Unreadable()
			}
		}()
	}
	// Foreign readers build their own store over the same directory. Several
	// gates do this per shard, one of them on every scheduler tick, so they run
	// against a shard that is writing.
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 64 {
				migrationRecordsAt(lsmPath, logger)
			}
		}()
	}
	wg.Wait()

	// The in-memory map first: a reload reads the whole directory back from
	// disk, so it answers correctly even for a store whose map the concurrent
	// writers left short.
	require.Len(t, store.Records(), writers*8,
		"every put publishes into the map the readers were walking")
	require.NoError(t, store.Load())
	require.Len(t, store.Records(), writers*8)
}

// TestDecodeMigrationRecordRejectsEscapingHandles pins the containment check:
// every path field is joined onto the shard root and reaches os.RemoveAll,
// and a join cleans "../" without refusing it, so a crafted handle (reachable
// via backup restore) could otherwise delete outside the shard.
func TestDecodeMigrationRecordRejectsEscapingHandles(t *testing.T) {
	tests := []struct {
		name   string
		place  func(*MigrationSubject, *migrationFlipEnvelope, string)
		handle string
		// wantErr asks for an error; wantField, where set, pins which group
		// named it. Without that a build that labelled every group the same
		// would keep the whole table green.
		wantErr   bool
		wantField string
	}{
		{
			name:   "tracker directory",
			place:  func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) { s.TrackerDir = h },
			handle: "../../../../etc", wantErr: true,
		},
		{
			name: "staged directory",
			place: func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) {
				s.StagedDirs = map[string]string{"title": h}
			},
			handle: "/var/lib/weaviate", wantErr: true, wantField: "staged directory",
		},
		{
			name: "canonical directory",
			place: func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) {
				s.CanonicalDirs = map[string]string{"title": h}
			},
			handle: "../sibling_shard/property_title", wantErr: true, wantField: "canonical directory",
		},
		{
			name: "sidecar directory",
			place: func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) {
				s.SidecarDirs = map[string]string{"title": h}
			},
			handle: "..", wantErr: true, wantField: "sidecar directory",
		},
		{
			name: "displaced directory",
			place: func(_ *MigrationSubject, f *migrationFlipEnvelope, h string) {
				f.DisplacedDirs = map[string]string{"title": h}
			},
			handle: "/", wantErr: true, wantField: "displaced directory",
		},
		{
			name: "a handle that only looks like an escape stays inside",
			place: func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) {
				s.StagedDirs = map[string]string{"title": h}
			},
			handle: "property_..title__g42_ingest",
		},
		{
			// No writer emits a nested handle: every one is a strategy prefix
			// plus sorted property names, none of which can carry a separator.
			// Accepting one is what lets the rest of this table be evaded.
			name: "a nested handle names no directory a writer can produce",
			place: func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) {
				s.SidecarDirs = map[string]string{"title": h}
			},
			handle: "property_tracker__g42_ingest/searchable/title", wantErr: true,
		},
		{
			name:   "an empty handle is the ordinary names-none",
			place:  func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) { s.TrackerDir = h },
			handle: "",
		},
		// The handles a join resolves back to the shard root itself, which is
		// then what os.RemoveAll is handed. filepath.IsLocal accepts every one
		// of them.
		{
			name: "the current directory",
			place: func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) {
				s.SidecarDirs = map[string]string{"title": h}
			},
			handle: ".", wantErr: true,
		},
		{
			name:   "a descent and an ascent that cancel",
			place:  func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) { s.TrackerDir = h },
			handle: "x/..", wantErr: true,
		},
		// Property names are the other family: the sweeps compose bucket and
		// sidecar directory names out of them and then remove those.
		{
			name:   "a property name that escapes",
			place:  func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) { s.Properties = []string{h} },
			handle: "x/../../../../etc", wantErr: true, wantField: "property",
		},
		{
			name:   "an empty property name, which composes into another property's bucket",
			place:  func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) { s.Properties = []string{h} },
			handle: "", wantErr: true,
		},
		{
			name: "a poisoned staged-dirs key",
			place: func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) {
				s.StagedDirs = map[string]string{h: "property_title__g42_ingest"}
			},
			handle: "../../evil", wantErr: true,
		},
		{
			name: "a poisoned canonical-dirs key",
			place: func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) {
				s.CanonicalDirs = map[string]string{h: "property_title"}
			},
			handle: "../../evil", wantErr: true,
		},
		{
			name: "a poisoned sidecar-dirs key",
			place: func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) {
				s.SidecarDirs = map[string]string{h: "property_title__g42_ingest"}
			},
			handle: "../../evil", wantErr: true,
		},
		{
			name: "a poisoned displaced-dirs key",
			place: func(_ *MigrationSubject, f *migrationFlipEnvelope, h string) {
				f.DisplacedDirs = map[string]string{h: "property_title__g42_ingest"}
			},
			handle: "../../evil", wantErr: true,
		},
		{
			name: "a poisoned flipped-properties entry",
			place: func(_ *MigrationSubject, f *migrationFlipEnvelope, h string) {
				f.Flipped = []string{h}
			},
			handle: "../../evil", wantErr: true,
		},
		{
			name:   "an ordinary property name decodes",
			place:  func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) { s.Properties = []string{h} },
			handle: "title_2",
		},
		{
			// Property names are user-chosen, so the reserved set is scoped to
			// directory handles. A blanket check would refuse this collection
			// a migration outright.
			name:   "a property named after the record store decodes",
			place:  func(s *MigrationSubject, _ *migrationFlipEnvelope, h string) { s.Properties = []string{h} },
			handle: migrationRecordsDirName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subject := testMigrationSubject(42, StrategyCodeEnableFilterable, "title")
			subject.TrackerDir, subject.StagedDirs, subject.CanonicalDirs, subject.SidecarDirs = "", nil, nil, nil
			flip := migrationFlipEnvelope{Flipped: []string{"title"}}
			tt.place(&subject, &flip, tt.handle)

			data, err := json.Marshal(migrationRecordEnvelope{
				FormatVersion: migrationRecordFormatVersion,
				State:         MigrationStateSwapped,
				Subject:       subject,
				Flip:          &flip,
			})
			require.NoError(t, err)

			rec, err := decodeMigrationRecord(data)
			if tt.wantErr {
				require.Error(t, err, "a handle that leaves the shard root must not decode")
				require.Nil(t, rec)
				if tt.wantField != "" {
					require.Contains(t, err.Error(), fmt.Sprintf("names %s %q", tt.wantField, tt.handle))
				}
				return
			}
			require.NoError(t, err)
			require.NotNil(t, rec)
		})
	}
}

// TestTheWriterRefusesWhatTheLoaderWouldReject pins the two directions on one
// predicate: a record only the writer accepts is worse than one neither does
// — it lands under a name the next load refuses, wedging that key and
// withholding every removal on the shard until fixed by hand.
func TestTheWriterRefusesWhatTheLoaderWouldReject(t *testing.T) {
	// wantErr names the rule the row breaks. Without it a build that collapsed
	// every validation into one error would keep all of these green.
	tests := []struct {
		name    string
		mangle  func(*MigrationSubject)
		because string
		wantErr string
	}{
		{
			name:    "a tracker directory that leaves the shard root",
			mangle:  func(s *MigrationSubject) { s.TrackerDir = "../../../etc" },
			because: "the tracker directory is joined onto the shard and handed to a recursive delete",
			wantErr: "names tracker directory \"../../../etc\"",
		},
		{
			name:    "a sidecar directory that is the shard's migrations tree",
			mangle:  func(s *MigrationSubject) { s.SidecarDirs = map[string]string{"title": migrationsDir} },
			because: "a sidecar handle is reclaimed by os.RemoveAll like every other owned directory",
			wantErr: `names sidecar directory ".migrations", which is a directory no migration may own`,
		},
		{
			name: "a staged directory that is a live bucket of another property",
			mangle: func(s *MigrationSubject) {
				s.StagedDirs = map[string]string{"title": "property_body_searchable"}
			},
			because: "a staged handle is reclaimed on every teardown path, so a live bucket named there is deleted",
			wantErr: `names staged directory "property_body_searchable", which is not shaped like a sidecar of a property bucket`,
		},
		{
			name: "a staged directory that is the object store",
			mangle: func(s *MigrationSubject) {
				s.StagedDirs = map[string]string{"title": "objects"}
			},
			because: "the object store is the shard's whole object store",
			wantErr: `names staged directory "objects", which is a directory no migration may own`,
		},
		{
			name:    "a strategy code outside the known set",
			mangle:  func(s *MigrationSubject) { s.Key.StrategyCode = "bogus" },
			because: "the code is in the file name, so the loader refuses a file the writer chose",
			wantErr: "record key \"42/bogus/shard-1__node-0\" is incomplete or names an unknown strategy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			tt.mangle(&subject)
			rec := NewMigrationRecordMerged(subject)

			_, err := encodeMigrationRecord(rec)
			require.Error(t, err, tt.because)
			require.Contains(t, err.Error(), tt.wantErr)

			logger, _ := test.NewNullLogger()
			store := NewMigrationRecordStore(t.TempDir(), logger)
			require.Error(t, store.Put(rec), "and Put refuses it for the same reason")
			require.Empty(t, store.Records(), "nothing a load would refuse reaches the map either")
		})
	}
}

// TestTheLargestRecordTheWriterCanBuildFitsTheLoadersBound makes the size
// derivation executable. The writer has no size check — one would be
// unreachable — so the bound holds only as long as every per-property field
// stays bounded, and a field that grows with the data rather than with the
// property count breaks it silently: the writer accepts the record, the next
// load refuses it, and that key is then wedged with every destructive and
// promoting action on the shard withheld.
func TestTheLargestRecordTheWriterCanBuildFitsTheLoadersBound(t *testing.T) {
	// maxDirEntryNameBytes is what a filesystem allows in one directory entry.
	// Every handle a record carries names one, and a property name composes
	// into one, so it caps them all.
	const maxDirEntryNameBytes = 255

	longest := func(role string, i int) string {
		s := fmt.Sprintf("%s_%d_", role, i)
		return s + strings.Repeat("x", maxDirEntryNameBytes-len(s))
	}
	// Staged and sidecar handles must carry the shape the writer emits.
	longestSidecar := func(role string, i int) string {
		s := fmt.Sprintf("property_%s_%d__", role, i)
		const tail = "_ingest"
		return s + strings.Repeat("x", maxDirEntryNameBytes-len(s)-len(tail)) + tail
	}
	// Canonical and displaced handles must be property buckets. Still the
	// full entry length, so the bound is measured at the maximum either way.
	longestBucket := func(role string, i int) string {
		s := fmt.Sprintf("property_%s_%d_", role, i)
		return s + strings.Repeat("x", maxDirEntryNameBytes-len(s))
	}

	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize)
	subject.TrackerDir = longest("tracker", 0)
	subject.Properties = make([]string, maxReindexPropertiesPerTask)
	subject.StagedDirs = map[string]string{}
	subject.CanonicalDirs = map[string]string{}
	subject.SidecarDirs = map[string]string{}
	displaced := map[string]string{}
	for i := range subject.Properties {
		prop := longest("property", i)
		subject.Properties[i] = prop
		subject.StagedDirs[prop] = longestSidecar("staged", i)
		subject.CanonicalDirs[prop] = longestBucket("canonical", i)
		subject.SidecarDirs[prop] = longestSidecar("sidecar", i)
		displaced[prop] = longestBucket("displaced", i)
	}

	swapped := NewMigrationRecordSwapped(subject, slices.Clone(subject.Properties), displaced)
	for _, prop := range subject.Properties {
		swapped = swapped.WithPromotionAt(prop, migrationPromotionFinished)
	}

	// A Swapped envelope carries no checkpoint block, so measuring only that
	// variant leaves the one field that is not a directory handle unmeasured.
	// The only indexKey this build produces is a UUID's 16 bytes.
	iterating := NewMigrationRecordIterating(subject, MigrationCheckpoint{
		LastProcessedKey: bytes.Repeat([]byte{0xff}, 16),
	})

	for _, rec := range []MigrationRecord{swapped, iterating} {
		t.Run(string(rec.State()), func(t *testing.T) {
			data, err := encodeMigrationRecord(rec)
			require.NoError(t, err)
			require.Less(t, len(data), maxMigrationRecordBytes,
				"the writer must not be able to build a record the loader refuses")

			// And the loader really does take it, so the bound is the one being
			// cleared rather than a number this test made up.
			logger, _ := test.NewNullLogger()
			store := NewMigrationRecordStore(t.TempDir(), logger)
			require.NoError(t, store.Put(rec))
			require.NoError(t, store.Load())
			require.Len(t, store.Records(), 1)
			require.Empty(t, store.Unreadable())
		})
	}
}

// TestAnOversizedRecordIsRefusedByTheWriterToo pins the other half of the same
// symmetry: a record the loader would refuse must not reach disk, because the
// key it lands on can afterwards be neither read, written, nor removed.
func TestAnOversizedRecordIsRefusedByTheWriterToo(t *testing.T) {
	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	for i := 0; i < 40_000; i++ {
		pad := fmt.Sprintf("property_pad__g42%06d%s_ingest", i, strings.Repeat("x", 200))
		subject.SidecarDirs[fmt.Sprintf("pad_%06d", i)] = pad
	}
	oversized := NewMigrationRecordMerged(subject)

	raw, err := json.MarshalIndent(oversized.toEnvelope(), "", "  ")
	require.NoError(t, err)
	require.Greater(t, len(raw), maxMigrationRecordBytes,
		"the fixture has to exceed the bound, or there is nothing to refuse")

	_, err = encodeMigrationRecord(oversized)
	require.ErrorContains(t, err, "bound is")

	logger, _ := test.NewNullLogger()
	store := NewMigrationRecordStore(t.TempDir(), logger)
	require.Error(t, store.Put(oversized))

	// The key is still usable, which is the whole point: an accepted oversized
	// write freezes it forever. Asserting the file is absent would not show
	// that — this reads the record back.
	inBound := NewMigrationRecordMerged(testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title"))
	require.NoError(t, store.Put(inBound))
	require.NoError(t, store.Load())
	require.Len(t, store.Records(), 1)
	require.Equal(t, MigrationStateMerged, store.Records()[0].State())
	require.Empty(t, store.Unreadable())
}

// TestEveryPropertyBucketCarriesTheMigrationPrefix keeps
// migrationPropertyBucketPrefix honest: the shape rule that keeps a live
// bucket out of a staged handle recognizes a bucket by this prefix, so a
// helper that stopped emitting it would make the rule inert without failing
// anything else.
func TestEveryPropertyBucketCarriesTheMigrationPrefix(t *testing.T) {
	for _, bucket := range []string{
		helpers.BucketFromPropNameLSM("title"),
		helpers.BucketSearchableFromPropNameLSM("title"),
		helpers.BucketRangeableFromPropNameLSM("title"),
	} {
		require.True(t, strings.HasPrefix(bucket, migrationPropertyBucketPrefix), bucket)
		require.False(t, migrationHandleIsSidecarShaped(bucket),
			"a property's own bucket must never pass as a migration's staged copy")
	}
}

// TestNoStoreTheShardServesFromCanBeNamedInAnyDirectoryRole is the rule-3 half
// of the shape check. Every directory role reaches os.RemoveAll — the staged
// and sidecar ones on every teardown path, the canonical and displaced ones
// through the promotion that replaces them — so every store the shard serves
// from has to be refused in all four, not the four names someone remembered.
//
// The names come from the helpers that build them rather than from a literal
// list here, so a store added later fails this test instead of passing
// silently.
func TestNoStoreTheShardServesFromCanBeNamedInAnyDirectoryRole(t *testing.T) {
	// Not property buckets at all, so no role may hold one.
	stores := []string{
		helpers.ObjectsBucketLSM,
		helpers.DimensionsBucketLSM,
		migrationsDir,
		migrationRecordsDirName,
	}
	// Both the unnamed vector and a named one: their artifact sets differ by
	// more than a suffix, and the multivector and hfresh buckets are keyed on
	// the index ID rather than on the vector name.
	for _, targetVector := range []string{"", "myNamedVector"} {
		stores = append(stores, helpers.VectorIndexArtifactsFor(targetVector, nil).LSMBuckets...)
	}

	// A property's own live bucket, which only the sidecar-shaped roles
	// refuse: a migration's own copy is never the live bucket, while the
	// canonical and displaced roles are exactly where a live bucket belongs.
	// What that costs is pinned by
	// TestThePropertyBucketRuleAcceptsTheShardsOwnBuckets below.
	liveBuckets := []string{
		helpers.BucketFromPropNameLSM("title"),
		helpers.BucketSearchableFromPropNameLSM("title"),
		helpers.BucketRangeableFromPropNameLSM("title"),
	}

	for _, role := range migrationShardRootDirectoryRoles(t) {
		refused := stores
		if role.shape == migrationShapeSidecar {
			refused = slices.Concat(stores, liveBuckets)
		}
		for _, store := range refused {
			t.Run(role.field+"/"+store, func(t *testing.T) {
				env := migrationRecordEnvelope{
					Subject:    testMigrationSubject(1, StrategyCodeSearchableRetokenize, "title"),
					State:      MigrationStateIterating,
					Checkpoint: &MigrationCheckpoint{},
				}
				migrationDirectoryRolePlacers[role.field](&env, "title", store)
				err := validateMigrationHandles(env)
				require.Errorf(t, err, "a record naming this as its %s hands it to os.RemoveAll", role.field)
				require.Contains(t, err.Error(), store)
			})
		}
	}
}

// migrationDirectoryRolePlacers puts a handle in one role, keyed by that
// role's name in [migrationHandleGroups]. Only the placing is written out
// here: which roles exist, and what each one refuses, come from the
// production table, so a role added there is picked up by every test below.
var migrationDirectoryRolePlacers = map[string]func(env *migrationRecordEnvelope, prop, handle string){
	"staged directory": func(env *migrationRecordEnvelope, prop, handle string) {
		env.Subject.StagedDirs[prop] = handle
	},
	"sidecar directory": func(env *migrationRecordEnvelope, prop, handle string) {
		env.Subject.SidecarDirs[prop] = handle
	},
	"canonical directory": func(env *migrationRecordEnvelope, prop, handle string) {
		env.Subject.CanonicalDirs[prop] = handle
	},
	"displaced directory": func(env *migrationRecordEnvelope, prop, handle string) {
		env.Flip = &migrationFlipEnvelope{
			Flipped:       []string{prop},
			DisplacedDirs: map[string]string{prop: handle},
		}
	},
}

// migrationShardRootDirectoryRoles are the roles whose handle names a
// directory in the shard's LSM directory, where every store the shard serves
// from also lives. The tracker is not one: it is joined onto .migrations, so
// no store is reachable from it.
//
// A role with no placer fails here rather than going untested.
func migrationShardRootDirectoryRoles(t *testing.T) []migrationHandleGroup {
	t.Helper()
	var out []migrationHandleGroup
	for _, group := range migrationHandleGroups {
		if !group.namesDirectory || group.underMigrationsDir {
			continue
		}
		require.Containsf(t, migrationDirectoryRolePlacers, group.field,
			"the %s role has no placer here, so no test below covers it", group.field)
		out = append(out, group)
	}
	return out
}

// TestEveryDirectoryRoleUnderTheShardRootCarriesAShapeRule is what stops the
// next role from arriving with no rule at all. The two direction tests around
// it walk the same table, so a role carrying neither shape gives them nothing
// to assert and passes both in silence — while still handing its handle to
// os.RemoveAll beside the stores the shard serves from.
func TestEveryDirectoryRoleUnderTheShardRootCarriesAShapeRule(t *testing.T) {
	for _, group := range migrationShardRootDirectoryRoles(t) {
		t.Run(group.field, func(t *testing.T) {
			require.Containsf(t,
				[]migrationHandleShape{migrationShapeSidecar, migrationShapePropertyBucket},
				group.shape,
				"the %s role reaches os.RemoveAll in the shard's LSM directory, so its handle has to take either the sidecar shape or the property-bucket rule",
				group.field)
		})
	}
}

// TestEveryWriterEmittedSidecarNameIsAccepted is the other direction, and the
// one a false positive would break: a shape rule refusing a name a strategy
// really emits refuses a legitimate migration outright. It covers both rules
// — the sidecar shape the staged and sidecar roles take, and the property
// bucket the canonical and displaced roles take.
//
// Driven off the strategies themselves, over property names carrying the
// separators the rule reads ("__" and a trailing role word), so a strategy
// whose suffix stopped matching fails here rather than in production.
func TestEveryWriterEmittedSidecarNameIsAccepted(t *testing.T) {
	// "a__b" is its own property whose bucket contains the separator;
	// "x_ingest" and "a__reindex" end in role words of their own.
	props := []string{"title", "a__b", "x_ingest", "a__reindex"}

	// Per property, because two strategies take theirs at construction and
	// ignore the argument. The count is checked against the sweep's own table
	// so a strategy added there and not here fails rather than goes untested.
	strategiesFor := func(prop string, generation int) []MigrationStrategy {
		return []MigrationStrategy{
			&MapToBlockmaxStrategy{generation: generation},
			&RoaringSetRefreshStrategy{generation: generation},
			&FilterableToRangeableStrategy{propNames: []string{prop}, generation: generation},
			&SearchableRetokenizeStrategy{propName: prop, generation: generation},
			&FilterableRetokenizeStrategy{propName: prop, generation: generation},
			&EnableFilterableStrategy{propNames: []string{prop}, generation: generation},
			&EnableSearchableStrategy{propNames: []string{prop}, generation: generation},
			&RebuildSearchableStrategy{propNames: []string{prop}, generation: generation},
		}
	}
	require.Len(t, strategiesFor("title", 1), len(strategiesByMigrationDir(1)))

	for _, generation := range []int{1, 2, 11} {
		for _, prop := range props {
			for _, strategy := range strategiesFor(prop, generation) {
				// The composition the writer uses: no generation is appended
				// here, since each strategy's suffix already carries its own.
				main := strategy.SourceBucketName(prop)
				// A predecessor that flipped and never promoted leaves live
				// data at a staged name, so both the main bucket and a
				// sidecar of it are names the promote roles legitimately hold.
				requireAcceptedInPromoteRoles(t, main)
				for _, suffix := range []string{
					strategy.ReindexSuffix(), strategy.IngestSuffix(), strategy.BackupSuffix(),
				} {
					name := main + suffix
					require.Truef(t, migrationHandleIsSidecarShaped(name),
						"%T emits %q, and refusing it refuses the migration", strategy, name)
					requireAcceptedInPromoteRoles(t, name)
				}
			}
		}
	}
	// The canonical name the promote path renames onto, which no strategy
	// suffix reaches.
	for _, prop := range props {
		for _, indexType := range []string{"filterable", "searchable", "rangeable"} {
			main, ok := mainBucketForPropertyIndex(prop, indexType)
			require.True(t, ok, indexType)
			requireAcceptedInPromoteRoles(t, main)
		}
	}
}

// requireAcceptedInPromoteRoles asserts that handle passes validation in
// every role taking the property-bucket rule — the ones the promote path
// reclaims.
func requireAcceptedInPromoteRoles(t *testing.T, handle string) {
	t.Helper()
	for _, role := range migrationShardRootDirectoryRoles(t) {
		if role.shape != migrationShapePropertyBucket {
			continue
		}
		env := migrationRecordEnvelope{
			Subject:    testMigrationSubject(1, StrategyCodeSearchableRetokenize, "title"),
			State:      MigrationStateIterating,
			Checkpoint: &MigrationCheckpoint{},
		}
		migrationDirectoryRolePlacers[role.field](&env, "title", handle)
		require.NoErrorf(t, validateMigrationHandles(env),
			"a writer puts %q in the %s role", handle, role.field)
	}
}

// TestThePropertyBucketRuleAcceptsTheShardsOwnBuckets records what the
// canonical and displaced roles let through, so the decision is written down
// rather than left to be rediscovered. The rule is a property_ prefix and
// nothing more, and no later check narrows it: the cross-record guard asks
// only about staged and sidecar roles, which a bare bucket name can never be
// held in. A record naming one of these has it removed by the promotion that
// replaces the role's directory. Accepted by design, filed separately.
func TestThePropertyBucketRuleAcceptsTheShardsOwnBuckets(t *testing.T) {
	for _, tt := range []struct {
		name   string
		handle string
	}{
		{name: "the shard's own id index", handle: helpers.BucketFromPropNameLSM(filters.InternalPropID)},
		{name: "a property length index", handle: helpers.BucketFromPropNameLengthLSM("title")},
		{name: "another property's bucket", handle: helpers.BucketFromPropNameLSM("body")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			requireAcceptedInPromoteRoles(t, tt.handle)
		})
	}
}
