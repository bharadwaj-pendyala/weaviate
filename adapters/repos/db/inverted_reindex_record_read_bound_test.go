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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// TestOversizedMigrationRecordIsRefusedNotParsed pins the read bound: a Load
// runs inside the shard load, and activating a tenant runs that load inside
// the RAFT apply of an UpdateTenants command, so an oversized (but otherwise
// valid) record must be refused rather than decoded.
func TestOversizedMigrationRecordIsRefusedNotParsed(t *testing.T) {
	tests := []struct {
		name        string
		bytes       int64
		wantOutcome MigrationRecordLoadOutcome
	}{
		{
			name:        "under the bound: read and understood",
			bytes:       maxMigrationRecordBytes / 2,
			wantOutcome: MigrationRecordLoaded,
		},
		{
			name:        "over the bound: refused, and refused reads as not understood",
			bytes:       maxMigrationRecordBytes + (1 << 20),
			wantOutcome: MigrationRecordNotUnderstood,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lsm := t.TempDir()
			logger, _ := test.NewNullLogger()
			store := NewMigrationRecordStore(lsm, logger)

			key := plantRecordOfSize(t, store, tc.bytes)
			target := filepath.Join(store.Dir(), key.fileName())

			rec, outcome, err := loadMigrationRecordFile(target)
			require.Equal(t, tc.wantOutcome, outcome)

			require.NoError(t, store.Load())
			if tc.wantOutcome == MigrationRecordLoaded {
				require.NoError(t, err)
				require.NotNil(t, rec)
				require.Len(t, store.Records(), 1)
				require.Empty(t, store.Unreadable())
				return
			}

			require.Error(t, err)
			require.Empty(t, store.Records(), "a record nobody read is not a record")
			require.Len(t, store.Unreadable(), 1)
			require.Equal(t, key.fileName(), store.Unreadable()[0].FileName)

			// The refusal has to freeze writes on the same file, or the next
			// migration overwrites a record whose directories nothing can name.
			require.Error(t, store.Put(NewMigrationRecordMerged(
				testMigrationSubject(key.TaskVersion, key.StrategyCode, "title"))),
				"a record this build refused to read must not be written over")
		})
	}
}

// plantRecordOfSize writes a valid record of roughly size bytes, padded with
// the sidecar directories a migration over many properties really would carry.
func plantRecordOfSize(t *testing.T, store *MigrationRecordStore, size int64) MigrationRecordKey {
	t.Helper()
	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")

	const dirLen = 200
	for i := 0; int64(i)*(dirLen+8) < size; i++ {
		pad := fmt.Sprintf("property_pad__g42%06d%s_ingest", i, strings.Repeat("x", dirLen))
		subject.SidecarDirs[fmt.Sprintf("pad_%06d", i)] = pad
	}

	// Encoded straight from the envelope, not through encodeMigrationRecord:
	// this fixture stands in for a file some other build wrote, and this
	// build's writer refuses one over the bound.
	data, err := json.MarshalIndent(NewMigrationRecordMerged(subject).toEnvelope(), "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(store.Dir(), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(store.Dir(), subject.Key.fileName()), data, 0o600))
	return subject.Key
}
