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
	"io"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// TestCleanStaleMigrationDirsAt_PreservedGensLogAtDebug pins that preserving a
// deferred-finalize tracker dir logs at Debug: this runs inside the RAFT
// apply loop, and Info would serialize ~10k lines per property DELETE on a
// 10k-tenant class awaiting restart-finalize.
func TestCleanStaleMigrationDirsAt_PreservedGensLogAtDebug(t *testing.T) {
	lsm := t.TempDir()
	propName := "category"
	indexType := "filterable"

	const preservedGens = 3
	for gen := 1; gen <= preservedGens; gen++ {
		dir := migrationDirWithProps(MigrationDirPrefixEnableFilterable, []string{propName}) + genSuffix(gen)
		mkTrackerDir(t, lsm, dir)
		mkMigrationRecord(t, lsm, dir, MigrationStateSwapped, map[string]string{
			propName: "property_" + propName + "__enable_filterable_ingest" + genSuffix(gen),
		})
	}

	hookLogger, hook := test.NewNullLogger()
	hookLogger.SetLevel(logrus.DebugLevel)

	cleanStaleMigrationDirsAt(t.Context(), lsm, propName, indexType, hookLogger, nil)

	var infoCount, preservedCount int
	for _, e := range hook.AllEntries() {
		if e.Level == logrus.InfoLevel {
			infoCount++
		}
		// By message: the sweep reads the records first, and that read has a
		// Debug line of its own that says nothing about preservation.
		if e.Level == logrus.DebugLevel && strings.Contains(e.Message, "preserving a tracker dir") {
			preservedCount++
		}
	}
	require.Equal(t, 0, infoCount,
		"preserving a deferred-finalize tracker dir must not log at Info inside the RAFT apply loop")
	require.Equal(t, preservedGens, preservedCount, "one Debug line per preserved generation")
}

// The record read runs per shard inside the RAFT apply of a property DELETE,
// so its healthy-path Debug line must cost nothing when Debug is off: logrus
// builds the entry and copies the field map before consulting the level.
// Measured: 14 allocations unguarded, 6 with the guard.
func TestReadingRecordsBuildsNoLogEntryWhenDebugIsOff(t *testing.T) {
	lsm := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetOutput(io.Discard)
	allocs := testing.AllocsPerRun(300, func() {
		migrationRecordsAt(lsm, logger)
	})
	require.LessOrEqual(t, allocs, 8.0,
		"reading a shard's records at Info level must not pay for a Debug entry")
}
