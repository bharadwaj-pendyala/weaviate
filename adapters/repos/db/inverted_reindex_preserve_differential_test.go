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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// preserveDifferentialProp is the property the differential below sweeps. Its
// three sidecar roles are the whole population a completed migration owns.
const preserveDifferentialProp = "title"

// preserveDifferentialDirs names the three sidecar roles of one completed
// searchable-retokenize migration on preserveDifferentialProp, and the answer
// the merge base f1cf001dd4 gave for each: all three preserved, whatever the
// tracker's payload says, because that release composed the suffixes from the
// tracker's own name and never read a property list to do it.
var preserveDifferentialDirs = []struct {
	dir  string
	role string
}{
	{dir: "property_title_searchable__retokenize_ingest_1", role: "ingest"},
	{dir: "property_title_searchable__retokenize_backup_1", role: "backup"},
	{dir: "property_title_searchable__retokenize_reindex_1", role: "reindex"},
}

// TestThePreserveSetKeepsWhatTheMergeBaseKept pins the preserve set against
// f1cf001dd4's answer rather than against its own self-consistency. A
// completed migration's three sidecar directories hold the property's only
// copy until the deferred finalize renames one onto the canonical name, so a
// sweep that stops naming one of them deletes data nothing else accounts for.
//
// Every cell asserts the directory's CONTENT survives, not that a directory
// exists: shard init creates directories, so an existence check passes on a
// build that deleted the data.
func TestThePreserveSetKeepsWhatTheMergeBaseKept(t *testing.T) {
	// payload names what the tracker can say about itself. The upgrade path
	// produces "absent": payload.mig first ships in v1.38.0 while tidied.mig
	// already existed in v1.37.4, so a tracker written by (or restored from) a
	// v1.37.x node carries a completion marker and no payload.
	for _, marker := range []string{"tidied.mig", "merged.mig"} {
		for _, payload := range []string{"present", "absent", "empty"} {
			t.Run(marker+"/payload_"+payload, func(t *testing.T) {
				ctx := testCtx()
				className := "PreserveDiff_" + uuid.NewString()[:8]
				class := newTestClassWithProps(className, []string{preserveDifferentialProp})
				shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
					false, false, false)
				shard := shd.(*Shard)
				defer shard.Shutdown(ctx)
				lsm := shard.pathLSM()

				const tracker = "searchable_retokenize_title_1"
				mkCompletedTracker(t, lsm, tracker, marker)
				switch payload {
				case "present":
					mkRecoveryPayload(t, lsm, tracker, preserveDifferentialProp)
				case "empty":
					mkRecoveryPayload(t, lsm, tracker)
				}
				for _, sc := range preserveDifferentialDirs {
					mkSidecarWithData(t, lsm, sc.dir)
				}

				// The preserve set on its own, before any removal runs.
				committed := migrationPreservedStateAt(lsm, shard.index.logger)
				for _, sc := range preserveDifferentialDirs {
					require.True(t, committed.preservesBucket(sc.dir),
						"the merge base preserved the %s sidecar of a completed migration; this build must too",
						sc.role)
				}

				// And the real removal loop, which is what actually deletes.
				cleanSweep(t, ctx, shard, preserveDifferentialProp, "searchable")
				for _, sc := range preserveDifferentialDirs {
					require.Equal(t, sidecarDataFor(sc.dir), readSidecarData(t, lsm, sc.dir),
						"the %s sidecar of a completed migration lost its data to the sweep", sc.role)
				}
			})
		}
	}
}

// sidecarDataFor is the byte string one fixture sidecar directory holds, so an
// assertion fails on a directory recreated empty as loudly as on a deleted one.
func sidecarDataFor(dir string) string {
	return "segment-of-" + dir
}

// mkCompletedTracker plants a tracker directory and the completion marker that
// says its staged data became the property's data.
func mkCompletedTracker(t *testing.T, lsmPath, name, marker string) {
	t.Helper()
	mkTrackerDir(t, lsmPath, name)
	require.NoError(t, os.WriteFile(
		filepath.Join(lsmPath, migrationsDir, name, marker), []byte("x"), 0o600))
}

func mkSidecarWithData(t *testing.T, lsmPath, name string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, name), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(lsmPath, name, "segment-0.db"),
		[]byte(sidecarDataFor(name)), 0o644))
}

func readSidecarData(t *testing.T, lsmPath, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(lsmPath, name, "segment-0.db"))
	if err != nil {
		return "<gone: " + err.Error() + ">"
	}
	return string(data)
}

// TestAnUnreadableCompletionMarkerWithholds pins the third outcome. Whether a
// tracker completed is answered by a stat, and a stat that fails for any reason
// other than absence answers nothing — while a completed tracker's staged
// directory holds the property's only copy. Reading that as "no marker" drops
// it out of the preserve set entirely.
func TestAnUnreadableCompletionMarkerWithholds(t *testing.T) {
	ctx := testCtx()
	className := "MarkerUnreadable_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{preserveDifferentialProp})
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)
	lsm := shard.pathLSM()

	const tracker = "searchable_retokenize_title_1"
	mkCompletedTracker(t, lsm, tracker, "tidied.mig")
	mkRecoveryPayload(t, lsm, tracker, preserveDifferentialProp)
	for _, sc := range preserveDifferentialDirs {
		mkSidecarWithData(t, lsm, sc.dir)
	}

	trackerPath := filepath.Join(lsm, migrationsDir, tracker)
	require.NoError(t, os.Chmod(trackerPath, 0o600))
	t.Cleanup(func() { os.Chmod(trackerPath, 0o755) })
	if _, err := os.Stat(filepath.Join(trackerPath, "tidied.mig")); err == nil {
		t.Skip("this user can stat inside a non-traversable directory, so the failure cannot be staged")
	}

	committed := migrationPreservedStateAt(lsm, shard.index.logger)
	require.True(t, committed.withholdEverything,
		"a tracker whose completion could not be read leaves the whole shard withheld")

	cleanSweep(t, ctx, shard, preserveDifferentialProp, "searchable")
	for _, sc := range preserveDifferentialDirs {
		require.Equal(t, sidecarDataFor(sc.dir), readSidecarData(t, lsm, sc.dir),
			"the %s sidecar was swept on the strength of a marker nobody could read", sc.role)
	}
}
