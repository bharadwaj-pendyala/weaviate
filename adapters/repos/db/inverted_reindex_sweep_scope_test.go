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
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// unrelatedCompletedTrackers is how many other properties' completed
// migrations the shard below carries. Each holds a payload the sweep must not
// parse: a production payload runs to megabytes and the parse happens inside
// the RAFT apply a property DELETE holds cluster-wide.
const unrelatedCompletedTrackers = 10

// TestTheDeleteSweepParsesOnlyTheSweptPropertysTrackers pins the cost of one
// property DELETE against the shard's migration history: a tracker whose own
// directory name proves it stages nothing of the swept property is settled by
// that name, and its payload is never opened.
//
// The read count is asserted against the summary line an operator actually
// sees, so a sweep that parses payloads while reporting none fails here.
func TestTheDeleteSweepParsesOnlyTheSweptPropertysTrackers(t *testing.T) {
	ctx := testCtx()
	className := "SweepScope_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"swept", "keep"})
	hookLogger, hook := test.NewNullLogger()
	shd, idx := testShardWithSettings(t, ctx, class,
		enthnsw.UserConfig{Skip: true}, false, false, false,
		func(i *Index) { i.logger = hookLogger })
	defer shd.Shutdown(ctx)
	lsm := shd.(*Shard).pathLSM()

	for i := 0; i < unrelatedCompletedTrackers; i++ {
		dir := fmt.Sprintf("enable_filterable_alpha%03d_1", i)
		mkTrackerDir(t, lsm, dir, "tidied.mig")
		mkRecoveryPayload(t, lsm, dir, fmt.Sprintf("alpha%03d", i))
	}
	// The swept property's own completed migration: its staged data is the
	// property's only copy, so this one has to be read and preserved.
	const sweptTracker = "enable_filterable_swept_1"
	const sweptStaged = "property_swept__enable_filterable_ingest_1"
	mkTrackerDir(t, lsm, sweptTracker, "tidied.mig")
	mkRecoveryPayload(t, lsm, sweptTracker, "swept")
	mkSidecarWithData(t, lsm, sweptStaged)

	// The preserve set the sweep is built on, read before any removal runs.
	sweep := migrationSweepStateFor(lsm, "swept", shd.(*Shard).index.logger)
	require.Equal(t, 1, sweep.reads(),
		"only the swept property's own tracker is ambiguous from its name; "+
			"the other %d are settled by theirs", unrelatedCompletedTrackers)
	// Without these the count above would also hold for a sweep that read one
	// payload and then threw the answer away.
	require.True(t, sweep.committed.preservesBucket(sweptStaged),
		"the one payload read is what puts this migration's staged directory in the preserve set")
	require.False(t, sweep.committed.preservesBucket("property_alpha000__enable_filterable_ingest_1"),
		"an unrelated property's tracker was never parsed, so nothing of its is preserved here")

	hook.Reset()
	require.NoError(t, idx.updateProperty(ctx, &models.Property{
		Name:            "swept",
		DataType:        schema.DataTypeText.PropString(),
		Tokenization:    models.PropertyTokenizationWord,
		IndexFilterable: boolPtr(false),
		IndexSearchable: boolPtr(true),
	}))

	lines := sweepCompletionLines(hook)
	require.Len(t, lines, 1, "a sweep that opened a payload reports one summary line")
	require.EqualValues(t, 1, lines[0].Data["payload_reads"],
		"the summary must count the payload the preserve pass parsed, not only the ones "+
			"the deletion loop parsed")
}

// TestAClassLevelCompletedTrackerIsAlwaysParsed pins the one tracker shape the
// name cannot settle. A class-level migration's directory names no property at
// all, so only its payload says which buckets it staged — and it stages one per
// searchable property of the whole collection.
func TestAClassLevelCompletedTrackerIsAlwaysParsed(t *testing.T) {
	ctx := testCtx()
	className := "SweepScopeClass_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"swept", "keep"})
	shd, _ := testShardWithSettings(t, ctx, class,
		enthnsw.UserConfig{Skip: true}, false, false, false)
	defer shd.Shutdown(ctx)
	shard := shd.(*Shard)
	lsm := shard.pathLSM()

	const classTracker = "searchable_map_to_blockmax_1"
	const classStaged = "property_swept_searchable__blockmax_ingest_1"
	mkTrackerDir(t, lsm, classTracker, "merged.mig")
	mkRecoveryPayload(t, lsm, classTracker, "swept")
	mkSidecarWithData(t, lsm, classStaged)

	sweep := migrationSweepStateFor(lsm, "swept", shard.index.logger)
	require.Equal(t, 1, sweep.reads(),
		"a class-level tracker names no property, so its payload has to be read")
	require.True(t, sweep.committed.preservesBucket(classStaged),
		"the class-level migration's staged directory holds this property's only copy")

	// And the real removal loop leaves the data alone.
	cleanSweep(t, ctx, shard, "swept", "searchable")
	require.Equal(t, sidecarDataFor(classStaged), readSidecarData(t, lsm, classStaged))
}

// TestAMarkerEraTrackerIsPreservedForEveryPropertyItsNameCouldOwn pins that a
// marker-era tracker whose name could own the swept property enters the
// preserve set even when the exact-name clause does not match. Dropping the
// token arm of migrationTrackerMayOwnProperty is silent #10675-shape data loss.
func TestAMarkerEraTrackerIsPreservedForEveryPropertyItsNameCouldOwn(t *testing.T) {
	tests := []struct {
		dir  string
		prop string
		want bool
	}{
		{dir: "enable_filterable_a_b_1", prop: "a", want: true},
		{dir: "enable_filterable_a_b_1", prop: "a_b", want: true},
		{dir: "enable_filterable_b_a_1", prop: "a", want: true},
		{dir: "enable_filterable_x_a_y_1", prop: "a", want: true},
		{dir: "enable_filterable_a_b_c_1", prop: "b", want: true},
		{dir: "filterable_roaringset_refresh_1", prop: "cat", want: true},
		{dir: "enable_filterable_other_1", prop: "cat", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.dir+"/"+tt.prop, func(t *testing.T) {
			lsm := t.TempDir()
			mkTrackerDir(t, lsm, tt.dir, "tidied.mig")
			logger, _ := test.NewNullLogger()
			sweep := migrationSweepStateFor(lsm, tt.prop, logger)
			require.Equal(t, tt.want, sweep.committed.preservesTracker(tt.dir))
		})
	}
}
