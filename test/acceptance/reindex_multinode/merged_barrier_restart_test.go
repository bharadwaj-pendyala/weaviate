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

package reindex_multinode

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
)

// TestMultiNode_RestartInsideMergedBarrier_CommitsAndServes covers the journey
// the migration record was built for: a node goes down holding staged data that
// is complete but not yet live, and comes back to decide what becomes of it.
//
// Every shard must finish its rebuild before any shard flips, so between those
// two points a node's records sit at Merged. Whether that staged data should
// ever become live is a cluster fact, and the node reads it from the task map
// its own RAFT log has applied — a map that is not installed yet while shards
// load during catch-up. A node that decides nothing at that load and is never
// loaded again serves pre-migration data for the rest of its life, and the next
// restart repeats the same ordering.
//
// The assertion is per replica on purpose. A cluster-level query is answered by
// whichever replica responds, so a single stranded node hides behind the two
// that promoted. Each node is asked directly, before and after a second full
// restart: the second round is what separates "promoted" from "still serving
// the pre-migration bucket and about to lose it".
func TestMultiNode_RestartInsideMergedBarrier_CommitsAndServes(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const (
		className    = "MergedBarrierRestart"
		totalObjects = 20_000
		// The restarted node. Node 1 stays up so the test always has a REST
		// endpoint to submit and poll through.
		restartedNode = 2
	)
	paths := []string{"alpha-path", "beta-path", "gamma-path", "delta-path"}
	const expectedPerPath = totalObjects / 4

	trueVal := true
	createCollection(t, compose, restURIOf(compose, 1), className, 3, 3, []*models.Property{
		{
			Name:            "path",
			DataType:        []string{"text"},
			IndexFilterable: &trueVal,
			Tokenization:    "word",
		},
	})
	defer func() { deleteCollection(t, restURIOf(compose, 1), className) }()

	batchImportMultiProp(t, restURIOf(compose, 1), className, totalObjects, func(i int) map[string]interface{} {
		return map[string]interface{}{"path": paths[i%len(paths)]}
	})

	requireEveryReplicaServes(t, compose, className, "path", paths[0], expectedPerPath, "pre-migration")

	uri := restURIOf(compose, 1)
	taskID := reindexhelpers.SubmitIndexUpsert(t, uri, className, "path", "searchable",
		`{"tokenization":"field"}`)
	t.Logf("submitted change-tokenization task %s", taskID)

	// PREPARING is the barrier: every node's rebuild is done or finishing, and
	// no node has flipped. A record written in this window says Merged.
	observed := awaitReindexReachedFinalizing(t, uri, taskID)
	t.Logf("task %s reached %s — killing node %d inside that window", taskID, observed, restartedNode)
	// The shared helper also returns on SWAPPING and FINISHED. Both are past
	// the barrier, so a kill there never lands on a Merged record and this
	// test asserts nothing it exists to assert. Failing is what keeps a missed
	// window visible.
	require.Equalf(t, "PREPARING", observed,
		"the merged window closed before the kill, so the scenario never ran; "+
			"raise totalObjects (currently %d) to widen it", totalObjects)

	// SIGKILL, so nothing is flushed on the way out and the node comes back
	// with exactly what the record says.
	cycleNodeFastKill(ctx, t, compose, restartedNode-1)
	t.Logf("node %d restarted", restartedNode)

	reindexhelpers.AwaitReindexFinished(t, restURIOf(compose, 1), taskID,
		reindexhelpers.WithTimeout(240*time.Second))

	// The schema now says field, so the query tokenizes the whole value as one
	// term. A replica still serving its pre-migration word-tokenized bucket has
	// no such term and answers zero.
	requireEveryReplicaServes(t, compose, className, "path", paths[0], expectedPerPath, "after the task finished")

	// The promotion of the staged directory onto the canonical name happens at
	// a load, because a live bucket's directory cannot be renamed underneath
	// it. This restart is where a node that never decided runs out of chances:
	// it re-reads the same records with the same task map.
	rollingRestartCluster(ctx, t, compose)
	requireEveryReplicaServes(t, compose, className, "path", paths[0], expectedPerPath, "after a second restart")

	// Each node's own schema, not the leader's answer three times over.
	awaitTokenizationOnAllNodes(t, compose, className, "path", "field")
}

// requireEveryReplicaServes asks each node for its own answer. A filtered
// Aggregate runs on the local replica of every shard the node holds, and at
// replication factor 3 on 3 nodes that is all of them — so a stranded node
// answers zero here instead of hiding behind the two that did the work.
//
// The assertions run inside the poll so the failure says which of the two it
// was: a node serving 0 from its pre-migration bucket is the bug, a node that
// cannot be reached at all is a broken fixture.
func requireEveryReplicaServes(t *testing.T, compose *docker.DockerCompose,
	className, propName, value string, want int, phase string,
) {
	t.Helper()
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		node := nodeIdx
		require.EventuallyWithTf(t, func(ct *assert.CollectT) {
			got, err := equalCount(restURIOf(compose, node), className, propName, value)
			if !assert.NoErrorf(ct, err, "node %d could not be queried", node) {
				return
			}
			assert.Equalf(ct, want, got, "node %d serves the wrong count for %s=%q", node, propName, value)
		}, 90*time.Second, 200*time.Millisecond,
			"node %d must serve %d objects for %s=%q %s", node, want, propName, value, phase)
	}
}
