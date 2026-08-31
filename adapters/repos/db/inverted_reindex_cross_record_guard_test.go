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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// TestAPromotionRefusesToReplaceAnotherRecordsData pins the roles a promotion
// may not take. A promotion removes the directory it is about to rename onto,
// and removes the one its flip displaced; both are unguarded, so a second
// record naming either as its own staged or sidecar directory loses its only
// copy to a promotion that is not even about it.
//
// A promotion that cannot clear its target must not promote: leaving the
// record Swapped is what every other unpromotable property already gets, and
// the next load asks again once the other record has answered for itself.
func TestAPromotionRefusesToReplaceAnotherRecordsData(t *testing.T) {
	tests := []struct {
		name string
		// role is how the bystander holds the contested directory.
		role migrationDirRole
	}{
		{name: "another record's staged copy", role: migrationRoleStaged},
		{name: "another record's sidecar copy", role: migrationRoleSidecar},
	}

	// Only the displaced directory is driven end to end. The canonical
	// directory takes the identical guard, but a canonical name's last word is
	// the index type and never a sidecar role word, so no record can name one
	// in a staged or sidecar role: migrationHandleIsSidecarShaped refuses it at
	// the writer, and that collision has no producer left to drive it from.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")

			// The bystander: an ordinary in-flight migration of another
			// property, naming the contested directory in a live-data role.
			const contested = "property_contested__g41_ingest"
			bystander := testMigrationSubject(41, StrategyCodeEnableFilterable, "body")
			migrationDirsInRole(bystander, tt.role)["body"] = contested
			f.put(NewMigrationRecordIterating(bystander, MigrationCheckpoint{}))

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			f.mkdirs("property_title__g42_ingest", contested, "property_title")
			f.put(NewMigrationRecordSwapped(subject, []string{"title"},
				map[string]string{"title": contested}))

			f.reconcile()

			require.Equal(t, contested, f.contentOf(contested),
				"the promotion took the other record's only copy")
			state, present := f.state(subject.Key)
			require.True(t, present)
			require.Equal(t, MigrationStateSwapped, state,
				"a promotion that cannot clear its target must not report itself promoted")
			bystanderState, stillThere := f.state(bystander.Key)
			require.True(t, stillThere, "and the record that owns the directory is untouched")
			require.Equal(t, MigrationStateIterating, bystanderState)
		})
	}
}

// TestATeardownKeepsASurvivorsTrackerDirectory pins the tracker role. Two
// records can name one tracker directory, and it holds each one's payload.mig,
// so one record's teardown must not leave the other naming a path that is gone.
func TestATeardownKeepsASurvivorsTrackerDirectory(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")

	// A live migration, and a cancelled one sharing its tracker directory.
	live := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	f.put(NewMigrationRecordIterating(live, MigrationCheckpoint{}))

	cancelled := testMigrationSubject(41, StrategyCodeEnableFilterable, "body")
	cancelled.TrackerDir = live.TrackerDir
	f.tasks = []*distributedtask.Task{
		testTask(live.TaskID, 42, distributedtask.TaskStatusStarted),
		testTask(cancelled.TaskID, 41, distributedtask.TaskStatusCancelled),
	}
	f.put(NewMigrationRecordIterating(cancelled, MigrationCheckpoint{}))

	f.reconcile()

	_, stillThere := f.state(live.Key)
	require.True(t, stillThere, "the live record is not this teardown's business")
	require.True(t, f.trackerDirExists(live),
		"the survivor's tracker directory holds its payload; removing it strands the record naming it")
	require.Equal(t, live.TaskID, f.trackerPayloadOf(live),
		"and the payload inside it is still the survivor's")
}

// TestCommitMergedRefusesARecordItCouldNeverPromote pins the one wedge the
// reconciler could manufacture itself: a Swapped record written for a property
// naming no canonical directory can never promote, on any later pass.
func TestCommitMergedRefusesARecordItCouldNeverPromote(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")

	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	subject.CanonicalDirs["title"] = ""
	f.mkdirs("property_title__g42_ingest")
	f.tasks = []*distributedtask.Task{testTask(subject.TaskID, 42, distributedtask.TaskStatusFinished)}
	f.put(NewMigrationRecordMerged(subject))

	for pass := 1; pass <= 3; pass++ {
		f.reconcile()
		state, present := f.state(subject.Key)
		require.True(t, present)
		require.Equal(t, MigrationStateMerged, state,
			"pass %d wrote a flip whose promotion can never run", pass)
	}
	require.True(t, f.logged("refusing to commit the flip"),
		"the refusal has to say why, or an operator sees a migration that simply stops")
	require.Equal(t, "property_title__g42_ingest", f.contentOf("property_title__g42_ingest"),
		"and the staged data the flip would have promoted is untouched")
}

// TestADiscardKeepsAnotherRecordsStagedCopy pins the reclaim side of the same
// query: two records may name one staged directory, and post-flip that
// directory is live data for whichever of them still serves from it.
func TestADiscardKeepsAnotherRecordsStagedCopy(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")

	const contested = "property_shared__g1_ingest"
	bystander := testMigrationSubject(41, StrategyCodeEnableFilterable, "body")
	bystander.StagedDirs["body"] = contested
	f.put(NewMigrationRecordIterating(bystander, MigrationCheckpoint{}))

	cancelled := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	cancelled.StagedDirs["title"] = contested
	f.mkdirs(contested, "property_title__s42_reindex")
	f.tasks = []*distributedtask.Task{
		testTask(bystander.TaskID, 41, distributedtask.TaskStatusStarted),
		testTask(cancelled.TaskID, 42, distributedtask.TaskStatusCancelled),
	}
	f.put(NewMigrationRecordIterating(cancelled, MigrationCheckpoint{}))

	f.reconcile()

	require.Equal(t, contested, f.contentOf(contested),
		"the discard took a directory another record still stages into")
	state, stillThere := f.state(bystander.Key)
	require.True(t, stillThere)
	require.Equal(t, MigrationStateIterating, state)
}
