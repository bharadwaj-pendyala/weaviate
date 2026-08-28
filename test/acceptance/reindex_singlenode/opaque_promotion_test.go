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

package reindex_singlenode

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/helpers/reindexrecords"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const opaquePromotionObjectCount = 30

// testPromotionRunsOnRecordedHandles pins that no directory name is ever
// inferred: a migration's live data sits at a randomly named directory only
// the record can locate, and a restart must promote it to the canonical
// name from that record. Deriving the name instead finds nothing, serving
// an empty bucket under a schema that reports ready.
func testPromotionRunsOnRecordedHandles(t *testing.T, compose *docker.DockerCompose) {
	const class = "OpaquePromotion"
	ctx := context.Background()
	trueVal := true

	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{Name: "score", DataType: []string{"int"}, IndexFilterable: &trueVal},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	for i := 0; i < opaquePromotionObjectCount; i++ {
		score := 10
		if i%2 == 0 {
			score = 100
		}
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class: class, Properties: map[string]interface{}{"score": score},
		}))
	}
	require.Equal(t, opaquePromotionObjectCount/2, rangeFilterHits(t, class, "score", 50),
		"the fixture must serve before anything is moved")

	container := compose.GetWeaviate().Container()
	lsmPath := findShardPathInContainer(t, container, class) + "/lsm"

	// A name with a random infix: no prefix table, property name or generation
	// suffix in the codebase can produce it, so a reader that finds this
	// directory found it through the record.
	staged := "m_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	code, _, err := container.Exec(ctx, []string{
		"mv", lsmPath + "/property_score", lsmPath + "/" + staged,
	})
	require.NoError(t, err)
	require.Zero(t, code, "moving the live bucket aside must succeed")

	plantSwappedRecordAcrossRestart(t, compose, lsmPath, staged)

	require.Equal(t, opaquePromotionObjectCount/2, rangeFilterHits(t, class, "score", 50),
		"the recorded staged directory %q was not promoted to the canonical name; the "+
			"property is answering from a bucket that holds none of its data while the "+
			"schema reports it ready", staged)

	code, _, err = compose.GetWeaviate().Container().Exec(ctx, []string{"test", "-d", lsmPath + "/" + staged})
	require.NoError(t, err)
	require.NotZero(t, code, "the staged directory must be gone once its data is at the canonical name")
}

// plantSwappedRecordAcrossRestart writes the record of a migration whose flip
// decision is durable but whose promotion never ran, then restarts the node so
// reconciliation meets it at load. The staged directory is the one holding the
// live data; the canonical name is where promotion has to put it.
func plantSwappedRecordAcrossRestart(t *testing.T, compose *docker.DockerCompose, lsmPath, staged string) {
	t.Helper()
	ctx := context.Background()

	subject := opaqueMigrationSubject(4711, "opaque-promotion", "opaque_promotion_tracker", staged)
	recordName, record := reindexrecords.Encode(t, db.NewMigrationRecordSwapped(
		subject, []string{"score"}, map[string]string{"score": "property_score"}))

	// Repoint on every exit path: the restart rebinds the host port, and a
	// failure in between would otherwise strand the client on the old one.
	defer func() { helper.SetupClient(compose.GetWeaviate().URI()) }()

	require.NoError(t, compose.StopAt(ctx, 0, nil),
		"graceful stop before planting the record must succeed")

	// CopyDirToContainer works against a stopped container; docker exec does not.
	stagedRoot := t.TempDir()
	dotMigrations := filepath.Join(stagedRoot, ".migrations")
	require.NoError(t, os.MkdirAll(filepath.Join(dotMigrations, "records"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(dotMigrations, "opaque_promotion_tracker"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(dotMigrations, "records", recordName), []byte(record), 0o666))

	require.NoError(t,
		compose.GetWeaviate().Container().CopyDirToContainer(ctx, dotMigrations, lsmPath+"/.migrations", 0o755),
		"CopyDirToContainer must succeed against the stopped container")

	require.NoError(t, compose.StartAt(ctx, 0), "restart after planting must succeed")
}

// opaqueMigrationSubject is the one-property repair-filterable both planters
// record, differing only in which migration it is and where its data sits.
func opaqueMigrationSubject(taskVersion uint64, taskID, trackerDir, staged string) db.MigrationSubject {
	return db.MigrationSubject{
		Key: db.MigrationRecordKey{
			TaskVersion:  taskVersion,
			StrategyCode: db.StrategyCodeFilterableRoaringsetRefresh,
			UnitID:       "u0",
		},
		TaskID:          taskID,
		MigrationType:   db.ReindexTypeRepairFilterable,
		Properties:      []string{"score"},
		IterationCutoff: time.Now().UTC(),
		TrackerDir:      trackerDir,
		StagedDirs:      map[string]string{"score": staged},
		CanonicalDirs:   map[string]string{"score": "property_score"},
	}
}
