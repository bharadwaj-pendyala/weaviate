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

package reindex_mt

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// A migration that flips a property's bucket moves the pre-swap main bucket
// aside into a __<strategy>_backup_<gen> dir. If the run is abandoned, that
// dir is the only copy of what the property held before the flip, and nothing
// the startup audit can read tells a migration that still needs it from one
// that does not.
//
// So the audit reclaims the ingest and reindex dirs of an abandoned run and
// leaves the backup dir alone. The property DELETE is what reclaims it, once
// the property is provably gone and the copy can no longer be wanted.
//
// A COLD tenant is the population that proves it: its shard is out of the
// index's shard map, so the audit reaches it from the directory listing alone
// and takes the unloaded branch, which is the branch that composes the dir
// names rather than calling into a live shard.
//
// The journey runs in two halves around the suite's existing restart, since a
// restart is the only thing that runs the audit.
const (
	orphanAuditClass  = "MTOrphanAuditBackup"
	orphanAuditTenant = "orphan_audit_cold"
	orphanAuditProp   = "title"

	// Generation 9 is far above anything this collection's own migrations
	// claim, so the residue cannot be mistaken for a run of the suite's.
	orphanAuditTracker = "searchable_retokenize_" + orphanAuditProp + "_9"
	orphanAuditIngest  = "property_" + orphanAuditProp + "_searchable__retokenize_ingest_9"
	orphanAuditReindex = "property_" + orphanAuditProp + "_searchable__retokenize_reindex_9"
	orphanAuditBackup  = "property_" + orphanAuditProp + "_searchable__retokenize_backup_9"

	// The audit only destroys on the sweep that finds the quarantine sentinel
	// older than its window, and the window is minutes. The plant writes the
	// sentinel with an mtime far in the past so the one sweep this suite's
	// restart affords is the destructive one.
	orphanAuditAgedSentinelStamp = "202001010000"

	orphanAuditObjects = 20
)

// plantOrphanAuditResidue builds the COLD tenant and writes the abandoned
// run's on-disk state onto it. Runs before the suite's restart.
func plantOrphanAuditResidue(ctx context.Context, t *testing.T, c testcontainers.Container) {
	t.Helper()

	createMTClass(t, orphanAuditClass, []*models.Property{
		{Name: "name", DataType: []string{"text"}, Tokenization: "word"},
		{Name: orphanAuditProp, DataType: []string{"text"}, Tokenization: "word"},
	})
	addTenants(t, orphanAuditClass, []string{orphanAuditTenant})

	objs := make([]*models.Object, 0, orphanAuditObjects)
	for i := 0; i < orphanAuditObjects; i++ {
		objs = append(objs, &models.Object{
			Class:      orphanAuditClass,
			Properties: map[string]interface{}{"name": "corpus doc", orphanAuditProp: "planted title"},
			Tenant:     orphanAuditTenant,
		})
	}
	helper.CreateObjectsBatch(t, objs)

	// Deactivate first: a COLD tenant leaves the index's shard map, and it is
	// also the only moment nothing holds the shard's files open.
	setTenantStatusIn(t, orphanAuditClass, []string{orphanAuditTenant},
		models.TenantActivityStatusCOLD)

	lsm := tenantLSMPathIn(orphanAuditClass, orphanAuditTenant)
	tracker := lsm + "/.migrations/" + orphanAuditTracker
	payload, err := json.Marshal(map[string]interface{}{
		// A task id no scheduler knows is what makes the tracker an orphan.
		"taskID":      "orphan-audit-no-such-task",
		"taskVersion": 4242,
		"unitID":      "orphan-audit-no-such-unit",
		"payload": map[string]interface{}{
			"migrationType": "change-tokenization",
			"collection":    orphanAuditClass,
			"properties":    []string{orphanAuditProp},
		},
	})
	require.NoError(t, err)

	var cmd strings.Builder
	fmt.Fprintf(&cmd, "mkdir -p %s && printf '%%s' 2020-01-01T00:00:00.000000000Z > %s/started.mig && ",
		tracker, tracker)
	fmt.Fprintf(&cmd, "printf '%%s' '%s' | base64 -d > %s/payload.mig && ",
		base64.StdEncoding.EncodeToString(payload), tracker)
	fmt.Fprintf(&cmd, ": > %s/audit_quarantined.mig && touch -t %s %s/audit_quarantined.mig",
		tracker, orphanAuditAgedSentinelStamp, tracker)
	for _, dir := range []string{orphanAuditIngest, orphanAuditReindex, orphanAuditBackup} {
		fmt.Fprintf(&cmd, " && mkdir -p %s/%s && printf '%%s' residue > %s/%s/residue.marker",
			lsm, dir, lsm, dir)
	}
	execInContainer(ctx, t, c, cmd.String())

	require.True(t, containsDir(trackerDirsIn(ctx, t, c, orphanAuditClass, orphanAuditTenant), orphanAuditTracker),
		"planted tracker must be on disk before the restart")
	planted := lsmDirsIn(ctx, t, c, orphanAuditClass, orphanAuditTenant)
	for _, dir := range []string{orphanAuditIngest, orphanAuditReindex, orphanAuditBackup} {
		require.Truef(t, containsDir(planted, dir),
			"planted sidecar %q must be on disk before the restart. dirs: %v", dir, planted)
	}
}

// testOrphanAuditKeepsBackupSidecar asserts the split the audit makes, then
// shows the backup dir is only deferred by deleting the property's index and
// watching the compensating sweep take it. Runs after the suite's restart.
func testOrphanAuditKeepsBackupSidecar(ctx context.Context, t *testing.T,
	restURI string, c testcontainers.Container,
) {
	// The audit runs from a startup goroutine that first waits for the task
	// scheduler, so poll rather than assume it has already finished. The
	// reindex dir is the last one it removes.
	require.Eventuallyf(t, func() bool {
		return !containsDir(lsmDirsIn(ctx, t, c, orphanAuditClass, orphanAuditTenant), orphanAuditReindex)
	}, 120*time.Second, time.Second,
		"the startup audit never reclaimed %q on the COLD tenant; dirs: %v",
		orphanAuditReindex, lsmDirsIn(ctx, t, c, orphanAuditClass, orphanAuditTenant))

	dirs := lsmDirsIn(ctx, t, c, orphanAuditClass, orphanAuditTenant)
	assert.Falsef(t, containsDir(dirs, orphanAuditIngest),
		"the ingest dir holds only the abandoned run's own staged data, so the audit must reclaim it. dirs: %v", dirs)
	assert.Truef(t, containsDir(dirs, orphanAuditBackup),
		"the backup dir holds the pre-swap copy of %q's bucket and the audit cannot tell whether a "+
			"migration still needs it; reclaiming it here is silent data loss. dirs: %v", orphanAuditProp, dirs)
	trackers := trackerDirsIn(ctx, t, c, orphanAuditClass, orphanAuditTenant)
	assert.Falsef(t, containsDir(trackers, orphanAuditTracker),
		"the audit removes the orphan's tracker dir. dirs: %v", trackers)

	// Reactivate and hydrate. The property DELETE only sweeps sidecar dirs on
	// a loaded shard, so a tenant that is merely HOT is not enough.
	setTenantStatusIn(t, orphanAuditClass, []string{orphanAuditTenant}, models.TenantActivityStatusHOT)
	require.NotEmpty(t, bm25QueryTenant(t, orphanAuditClass, "name", "corpus", orphanAuditTenant),
		"reactivated tenant must serve its objects, which is also what loads its shard")

	deletePropertyIndex(t, restURI, orphanAuditClass, orphanAuditProp, "searchable")

	require.Eventuallyf(t, func() bool {
		return !containsDir(lsmDirsIn(ctx, t, c, orphanAuditClass, orphanAuditTenant), orphanAuditBackup)
	}, 60*time.Second, time.Second,
		"deleting the %q index leaves the property no bucket to come back to, so the backup copy the "+
			"audit deferred must be reclaimed here or never; dirs: %v",
		orphanAuditProp, lsmDirsIn(ctx, t, c, orphanAuditClass, orphanAuditTenant))
}

// =============================================================================
// Helpers
// =============================================================================

func deletePropertyIndex(t *testing.T, restURI, className, property, indexType string) {
	t.Helper()
	url := fmt.Sprintf("http://%s/v1/schema/%s/properties/%s/index/%s",
		restURI, className, property, indexType)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Equalf(t, http.StatusOK, resp.StatusCode,
		"DELETE %s: %s", url, string(body))
}

func tenantLSMPathIn(className, tenant string) string {
	return fmt.Sprintf("/data/%s/%s/lsm", strings.ToLower(className), tenant)
}

// lsmDirsIn lists the tenant's sidecar bucket dirs at the LSM root.
func lsmDirsIn(ctx context.Context, t *testing.T, c testcontainers.Container,
	className, tenant string,
) []string {
	t.Helper()
	return execLines(ctx, t, c,
		fmt.Sprintf("ls -1 %s 2>/dev/null || true", tenantLSMPathIn(className, tenant)))
}

func trackerDirsIn(ctx context.Context, t *testing.T, c testcontainers.Container,
	className, tenant string,
) []string {
	t.Helper()
	return execLines(ctx, t, c,
		fmt.Sprintf("ls -1 %s/.migrations 2>/dev/null || true", tenantLSMPathIn(className, tenant)))
}

func execLines(ctx context.Context, t *testing.T, c testcontainers.Container, cmd string) []string {
	t.Helper()
	var out []string
	for _, line := range strings.Split(execInContainer(ctx, t, c, cmd), "\n") {
		if cleaned := cleanExecLine(line); cleaned != "" {
			out = append(out, cleaned)
		}
	}
	return out
}

func setTenantStatusIn(t *testing.T, className string, names []string, status string) {
	t.Helper()
	updates := make([]*models.Tenant, len(names))
	for i, name := range names {
		updates[i] = &models.Tenant{Name: name, ActivityStatus: status}
	}
	helper.UpdateTenants(t, className, updates)

	wanted := map[string]bool{status: true}
	switch status {
	case models.TenantActivityStatusCOLD:
		wanted[models.TenantActivityStatusINACTIVE] = true
	case models.TenantActivityStatusHOT:
		wanted[models.TenantActivityStatusACTIVE] = true
	}
	require.Eventually(t, func() bool {
		got, err := helper.GetTenants(t, className)
		if err != nil {
			return false
		}
		pending := len(names)
		for _, tenant := range got.Payload {
			for _, name := range names {
				if tenant.Name == name && wanted[tenant.ActivityStatus] {
					pending--
				}
			}
		}
		return pending == 0
	}, 60*time.Second, 200*time.Millisecond,
		"tenants %v should report activity status %q", names, status)
}
