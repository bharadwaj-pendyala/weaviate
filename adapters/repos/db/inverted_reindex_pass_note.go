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
	"sort"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// migrationSettledNoteFile holds the directories the last reconciliation pass
// on this shard left exactly as it found them. It sits beside the tracker
// directories rather than inside the record store, so nothing that enumerates
// records has to know about it, and every reader of .migrations already skips
// non-directories.
const migrationSettledNoteFile = lsmkv.MigrationSettledNoteFile

// The note is a cache, not a state, and that difference is what makes it
// admissible where a sixth record state is not.
//
// It says one thing: "the last load reconciled these directories and changed
// nothing". A sweep deciding whether to wake a cold tenant reads that as "and
// this load would not change anything either", which is exactly the question
// the sweep asks and cannot otherwise answer without hydrating.
//
// Losing it costs one hydration and never correctness — the sweep falls back
// to hydrating, which is what it does today. A stale one costs a directory
// left on disk until the tenant loads for some other reason.
//
// Record writes and removals are not the only events that change what a load
// would do: this node's applied task map is a third, and it changes with no
// record write at all. So a pass names a directory here only when it reached
// an answer for its record that no later load revisits, which is the same
// signal it must set to report the record as wedged
// ([migrationReconciler.wedged]). A pass that merely could not decide names
// nothing, and the tenant is woken as it was before the note existed.
func migrationSettledNotePath(lsmPath string) string {
	return filepath.Join(lsmPath, migrationsDir, migrationSettledNoteFile)
}

// migrationReadSettledNote returns the directories the last pass settled. A
// note that cannot be read is no note: the caller then hydrates, which is the
// answer this whole file exists to avoid but never the wrong one.
func migrationReadSettledNote(lsmPath string) map[string]bool {
	data, err := os.ReadFile(migrationSettledNotePath(lsmPath))
	if err != nil {
		return nil
	}
	out := map[string]bool{}
	for _, line := range strings.Split(string(data), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			out[line] = true
		}
	}
	return out
}

// migrationWriteSettledNote replaces the note with dirs, or removes it when
// the pass settled nothing. Failures are the caller's to log: a note that
// could not be written costs the hydration it would have saved.
func migrationWriteSettledNote(lsmPath string, dirs []string) error {
	path := migrationSettledNotePath(lsmPath)
	if len(dirs) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	sort.Strings(dirs)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(strings.Join(dirs, "\n")+"\n"), 0o600)
}

// migrationDiscardSettledNote drops the note. Every record write and removal
// calls this: those are the only events that change what a load would do, so
// dropping it there is what keeps the note from outliving its evidence.
func migrationDiscardSettledNote(lsmPath string) {
	os.Remove(migrationSettledNotePath(lsmPath))
}

// migrationRecordFingerprint is what a pass compares to tell a record it
// changed from one it left alone. The encoding is the whole record, so no
// transition can slip past by living in a field this forgot to name.
func migrationRecordFingerprint(rec MigrationRecord) string {
	data, err := encodeMigrationRecord(rec)
	if err != nil {
		// Unencodable reads as "changed", so such a record never settles.
		return ""
	}
	return string(data)
}
