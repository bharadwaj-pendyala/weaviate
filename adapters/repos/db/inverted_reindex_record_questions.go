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

// The questions every passive reader may ask a record; a reader needing
// something finer switches on the variant instead.
//
// Composition stays out of call sites: the rule needing two answers
// (StagedDataComplete and not PointerSwapped means discardable) lives in
// reconciliation's cancel edge.
type migrationRecordQuestions interface {
	// StagedDataComplete reports whether this migration's output is fully
	// staged. Not a commitment: until PointerSwapped, the canonical bucket is
	// still primary, and a cancelled task discards the staged copy whole.
	StagedDataComplete() bool

	// PointerSwapped reports whether the flip decision is durable. From here
	// the migration is irreversible: the new buckets may hold acknowledged
	// writes the old copy never received.
	PointerSwapped() bool

	// IterationComplete reports whether the pass over objects has finished.
	// False is the only answer a resume may act on; past true, a second pass
	// would re-run iteration over data already written.
	IterationComplete() bool

	// OwnsBucket reports whether dir is one this migration created. No
	// record-driven reclaimer removes a directory no record attributes; the
	// marker-era sweeps reclaim by tracker name and consult no record.
	OwnsBucket(dir string) bool
}

// OwnsBucket is a subject fact, not a state fact: a migration owns directories
// it created from creation until they are gone. The canonical directory is
// deliberately not among them — it predates the migration and outlives it.
//
// Keyed by Properties, like [migrationOwnedDirs]: the one caller is the
// invariant check that no directory survives unattributed, and an answer more
// generous than the reclaimer's would pass on a state the reclaimer leaks.
func (b migrationRecordBase) OwnsBucket(dir string) bool {
	if dir == "" {
		return false
	}
	for _, prop := range b.subject.Properties {
		if b.subject.SidecarDirs[prop] == dir || b.subject.StagedDirs[prop] == dir {
			return true
		}
	}
	return false
}

func (r MigrationRecordIterating) StagedDataComplete() bool { return false }
func (r MigrationRecordIterated) StagedDataComplete() bool  { return false }
func (r MigrationRecordMerged) StagedDataComplete() bool    { return true }
func (r MigrationRecordSwapped) StagedDataComplete() bool   { return true }
func (r MigrationRecordPromoted) StagedDataComplete() bool  { return true }

func (r MigrationRecordIterating) IterationComplete() bool { return false }
func (r MigrationRecordIterated) IterationComplete() bool  { return true }
func (r MigrationRecordMerged) IterationComplete() bool    { return true }
func (r MigrationRecordSwapped) IterationComplete() bool   { return true }
func (r MigrationRecordPromoted) IterationComplete() bool  { return true }

func (r MigrationRecordIterating) PointerSwapped() bool { return false }
func (r MigrationRecordIterated) PointerSwapped() bool  { return false }
func (r MigrationRecordMerged) PointerSwapped() bool    { return false }
func (r MigrationRecordSwapped) PointerSwapped() bool   { return true }
func (r MigrationRecordPromoted) PointerSwapped() bool  { return true }
