#pragma once

#include <vector>

#include "osd/osd_types_fmt.h"

namespace crimson::osd {

struct clone_candidate_t {
  hobject_t clone;
  interval_set<uint64_t> overlap;
};

/// Pure overlap plan: candidates in preference order (best-first).
/// Locking / committing a candidate is the caller's responsibility.
struct clone_overlap_plan_t {
  interval_set<uint64_t> data_subset;
  std::vector<clone_candidate_t> older_candidates;
  std::vector<clone_candidate_t> newer_candidates;
};

struct subsets_t {
  interval_set<uint64_t> data_subset;
  std::map<hobject_t, interval_set<uint64_t>> clone_subsets;
};

clone_overlap_plan_t calc_clone_subsets(
  SnapSet& snapset, const hobject_t& soid,
  const pg_missing_t& missing,
  const hobject_t &last_backfill);
clone_overlap_plan_t calc_head_subsets(
  uint64_t obj_size,
  SnapSet& snapset,
  const hobject_t& head,
  const pg_missing_t& missing,
  const hobject_t &last_backfill);

/// Build final subsets from a plan, taking the first candidate in each
/// list (no locking). Used by unit tests.
subsets_t commit_subsets_unlocked(clone_overlap_plan_t plan);

void set_subsets(
  const subsets_t& subsets,
  ObjectRecoveryInfo& recovery_info);
}
