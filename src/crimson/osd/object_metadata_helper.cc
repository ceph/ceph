// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab expandtab

#include "crimson/osd/object_metadata_helper.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

/*
 *   The clone object content may already overlap with the
 *   next older and the next newest clone obejct.
 *   Use the existing (next) clones object overlaps instead
 *   of pushing the whole clone object to the replica.
 *
 *   Returns candidates in preference order (closest first).
 *   The caller locks the first usable candidate from each list.
 */

clone_overlap_plan_t calc_clone_subsets(
  SnapSet& snapset, const hobject_t& soid,
  const pg_missing_t& missing,
  const hobject_t &last_backfill)
{
  clone_overlap_plan_t plan;
  logger().debug("{}: {} clone_overlap {} ",
                 __func__, soid, snapset.clone_overlap);
  assert(missing.get_items().contains(soid));
  const pg_missing_item &missing_item = missing.get_items().at(soid);
  auto dirty_regions = missing_item.clean_regions.get_dirty_regions();
  if (dirty_regions.empty()) {
    logger().debug(
      "{} {} not touched, no need to recover, skipping",
      __func__,
      soid);
    return plan;
  }
  uint64_t size = snapset.clone_size[soid.snap];
  if (size) {
    plan.data_subset.insert(0, size);
  }

  // let data_subset store only the modified content of the object.
  plan.data_subset.intersection_of(dirty_regions);
  logger().debug("{} {} data_subset {}",
                 __func__, soid, plan.data_subset);

  // TODO: make sure CEPH_FEATURE_OSD_CACHEPOOL is not supported in Crimson
  // Skips clone subsets if caching was enabled (allow_incomplete_clones).

#ifndef UNIT_TESTS_BUILT
  if (!crimson::common::local_conf()->osd_recover_clone_overlap) {
    logger().debug("{} {} -- osd_recover_clone_overlap is disabled",
                   __func__, soid);
    return plan;
  }
#endif

  if (snapset.clones.empty()) {
    logger().debug("{} {} -- no clones", __func__, soid);
    return plan;
  }

  auto soid_snap_iter = find(snapset.clones.begin(),
                             snapset.clones.end(),
                             soid.snap);
  assert(soid_snap_iter != snapset.clones.end());
  auto soid_snap_index = soid_snap_iter - snapset.clones.begin();

  // older clones, closest first
  interval_set<uint64_t> prev;
  if (size) {
    prev.insert(0, size);
  }
  for (int i = soid_snap_index - 1; i >= 0; i--) {
    hobject_t clone = soid;
    clone.snap = snapset.clones[i];
    // clone_overlap of i holds the overlap between i to i+1
    prev.intersection_of(snapset.clone_overlap[snapset.clones[i]]);
    if (!missing.is_missing(clone) && clone < last_backfill) {
      logger().debug("{} {} candidate prev {} overlap {}",
                     __func__, soid, clone, prev);
      plan.older_candidates.push_back(
        clone_candidate_t{clone, prev});
    } else {
      logger().debug("{} {} does not have prev {} overlap {}",
                     __func__, soid, clone, prev);
    }
  }

  // newer clones, closest first
  interval_set<uint64_t> next;
  if (size) {
    next.insert(0, size);
  }
  for (unsigned i = soid_snap_index + 1;
       i < snapset.clones.size(); i++) {
    hobject_t clone = soid;
    clone.snap = snapset.clones[i];
    // clone_overlap of i-1 holds the overlap between i-1 to i
    next.intersection_of(snapset.clone_overlap[snapset.clones[i - 1]]);
    if (!missing.is_missing(clone) && clone < last_backfill) {
      logger().debug("{} {} candidate next {} overlap {}",
                     __func__, soid, clone, next);
      plan.newer_candidates.push_back(
        clone_candidate_t{clone, next});
    } else {
      logger().debug("{} {} does not have next {} overlap {}",
                     __func__, soid, clone, next);
    }
  }

  logger().debug("{} {} data_subset {} older_candidates {} newer_candidates {}",
                 __func__, soid, plan.data_subset,
                 plan.older_candidates.size(), plan.newer_candidates.size());
  return plan;
}

/*
 * Instead of pushing the whole object to the replica,
 * make use of:
 * 1) ObjectCleanRegion - push modified content only.
 *    - See: dev/osd_internals/partial_object_recovery
 * 2) The modified content may already overlap with the
 *    next older clone obejct. Use the existing clone
 *    object overlap as well.
 *
 * Returns older candidates in preference order (newest clone first).
 */

clone_overlap_plan_t calc_head_subsets(
  uint64_t obj_size,
  SnapSet& snapset,
  const hobject_t& head,
  const pg_missing_t& missing,
  const hobject_t &last_backfill)
{
  logger().debug("{}: {} clone_overlap {} ",
                 __func__, head, snapset.clone_overlap);

  clone_overlap_plan_t plan;

// 1) Calculate modified content only
  if (obj_size) {
    plan.data_subset.insert(0, obj_size);
  }
  assert(missing.get_items().contains(head));
  const pg_missing_item &missing_item = missing.get_items().at(head);
  // let data_subset store only the modified content of the object.
  plan.data_subset.intersection_of(missing_item.clean_regions.get_dirty_regions());
  logger().debug("{} {} data_subset {}",
                 __func__, head, plan.data_subset);

  // TODO: make sure CEPH_FEATURE_OSD_CACHEPOOL is not supported in Crimson
  // Skips clone subsets if caching was enabled (allow_incomplete_clones).

#ifndef UNIT_TESTS_BUILT
  if (!crimson::common::local_conf()->osd_recover_clone_overlap) {
    logger().debug("{} {} -- osd_recover_clone_overlap is disabled",
                   __func__, head);
    return plan;
  }
#endif

  if (snapset.clones.empty()) {
    logger().debug("{} {} -- no clones", __func__, head);
    return plan;
  }

  // 2) Candidates: next older clones, newest first
  interval_set<uint64_t> prev;
  hobject_t clone = head;
  if (obj_size) {
    prev.insert(0, obj_size);
  }
  for (int i = snapset.clones.size()-1; i >= 0; i--) {
    clone.snap = snapset.clones[i];
    // let prev store only the overlap with clone i
    prev.intersection_of(snapset.clone_overlap[snapset.clones[i]]);
    if (!missing.is_missing(clone) && clone < last_backfill) {
      interval_set<uint64_t> overlap = prev;
      overlap.intersection_of(plan.data_subset);
      if (overlap.empty()) {
        logger().debug("{} {} candidate prev {} overlap empty after data_subset",
                       __func__, head, clone);
      } else {
        logger().debug("{} {} candidate prev {} overlap {}",
                       __func__, head, clone, overlap);
        plan.older_candidates.push_back(
          clone_candidate_t{clone, std::move(overlap)});
      }
    } else {
      logger().debug("{} {} does not have prev {} overlap {}",
                     __func__, head, clone, prev);
    }
  }

  logger().debug("{} {} data_subset {} older_candidates {}",
                 __func__, head, plan.data_subset,
                 plan.older_candidates.size());
  return plan;
}

subsets_t commit_subsets_unlocked(clone_overlap_plan_t plan)
{
  subsets_t subsets;
  subsets.data_subset = std::move(plan.data_subset);
  interval_set<uint64_t> cloning;
  if (!plan.older_candidates.empty()) {
    const auto& c = plan.older_candidates.front();
    subsets.clone_subsets[c.clone] = c.overlap;
    cloning.union_of(c.overlap);
  }
  if (!plan.newer_candidates.empty()) {
    const auto& c = plan.newer_candidates.front();
    subsets.clone_subsets[c.clone] = c.overlap;
    cloning.union_of(c.overlap);
  }
  subsets.data_subset.subtract(cloning);
  return subsets;
}

void set_subsets(
  const subsets_t& subsets,
  ObjectRecoveryInfo& recovery_info)
{
  recovery_info.copy_subset = subsets.data_subset;
  recovery_info.clone_subset = subsets.clone_subsets;
}


}
