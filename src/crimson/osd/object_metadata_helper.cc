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
 */

subsets_t calc_clone_subsets(
  SnapSet& snapset, const hobject_t& soid,
  const pg_missing_t& missing,
  const hobject_t &last_backfill)
{
  subsets_t subsets;
  logger().debug("{}: {} clone_overlap {} ",
                 __func__, soid, snapset.clone_overlap);

  uint64_t size = snapset.clone_size[soid.snap];
  if (size) {
    subsets.data_subset.insert(0, size);
  }

  // TODO: make sure CEPH_FEATURE_OSD_CACHEPOOL is not supported in Crimson
  // Skips clone subsets if caching was enabled (allow_incomplete_clones).

#ifndef UNIT_TESTS_BUILT
  if (!crimson::common::local_conf()->osd_recover_clone_overlap) {
    logger().debug("{} {} -- osd_recover_clone_overlap is disabled",
                   __func__, soid); ;
    return subsets;
  }
#endif

  if (snapset.clones.empty()) {
    logger().debug("{} {} -- no clones", __func__, soid);
    return subsets;
  }

  auto soid_snap_iter = find(snapset.clones.begin(),
                             snapset.clones.end(),
                             soid.snap);
  assert(soid_snap_iter != snapset.clones.end());
  auto soid_snap_index = soid_snap_iter - snapset.clones.begin();

  // any overlap with next older clone?
  interval_set<uint64_t> cloning;
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
      logger().debug("{} {} has prev {} overlap {}",
                     __func__, soid, clone, prev);
      subsets.clone_subsets[clone] = prev;
      cloning.union_of(prev);
      break;
    }
    logger().debug("{} {} does not have prev {} overlap {}",
                   __func__, soid, clone, prev);
  }

  // overlap with next newest?
  interval_set<uint64_t> next;
  if (size) {
    next.insert(0, size);
  }
  for (unsigned i = soid_snap_index+1;
       i < snapset.clones.size(); i++) {
    hobject_t clone = soid;
    clone.snap = snapset.clones[i];
    // clone_overlap of i-1 holds the overlap between i-1 to i
    next.intersection_of(snapset.clone_overlap[snapset.clones[i - 1]]);
    if (!missing.is_missing(clone) && clone < last_backfill) {
      logger().debug("{} {} has next {} overlap {}",
                     __func__, soid, clone, next);
      subsets.clone_subsets[clone] = next;
      cloning.union_of(next);
      break;
    }
    logger().debug("{} {} does not have next {} overlap {}",
                   __func__, soid, clone, next);
  }

#ifndef UNIT_TESTS_BUILT
  if (cloning.num_intervals() >
      crimson::common::local_conf().get_val<uint64_t>
      ("osd_recover_clone_overlap_limit")) {
    logger().debug("skipping clone, too many holes");
    subsets.clone_subsets.clear();
    cloning.clear();
  }
#endif

  // what's left for us to push?
  subsets.data_subset.subtract(cloning);
  logger().debug("{} {} data_subsets {}"
                 "clone_subsets {}",
                 __func__, soid, subsets.data_subset, subsets.clone_subsets);
  return subsets;
}

/*
 * Instead of pushing the whole object to the replica,
 * make use of:
 * 1) ObjectCleanRegion - push modified content only.
 *    - See: dev/osd_internals/partial_object_recovery
 * 2) The modified content may already overlap with the
 *    next older clone obejct. Use the existing clone
 *    object overlap as well.
 */

subsets_t calc_head_subsets(
  uint64_t obj_size,
  SnapSet& snapset,
  const hobject_t& head,
  const pg_missing_t& missing,
  const hobject_t &last_backfill)
{
  logger().debug("{}: {} clone_overlap {} ",
                 __func__, head, snapset.clone_overlap);

  subsets_t subsets;

// 1) Calculate modified content only
  if (obj_size) {
    subsets.data_subset.insert(0, obj_size);
  }
  assert(missing.get_items().contains(head));
  const pg_missing_item missing_item = missing.get_items().at(head);
  // let data_subset store only the modified content of the object.
  subsets.data_subset.intersection_of(missing_item.clean_regions.get_dirty_regions());
  logger().debug("{} {} data_subset {}",
                 __func__, head, subsets.data_subset);

  // TODO: make sure CEPH_FEATURE_OSD_CACHEPOOL is not supported in Crimson
  // Skips clone subsets if caching was enabled (allow_incomplete_clones).

#ifndef UNIT_TESTS_BUILT
  if (!crimson::common::local_conf()->osd_recover_clone_overlap) {
    logger().debug("{} {} -- osd_recover_clone_overlap is disabled",
                   __func__, head);
    return subsets;
  }
#endif

  if (snapset.clones.empty()) {
    logger().debug("{} {} -- no clones", __func__, head);
    return subsets;
  }

  // 2) Find any overlap with next older clone
  interval_set<uint64_t> cloning;
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
      logger().debug("{} {} has prev {} overlap {}",
                     __func__, head, clone, prev);
      cloning = prev;
      break;
    }
    logger().debug("{} {} does not have prev {} overlap {}",
                   __func__, head, clone, prev);
  }

  // let cloning store only the overlap with data_subset
  cloning.intersection_of(subsets.data_subset);
  if (cloning.empty()) {
    logger().debug("skipping clone, nothing needs to clone");
    return subsets;
  }

#ifndef UNIT_TESTS_BUILT
  if (cloning.num_intervals() >
      crimson::common::local_conf().get_val<uint64_t>
      ("osd_recover_clone_overlap_limit")) {
    logger().debug("skipping clone, too many holes");
    subsets.clone_subsets.clear();
    cloning.clear();
  }
#endif

  // what's left for us to push?
  subsets.clone_subsets[clone] = cloning;
  subsets.data_subset.subtract(cloning);
  logger().debug("{} {} data_subsets {}"
                 "clone_subsets {}",
                 __func__, head, subsets.data_subset, subsets.clone_subsets);

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
