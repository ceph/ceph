// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "journal.h"
#include "journal/segmented_journal.h"
#include "journal/circular_bounded_journal.h"

namespace crimson::os::seastore {

Journal::scan_alloc_map_ret
Journal::scan_alloc_map() {
  alloc_map_t map;
  journal_seq_t tail = get_dirty_tail() <= get_alloc_tail() ?
    get_dirty_tail() : get_alloc_tail();
  auto build_paddr_seq_map = [&map](
    const auto &offsets,
    const auto &e,
    sea_time_point modify_time)
  {
    if (e.type == extent_types_t::ALLOC_INFO) {
      alloc_delta_t alloc_delta;
      decode(alloc_delta, e.bl);
      if (alloc_delta.op == alloc_delta_t::op_types_t::CLEAR) {
        for (auto &alloc_blk : alloc_delta.alloc_blk_ranges) {
          map[alloc_blk.paddr] = offsets.write_result.start_seq;
        }
      }
    }
    return replay_ertr::make_ready_future<bool>(true);
  };
  // build the paddr->journal_seq_t map from extent allocations
  co_await scan_valid_record_delta(std::move(build_paddr_seq_map), tail);
  co_return map;
}

namespace journal {

JournalRef make_segmented(
  store_index_t store_index,
  SegmentProvider &provider,
  JournalTrimmer &trimmer,
  bool scan_alloc_on_boot)
{
  return std::make_unique<SegmentedJournal>(
    store_index, provider, trimmer, scan_alloc_on_boot);
}

JournalRef make_circularbounded(
  store_index_t store_index,
  JournalTrimmer &trimmer,
  crimson::os::seastore::random_block_device::RBMDevice* device,
  std::string path)
{
  return std::make_unique<CircularBoundedJournal>(store_index, trimmer, device, path);
}

} // namespace journal

} // namespace crimson::os::seastore
