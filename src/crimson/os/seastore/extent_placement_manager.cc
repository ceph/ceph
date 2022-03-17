// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/os/seastore/extent_placement_manager.h"

#include "crimson/common/config_proxy.h"

SET_SUBSYS(seastore_journal);

namespace crimson::os::seastore {

SegmentedAllocator::SegmentedAllocator(
  SegmentProvider& sp,
  SegmentManager& sm)
  : cold_writer{"COLD", sp, sm},
    rewrite_writer{"REWRITE", sp, sm}
{
}

SegmentedAllocator::Writer::Writer(
  std::string name,
  SegmentProvider& sp,
  SegmentManager& sm)
  : segment_allocator(name, segment_type_t::OOL, sp, sm),
    record_submitter(crimson::common::get_conf<uint64_t>(
                       "seastore_journal_iodepth_limit"),
                     crimson::common::get_conf<uint64_t>(
                       "seastore_journal_batch_capacity"),
                     crimson::common::get_conf<Option::size_t>(
                       "seastore_journal_batch_flush_size"),
                     crimson::common::get_conf<double>(
                       "seastore_journal_batch_preferred_fullness"),
                     segment_allocator)
{
}

SegmentedAllocator::Writer::write_ertr::future<>
SegmentedAllocator::Writer::write_record(
  Transaction& t,
  record_t&& record,
  std::list<LogicalCachedExtentRef>&& extents)
{
  LOG_PREFIX(SegmentedAllocator::Writer::write_record);
  assert(extents.size());
  assert(extents.size() == record.extents.size());
  assert(!record.deltas.size());

  // account transactional ool writes before write()
  // TODO: drop the incorrect size and fix the metrics
  auto record_size = record_group_size_t(
      record.size, segment_allocator.get_block_size());
  auto& stats = t.get_ool_write_stats();
  stats.extents.num += extents.size();
  stats.extents.bytes += record_size.dlength;
  stats.header_raw_bytes += record_size.get_raw_mdlength();
  stats.header_bytes += record_size.get_mdlength();
  stats.data_bytes += record_size.dlength;
  stats.num_records += 1;

  return record_submitter.submit(std::move(record)
  ).safe_then([this, FNAME, &t, extents=std::move(extents)
              ](record_locator_t ret) mutable {
    assert(ret.write_result.start_seq.segment_seq == OOL_SEG_SEQ);
    DEBUGT("{} finish with {} and {} extents",
           t, segment_allocator.get_name(),
           ret, extents.size());
    paddr_t extent_addr = ret.record_block_base;
    for (auto& extent : extents) {
      TRACET("{} ool extent written at {} -- {}",
             t, segment_allocator.get_name(),
             extent_addr, *extent);
      extent->hint = placement_hint_t::NUM_HINTS; // invalidate hint
      t.mark_delayed_extent_ool(extent, extent_addr);
      extent_addr = extent_addr.as_seg_paddr().add_offset(
          extent->get_length());
    }
  });
}

SegmentedAllocator::Writer::write_iertr::future<>
SegmentedAllocator::Writer::do_write(
  Transaction& t,
  std::list<LogicalCachedExtentRef>& extents)
{
  LOG_PREFIX(SegmentedAllocator::Writer::do_write);
  assert(!extents.empty());
  if (!record_submitter.is_available()) {
    DEBUGT("{} extents={} wait ...",
           t, segment_allocator.get_name(),
           extents.size());
    return trans_intr::make_interruptible(
      record_submitter.wait_available()
    ).si_then([this, &t, &extents] {
      return do_write(t, extents);
    });
  }
  record_t record;
  std::list<LogicalCachedExtentRef> pending_extents;

  auto commit_time = seastar::lowres_system_clock::now();
  record_commit_type_t commit_type;
  if (t.get_src() == Transaction::src_t::MUTATE) {
    commit_type = record_commit_type_t::MODIFY;
  } else {
    assert(t.get_src() == Transaction::src_t::CLEANER_TRIM ||
           t.get_src() == Transaction::src_t::CLEANER_RECLAIM);
    commit_type = record_commit_type_t::REWRITE;
  }
  record.commit_time = commit_time.time_since_epoch().count();
  record.commit_type = commit_type;

  for (auto it = extents.begin(); it != extents.end();) {
    auto& extent = *it;
    record_size_t wouldbe_rsize = record.size;
    wouldbe_rsize.account_extent(extent->get_bptr().length());
    using action_t = journal::RecordSubmitter::action_t;
    action_t action = record_submitter.check_action(wouldbe_rsize);
    if (action == action_t::ROLL) {
      auto num_extents = pending_extents.size();
      DEBUGT("{} extents={} submit {} extents and roll, unavailable ...",
             t, segment_allocator.get_name(),
             extents.size(), num_extents);
      auto fut_write = write_ertr::now();
      if (num_extents > 0) {
        assert(record_submitter.check_action(record.size) !=
               action_t::ROLL);
        fut_write = write_record(
            t, std::move(record), std::move(pending_extents));
      }
      return trans_intr::make_interruptible(
        record_submitter.roll_segment(
        ).safe_then([fut_write=std::move(fut_write)]() mutable {
          return std::move(fut_write);
        })
      ).si_then([this, &t, &extents] {
        return do_write(t, extents);
      });
    }

    TRACET("{} extents={} add extent to record -- {}",
           t, segment_allocator.get_name(),
           extents.size(), *extent);
    if (commit_type == record_commit_type_t::MODIFY) {
      extent->set_last_modified(commit_time);
    } else {
      assert(commit_type == record_commit_type_t::REWRITE);
      extent->set_last_rewritten(commit_time);
    }
    ceph::bufferlist bl;
    extent->prepare_write();
    bl.append(extent->get_bptr());
    assert(bl.length() == extent->get_length());
    record.push_back(extent_t{
      extent->get_type(),
      extent->get_laddr(),
      std::move(bl),
      extent->get_last_modified().time_since_epoch().count()});
    pending_extents.push_back(extent);
    it = extents.erase(it);

    assert(record_submitter.check_action(record.size) == action);
    if (action == action_t::SUBMIT_FULL) {
      DEBUGT("{} extents={} submit {} extents ...",
             t, segment_allocator.get_name(),
             extents.size(), pending_extents.size());
      return trans_intr::make_interruptible(
        write_record(t, std::move(record), std::move(pending_extents))
      ).si_then([this, &t, &extents] {
        if (!extents.empty()) {
          return do_write(t, extents);
        } else {
          return write_iertr::now();
        }
      });
    }
    // SUBMIT_NOT_FULL: evaluate the next extent
  }

  auto num_extents = pending_extents.size();
  DEBUGT("{} submit the rest {} extents ...",
         t, segment_allocator.get_name(),
         num_extents);
  assert(num_extents > 0);
  return trans_intr::make_interruptible(
    write_record(t, std::move(record), std::move(pending_extents)));
}

SegmentedAllocator::Writer::write_iertr::future<>
SegmentedAllocator::Writer::write(
  Transaction& t,
  std::list<LogicalCachedExtentRef>& extents)
{
  if (extents.empty()) {
    return write_iertr::now();
  }
  return seastar::with_gate(write_guard, [this, &t, &extents] {
    return do_write(t, extents);
  });
}

}
