// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/denc.h"
#include "include/intarith.h"

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/journal.h"

SET_SUBSYS(seastore_tm);

namespace crimson::os::seastore {

TransactionManager::TransactionManager(
  SegmentManager &_segment_manager,
  SegmentCleanerRef _segment_cleaner,
  JournalRef _journal,
  CacheRef _cache,
  LBAManagerRef _lba_manager,
  ExtentPlacementManagerRef&& epm,
  ExtentReader& scanner)
  : segment_manager(_segment_manager),
    segment_cleaner(std::move(_segment_cleaner)),
    cache(std::move(_cache)),
    lba_manager(std::move(_lba_manager)),
    journal(std::move(_journal)),
    epm(std::move(epm)),
    scanner(scanner)
{
  segment_cleaner->set_extent_callback(this);
  journal->set_write_pipeline(&write_pipeline);
  register_metrics();
}

TransactionManager::mkfs_ertr::future<> TransactionManager::mkfs()
{
  LOG_PREFIX(TransactionManager::mkfs);
  segment_cleaner->mount(
    segment_manager.get_device_id(),
    scanner.get_segment_managers());
  return journal->open_for_write().safe_then([this, FNAME](auto addr) {
    DEBUG("about to do_with");
    segment_cleaner->init_mkfs(addr);
    return with_transaction_intr(
      Transaction::src_t::MUTATE,
      "mkfs_tm",
      [this, FNAME](auto& t)
    {
      DEBUGT("about to cache->mkfs", t);
      cache->init();
      return cache->mkfs(t
      ).si_then([this, &t] {
        return lba_manager->mkfs(t);
      }).si_then([this, FNAME, &t] {
        DEBUGT("about to submit_transaction", t);
        return submit_transaction_direct(t);
      });
    }).handle_error(
      crimson::ct_error::eagain::handle([] {
        ceph_assert(0 == "eagain impossible");
        return mkfs_ertr::now();
      }),
      mkfs_ertr::pass_further{}
    );
  }).safe_then([this] {
    return close();
  });
}

TransactionManager::mount_ertr::future<> TransactionManager::mount()
{
  LOG_PREFIX(TransactionManager::mount);
  cache->init();
  segment_cleaner->mount(
    segment_manager.get_device_id(),
    scanner.get_segment_managers());
  return segment_cleaner->init_segments().safe_then(
    [this](auto&& segments) {
    return journal->replay(
      std::move(segments),
      [this](const auto &offsets, const auto &e) {
      auto start_seq = offsets.write_result.start_seq;
      segment_cleaner->update_journal_tail_target(
          cache->get_oldest_dirty_from().value_or(start_seq));
      return cache->replay_delta(
          start_seq,
          offsets.record_block_base,
          e);
    });
  }).safe_then([this] {
    return journal->open_for_write();
  }).safe_then([this, FNAME](auto addr) {
    segment_cleaner->set_journal_head(addr);
    return seastar::do_with(
      create_weak_transaction(
        Transaction::src_t::READ, "mount"),
      [this, FNAME](auto &tref) {
	return with_trans_intr(
	  *tref,
	  [this, FNAME](auto &t) {
	    return cache->init_cached_extents(t, [this](auto &t, auto &e) {
	      return lba_manager->init_cached_extent(t, e);
	    }).si_then([this, FNAME, &t] {
	      assert(segment_cleaner->debug_check_space(
		       *segment_cleaner->get_empty_space_tracker()));
	      return lba_manager->scan_mapped_space(
		t,
		[this, FNAME, &t](paddr_t addr, extent_len_t len) {
		  TRACET(
		    "marking {}~{} used",
		    t,
		    addr,
		    len);
		  if (addr.is_real()) {
		    segment_cleaner->mark_space_used(
		      addr,
		      len ,
		      /* init_scan = */ true);
		  }
		});
	    });
	  });
      });
  }).safe_then([this] {
    segment_cleaner->complete_init();
  }).handle_error(
    mount_ertr::pass_further{},
    crimson::ct_error::all_same_way([] {
      ceph_assert(0 == "unhandled error");
      return mount_ertr::now();
    }));
}

TransactionManager::close_ertr::future<> TransactionManager::close() {
  LOG_PREFIX(TransactionManager::close);
  DEBUG("enter");
  return segment_cleaner->stop(
  ).then([this] {
    return cache->close();
  }).safe_then([this] {
    cache->dump_contents();
    return journal->close();
  }).safe_then([FNAME] {
    DEBUG("completed");
    return seastar::now();
  });
}

TransactionManager::ref_ret TransactionManager::inc_ref(
  Transaction &t,
  LogicalCachedExtentRef &ref)
{
  return lba_manager->incref_extent(t, ref->get_laddr()).si_then([](auto r) {
    return r.refcount;
  }).handle_error_interruptible(
    ref_iertr::pass_further{},
    ct_error::all_same_way([](auto e) {
      ceph_assert(0 == "unhandled error, TODO");
    }));
}

TransactionManager::ref_ret TransactionManager::inc_ref(
  Transaction &t,
  laddr_t offset)
{
  return lba_manager->incref_extent(t, offset).si_then([](auto result) {
    return result.refcount;
  });
}

TransactionManager::ref_ret TransactionManager::dec_ref(
  Transaction &t,
  LogicalCachedExtentRef &ref)
{
  LOG_PREFIX(TransactionManager::dec_ref);
  return lba_manager->decref_extent(t, ref->get_laddr()
  ).si_then([this, FNAME, &t, ref](auto ret) {
    if (ret.refcount == 0) {
      DEBUGT(
	"extent {} refcount 0",
	t,
	*ref);
      cache->retire_extent(t, ref);
      stats.extents_retired_total++;
      stats.extents_retired_bytes += ref->get_length();
    }
    return ret.refcount;
  });
}

TransactionManager::ref_ret TransactionManager::dec_ref(
  Transaction &t,
  laddr_t offset)
{
  LOG_PREFIX(TransactionManager::dec_ref);
  return lba_manager->decref_extent(t, offset
  ).si_then([this, FNAME, offset, &t](auto result) -> ref_ret {
    if (result.refcount == 0 && !result.addr.is_zero()) {
      DEBUGT("offset {} refcount 0", t, offset);
      return cache->retire_extent_addr(
	t, result.addr, result.length
      ).si_then([result, this] {
	stats.extents_retired_total++;
	stats.extents_retired_bytes += result.length;
	return ref_ret(
	  interruptible::ready_future_marker{},
	  0);
      });
    } else {
      return ref_ret(
	interruptible::ready_future_marker{},
	result.refcount);
    }
  });
}

TransactionManager::refs_ret TransactionManager::dec_ref(
  Transaction &t,
  std::vector<laddr_t> offsets)
{
  return seastar::do_with(std::move(offsets), std::vector<unsigned>(),
      [this, &t] (auto &&offsets, auto &refcnt) {
      return trans_intr::do_for_each(offsets.begin(), offsets.end(),
        [this, &t, &refcnt] (auto &laddr) {
        return this->dec_ref(t, laddr).si_then([&refcnt] (auto ref) {
          refcnt.push_back(ref);
          return ref_iertr::now();
        });
      }).si_then([&refcnt] {
        return ref_iertr::make_ready_future<std::vector<unsigned>>(std::move(refcnt));
      });
    });
}

TransactionManager::submit_transaction_iertr::future<>
TransactionManager::submit_transaction(
  Transaction &t)
{
  LOG_PREFIX(TransactionManager::submit_transaction);
  return trans_intr::make_interruptible(
    t.get_handle().enter(write_pipeline.reserve_projected_usage)
  ).then_interruptible([this, FNAME, &t] {
    size_t projected_usage = t.get_allocation_size();
    DEBUGT("waiting for projected_usage: {}", t, projected_usage);
    return trans_intr::make_interruptible(
      segment_cleaner->reserve_projected_usage(projected_usage)
    ).then_interruptible([this, &t] {
      return submit_transaction_direct(t);
    }).finally([this, FNAME, projected_usage, &t] {
      DEBUGT("releasing projected_usage: {}", t, projected_usage);
      segment_cleaner->release_projected_usage(projected_usage);
    });
  });
}

TransactionManager::submit_transaction_direct_ret
TransactionManager::submit_transaction_direct(
  Transaction &tref)
{
  LOG_PREFIX(TransactionManager::submit_transaction_direct);
  DEBUGT("about to alloc delayed extents", tref);

  return trans_intr::make_interruptible(
    tref.get_handle().enter(write_pipeline.ool_writes)
  ).then_interruptible([this, &tref] {
    return epm->delayed_alloc_or_ool_write(tref
    ).handle_error_interruptible(
      crimson::ct_error::input_output_error::pass_further(),
      crimson::ct_error::assert_all("invalid error")
    );
  }).si_then([this, FNAME, &tref] {
    DEBUGT("about to prepare", tref);
    return tref.get_handle().enter(write_pipeline.prepare);
  }).si_then([this, FNAME, &tref]() mutable
	      -> submit_transaction_iertr::future<> {
    auto record = cache->prepare_record(tref);

    tref.get_handle().maybe_release_collection_lock();

    DEBUGT("about to submit to journal", tref);

    return journal->submit_record(std::move(record), tref.get_handle()
    ).safe_then([this, FNAME, &tref](auto submit_result) mutable {
      auto start_seq = submit_result.write_result.start_seq;
      auto end_seq = submit_result.write_result.get_end_seq();
      DEBUGT("journal commit to record_block_base={}, start_seq={}, end_seq={}",
             tref,
             submit_result.record_block_base,
             start_seq,
             end_seq);
      segment_cleaner->set_journal_head(end_seq);
      cache->complete_commit(
          tref,
          submit_result.record_block_base,
          start_seq,
          segment_cleaner.get());
      lba_manager->complete_transaction(tref);
      segment_cleaner->update_journal_tail_target(
	cache->get_oldest_dirty_from().value_or(start_seq));
      auto to_release = tref.get_segment_to_release();
      if (to_release != NULL_SEG_ID) {
	return segment_manager.release(to_release
	).safe_then([this, to_release] {
	  segment_cleaner->mark_segment_released(to_release);
	});
      } else {
	return SegmentManager::release_ertr::now();
      }
    }).safe_then([&tref] {
      return tref.get_handle().complete();
    }).handle_error(
      submit_transaction_iertr::pass_further{},
      crimson::ct_error::all_same_way([](auto e) {
	ceph_assert(0 == "Hit error submitting to journal");
      })
    );
  }).finally([&tref]() {
      tref.get_handle().exit();
  });
}

TransactionManager::get_next_dirty_extents_ret
TransactionManager::get_next_dirty_extents(
  Transaction &t,
  journal_seq_t seq,
  size_t max_bytes)
{
  return cache->get_next_dirty_extents(t, seq, max_bytes);
}

TransactionManager::rewrite_extent_ret
TransactionManager::rewrite_logical_extent(
  Transaction& t,
  LogicalCachedExtentRef extent)
{
  LOG_PREFIX(TransactionManager::rewrite_logical_extent);
  if (extent->has_been_invalidated()) {
    ERRORT("{} has been invalidated", t, *extent);
  }
  assert(!extent->has_been_invalidated());
  DEBUGT("rewriting {}", t, *extent);

  auto lextent = extent->cast<LogicalCachedExtent>();
  cache->retire_extent(t, extent);
  auto nlextent = epm->alloc_new_extent_by_type(
    t,
    lextent->get_type(),
    lextent->get_length(),
    placement_hint_t::REWRITE)->cast<LogicalCachedExtent>();
  lextent->get_bptr().copy_out(
    0,
    lextent->get_length(),
    nlextent->get_bptr().c_str());
  nlextent->set_laddr(lextent->get_laddr());
  nlextent->set_pin(lextent->get_pin().duplicate());

  DEBUGT(
    "rewriting {} into {}",
    t,
    *lextent,
    *nlextent);

  /* This update_mapping is, strictly speaking, unnecessary for delayed_alloc
   * extents since we're going to do it again once we either do the ool write
   * or allocate a relative inline addr.  TODO: refactor SegmentCleaner to
   * avoid this complication. */
  return lba_manager->update_mapping(
    t,
    lextent->get_laddr(),
    lextent->get_paddr(),
    nlextent->get_paddr());
}

TransactionManager::rewrite_extent_ret TransactionManager::rewrite_extent(
  Transaction &t,
  CachedExtentRef extent)
{
  LOG_PREFIX(TransactionManager::rewrite_extent);
  {
    auto updated = cache->update_extent_from_transaction(t, extent);
    if (!updated) {
      DEBUGT("{} is already retired, skipping", t, *extent);
      return rewrite_extent_iertr::now();
    }
    extent = updated;
  }

  if (extent->get_type() == extent_types_t::ROOT) {
    DEBUGT("marking root {} for rewrite", t, *extent);
    cache->duplicate_for_write(t, extent);
    return rewrite_extent_iertr::now();
  }

  if (extent->is_logical()) {
    return rewrite_logical_extent(t, extent->cast<LogicalCachedExtent>());
  } else {
    return lba_manager->rewrite_extent(t, extent);
  }
}

TransactionManager::get_extent_if_live_ret TransactionManager::get_extent_if_live(
  Transaction &t,
  extent_types_t type,
  paddr_t addr,
  laddr_t laddr,
  segment_off_t len)
{
  LOG_PREFIX(TransactionManager::get_extent_if_live);
  DEBUGT("type {}, addr {}, laddr {}, len {}", t, type, addr, laddr, len);

  return cache->get_extent_if_cached(t, addr, type
  ).si_then([this, FNAME, &t, type, addr, laddr, len](auto extent)
	    -> get_extent_if_live_ret {
    if (extent) {
      return get_extent_if_live_ret (
	interruptible::ready_future_marker{},
	extent);
    }

    if (is_logical_type(type)) {
      using inner_ret = LBAManager::get_mapping_iertr::future<CachedExtentRef>;
      return lba_manager->get_mapping(
	t,
	laddr).si_then([=, &t] (LBAPinRef pin) -> inner_ret {
	  ceph_assert(pin->get_laddr() == laddr);
	  if (pin->get_paddr() == addr) {
	    if (pin->get_length() != (extent_len_t)len) {
	      ERRORT(
		"Invalid pin laddr {} paddr {} len {} found for "
		"extent laddr {} len{}",
		t,
		pin->get_laddr(),
		pin->get_paddr(),
		pin->get_length(),
		laddr,
		len);
	    }
	    ceph_assert(pin->get_length() == (extent_len_t)len);
	    return cache->get_extent_by_type(
	      t,
	      type,
	      addr,
	      laddr,
	      len,
	      [this, pin=std::move(pin)](CachedExtent &extent) mutable {
		auto lref = extent.cast<LogicalCachedExtent>();
		assert(!lref->has_pin());
		assert(!lref->has_been_invalidated());
		assert(!pin->has_been_invalidated());
		lref->set_pin(std::move(pin));
		lba_manager->add_pin(lref->get_pin());
	      });
	  } else {
	    return inner_ret(
	      interruptible::ready_future_marker{},
	      CachedExtentRef());
	  }
	}).handle_error_interruptible(crimson::ct_error::enoent::handle([] {
	  return CachedExtentRef();
	}), crimson::ct_error::pass_further_all{});
    } else {
      DEBUGT("non-logical extent {}", t, addr);
      return lba_manager->get_physical_extent_if_live(
	t,
	type,
	addr,
	laddr,
	len);
    }
  });
}

TransactionManager::~TransactionManager() {}

void TransactionManager::register_metrics()
{
  namespace sm = seastar::metrics;
  metrics.add_group("tm", {
    sm::make_counter("extents_retired_total", stats.extents_retired_total,
		     sm::description("total number of retired extents in TransactionManager")),
    sm::make_counter("extents_retired_bytes", stats.extents_retired_bytes,
		     sm::description("total size of retired extents in TransactionManager")),
    sm::make_counter("extents_mutated_total", stats.extents_mutated_total,
		     sm::description("total number of mutated extents in TransactionManager")),
    sm::make_counter("extents_mutated_bytes", stats.extents_mutated_bytes,
		     sm::description("total size of mutated extents in TransactionManager")),
    sm::make_counter("extents_allocated_total", stats.extents_allocated_total,
		     sm::description("total number of allocated extents in TransactionManager")),
    sm::make_counter("extents_allocated_bytes", stats.extents_allocated_bytes,
		     sm::description("total size of allocated extents in TransactionManager")),
  });
}

}
