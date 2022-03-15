// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/denc.h"
#include "include/intarith.h"

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/journal.h"

/*
 * TransactionManager logs
 *
 * levels:
 * - INFO: major initiation, closing operations
 * - DEBUG: major extent related operations, INFO details
 * - TRACE: DEBUG details
 * - seastore_t logs
 */
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
}

TransactionManager::mkfs_ertr::future<> TransactionManager::mkfs()
{
  LOG_PREFIX(TransactionManager::mkfs);
  INFO("enter");
  return segment_cleaner->mount(
    segment_manager.get_device_id(),
    scanner.get_segment_managers()
  ).safe_then([this] {
    return journal->open_for_write();
  }).safe_then([this, FNAME](auto addr) {
    segment_cleaner->init_mkfs(addr);
    return with_transaction_intr(
      Transaction::src_t::MUTATE,
      "mkfs_tm",
      [this, FNAME](auto& t)
    {
      cache->init();
      return cache->mkfs(t
      ).si_then([this, &t] {
        return lba_manager->mkfs(t);
      }).si_then([this, FNAME, &t] {
        INFOT("submitting mkfs transaction", t);
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
  }).safe_then([FNAME] {
    INFO("completed");
  });
}

TransactionManager::mount_ertr::future<> TransactionManager::mount()
{
  LOG_PREFIX(TransactionManager::mount);
  INFO("enter");
  cache->init();
  return segment_cleaner->mount(
    segment_manager.get_device_id(),
    scanner.get_segment_managers()
  ).safe_then([this] {
    return journal->replay(
      [this](const auto &offsets, const auto &e, auto last_modified) {
	auto start_seq = offsets.write_result.start_seq;
	segment_cleaner->update_journal_tail_target(
	  cache->get_oldest_dirty_from().value_or(start_seq));
	return cache->replay_delta(
	  start_seq,
	  offsets.record_block_base,
	  e,
	  last_modified);
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
		      seastar::lowres_system_clock::time_point(),
		      seastar::lowres_system_clock::time_point(),
		      /* init_scan = */ true);
		  }
		});
	    });
	  });
      });
  }).safe_then([this, FNAME] {
    segment_cleaner->complete_init();
    INFO("completed");
  }).handle_error(
    mount_ertr::pass_further{},
    crimson::ct_error::all_same_way([] {
      ceph_assert(0 == "unhandled error");
      return mount_ertr::now();
    }));
}

TransactionManager::close_ertr::future<> TransactionManager::close() {
  LOG_PREFIX(TransactionManager::close);
  INFO("enter");
  return segment_cleaner->stop(
  ).then([this] {
    return cache->close();
  }).safe_then([this] {
    cache->dump_contents();
    return journal->close();
  }).safe_then([FNAME] {
    INFO("completed");
    return seastar::now();
  });
}

TransactionManager::ref_ret TransactionManager::inc_ref(
  Transaction &t,
  LogicalCachedExtentRef &ref)
{
  LOG_PREFIX(TransactionManager::inc_ref);
  TRACET("{}", t, *ref);
  return lba_manager->incref_extent(t, ref->get_laddr()
  ).si_then([FNAME, ref, &t](auto result) {
    DEBUGT("extent refcount is incremented to {} -- {}",
           t, result.refcount, *ref);
    return result.refcount;
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
  LOG_PREFIX(TransactionManager::inc_ref);
  TRACET("{}", t, offset);
  return lba_manager->incref_extent(t, offset
  ).si_then([FNAME, offset, &t](auto result) {
    DEBUGT("extent refcount is incremented to {} -- {}~{}, {}",
           t, result.refcount, offset, result.length, result.addr);
    return result.refcount;
  });
}

TransactionManager::ref_ret TransactionManager::dec_ref(
  Transaction &t,
  LogicalCachedExtentRef &ref)
{
  LOG_PREFIX(TransactionManager::dec_ref);
  TRACET("{}", t, *ref);
  return lba_manager->decref_extent(t, ref->get_laddr()
  ).si_then([this, FNAME, &t, ref](auto result) {
    DEBUGT("extent refcount is decremented to {} -- {}",
           t, result.refcount, *ref);
    if (result.refcount == 0) {
      cache->retire_extent(t, ref);
    }
    return result.refcount;
  });
}

TransactionManager::ref_ret TransactionManager::dec_ref(
  Transaction &t,
  laddr_t offset)
{
  LOG_PREFIX(TransactionManager::dec_ref);
  TRACET("{}", t, offset);
  return lba_manager->decref_extent(t, offset
  ).si_then([this, FNAME, offset, &t](auto result) -> ref_ret {
    DEBUGT("extent refcount is decremented to {} -- {}~{}, {}",
           t, result.refcount, offset, result.length, result.addr);
    if (result.refcount == 0 && !result.addr.is_zero()) {
      return cache->retire_extent_addr(
	t, result.addr, result.length
      ).si_then([] {
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
  LOG_PREFIX(TransactionManager::dec_ref);
  DEBUG("{} offsets", offsets.size());
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
  SUBTRACET(seastore_t, "start", t);
  return trans_intr::make_interruptible(
    t.get_handle().enter(write_pipeline.reserve_projected_usage)
  ).then_interruptible([this, FNAME, &t] {
    size_t projected_usage = t.get_allocation_size();
    SUBTRACET(seastore_t, "waiting for projected_usage: {}", t, projected_usage);
    return trans_intr::make_interruptible(
      segment_cleaner->reserve_projected_usage(projected_usage)
    ).then_interruptible([this, &t] {
      return submit_transaction_direct(t);
    }).finally([this, FNAME, projected_usage, &t] {
      SUBTRACET(seastore_t, "releasing projected_usage: {}", t, projected_usage);
      segment_cleaner->release_projected_usage(projected_usage);
    });
  });
}

TransactionManager::submit_transaction_direct_ret
TransactionManager::submit_transaction_direct(
  Transaction &tref)
{
  LOG_PREFIX(TransactionManager::submit_transaction_direct);
  SUBTRACET(seastore_t, "start", tref);
  return trans_intr::make_interruptible(
    tref.get_handle().enter(write_pipeline.ool_writes)
  ).then_interruptible([this, FNAME, &tref] {
    auto delayed_extents = tref.get_delayed_alloc_list();
    auto num_extents = delayed_extents.size();
    SUBTRACET(seastore_t, "process {} delayed extents", tref, num_extents);
    std::vector<paddr_t> delayed_paddrs;
    delayed_paddrs.reserve(num_extents);
    for (auto& ext : delayed_extents) {
      assert(ext->get_paddr().is_delayed());
      delayed_paddrs.push_back(ext->get_paddr());
    }
    return seastar::do_with(
      std::move(delayed_extents),
      std::move(delayed_paddrs),
      [this, FNAME, &tref](auto& delayed_extents, auto& delayed_paddrs)
    {
      return epm->delayed_alloc_or_ool_write(tref, delayed_extents
      ).si_then([this, FNAME, &tref, &delayed_extents, &delayed_paddrs] {
        SUBTRACET(seastore_t, "update delayed extent mappings", tref);
        return lba_manager->update_mappings(tref, delayed_extents, delayed_paddrs);
      }).handle_error_interruptible(
        crimson::ct_error::input_output_error::pass_further(),
        crimson::ct_error::assert_all("invalid error")
      );
    });
  }).si_then([this, FNAME, &tref] {
    SUBTRACET(seastore_t, "about to prepare", tref);
    return tref.get_handle().enter(write_pipeline.prepare);
  }).si_then([this, FNAME, &tref]() mutable
	      -> submit_transaction_iertr::future<> {
    auto record = cache->prepare_record(tref);

    tref.get_handle().maybe_release_collection_lock();

    SUBTRACET(seastore_t, "about to submit to journal", tref);
    return journal->submit_record(std::move(record), tref.get_handle()
    ).safe_then([this, FNAME, &tref](auto submit_result) mutable {
      SUBDEBUGT(seastore_t, "committed with {}", tref, submit_result);
      auto start_seq = submit_result.write_result.start_seq;
      auto end_seq = submit_result.write_result.get_end_seq();
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
        SUBDEBUGT(seastore_t, "releasing segment {}", tref, to_release);
	return segment_manager.release(to_release
	).safe_then([this, to_release] {
	  segment_cleaner->mark_segment_released(to_release);
	});
      } else {
	return SegmentManager::release_ertr::now();
      }
    }).safe_then([FNAME, &tref] {
      SUBTRACET(seastore_t, "completed", tref);
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

seastar::future<> TransactionManager::flush(OrderingHandle &handle)
{
  LOG_PREFIX(TransactionManager::flush);
  SUBDEBUG(seastore_t, "H{} start", (void*)&handle);
  return handle.enter(write_pipeline.reserve_projected_usage
  ).then([this, &handle] {
    return handle.enter(write_pipeline.ool_writes);
  }).then([this, &handle] {
    return handle.enter(write_pipeline.prepare);
  }).then([this, &handle] {
    handle.maybe_release_collection_lock();
    return journal->flush(handle);
  }).then([FNAME, &handle] {
    SUBDEBUG(seastore_t, "H{} completed", (void*)&handle);
  });
}

TransactionManager::get_next_dirty_extents_ret
TransactionManager::get_next_dirty_extents(
  Transaction &t,
  journal_seq_t seq,
  size_t max_bytes)
{
  LOG_PREFIX(TransactionManager::get_next_dirty_extents);
  DEBUGT("max_bytes={}B, seq={}", t, max_bytes, seq);
  return cache->get_next_dirty_extents(t, seq, max_bytes);
}

TransactionManager::rewrite_extent_ret
TransactionManager::rewrite_logical_extent(
  Transaction& t,
  LogicalCachedExtentRef extent)
{
  LOG_PREFIX(TransactionManager::rewrite_logical_extent);
  if (extent->has_been_invalidated()) {
    ERRORT("extent has been invalidated -- {}", t, *extent);
    ceph_abort();
  }
  TRACET("rewriting extent -- {}", t, *extent);

  auto lextent = extent->cast<LogicalCachedExtent>();
  cache->retire_extent(t, extent);
  auto nlextent = cache->alloc_new_extent_by_type(
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
  nlextent->last_modified = lextent->last_modified;

  DEBUGT("rewriting extent -- {} to {}", t, *lextent, *nlextent);

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
      DEBUGT("extent is already retired, skipping -- {}", t, *extent);
      return rewrite_extent_iertr::now();
    }
    extent = updated;
  }

  if (extent->get_type() == extent_types_t::ROOT) {
    DEBUGT("marking root for rewrite -- {}", t, *extent);
    cache->duplicate_for_write(t, extent);
    return rewrite_extent_iertr::now();
  }

  if (extent->is_logical()) {
    return rewrite_logical_extent(t, extent->cast<LogicalCachedExtent>());
  } else {
    DEBUGT("rewriting physical extent -- {}", t, *extent);
    return lba_manager->rewrite_extent(t, extent);
  }
}

TransactionManager::get_extent_if_live_ret TransactionManager::get_extent_if_live(
  Transaction &t,
  extent_types_t type,
  paddr_t addr,
  laddr_t laddr,
  seastore_off_t len)
{
  LOG_PREFIX(TransactionManager::get_extent_if_live);
  TRACET("{} {}~{} {}", t, type, laddr, len, addr);

  return cache->get_extent_if_cached(t, addr, type
  ).si_then([this, FNAME, &t, type, addr, laddr, len](auto extent)
	    -> get_extent_if_live_ret {
    if (extent) {
      DEBUGT("{} {}~{} {} is live in cache -- {}",
             t, type, laddr, len, addr, *extent);
      return get_extent_if_live_ret (
	interruptible::ready_future_marker{},
	extent);
    }

    if (is_logical_type(type)) {
      using inner_ret = LBAManager::get_mapping_iertr::future<CachedExtentRef>;
      return lba_manager->get_mapping(
	t,
	laddr).si_then([=, &t] (LBAPinRef pin) -> inner_ret {
	  ceph_assert(pin->get_key() == laddr);
	  if (pin->get_paddr() == addr) {
	    if (pin->get_length() != (extent_len_t)len) {
	      ERRORT(
		"Invalid pin {}~{} {} found for "
		"extent {} {}~{} {}",
		t,
		pin->get_key(),
		pin->get_length(),
		pin->get_paddr(),
		type,
		laddr,
		len,
		addr);
	      ceph_abort();
	    }
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
	      }).si_then([=, &t](auto ret) {;
		DEBUGT("{} {}~{} {} is live as logical extent -- {}",
		       t, type, laddr, len, addr, extent);
		return ret;
	      });
	  } else {
	    DEBUGT("{} {}~{} {} is not live as logical extent",
	           t, type, laddr, len, addr);
	    return inner_ret(
	      interruptible::ready_future_marker{},
	      CachedExtentRef());
	  }
	}).handle_error_interruptible(crimson::ct_error::enoent::handle([] {
	  return CachedExtentRef();
	}), crimson::ct_error::pass_further_all{});
    } else {
      return lba_manager->get_physical_extent_if_live(
	t,
	type,
	addr,
	laddr,
	len
      ).si_then([=, &t](auto ret) {
        if (ret) {
          DEBUGT("{} {}~{} {} is live as physical extent -- {}",
                 t, type, laddr, len, addr, *ret);
        } else {
          DEBUGT("{} {}~{} {} is not live as physical extent",
                 t, type, laddr, len, addr);
        }
        return ret;
      });
    }
  });
}

TransactionManager::~TransactionManager() {}

}
