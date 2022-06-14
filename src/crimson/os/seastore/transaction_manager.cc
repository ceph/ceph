// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/denc.h"
#include "include/intarith.h"

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"

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
  SegmentCleanerRef _segment_cleaner,
  JournalRef _journal,
  CacheRef _cache,
  LBAManagerRef _lba_manager,
  ExtentPlacementManagerRef &&epm,
  BackrefManagerRef&& backref_manager,
  tm_make_config_t config)
  : segment_cleaner(std::move(_segment_cleaner)),
    cache(std::move(_cache)),
    lba_manager(std::move(_lba_manager)),
    journal(std::move(_journal)),
    epm(std::move(epm)),
    backref_manager(std::move(backref_manager)),
    sm_group(*segment_cleaner->get_segment_manager_group()),
    config(config)
{
  segment_cleaner->set_extent_callback(this);
  journal->set_write_pipeline(&write_pipeline);
}

TransactionManager::mkfs_ertr::future<> TransactionManager::mkfs()
{
  LOG_PREFIX(TransactionManager::mkfs);
  INFO("enter");
  return segment_cleaner->mount(
  ).safe_then([this] {
    return journal->open_for_write();
  }).safe_then([this](auto) {
    segment_cleaner->init_mkfs();
    return epm->open();
  }).safe_then([this, FNAME]() {
    return with_transaction_intr(
      Transaction::src_t::MUTATE,
      "mkfs_tm",
      [this, FNAME](auto& t)
    {
      cache->init();
      return cache->mkfs(t
      ).si_then([this, &t] {
        return lba_manager->mkfs(t);
      }).si_then([this, &t] {
        return backref_manager->mkfs(t);
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
  ).safe_then([this] {
    return journal->replay(
      [this](
	const auto &offsets,
	const auto &e,
	const journal_seq_t alloc_replay_from,
	auto last_modified)
      {
	auto start_seq = offsets.write_result.start_seq;
	segment_cleaner->update_journal_tail_target(
	  cache->get_oldest_dirty_from().value_or(start_seq),
	  cache->get_oldest_backref_dirty_from().value_or(start_seq));
	return cache->replay_delta(
	  start_seq,
	  offsets.record_block_base,
	  e,
	  alloc_replay_from,
	  last_modified);
      });
  }).safe_then([this] {
    return journal->open_for_write();
  }).safe_then([this, FNAME](auto) {
    return seastar::do_with(
      create_weak_transaction(
        Transaction::src_t::READ, "mount"),
      [this, FNAME](auto &tref) {
	return with_trans_intr(
	  *tref,
	  [this, FNAME](auto &t) {
	    return cache->init_cached_extents(t, [this](auto &t, auto &e) {
	      if (is_backref_node(e->get_type()))
		return backref_manager->init_cached_extent(t, e);
	      else
		return lba_manager->init_cached_extent(t, e);
	    }).si_then([this, FNAME, &t] {
	      assert(segment_cleaner->debug_check_space(
		       *segment_cleaner->get_empty_space_tracker()));
	      return backref_manager->scan_mapped_space(
		t,
		[this, FNAME, &t](paddr_t addr, extent_len_t len, depth_t depth) {
		  TRACET(
		    "marking {}~{} used",
		    t,
		    addr,
		    len);
		  if (addr.is_real() &&
		      !backref_manager->backref_should_be_removed(addr)) {
		    segment_cleaner->mark_space_used(
		      addr,
		      len ,
		      seastar::lowres_system_clock::time_point(),
		      seastar::lowres_system_clock::time_point(),
		      /* init_scan = */ true);
		  }
		  if (depth) {
		    if (depth > 1) {
		      backref_manager->cache_new_backref_extent(
			addr, extent_types_t::BACKREF_INTERNAL);
		    } else {
		      backref_manager->cache_new_backref_extent(
			addr, extent_types_t::BACKREF_LEAF);
		    }
		  }
		}).si_then([this] {
		  LOG_PREFIX(TransactionManager::mount);
		  auto &backrefs = backref_manager->get_cached_backrefs();
		  DEBUG("marking {} backrefs used", backrefs.size());
		  for (auto &backref : backrefs) {
		    segment_cleaner->mark_space_used(
		      backref.paddr,
		      backref.len,
		      seastar::lowres_system_clock::time_point(),
		      seastar::lowres_system_clock::time_point(),
		      true);
		  }
		  return seastar::now();
		});
	    });
	  });
      });
  }).safe_then([this] {
    return epm->open();
  }).safe_then([FNAME, this] {
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
  }).safe_then([this] {
    return epm->close();
  }).safe_then([FNAME, this] {
    INFO("completed");
    sm_group.reset();
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
  Transaction &tref,
  std::optional<journal_seq_t> seq_to_trim)
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
  }).si_then([this, FNAME, &tref, seq_to_trim=std::move(seq_to_trim)]() mutable
	      -> submit_transaction_iertr::future<> {
    auto record = cache->prepare_record(tref, segment_cleaner.get());

    tref.get_handle().maybe_release_collection_lock();

    SUBTRACET(seastore_t, "about to submit to journal", tref);
    return journal->submit_record(std::move(record), tref.get_handle()
    ).safe_then([this, FNAME, &tref, seq_to_trim=std::move(seq_to_trim)]
      (auto submit_result) mutable {
      SUBDEBUGT(seastore_t, "committed with {}", tref, submit_result);
      auto start_seq = submit_result.write_result.start_seq;
      if (seq_to_trim && *seq_to_trim != JOURNAL_SEQ_NULL) {
	cache->trim_backref_bufs(*seq_to_trim);
      }
      cache->complete_commit(
          tref,
          submit_result.record_block_base,
          start_seq,
          segment_cleaner.get());

      std::vector<CachedExtentRef> lba_to_clear;
      std::vector<CachedExtentRef> backref_to_clear;
      lba_to_clear.reserve(tref.get_retired_set().size());
      backref_to_clear.reserve(tref.get_retired_set().size());
      for (auto &e: tref.get_retired_set()) {
	if (e->is_logical() || is_lba_node(e->get_type()))
	  lba_to_clear.push_back(e);
	else if (is_backref_node(e->get_type()))
	  backref_to_clear.push_back(e);
      }

      // ...but add_pin from parent->leaf
      std::vector<CachedExtentRef> lba_to_link;
      std::vector<CachedExtentRef> backref_to_link;
      lba_to_link.reserve(tref.get_fresh_block_stats().num);
      backref_to_link.reserve(tref.get_fresh_block_stats().num);
      tref.for_each_fresh_block([&](auto &e) {
	if (e->is_valid()) {
	  if (is_lba_node(e->get_type()) || e->is_logical())
	    lba_to_link.push_back(e);
	  else if (is_backref_node(e->get_type()))
	    backref_to_link.push_back(e);
	}
      });

      lba_manager->complete_transaction(tref, lba_to_clear, lba_to_link);
      backref_manager->complete_transaction(tref, backref_to_clear, backref_to_link);

      segment_cleaner->update_journal_tail_target(
	cache->get_oldest_dirty_from().value_or(start_seq),
	cache->get_oldest_backref_dirty_from().value_or(start_seq));
      return segment_cleaner->maybe_release_segment(tref);
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

  DEBUGT("rewriting logical extent -- {} to {}", t, *lextent, *nlextent);

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

  t.get_rewrite_version_stats().increment(extent->get_version());

  if (is_backref_node(extent->get_type())) {
    DEBUGT("rewriting backref extent -- {}", t, *extent);
    return backref_manager->rewrite_extent(t, extent);
  }

  if (extent->get_type() == extent_types_t::ROOT) {
    DEBUGT("rewriting root extent -- {}", t, *extent);
    cache->duplicate_for_write(t, extent);
    return rewrite_extent_iertr::now();
  }

  auto fut = rewrite_extent_iertr::now();
  if (extent->is_logical()) {
    fut = rewrite_logical_extent(t, extent->cast<LogicalCachedExtent>());
  } else {
    DEBUGT("rewriting physical extent -- {}", t, *extent);
    fut = lba_manager->rewrite_extent(t, extent);
  }

  return fut.si_then([this, extent, &t] {
    t.dont_record_release(extent);
    return backref_manager->remove_mapping(
      t, extent->get_paddr()).si_then([](auto) {
      return seastar::now();
    }).handle_error_interruptible(
      crimson::ct_error::input_output_error::pass_further(),
      crimson::ct_error::assert_all()
    );
  });
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
	  if (pin->get_val() == addr) {
	    if (pin->get_length() != (extent_len_t)len) {
	      ERRORT(
		"Invalid pin {}~{} {} found for "
		"extent {} {}~{} {}",
		t,
		pin->get_key(),
		pin->get_length(),
		pin->get_val(),
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

TransactionManagerRef make_transaction_manager(tm_make_config_t config)
{
  LOG_PREFIX(make_transaction_manager);
  auto epm = std::make_unique<ExtentPlacementManager>();
  auto cache = std::make_unique<Cache>(*epm);
  auto lba_manager = lba_manager::create_lba_manager(*cache);
  auto sms = std::make_unique<SegmentManagerGroup>();
  auto backref_manager = create_backref_manager(*sms, *cache);

  bool cleaner_is_detailed;
  SegmentCleaner::config_t cleaner_config;
  if (config.is_test) {
    cleaner_is_detailed = true;
    cleaner_config = SegmentCleaner::config_t::get_test();
  } else {
    cleaner_is_detailed = false;
    cleaner_config = SegmentCleaner::config_t::get_default();
  }
  auto segment_cleaner = std::make_unique<SegmentCleaner>(
    cleaner_config,
    std::move(sms),
    *backref_manager,
    cleaner_is_detailed);

  JournalRef journal;
  if (config.j_type == journal_type_t::SEGMENT_JOURNAL) {
    journal = journal::make_segmented(*segment_cleaner);
  } else {
    journal = journal::make_circularbounded(
      nullptr, "");
    segment_cleaner->set_disable_trim(true);
    ERROR("disabling journal trimming since support for CircularBoundedJournal\
	  hasn't been added yet");
  }
  epm->init_ool_writers(
      *segment_cleaner,
      segment_cleaner->get_ool_segment_seq_allocator());

  return std::make_unique<TransactionManager>(
    std::move(segment_cleaner),
    std::move(journal),
    std::move(cache),
    std::move(lba_manager),
    std::move(epm),
    std::move(backref_manager),
    config);
}

}
