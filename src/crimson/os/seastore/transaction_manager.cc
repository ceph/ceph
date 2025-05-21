// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "include/denc.h"
#include "include/intarith.h"

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/journal/circular_bounded_journal.h"
#include "crimson/os/seastore/lba/lba_btree_node.h"
#include "crimson/os/seastore/random_block_manager/rbm_device.h"

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
  JournalRef _journal,
  CacheRef _cache,
  LBAManagerRef _lba_manager,
  ExtentPlacementManagerRef &&_epm,
  BackrefManagerRef&& _backref_manager,
  shard_stats_t& _shard_stats)
  : cache(std::move(_cache)),
    lba_manager(std::move(_lba_manager)),
    journal(std::move(_journal)),
    epm(std::move(_epm)),
    backref_manager(std::move(_backref_manager)),
    full_extent_integrity_check(
      crimson::common::get_conf<bool>(
        "seastore_full_integrity_check")),
    shard_stats(_shard_stats)
{
  epm->set_extent_callback(this);
  journal->set_write_pipeline(&write_pipeline);
}

TransactionManager::mkfs_ertr::future<> TransactionManager::mkfs()
{
  LOG_PREFIX(TransactionManager::mkfs);
  INFO("...");
  return epm->mount(
  ).safe_then([this] {
    return journal->open_for_mkfs();
  }).safe_then([this](auto start_seq) {
    journal->get_trimmer().update_journal_tails(start_seq, start_seq);
    journal->get_trimmer().set_journal_head(start_seq);
    return epm->open_for_write();
  }).safe_then([this, FNAME]() {
    ++(shard_stats.io_num);
    ++(shard_stats.pending_io_num);
    // For submit_transaction_direct()
    ++(shard_stats.processing_inlock_io_num);
    ++(shard_stats.repeat_io_num);

    return with_transaction_intr(
      Transaction::src_t::MUTATE,
      "mkfs_tm",
      CACHE_HINT_TOUCH,
      [this, FNAME](auto& t)
    {
      cache->init();
      return cache->mkfs(t
      ).si_then([this, &t] {
        return lba_manager->mkfs(t);
      }).si_then([this, &t] {
        return backref_manager->mkfs(t);
      }).si_then([this, &t] {
        return init_root_meta(t);
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
    ).finally([this] {
      assert(shard_stats.pending_io_num);
      --(shard_stats.pending_io_num);
      // XXX: it's wrong to assume no failure,
      // but failure leads to fatal error
      --(shard_stats.processing_postlock_io_num);
    });
  }).safe_then([this] {
    return close();
  }).safe_then([FNAME] {
    INFO("done");
  });
}

TransactionManager::mount_ertr::future<>
TransactionManager::mount()
{
  LOG_PREFIX(TransactionManager::mount);
  INFO("...");
  cache->init();
  return epm->mount(
  ).safe_then([this] {
    return journal->replay(
      [this](
	const auto &offsets,
	const auto &e,
	const journal_seq_t &dirty_tail,
	const journal_seq_t &alloc_tail,
	sea_time_point modify_time)
      {
	auto start_seq = offsets.write_result.start_seq;
	return cache->replay_delta(
	  start_seq,
	  offsets.record_block_base,
	  e,
	  dirty_tail,
	  alloc_tail,
	  modify_time);
      });
  }).safe_then([this] {
    return journal->open_for_mount();
  }).safe_then([this](auto start_seq) {
    journal->get_trimmer().set_journal_head(start_seq);
    return with_transaction_weak(
      "mount",
      CACHE_HINT_TOUCH,
      [this](auto &t)
    {
      return cache->init_cached_extents(t, [this](auto &t, auto &e) {
        if (is_backref_node(e->get_type())) {
          return backref_manager->init_cached_extent(t, e);
        } else {
          return lba_manager->init_cached_extent(t, e);
        }
      }).si_then([this, &t] {
        epm->start_scan_space();
        return backref_manager->scan_mapped_space(
          t,
          [this](
            paddr_t paddr,
	    paddr_t backref_key,
            extent_len_t len,
            extent_types_t type,
            laddr_t laddr) {
          assert(paddr.is_absolute());
          if (is_backref_node(type)) {
            assert(laddr == L_ADDR_NULL);
	    assert(backref_key.is_absolute() || backref_key == P_ADDR_MIN);
            backref_manager->cache_new_backref_extent(paddr, backref_key, type);
            cache->update_tree_extents_num(type, 1);
            epm->mark_space_used(paddr, len);
          } else if (laddr == L_ADDR_NULL) {
	    assert(backref_key == P_ADDR_NULL);
            cache->update_tree_extents_num(type, -1);
            epm->mark_space_free(paddr, len);
          } else {
	    assert(backref_key == P_ADDR_NULL);
            cache->update_tree_extents_num(type, 1);
            epm->mark_space_used(paddr, len);
          }
        });
      });
    });
  }).safe_then([this] {
    return epm->open_for_write();
  }).safe_then([FNAME, this] {
    epm->start_background();
    INFO("done");
  }).handle_error(
    mount_ertr::pass_further{},
    crimson::ct_error::assert_all{"unhandled error"}
  );
}

TransactionManager::close_ertr::future<>
TransactionManager::close() {
  LOG_PREFIX(TransactionManager::close);
  INFO("...");
  return epm->stop_background(
  ).then([this] {
    return cache->close();
  }).safe_then([this] {
    cache->dump_contents();
    return journal->close();
  }).safe_then([this] {
    return epm->close();
  }).safe_then([FNAME] {
    INFO("done");
    return seastar::now();
  });
}

TransactionManager::ref_ret TransactionManager::remove(
  Transaction &t,
  LogicalChildNodeRef &ref)
{
  LOG_PREFIX(TransactionManager::remove);
  DEBUGT("{} ...", t, *ref);
  return lba_manager->remove_mapping(t, ref->get_laddr()
  ).si_then([this, FNAME, &t, ref](auto result) {
    if (result.refcount == 0) {
      cache->retire_extent(t, ref);
    }
    DEBUGT("removed {}~0x{:x} refcount={} -- {}",
           t, result.addr, result.length, result.refcount, *ref);
    return result.refcount;
  });
}

TransactionManager::ref_ret TransactionManager::remove(
  Transaction &t,
  laddr_t offset)
{
  LOG_PREFIX(TransactionManager::remove);
  DEBUGT("{} ...", t, offset);
  return lba_manager->remove_mapping(t, offset
  ).si_then([this, FNAME, offset, &t](auto result) -> ref_ret {
    auto fut = ref_iertr::now();
    if (result.refcount == 0) {
      if (result.addr.is_paddr() &&
          !result.addr.get_paddr().is_zero()) {
        fut = cache->retire_extent_addr(
          t, result.addr.get_paddr(), result.length);
      }
    }
    return fut.si_then([result=std::move(result), offset, &t, FNAME] {
      DEBUGT("removed {}~0x{:x} refcount={} -- offset={}",
             t, result.addr, result.length, result.refcount, offset);
      return result.refcount;
    });
  });
}

TransactionManager::refs_ret TransactionManager::remove(
  Transaction &t,
  std::vector<laddr_t> offsets)
{
  LOG_PREFIX(TransactionManager::remove);
  DEBUGT("{} offsets ...", t, offsets.size());
  return seastar::do_with(std::move(offsets), std::vector<unsigned>(),
    [this, &t, FNAME](auto &&offsets, auto &refcnts) {
    return trans_intr::do_for_each(offsets.begin(), offsets.end(),
      [this, &t, &refcnts](auto &laddr) {
      return this->remove(t, laddr
      ).si_then([&refcnts](auto ref) {
        refcnts.push_back(ref);
        return ref_iertr::now();
      });
    }).si_then([&refcnts, &t, FNAME] {
      DEBUGT("removed {} offsets", t, refcnts.size());
      return ref_iertr::make_ready_future<std::vector<unsigned>>(std::move(refcnts));
    });
  });
}

TransactionManager::submit_transaction_iertr::future<>
TransactionManager::submit_transaction(
  Transaction &t)
{
  LOG_PREFIX(TransactionManager::submit_transaction);
  SUBDEBUGT(seastore_t, "start, entering reserve_projected_usage", t);
  return trans_intr::make_interruptible(
    t.get_handle().enter(write_pipeline.reserve_projected_usage)
  ).then_interruptible([this, FNAME, &t] {
    auto dispatch_result = epm->dispatch_delayed_extents(t);
    auto projected_usage = dispatch_result.usage;
    SUBTRACET(seastore_t, "waiting for projected_usage: {}", t, projected_usage);
    return trans_intr::make_interruptible(
      epm->reserve_projected_usage(projected_usage)
    ).then_interruptible([this, &t, dispatch_result = std::move(dispatch_result)] {
      return do_submit_transaction(t, std::move(dispatch_result));
    }).finally([this, FNAME, projected_usage, &t] {
      SUBTRACET(seastore_t, "releasing projected_usage: {}", t, projected_usage);
      epm->release_projected_usage(projected_usage);
    });
  });
}

TransactionManager::submit_transaction_direct_ret
TransactionManager::submit_transaction_direct(
  Transaction &tref,
  std::optional<journal_seq_t> trim_alloc_to)
{
  return do_submit_transaction(
    tref,
    epm->dispatch_delayed_extents(tref),
    trim_alloc_to);
}

TransactionManager::update_lba_mappings_ret
TransactionManager::update_lba_mappings(
  Transaction &t,
  std::list<CachedExtentRef> &pre_allocated_extents)
{
  LOG_PREFIX(TransactionManager::update_lba_mappings);
  SUBTRACET(seastore_t, "update extent lba mappings", t);
  return seastar::do_with(
    std::list<LogicalChildNodeRef>(),
    std::list<CachedExtentRef>(),
    [this, &t, &pre_allocated_extents](auto &lextents, auto &pextents) {
    auto chksum_func = [&lextents, &pextents, this](auto &extent) {
      if (!extent->is_valid() ||
          !extent->is_fully_loaded() ||
          // EXIST_MUTATION_PENDING extents' crc will be calculated when
          // preparing records
          extent->is_exist_mutation_pending()) {
        return;
      }
      if (extent->is_logical()) {
        assert(is_logical_type(extent->get_type()));
        // for rewritten extents, last_committed_crc should have been set
        // because the crc of the original extent may be reused.
        // also see rewrite_logical_extent()
	if (!extent->get_last_committed_crc()) {
	  if (get_checksum_needed(extent->get_paddr())) {
	    extent->set_last_committed_crc(extent->calc_crc32c());
	  } else {
	    extent->set_last_committed_crc(CRC_NULL);
	  }
	}
#ifndef NDEBUG
	if (get_checksum_needed(extent->get_paddr())) {
	  assert(extent->get_last_committed_crc() == extent->calc_crc32c());
	} else {
	  assert(extent->get_last_committed_crc() == CRC_NULL);
	}
#endif
        lextents.emplace_back(extent->template cast<LogicalChildNode>());
      } else {
        assert(is_physical_type(extent->get_type()));
        pextents.emplace_back(extent);
      }
    };

    // For delayed-ool fresh logical extents, update lba-leaf crc and paddr.
    // For other fresh logical extents, update lba-leaf crc.
    t.for_each_finalized_fresh_block(chksum_func);
    // For existing-clean logical extents, update lba-leaf crc.
    t.for_each_existing_block(chksum_func);
    // For pre-allocated fresh logical extents, update lba-leaf crc.
    // For inplace-rewrite dirty logical extents, update lba-leaf crc.
    std::for_each(
      pre_allocated_extents.begin(),
      pre_allocated_extents.end(),
      chksum_func);

    return lba_manager->update_mappings(
      t, lextents
    ).si_then([&pextents, this] {
      for (auto &extent : pextents) {
        assert(!extent->is_logical() && extent->is_valid());
        // for non-logical extents, we update its last_committed_crc
        // and in-extent checksum fields
        // For pre-allocated fresh physical extents, update in-extent crc.
	checksum_t crc;
	if (get_checksum_needed(extent->get_paddr())) {
	  crc = extent->calc_crc32c();
	} else {
	  crc = CRC_NULL;
	}
	extent->set_last_committed_crc(crc);
	extent->update_in_extent_chksum_field(crc);
      }
    });
  });
}

TransactionManager::submit_transaction_direct_ret
TransactionManager::do_submit_transaction(
  Transaction &tref,
  ExtentPlacementManager::dispatch_result_t dispatch_result,
  std::optional<journal_seq_t> trim_alloc_to)
{
  LOG_PREFIX(TransactionManager::do_submit_transaction);
  SUBDEBUGT(seastore_t, "start, entering ool_writes", tref);
  return trans_intr::make_interruptible(
    tref.get_handle().enter(write_pipeline.ool_writes_and_lba_updates)
  ).then_interruptible([this, FNAME, &tref,
			dispatch_result = std::move(dispatch_result)] {
    return seastar::do_with(std::move(dispatch_result),
			    [this, FNAME, &tref](auto &dispatch_result) {
      SUBTRACET(seastore_t, "write delayed ool extents", tref);
      return epm->write_delayed_ool_extents(tref, dispatch_result.alloc_map
      ).handle_error_interruptible(
        crimson::ct_error::input_output_error::pass_further(),
        crimson::ct_error::assert_all("invalid error")
      );
    });
  }).si_then([&tref, FNAME, this] {
    return seastar::do_with(
      tref.get_valid_pre_alloc_list(),
      [this, FNAME, &tref](auto &allocated_extents) {
      return update_lba_mappings(tref, allocated_extents
      ).si_then([this, FNAME, &tref, &allocated_extents] {
        auto num_extents = allocated_extents.size();
        SUBTRACET(seastore_t, "process {} allocated extents", tref, num_extents);
        return epm->write_preallocated_ool_extents(tref, allocated_extents
        ).handle_error_interruptible(
          crimson::ct_error::input_output_error::pass_further(),
          crimson::ct_error::assert_all("invalid error")
        );
      });
    });
  }).si_then([this, FNAME, &tref] {
    SUBTRACET(seastore_t, "entering prepare", tref);
    return tref.get_handle().enter(write_pipeline.prepare);
  }).si_then([this, FNAME, &tref, trim_alloc_to=std::move(trim_alloc_to)]() mutable
	      -> submit_transaction_iertr::future<> {
    if (trim_alloc_to && *trim_alloc_to != JOURNAL_SEQ_NULL) {
      SUBTRACET(seastore_t, "trim backref_bufs to {}", tref, *trim_alloc_to);
      cache->trim_backref_bufs(*trim_alloc_to);
    }

    auto record = cache->prepare_record(
      tref,
      journal->get_trimmer().get_journal_head(),
      journal->get_trimmer().get_dirty_tail());

    tref.get_handle().maybe_release_collection_lock();
    if (tref.get_src() == Transaction::src_t::MUTATE) {
      --(shard_stats.processing_inlock_io_num);
      ++(shard_stats.processing_postlock_io_num);
    }

    SUBTRACET(seastore_t, "submitting record", tref);
    return journal->submit_record(
      std::move(record),
      tref.get_handle(),
      tref.get_src(),
      [this, FNAME, &tref](record_locator_t submit_result)
    {
      SUBDEBUGT(seastore_t, "committed with {}", tref, submit_result);
      auto start_seq = submit_result.write_result.start_seq;
      journal->get_trimmer().set_journal_head(start_seq);
      cache->complete_commit(
          tref,
          submit_result.record_block_base,
          start_seq);
      journal->get_trimmer().update_journal_tails(
	cache->get_oldest_dirty_from().value_or(start_seq),
	cache->get_oldest_backref_dirty_from().value_or(start_seq));
    }).safe_then([&tref] {
      return tref.get_handle().complete();
    }).handle_error(
      submit_transaction_iertr::pass_further{},
      crimson::ct_error::assert_all{"Hit error submitting to journal"}
    );
  });
}

seastar::future<> TransactionManager::flush(OrderingHandle &handle)
{
  LOG_PREFIX(TransactionManager::flush);
  SUBDEBUG(seastore_t, "H{} start", (void*)&handle);
  return handle.enter(write_pipeline.reserve_projected_usage
  ).then([this, &handle] {
    return handle.enter(write_pipeline.ool_writes_and_lba_updates);
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
  DEBUGT("max_bytes=0x{:x}B, seq={}", t, max_bytes, seq);
  return cache->get_next_dirty_extents(t, seq, max_bytes);
}

TransactionManager::rewrite_extent_ret
TransactionManager::rewrite_logical_extent(
  Transaction& t,
  LogicalChildNodeRef extent)
{
  LOG_PREFIX(TransactionManager::rewrite_logical_extent);
  if (extent->has_been_invalidated()) {
    ERRORT("extent has been invalidated -- {}", t, *extent);
    ceph_abort();
  }

  if (get_extent_category(extent->get_type()) == data_category_t::METADATA) {
    assert(extent->is_fully_loaded());
    cache->retire_extent(t, extent);
    auto nextent = cache->alloc_new_non_data_extent_by_type(
      t,
      extent->get_type(),
      extent->get_length(),
      extent->get_user_hint(),
      // get target rewrite generation
      extent->get_rewrite_generation())->cast<LogicalChildNode>();
    nextent->rewrite(t, *extent, 0);

    DEBUGT("rewriting meta -- {} to {}", t, *extent, *nextent);

#ifndef NDEBUG
    if (get_checksum_needed(extent->get_paddr())) {
      assert(extent->get_last_committed_crc() == extent->calc_crc32c());
    } else {
      assert(extent->get_last_committed_crc() == CRC_NULL);
    }
#endif
    nextent->set_last_committed_crc(extent->get_last_committed_crc());
    /* This update_mapping is, strictly speaking, unnecessary for delayed_alloc
     * extents since we're going to do it again once we either do the ool write
     * or allocate a relative inline addr.  TODO: refactor AsyncCleaner to
     * avoid this complication. */
    return lba_manager->update_mapping(
      t,
      extent->get_laddr(),
      extent->get_length(),
      extent->get_paddr(),
      *nextent
    ).discard_result();
  } else {
    assert(get_extent_category(extent->get_type()) == data_category_t::DATA);
    auto length = extent->get_length();
    return cache->read_extent_maybe_partial(
      t, std::move(extent), 0, length
    ).si_then([this, FNAME, &t](auto extent) {
      assert(extent->is_fully_loaded());
      cache->retire_extent(t, extent);
      auto extents = cache->alloc_new_data_extents_by_type(
        t,
        extent->get_type(),
        extent->get_length(),
        extent->get_user_hint(),
        // get target rewrite generation
        extent->get_rewrite_generation());
      return seastar::do_with(
        std::move(extents),
        0,
        extent->get_length(),
        extent_ref_count_t(0),
        [this, FNAME, extent, &t]
        (auto &extents, auto &off, auto &left, auto &refcount)
      {
        return trans_intr::do_for_each(
          extents,
          [extent, this, FNAME, &t, &off, &left, &refcount](auto &_nextent)
        {
          auto nextent = _nextent->template cast<LogicalChildNode>();
          bool first_extent = (off == 0);
          ceph_assert(left >= nextent->get_length());
          nextent->rewrite(t, *extent, off);
          DEBUGT("rewriting data -- {} to {}", t, *extent, *nextent);

          /* This update_mapping is, strictly speaking, unnecessary for delayed_alloc
           * extents since we're going to do it again once we either do the ool write
           * or allocate a relative inline addr.  TODO: refactor AsyncCleaner to
           * avoid this complication. */
          auto fut = base_iertr::now();
          if (first_extent) {
            assert(off == 0);
            fut = lba_manager->update_mapping(
              t,
              extent->get_laddr(),
              extent->get_length(),
              extent->get_paddr(),
              *nextent
            ).si_then([&refcount](auto c) {
              refcount = c;
            });
          } else {
            ceph_assert(refcount != 0);
            fut = lba_manager->alloc_extent(
              t,
              (extent->get_laddr() + off).checked_to_laddr(),
              *nextent,
              refcount
            ).si_then([extent, nextent, off](auto mapping) {
              ceph_assert(mapping.get_key() == extent->get_laddr() + off);
              ceph_assert(mapping.get_val() == nextent->get_paddr());
              return seastar::now();
            });
          }
          return fut.si_then([&off, &left, nextent] {
            off += nextent->get_length();
            left -= nextent->get_length();
            return seastar::now();
          });
        });
      });
    });
  }
}

TransactionManager::rewrite_extent_ret TransactionManager::rewrite_extent(
  Transaction &t,
  CachedExtentRef extent,
  rewrite_gen_t target_generation,
  sea_time_point modify_time)
{
  LOG_PREFIX(TransactionManager::rewrite_extent);

  {
    auto updated = cache->update_extent_from_transaction(t, extent);
    if (!updated) {
      DEBUGT("target={} {} already retired, skipping -- {}", t,
             rewrite_gen_printer_t{target_generation},
             sea_time_point_printer_t{modify_time},
             *extent);
      return rewrite_extent_iertr::now();
    }

    extent = updated;
    DEBUGT("target={} {} -- {} ...", t,
           rewrite_gen_printer_t{target_generation},
           sea_time_point_printer_t{modify_time},
           *extent);
    ceph_assert(!extent->is_pending_io());
  }

  assert(extent->is_valid() && !extent->is_initial_pending());
  if (extent->is_dirty()) {
    assert(extent->get_version() > 0);
    if (is_root_type(extent->get_type())) {
      // pass
    } else if (extent->get_version() == 1 && extent->is_mutation_pending()) {
      t.get_rewrite_stats().account_n_dirty();
    } else {
      t.get_rewrite_stats().account_dirty(extent->get_version());
    }
    if (epm->can_inplace_rewrite(t, extent)) {
      // FIXME: is_dirty() is true for mutation pending extents
      // which shouldn't do inplace rewrite because a pending transaction
      // may fail.
      t.add_inplace_rewrite_extent(extent);
      extent->set_inplace_rewrite_generation();
      DEBUGT("rewritten as inplace rewrite -- {}", t, *extent);
      return rewrite_extent_iertr::now();
    }
    extent->set_target_rewrite_generation(INIT_GENERATION);
  } else {
    assert(!is_root_type(extent->get_type()));
    extent->set_target_rewrite_generation(target_generation);
    ceph_assert(modify_time != NULL_TIME);
    extent->set_modify_time(modify_time);
    assert(extent->get_version() == 0);
    t.get_rewrite_stats().account_n_dirty();
  }

  if (is_root_type(extent->get_type())) {
    cache->duplicate_for_write(t, extent);
    DEBUGT("rewritten root {}", t, *extent);
    return rewrite_extent_iertr::now();
  }

  auto fut = rewrite_extent_iertr::now();
  if (extent->is_logical()) {
    assert(is_logical_type(extent->get_type()));
    fut = rewrite_logical_extent(t, extent->cast<LogicalChildNode>());
  } else if (is_backref_node(extent->get_type())) {
    fut = backref_manager->rewrite_extent(t, extent);
  } else {
    assert(is_lba_node(extent->get_type()));
    fut = lba_manager->rewrite_extent(t, extent);
  }
  return fut.si_then([FNAME, &t] {
    DEBUGT("rewritten", t);
  });
}

TransactionManager::get_extents_if_live_ret
TransactionManager::get_extents_if_live(
  Transaction &t,
  extent_types_t type,
  paddr_t paddr,
  laddr_t laddr,
  extent_len_t len)
{
  LOG_PREFIX(TransactionManager::get_extents_if_live);
  DEBUGT("{} {}~0x{:x} {} ...", t, type, laddr, len, paddr);

  // This only works with segments to check if alive,
  // as parallel transactions may split the extent at the same time.
  ceph_assert(paddr.is_absolute_segmented());

  return cache->get_extent_if_cached(t, paddr, len, type
  ).si_then([this, FNAME, type, paddr, laddr, len, &t](auto extent)
	    -> get_extents_if_live_ret {
    if (extent) {
      DEBUGT("{} {}~0x{:x} {} is cached and alive -- {}",
             t, type, laddr, len, paddr, *extent);
      assert(extent->get_length() == len);
      std::list<CachedExtentRef> res;
      res.emplace_back(std::move(extent));
      return get_extents_if_live_ret(
	interruptible::ready_future_marker{},
	res);
    }

    if (is_logical_type(type)) {
      return lba_manager->get_mappings(
	t,
	laddr,
	len
      ).si_then([this, FNAME, type, paddr, laddr, len, &t](lba_mapping_list_t pin_list) {
	return seastar::do_with(
	  std::list<CachedExtentRef>(),
	  std::move(pin_list),
	  [this, FNAME, type, paddr, laddr, len, &t]
          (std::list<CachedExtentRef> &extent_list, auto& pin_list)
        {
          auto paddr_seg_id = paddr.as_seg_paddr().get_segment_id();
          return trans_intr::parallel_for_each(
            pin_list,
            [this, FNAME, type, paddr_seg_id, &extent_list, &t](
              LBAMapping& pin) -> Cache::get_extent_iertr::future<>
          {
            DEBUGT("got pin, try read in parallel ... -- {}", t, pin);
            auto pin_paddr = pin.get_val();
            if (!pin_paddr.is_absolute_segmented()) {
              return seastar::now();
            }
            auto &pin_seg_paddr = pin_paddr.as_seg_paddr();
            auto pin_paddr_seg_id = pin_seg_paddr.get_segment_id();
            // auto pin_len = pin->get_length();
            if (pin_paddr_seg_id != paddr_seg_id) {
              return seastar::now();
            }

            // pin may be out of the range paddr~len, consider the following scene:
            // 1. Trans.A writes the final record of Segment S, in which it overwrite
            //    another extent E in the same segment S;
            // 2. Before Trans.A "complete_commit", Trans.B tries to rewrite new
            //    records and roll the segments, which closes Segment S;
            // 3. Before Trans.A "complete_commit", a new cleaner Transaction C tries
            //    to clean the segment;
            //
            // In this scenario, C might see a part of extent E's laddr space mapped
            // to another location within the same segment S.
            //
            // FIXME: this assert should be re-enabled once we have space reclaiming
            //        recognize committed segments: https://tracker.ceph.com/issues/66941
            // ceph_assert(pin_seg_paddr >= paddr &&
            //             pin_seg_paddr.add_offset(pin_len) <= paddr.add_offset(len));
            return read_pin_by_type(t, std::move(pin), type
            ).si_then([&extent_list](auto ret) {
              extent_list.emplace_back(std::move(ret));
              return seastar::now();
            });
          }).si_then([&extent_list, &t, FNAME, type, laddr, len, paddr] {
            DEBUGT("{} {}~0x{:x} {} is alive as {} extents",
                   t, type, laddr, len, paddr, extent_list.size());
            return get_extents_if_live_ret(
              interruptible::ready_future_marker{},
              std::move(extent_list));
          });
        });
      }).handle_error_interruptible(crimson::ct_error::enoent::handle([] {
        return get_extents_if_live_ret(
            interruptible::ready_future_marker{},
            std::list<CachedExtentRef>());
      }), crimson::ct_error::pass_further_all{});
    } else {
      return lba_manager->get_physical_extent_if_live(
	t,
	type,
	paddr,
	laddr,
	len
      ).si_then([=, &t](auto ret) {
        std::list<CachedExtentRef> res;
        if (ret) {
          DEBUGT("{} {}~0x{:x} {} is absent and alive as physical extent -- {}",
                 t, type, laddr, len, paddr, *ret);
          res.emplace_back(std::move(ret));
        } else {
          DEBUGT("{} {}~0x{:x} {} is not alive as physical extent",
                 t, type, laddr, len, paddr);
        }
        return get_extents_if_live_ret(
	  interruptible::ready_future_marker{},
	  std::move(res));
      });
    }
  });
}

TransactionManager::~TransactionManager() {}

TransactionManagerRef make_transaction_manager(
    Device *primary_device,
    const std::vector<Device*> &secondary_devices,
    shard_stats_t& shard_stats,
    bool is_test)
{
  auto epm = std::make_unique<ExtentPlacementManager>();
  auto cache = std::make_unique<Cache>(*epm);
  auto lba_manager = lba::create_lba_manager(*cache);
  auto sms = std::make_unique<SegmentManagerGroup>();
  auto rbs = std::make_unique<RBMDeviceGroup>();
  auto backref_manager = create_backref_manager(*cache);
  SegmentManagerGroupRef cold_sms = nullptr;
  std::vector<SegmentProvider*> segment_providers_by_id{DEVICE_ID_MAX, nullptr};

  auto p_backend_type = primary_device->get_backend_type();

  if (p_backend_type == backend_type_t::SEGMENTED) {
    auto dtype = primary_device->get_device_type();
    ceph_assert(dtype != device_type_t::HDD &&
		dtype != device_type_t::EPHEMERAL_COLD);
    sms->add_segment_manager(static_cast<SegmentManager*>(primary_device));
  } else {
    auto rbm = std::make_unique<BlockRBManager>(
      static_cast<RBMDevice*>(primary_device), "", is_test);
    rbs->add_rb_manager(std::move(rbm));
  }

  for (auto &p_dev : secondary_devices) {
    if (p_dev->get_backend_type() == backend_type_t::SEGMENTED) {
      if (p_dev->get_device_type() == primary_device->get_device_type()) {
        sms->add_segment_manager(static_cast<SegmentManager*>(p_dev));
      } else {
        if (!cold_sms) {
          cold_sms = std::make_unique<SegmentManagerGroup>();
        }
        cold_sms->add_segment_manager(static_cast<SegmentManager*>(p_dev));
      }
    } else {
      auto rbm = std::make_unique<BlockRBManager>(
	static_cast<RBMDevice*>(p_dev), "", is_test);
      rbs->add_rb_manager(std::move(rbm));
    }
  }

  auto backend_type = p_backend_type;
  device_off_t roll_size;
  device_off_t roll_start;
  if (backend_type == backend_type_t::SEGMENTED) {
    roll_size = static_cast<SegmentManager*>(primary_device)->get_segment_size();
    roll_start = 0;
  } else {
    roll_size = static_cast<random_block_device::RBMDevice*>(primary_device)
		->get_journal_size() - primary_device->get_block_size();
    // see CircularBoundedJournal::get_records_start()
    roll_start = static_cast<random_block_device::RBMDevice*>(primary_device)
		 ->get_shard_journal_start() + primary_device->get_block_size();
    ceph_assert_always(roll_size <= DEVICE_OFF_MAX);
    ceph_assert_always((std::size_t)roll_size + roll_start <=
                       primary_device->get_available_size());
  }
  ceph_assert(roll_size % primary_device->get_block_size() == 0);
  ceph_assert(roll_start % primary_device->get_block_size() == 0);

  bool cleaner_is_detailed;
  SegmentCleaner::config_t cleaner_config;
  JournalTrimmerImpl::config_t trimmer_config;
  if (is_test) {
    cleaner_is_detailed = true;
    cleaner_config = SegmentCleaner::config_t::get_test();
    trimmer_config = JournalTrimmerImpl::config_t::get_test(
        roll_size, backend_type);
  } else {
    cleaner_is_detailed = false;
    cleaner_config = SegmentCleaner::config_t::get_default();
    trimmer_config = JournalTrimmerImpl::config_t::get_default(
        roll_size, backend_type);
  }

  auto journal_trimmer = JournalTrimmerImpl::create(
      *backref_manager, trimmer_config,
      backend_type, roll_start, roll_size);

  AsyncCleanerRef cleaner;
  JournalRef journal;

  SegmentCleanerRef cold_segment_cleaner = nullptr;

  if (cold_sms) {
    cold_segment_cleaner = SegmentCleaner::create(
      cleaner_config,
      std::move(cold_sms),
      *backref_manager,
      epm->get_ool_segment_seq_allocator(),
      cleaner_is_detailed,
      /* is_cold = */ true);
    if (backend_type == backend_type_t::SEGMENTED) {
      for (auto id : cold_segment_cleaner->get_device_ids()) {
        segment_providers_by_id[id] =
          static_cast<SegmentProvider*>(cold_segment_cleaner.get());
      }
    }
  }

  if (backend_type == backend_type_t::SEGMENTED) {
    cleaner = SegmentCleaner::create(
      cleaner_config,
      std::move(sms),
      *backref_manager,
      epm->get_ool_segment_seq_allocator(),
      cleaner_is_detailed);
    auto segment_cleaner = static_cast<SegmentCleaner*>(cleaner.get());
    for (auto id : segment_cleaner->get_device_ids()) {
      segment_providers_by_id[id] =
        static_cast<SegmentProvider*>(segment_cleaner);
    }
    segment_cleaner->set_journal_trimmer(*journal_trimmer);
    journal = journal::make_segmented(
      *segment_cleaner,
      *journal_trimmer);
  } else {
    cleaner = RBMCleaner::create(
      std::move(rbs),
      *backref_manager,
      cleaner_is_detailed);
    journal = journal::make_circularbounded(
      *journal_trimmer,
      static_cast<random_block_device::RBMDevice*>(primary_device),
      "");
  }

  cache->set_segment_providers(std::move(segment_providers_by_id));

  epm->init(std::move(journal_trimmer),
	    std::move(cleaner),
	    std::move(cold_segment_cleaner));
  epm->set_primary_device(primary_device);

  return std::make_unique<TransactionManager>(
    std::move(journal),
    std::move(cache),
    std::move(lba_manager),
    std::move(epm),
    std::move(backref_manager),
    shard_stats);
}

}
