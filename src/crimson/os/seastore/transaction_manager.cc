// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/denc.h"
#include "include/intarith.h"

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/journal.h"

namespace crimson::os::seastore {

TransactionManager::TransactionManager(
  SegmentManager &_segment_manager,
  SegmentCleanerRef _segment_cleaner,
  JournalRef _journal,
  CacheRef _cache,
  LBAManagerRef _lba_manager)
  : segment_manager(_segment_manager),
    segment_cleaner(std::move(_segment_cleaner)),
    cache(std::move(_cache)),
    lba_manager(std::move(_lba_manager)),
    journal(std::move(_journal))
{
  segment_cleaner->set_extent_callback(this);
  journal->set_write_pipeline(&write_pipeline);
  register_metrics();
}

TransactionManager::mkfs_ertr::future<> TransactionManager::mkfs()
{
  LOG_PREFIX(TransactionManager::mkfs);
  segment_cleaner->mount(segment_manager);
  return journal->open_for_write().safe_then([this, FNAME](auto addr) {
    DEBUG("about to do_with");
    segment_cleaner->init_mkfs(addr);
    return seastar::do_with(
      create_transaction(),
      [this, FNAME](auto &transaction) {
	DEBUGT(
	  "about to cache->mkfs",
	  *transaction);
	cache->init();
	return cache->mkfs(*transaction
	).safe_then([this, &transaction] {
	  return lba_manager->mkfs(*transaction);
	}).safe_then([this, FNAME, &transaction] {
	  DEBUGT("about to submit_transaction", *transaction);
	  return with_trans_intr(
	    *transaction,
	    [this, &transaction](auto&) {
	      return submit_transaction_direct(*transaction);
	    }
	  ).handle_error(
	    crimson::ct_error::eagain::handle([] {
	      ceph_assert(0 == "eagain impossible");
	      return mkfs_ertr::now();
	    }),
	    mkfs_ertr::pass_further{}
	  );
	});
      });
  }).safe_then([this] {
    return close();
  });
}

TransactionManager::mount_ertr::future<> TransactionManager::mount()
{
  LOG_PREFIX(TransactionManager::mount);
  cache->init();
  segment_cleaner->mount(segment_manager);
  return journal->replay([this](auto seq, auto paddr, const auto &e) {
    return cache->replay_delta(seq, paddr, e);
  }).safe_then([this] {
    return journal->open_for_write();
  }).safe_then([this, FNAME](auto addr) {
    segment_cleaner->set_journal_head(addr);
    return seastar::do_with(
      create_weak_transaction(),
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
  DEBUGT("about to await throttle", t);
  return trans_intr::make_interruptible(segment_cleaner->await_hard_limits()
  ).then_interruptible([this, &t]() {
    return submit_transaction_direct(t);
  });
}

TransactionManager::submit_transaction_direct_ret
TransactionManager::submit_transaction_direct(
  Transaction &tref)
{
  LOG_PREFIX(TransactionManager::submit_transaction_direct);
  DEBUGT("about to prepare", tref);
  return trans_intr::make_interruptible(
    tref.get_handle().enter(write_pipeline.prepare)
  ).then_interruptible([this, FNAME, &tref]() mutable
		       -> submit_transaction_iertr::future<> {
    auto record = cache->prepare_record(tref);

    tref.get_handle().maybe_release_collection_lock();

    DEBUGT("about to submit to journal", tref);

    return journal->submit_record(std::move(record), tref.get_handle()
    ).safe_then([this, FNAME, &tref](auto p) mutable {
      auto [addr, journal_seq] = p;
      DEBUGT("journal commit to {} seq {}", tref, addr, journal_seq);
      segment_cleaner->set_journal_head(journal_seq);
      cache->complete_commit(tref, addr, journal_seq, segment_cleaner.get());
      lba_manager->complete_transaction(tref);
      segment_cleaner->update_journal_tail_target(
	cache->get_oldest_dirty_from().value_or(journal_seq));
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
      }));
    }).finally([&tref]() {
      tref.get_handle().exit();
    });
}

TransactionManager::get_next_dirty_extents_ret
TransactionManager::get_next_dirty_extents(
  journal_seq_t seq,
  size_t max_bytes)
{
  return cache->get_next_dirty_extents(seq, max_bytes);
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
  return lba_manager->rewrite_extent(t, extent);
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

  return cache->get_extent_if_cached(t, addr
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
	  ceph_assert(pin->get_length() == (extent_len_t)len);
	  if (pin->get_paddr() == addr) {
	    return cache->get_extent_by_type(
	      t,
	      type,
	      addr,
	      laddr,
	      len).si_then(
		[this, pin=std::move(pin)](CachedExtentRef ret) mutable {
		  auto lref = ret->cast<LogicalCachedExtent>();
		  if (!lref->has_pin()) {
		    assert(!(pin->has_been_invalidated() ||
			     lref->has_been_invalidated()));
		    lref->set_pin(std::move(pin));
		    lba_manager->add_pin(lref->get_pin());
		  }
		  return inner_ret(
		    interruptible::ready_future_marker{},
		    ret);
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
