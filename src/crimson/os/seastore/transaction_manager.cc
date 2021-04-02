// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/denc.h"
#include "include/intarith.h"

#include "crimson/common/log.h"

#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/journal.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

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
}

TransactionManager::mkfs_ertr::future<> TransactionManager::mkfs()
{
  return journal->open_for_write().safe_then([this](auto addr) {
    logger().debug("TransactionManager::mkfs: about to do_with");
    segment_cleaner->init_mkfs(addr);
    return seastar::do_with(
      create_transaction(),
      [this](auto &transaction) {
	logger().debug("TransactionManager::mkfs: about to cache->mkfs");
	cache->init();
	return cache->mkfs(*transaction
	).safe_then([this, &transaction] {
	  return lba_manager->mkfs(*transaction);
	}).safe_then([this, &transaction] {
	  logger().debug("TransactionManager::mkfs: about to submit_transaction");
	  return submit_transaction_direct(std::move(transaction)).handle_error(
	    crimson::ct_error::eagain::handle([] {
	      ceph_assert(0 == "eagain impossible");
	      return mkfs_ertr::now();
	    }),
	    mkfs_ertr::pass_further{}
	  );
	});
      });
  }).safe_then([this] {
    return journal->close();
  });
}

TransactionManager::mount_ertr::future<> TransactionManager::mount()
{
  cache->init();
  return journal->replay([this](auto seq, auto paddr, const auto &e) {
    return cache->replay_delta(seq, paddr, e);
  }).safe_then([this] {
    return journal->open_for_write();
  }).safe_then([this](auto addr) {
    segment_cleaner->set_journal_head(addr);
    return seastar::do_with(
      make_weak_transaction(),
      [this](auto &t) {
	return cache->init_cached_extents(*t, [this](auto &t, auto &e) {
	  return lba_manager->init_cached_extent(t, e);
	}).safe_then([this, &t] {
          assert(segment_cleaner->debug_check_space(
                   *segment_cleaner->get_empty_space_tracker()));
          return lba_manager->scan_mapped_space(
            *t,
            [this](paddr_t addr, extent_len_t len) {
              logger().debug("TransactionManager::mount: marking {}~{} used",
                           addr,
                           len);
              segment_cleaner->mark_space_used(
                addr,
                len ,
                /* init_scan = */ true);
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
  return segment_cleaner->stop(
  ).then([this] {
    return cache->close();
  }).safe_then([this] {
    return journal->close();
  });
}

TransactionManager::ref_ret TransactionManager::inc_ref(
  Transaction &t,
  LogicalCachedExtentRef &ref)
{
  return lba_manager->incref_extent(t, ref->get_laddr()).safe_then([](auto r) {
    return r.refcount;
  }).handle_error(
    ref_ertr::pass_further{},
    ct_error::all_same_way([](auto e) {
      ceph_assert(0 == "unhandled error, TODO");
    }));
}

TransactionManager::ref_ret TransactionManager::inc_ref(
  Transaction &t,
  laddr_t offset)
{
  return lba_manager->incref_extent(t, offset).safe_then([](auto result) {
    return result.refcount;
  });
}

TransactionManager::ref_ret TransactionManager::dec_ref(
  Transaction &t,
  LogicalCachedExtentRef &ref)
{
  return lba_manager->decref_extent(t, ref->get_laddr()
  ).safe_then([this, &t, ref](auto ret) {
    if (ret.refcount == 0) {
      logger().debug(
	"TransactionManager::dec_ref: extent {} refcount 0",
	*ref);
      cache->retire_extent(t, ref);
    }
    return ret.refcount;
  });
}

TransactionManager::ref_ret TransactionManager::dec_ref(
  Transaction &t,
  laddr_t offset)
{
  return lba_manager->decref_extent(t, offset
  ).safe_then([this, offset, &t](auto result) -> ref_ret {
    if (result.refcount == 0) {
      logger().debug(
	"TransactionManager::dec_ref: offset {} refcount 0",
	offset);
      return cache->retire_extent_if_cached(
	t, result.addr, result.length
      ).safe_then([] {
	return ref_ret(
	  ref_ertr::ready_future_marker{},
	  0);
      });
    } else {
      return ref_ret(
	ref_ertr::ready_future_marker{},
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
      return crimson::do_for_each(offsets.begin(), offsets.end(),
        [this, &t, &refcnt] (auto &laddr) {
        return dec_ref(t, laddr).safe_then([&refcnt] (auto ref) {
          refcnt.push_back(ref);
          return ref_ertr::now();
        });
      }).safe_then([&refcnt] {
        return ref_ertr::make_ready_future<std::vector<unsigned>>(std::move(refcnt));
      });
    });
}

TransactionManager::submit_transaction_ertr::future<>
TransactionManager::submit_transaction(
  TransactionRef t)
{
  logger().debug("TransactionManager::submit_transaction");
  auto &tref = *t;
  return tref.handle.enter(write_pipeline.wait_throttle
  ).then([this] {
    return segment_cleaner->await_hard_limits();
  }).then([this, t=std::move(t)]() mutable {
    return submit_transaction_direct(std::move(t));
  });
}

TransactionManager::submit_transaction_direct_ret
TransactionManager::submit_transaction_direct(
  TransactionRef t)
{
  logger().debug("TransactionManager::submit_transaction_direct");
  auto &tref = *t;
  return tref.handle.enter(write_pipeline.prepare
  ).then([this, &tref]() mutable
	 -> submit_transaction_ertr::future<> {
    auto record = cache->try_construct_record(tref);
    if (!record) {
      logger().debug("TransactionManager::submit_transaction_direct: "
                     "conflict detected, returning eagain.");
      return crimson::ct_error::eagain::make();
    }

    return journal->submit_record(std::move(*record), tref.handle
    ).safe_then([this, &tref](auto p) mutable {
      auto [addr, journal_seq] = p;
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
      return tref.handle.complete();
    }).handle_error(
      submit_transaction_ertr::pass_further{},
      crimson::ct_error::all_same_way([](auto e) {
	ceph_assert(0 == "Hit error submitting to journal");
      }));
    }).finally([t=std::move(t)]() mutable {
      t->handle.exit();
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
  {
    auto updated = cache->update_extent_from_transaction(t, extent);
    if (!updated) {
      logger().debug(
	"{}: {} is already retired, skipping",
	__func__,
	*extent);
      return rewrite_extent_ertr::now();
    }
    extent = updated;
  }

  if (extent->get_type() == extent_types_t::ROOT) {
    logger().debug(
      "{}: marking root {} for rewrite",
      __func__,
      *extent);
    cache->duplicate_for_write(t, extent);
    return rewrite_extent_ertr::now();
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
  logger().debug(
    "TransactionManager::get_extent_if_live:"
    " type {}, addr {}, laddr {}, len {}",
    type,
    addr,
    laddr,
    len);

  return cache->get_extent_if_cached(t, addr
  ).then([this, &t, type, addr, laddr, len](auto extent)
	 -> get_extent_if_live_ret {
    if (extent) {
      return get_extent_if_live_ret(
	get_extent_if_live_ertr::ready_future_marker{},
	extent);
    }

    if (is_logical_type(type)) {
      return lba_manager->get_mapping(
	t,
	laddr,
	len).safe_then([=, &t](lba_pin_list_t pins) {
	  ceph_assert(pins.size() <= 1);
	  if (pins.empty()) {
	    return get_extent_if_live_ret(
	      get_extent_if_live_ertr::ready_future_marker{},
	      CachedExtentRef());
	  }

	  auto pin = std::move(pins.front());
	  pins.pop_front();
	  ceph_assert(pin->get_laddr() == laddr);
	  ceph_assert(pin->get_length() == (extent_len_t)len);
	  if (pin->get_paddr() == addr) {
	    return cache->get_extent_by_type(
	      t,
	      type,
	      addr,
	      laddr,
	      len).safe_then(
		[this, pin=std::move(pin)](CachedExtentRef ret) mutable
		-> get_extent_if_live_ret {
		  auto lref = ret->cast<LogicalCachedExtent>();
		  if (!lref->has_pin()) {
		    if (pin->has_been_invalidated() ||
			lref->has_been_invalidated()) {
		      return crimson::ct_error::eagain::make();
		    } else {
		      lref->set_pin(std::move(pin));
		      lba_manager->add_pin(lref->get_pin());
		    }
		  }
		  return get_extent_if_live_ret(
		    get_extent_if_live_ertr::ready_future_marker{},
		    ret);
		});
	  } else {
	    return get_extent_if_live_ret(
	      get_extent_if_live_ertr::ready_future_marker{},
	      CachedExtentRef());
	  }
	});
    } else {
      logger().debug(
	"TransactionManager::get_extent_if_live: non-logical extent {}",
	addr);
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

}
