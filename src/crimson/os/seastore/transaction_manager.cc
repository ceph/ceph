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
  SegmentManager &segment_manager,
  SegmentCleaner &segment_cleaner,
  Journal &journal,
  Cache &cache,
  LBAManager &lba_manager)
  : segment_manager(segment_manager),
    segment_cleaner(segment_cleaner),
    cache(cache),
    lba_manager(lba_manager),
    journal(journal)
{}

TransactionManager::mkfs_ertr::future<> TransactionManager::mkfs()
{
  return journal.open_for_write().safe_then([this](auto addr) {
    logger().debug("TransactionManager::mkfs: about to do_with");
    segment_cleaner.set_journal_head(addr);
    return seastar::do_with(
      create_transaction(),
      [this](auto &transaction) {
	logger().debug("TransactionManager::mkfs: about to cache.mkfs");
	cache.init();
	return cache.mkfs(*transaction
	).safe_then([this, &transaction] {
	  return lba_manager.mkfs(*transaction);
	}).safe_then([this, &transaction] {
	  logger().debug("TransactionManager::mkfs: about to submit_transaction");
	  return submit_transaction(std::move(transaction)).handle_error(
	    crimson::ct_error::eagain::handle([] {
	      ceph_assert(0 == "eagain impossible");
	      return mkfs_ertr::now();
	    }),
	    mkfs_ertr::pass_further{}
	  );
	});
      });
  }).safe_then([this] {
    return journal.close();
  });
}

TransactionManager::mount_ertr::future<> TransactionManager::mount()
{
  cache.init();
  return journal.replay([this](auto seq, auto paddr, const auto &e) {
    return cache.replay_delta(seq, paddr, e);
  }).safe_then([this] {
    return journal.open_for_write();
  }).safe_then([this](auto addr) {
    segment_cleaner.set_journal_head(addr);
    return seastar::do_with(
      create_transaction(),
      [this](auto &t) {
	return cache.init_cached_extents(*t, [this](auto &t, auto &e) {
	  return lba_manager.init_cached_extent(t, e);
	}).safe_then([this, &t]() mutable {
	  return submit_transaction(std::move(t));
	});
      });
  }).handle_error(
    mount_ertr::pass_further{},
    crimson::ct_error::all_same_way([] {
      ceph_assert(0 == "unhandled error");
      return mount_ertr::now();
    }));
}

TransactionManager::close_ertr::future<> TransactionManager::close() {
  return cache.close(
  ).safe_then([this] {
    return journal.close();
  });
}

TransactionManager::ref_ret TransactionManager::inc_ref(
  Transaction &t,
  LogicalCachedExtentRef &ref)
{
  return lba_manager.incref_extent(t, ref->get_laddr()).safe_then([](auto r) {
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
  return lba_manager.incref_extent(t, offset).safe_then([](auto result) {
    return result.refcount;
  });
}

TransactionManager::ref_ret TransactionManager::dec_ref(
  Transaction &t,
  LogicalCachedExtentRef &ref)
{
  return lba_manager.decref_extent(t, ref->get_laddr()
  ).safe_then([this, &t, ref](auto ret) {
    if (ret.refcount == 0) {
      logger().debug(
	"TransactionManager::dec_ref: extent {} refcount 0",
	*ref);
      cache.retire_extent(t, ref);
    }
    return ret.refcount;
  });
}

TransactionManager::ref_ret TransactionManager::dec_ref(
  Transaction &t,
  laddr_t offset)
{
  return lba_manager.decref_extent(t, offset
  ).safe_then([this, offset, &t](auto result) -> ref_ret {
    if (result.refcount == 0) {
      logger().debug(
	"TransactionManager::dec_ref: offset {} refcount 0",
	offset);
      return cache.retire_extent_if_cached(t, result.addr).safe_then([] {
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

TransactionManager::submit_transaction_ertr::future<>
TransactionManager::submit_transaction(
  TransactionRef t)
{
  logger().debug("TransactionManager::submit_transaction");
  return segment_cleaner.do_immediate_work(*t
  ).safe_then([this, t=std::move(t)]() mutable -> submit_transaction_ertr::future<> {
    auto record = cache.try_construct_record(*t);
    if (!record) {
      return crimson::ct_error::eagain::make();
    }

    return journal.submit_record(std::move(*record)).safe_then(
      [this, t=std::move(t)](auto p) mutable {
	auto [addr, journal_seq] = p;
	segment_cleaner.set_journal_head(journal_seq);
	cache.complete_commit(*t, addr, journal_seq);
	lba_manager.complete_transaction(*t);
      },
      submit_transaction_ertr::pass_further{},
      crimson::ct_error::all_same_way([](auto e) {
	ceph_assert(0 == "Hit error submitting to journal");
      }));
  });
}

TransactionManager::get_next_dirty_extents_ret
TransactionManager::get_next_dirty_extents(journal_seq_t seq)
{
  return cache.get_next_dirty_extents(seq);
}

TransactionManager::rewrite_extent_ret TransactionManager::rewrite_extent(
  Transaction &t,
  CachedExtentRef extent)
{
  {
    auto updated = cache.update_extent_from_transaction(t, extent);
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
    cache.duplicate_for_write(t, extent);
    return rewrite_extent_ertr::now();
  }
  return lba_manager.rewrite_extent(t, extent);
}

TransactionManager::~TransactionManager() {}

}
