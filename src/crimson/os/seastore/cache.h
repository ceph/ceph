// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "seastar/core/shared_future.hh"

#include "include/buffer.h"

#include "crimson/common/errorator.h"
#include "crimson/common/errorator-loop.h"
#include "crimson/os/seastore/backref_entry.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/extent_placement_manager.h"
#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/random_block_manager.h"
#include "crimson/os/seastore/root_block.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/transaction.h"
#include "crimson/os/seastore/transaction_interruptor.h"
#include "crimson/os/seastore/linked_tree_node.h"
#include "crimson/os/seastore/extent_pinboard.h"

namespace crimson::os::seastore::backref {
class BtreeBackrefManager;
}

namespace crimson::os::seastore {

class BackrefManager;
class SegmentProvider;

/**
 * Cache
 *
 * This component is responsible for buffer management, including
 * transaction lifecycle.
 *
 * Seastore transactions are expressed as an atomic combination of
 * 1) newly written blocks
 * 2) logical mutations to existing physical blocks
 *
 * See record_t
 *
 * As such, any transaction has 3 components:
 * 1) read_set: references to extents read during the transaction
 *       See Transaction::read_set
 * 2) write_set: references to extents to be written as:
 *    a) new physical blocks, see Transaction::fresh_block_list
 *    b) mutations to existing physical blocks,
 *       see Transaction::mutated_block_list
 * 3) retired_set: extent refs to be retired either due to 2b or
 *    due to releasing the extent generally.

 * In the case of 2b, the CachedExtent will have been copied into
 * a fresh CachedExtentRef such that the source extent ref is present
 * in the read set and the newly allocated extent is present in the
 * write_set.
 *
 * A transaction has 3 phases:
 * 1) construction: user calls Cache::get_transaction() and populates
 *    the returned transaction by calling Cache methods
 * 2) submission: user calls Cache::try_start_transaction().  If
 *    succcessful, the user may construct a record and submit the
 *    transaction to the journal.
 * 3) completion: once the transaction is durable, the user must call
 *    Cache::complete_commit() with the block offset to complete
 *    the transaction.
 *
 * Internally, in phase 1, the fields in Transaction are filled in.
 * - reads may block if the referenced extent is being written
 * - once a read obtains a particular CachedExtentRef for a paddr_t,
 *   it'll always get the same one until overwritten
 * - once a paddr_t is overwritten or written, subsequent reads of
 *   that addr will get the new ref
 *
 * In phase 2, if all extents in the read set are valid (not expired),
 * we can commit (otherwise, we fail and the user must retry).
 * - Expire all extents in the retired_set (they must all be valid)
 * - Remove all extents in the retired_set from Cache::extents
 * - Mark all extents in the write_set wait_io(), add promises to
 *   transaction
 * - Merge Transaction::write_set into Cache::extents_index
 *
 * After phase 2, the user will submit the record to the journal.
 * Once complete, we perform phase 3:
 * - For each CachedExtent in block_list, call
 *   CachedExtent::complete_initial_write(paddr_t) with the block's
 *   final offset (inferred from the extent's position in the block_list
 *   and extent lengths).
 * - For each block in mutation_list, call
 *   CachedExtent::delta_written(paddr_t) with the address of the start
 *   of the record
 * - Complete all promises with the final record start paddr_t
 *
 *
 * Cache logs
 *
 * levels:
 * - INFO: major initiation, closing operations
 * - DEBUG: major extent related operations, INFO details
 * - TRACE: DEBUG details
 * - seastore_t logs
 */
class Cache : public ExtentTransViewRetriever,
	      public RetiredExtentPlaceholderInvalidater {
public:
  Cache(ExtentPlacementManager &epm);
  ~Cache();

  cache_stats_t get_stats(bool report_detail, double seconds) const;

  /// Creates empty transaction by source
  TransactionRef create_transaction(
      Transaction::src_t src,
      const char* name,
      cache_hint_t cache_hint,
      bool is_weak) {
    LOG_PREFIX(Cache::create_transaction);

    ++(get_by_src(stats.trans_created_by_src, src));

    auto ret = std::make_unique<Transaction>(
      get_dummy_ordering_handle(),
      is_weak,
      src,
      [this](Transaction& t) {
        return on_transaction_destruct(t);
      },
      ++next_id,
      cache_hint
    );
    SUBDEBUGT(seastore_t, "created name={}, source={}, is_weak={}",
              *ret, name, src, is_weak);
    assert(!is_weak || src == Transaction::src_t::READ);
    return ret;
  }

  /// Resets transaction preserving
  void reset_transaction_preserve_handle(Transaction &t) {
    LOG_PREFIX(Cache::reset_transaction_preserve_handle);
    if (t.did_reset()) {
      SUBDEBUGT(seastore_t, "reset", t);
      ++(get_by_src(stats.trans_created_by_src, t.get_src()));
    }
    t.reset_preserve_handle();
  }

  /// Declare ref retired in t
  void retire_extent(Transaction &t, CachedExtentRef ref) {
    LOG_PREFIX(Cache::retire_extent);
    SUBDEBUGT(seastore_cache, "retire extent -- {}", t, *ref);
    t.add_present_to_retired_set(ref);
  }

  /// Declare paddr retired in t
  using retire_extent_iertr = base_iertr;
  using retire_extent_ret = base_iertr::future<>;
  retire_extent_ret retire_extent_addr(
    Transaction &t, paddr_t addr, extent_len_t length);

  CachedExtentRef retire_absent_extent_addr(
    Transaction &t, laddr_t laddr, paddr_t addr, extent_len_t length);

  /**
   * get_root
   *
   * returns ref to current root or t.root if modified in t
   */
  using get_root_iertr = base_iertr;
  using get_root_ret = get_root_iertr::future<RootBlockRef>;
  get_root_ret get_root(Transaction &t);

  /**
   * get_root_fast
   *
   * returns t.root and assume it is already present/read in t
   */
  RootBlockRef get_root_fast(Transaction &t) {
    LOG_PREFIX(Cache::get_root_fast);
    SUBTRACET(seastore_cache, "root already on t -- {}", t, *t.root);
    assert(t.root);
    return t.root;
  }

  /**
   * get_extent_if_cached
   *
   * Returns extent at offset if in cache
   */
  using get_extent_if_cached_iertr = base_iertr;
  using get_extent_if_cached_ret =
    get_extent_if_cached_iertr::future<CachedExtentRef>;
  get_extent_if_cached_ret get_extent_if_cached(
    Transaction &t,
    paddr_t paddr,
    extent_len_t len,
    extent_types_t type) {
    LOG_PREFIX(Cache::get_extent_if_cached);
    const auto t_src = t.get_src();
    CachedExtentRef ret;
    auto result = t.get_extent(paddr, &ret);
    cache_access_stats_t& access_stats = get_by_ext(
      get_by_src(stats.access_by_src_ext, t_src),
      type);
    if (result == Transaction::get_extent_ret::RETIRED) {
      SUBDEBUGT(seastore_cache,
        "{} {}~0x{:x} is retired on t",
        t, type, paddr, len);
      return get_extent_if_cached_iertr::make_ready_future<CachedExtentRef>();
    } else if (result == Transaction::get_extent_ret::PRESENT) {
      if (ret->get_length() != len) {
        SUBDEBUGT(seastore_cache,
          "{} {}~0x{:x} is present on t with inconsistent length 0x{:x} -- {}",
          t, type, paddr, len, ret->get_length(), *ret);
        return get_extent_if_cached_iertr::make_ready_future<CachedExtentRef>();
      }

      ceph_assert(ret->get_type() == type);

      if (ret->is_stable()) {
        if (ret->is_stable_dirty()) {
          ++access_stats.trans_dirty;
          ++stats.access.trans_dirty;
        } else {
          assert(ret->is_stable_clean());
          ++access_stats.trans_lru;
          ++stats.access.trans_lru;
        }
      } else {
        ++access_stats.trans_pending;
        ++stats.access.trans_pending;
      }

      if (!ret->is_fully_loaded()) {
        SUBDEBUGT(seastore_cache,
          "{} {}~0x{:x} is present on t without fully loaded -- {}",
          t, type, paddr, len, *ret);
        return trans_intr::make_interruptible(
          do_read_extent_maybe_partial<CachedExtent>(
            ret->cast<CachedExtent>(), 0, ret->get_length(), &t_src));
      }

      SUBTRACET(seastore_cache,
        "{} {}~0x{:x} is present on t -- {}",
        t, type, paddr, len, *ret);
      return ret->wait_io().then([ret] {
        return get_extent_if_cached_iertr::make_ready_future<CachedExtentRef>(ret);
      });
    } // result == Transaction::get_extent_ret::ABSENT

    assert(paddr.is_absolute());
    // get_extent_ret::ABSENT from transaction
    ret = query_cache(paddr);
    if (!ret) {
      SUBDEBUGT(seastore_cache,
        "{} {}~0x{:x} is absent in cache",
        t, type, paddr, len);
      return get_extent_if_cached_iertr::make_ready_future<CachedExtentRef>();
    }

    if (is_retired_placeholder_type(ret->get_type())) {
      // retired_placeholder is not really cached yet
      SUBDEBUGT(seastore_cache,
        "{} {}~0x{:x} ~0x{:x} is absent(placeholder) in cache",
        t, type, paddr, len, ret->get_length());
      return get_extent_if_cached_iertr::make_ready_future<CachedExtentRef>();
    }

    if (ret->get_length() != len) {
      SUBDEBUGT(seastore_cache,
        "{} {}~0x{:x} is present in cache with inconsistent length 0x{:x} -- {}",
        t, type, paddr, len, ret->get_length(), *ret);
      return get_extent_if_cached_iertr::make_ready_future<CachedExtentRef>();
    }

    ceph_assert(ret->get_type() == type);

    if (ret->is_stable_dirty()) {
      ++access_stats.cache_dirty;
      ++stats.access.cache_dirty;
    } else {
      assert(ret->is_stable_clean());
      ++access_stats.cache_lru;
      ++stats.access.cache_lru;
    }

    t.add_to_read_set(ret);
    touch_extent_fully(*ret, &t_src, t.get_cache_hint());
    if (!ret->is_fully_loaded()) {
      SUBDEBUGT(seastore_cache,
        "{} {}~0x{:x} is present without fully loaded in cache -- {}",
        t, type, paddr, len, *ret);
      return trans_intr::make_interruptible(
        do_read_extent_maybe_partial<CachedExtent>(
          ret->cast<CachedExtent>(), 0, ret->get_length(), &t_src));
    }

    // present in cache(fully loaded) and is not a retired_placeholder
    SUBDEBUGT(seastore_cache,
      "{} {}~0x{:x} is present in cache -- {}",
      t, type, paddr, len, *ret);
    return ret->wait_io().then([ret] {
      return get_extent_if_cached_iertr::make_ready_future<
        CachedExtentRef>(ret);
    });
  }

  /**
   * get_caching_extent
   *
   * returns ref to extent at offset~length of type T either from
   * - t if modified by t
   * - extent_set if already in cache
   * - disk
   *
   * t *must not* have retired offset
   *
   * Note, the current implementation leverages parent-child
   * pointers in LBA instead, so it should only be called in tests.
   *
   * This path won't be accounted by the cache_access_stats_t.
   */
  using get_extent_iertr = base_iertr;
  template <typename T>
  get_extent_iertr::future<TCachedExtentRef<T>>
  get_caching_extent(
    Transaction &t,
    paddr_t offset,
    extent_len_t length) {
    CachedExtentRef ret;
    LOG_PREFIX(Cache::get_caching_extent);
    const auto t_src = t.get_src();
    auto result = t.get_extent(offset, &ret);
    if (result == Transaction::get_extent_ret::RETIRED) {
      SUBERRORT(seastore_cache, "{} {}~0x{:x} is retired on t -- {}",
                t, T::TYPE, offset, length, *ret);
      ceph_abort_msg("impossible");
    } else if (result == Transaction::get_extent_ret::PRESENT) {
      assert(ret->get_length() == length);
      if (ret->is_fully_loaded()) {
        SUBTRACET(seastore_cache, "{} {}~0x{:x} is present on t -- {}",
                  t, T::TYPE, offset, length, *ret);
        return ret->wait_io().then([ret] {
	  return seastar::make_ready_future<TCachedExtentRef<T>>(
            ret->cast<T>());
        });
      } else {
        SUBDEBUGT(seastore_cache,
            "{} {}~0x{:x} is present on t without fully loaded, reading ... -- {}",
            t, T::TYPE, offset, length, *ret);
        return do_read_extent_maybe_partial<T>(ret->cast<T>(), 0, length, &t_src);
      }
    } else {
      SUBTRACET(seastore_cache, "{} {}~0x{:x} is absent on t, query cache ...",
                t, T::TYPE, offset, length);
      auto f = [&t, this, t_src](CachedExtent &ext) {
        t.add_to_read_set(CachedExtentRef(&ext));
        touch_extent_fully(ext, &t_src, t.get_cache_hint());
      };
      return trans_intr::make_interruptible(
        do_get_caching_extent<T>(
          offset, length, [](T &){}, std::move(f), &t_src)
      );
    }
  }

  void invalidate_retired_placeholder(
    Transaction &t,
    CachedExtent &retired_placeholder,
    CachedExtent &extent) {
    // replace placeholder in transactions
    while (!retired_placeholder.read_transactions.empty()) {
      auto t = retired_placeholder.read_transactions.begin()->t;
      t->replace_placeholder(retired_placeholder, extent);
    }
    retired_placeholder.set_invalid(t);
  }

  /*
   * get_absent_extent
   *
   * The extent in query is supposed to be absent in Cache.
   * partially load buffer from partial_off~partial_len if not present.
   */
  template <typename T, typename Func>
  get_extent_iertr::future<TCachedExtentRef<T>> get_absent_extent(
    Transaction &t,
    paddr_t offset,
    extent_len_t length,
    extent_len_t partial_off,
    extent_len_t partial_len,
    Func &&extent_init_func) {
    LOG_PREFIX(Cache::get_absent_extent);

#ifndef NDEBUG
    {
      CachedExtentRef ret;
      auto r = t.get_extent(offset, &ret);
      if (r != Transaction::get_extent_ret::ABSENT) {
	SUBERRORT(seastore_cache, "unexpected non-absent extent {}", t, *ret);
	ceph_abort();
      }
    }
#endif

    SUBTRACET(seastore_cache, "{} {}~0x{:x} is absent on t, query cache ...",
	      t, T::TYPE, offset, length);
    ceph_assert(!booting);
    const auto t_src = t.get_src();

    // partial read
    TCachedExtentRef<T> ret = CachedExtent::make_cached_extent_ref<T>(length);
    ret->init(CachedExtent::extent_state_t::CLEAN,
	      offset,
	      PLACEMENT_HINT_NULL,
	      NULL_GENERATION,
	      TRANS_ID_NULL);
    SUBDEBUG(seastore_cache,
	"{} {}~0x{:x} is absent, add extent and reading range 0x{:x}~0x{:x} ... -- {}",
	T::TYPE, offset, length, partial_off, partial_len, *ret);
    add_extent(ret);
    extent_init_func(*ret);
    cache_access_stats_t& access_stats = get_by_ext(
      get_by_src(stats.access_by_src_ext, t_src),
      T::TYPE);
    ++access_stats.load_absent;
    ++stats.access.load_absent;
    t.add_to_read_set(CachedExtentRef(ret));
    touch_extent_by_range(
      *ret, &t_src, t.get_cache_hint(),
      partial_off, partial_len);
    return trans_intr::make_interruptible(
      read_extent<T>(std::move(ret), partial_off, partial_len, &t_src));
  }

  /*
   * get_absent_extent
   *
   * Mostly the same as Cache::get_extent(), with the only difference
   * that get_absent_extent won't search the transaction's context for
   * the specific CachedExtent
   *
   * The extent in query is supposed to be absent in Cache.
   *
   * User is responsible to call get_extent_viewable_by_trans()
   * *atomically* prior to call this method.
   */
  template <typename T>
  get_extent_iertr::future<TCachedExtentRef<T>> get_absent_extent(
    Transaction &t,
    paddr_t offset,
    extent_len_t length) {
    return get_absent_extent<T>(t, offset, length, [](T &){});
  }

  template <typename T, typename Func>
  get_extent_iertr::future<TCachedExtentRef<T>> get_absent_extent(
    Transaction &t,
    paddr_t offset,
    extent_len_t length,
    Func &&extent_init_func) {
    return get_absent_extent<T>(t, offset, length, 0, length,
      std::forward<Func>(extent_init_func));
  }

  template <typename T, typename Func>
  get_extent_iertr::future<TCachedExtentRef<T>> maybe_get_absent_extent(
    Transaction &t,
    paddr_t offset,
    extent_len_t length,
    Func &&extent_init_func) {
    LOG_PREFIX(Cache::maybe_get_absent_extent);
    if (unlikely(booting)) {
#ifndef NDEBUG
      CachedExtentRef ret;
      auto r = t.get_extent(offset, &ret);
      if (r != Transaction::get_extent_ret::ABSENT) {
	SUBERRORT(seastore_cache, "unexpected non-absent extent {}", t, *ret);
	ceph_abort();
      }
#endif

      SUBTRACET(seastore_cache, "{} {}~0x{:x} is absent on t, query cache ...",
		t, T::TYPE, offset, length);
      const auto t_src = t.get_src();
      auto f = [&t, this, t_src](CachedExtent &ext) {
	// XXX: is_stable_dirty() may not be linked in lba tree
	assert(ext.is_stable());
	assert(T::TYPE == ext.get_type());
	cache_access_stats_t& access_stats = get_by_ext(
	  get_by_src(stats.access_by_src_ext, t_src),
	  T::TYPE);
	++access_stats.load_absent;
	++stats.access.load_absent;

	t.add_to_read_set(CachedExtentRef(&ext));
	touch_extent_fully(ext, &t_src, t.get_cache_hint());
      };
      return trans_intr::make_interruptible(
	do_get_caching_extent<T>(
	  offset, length, 0, length,
	  std::forward<Func>(extent_init_func), std::move(f), &t_src)
      );
    }
    return get_absent_extent<T>(t, offset, length, 0, length,
      std::forward<Func>(extent_init_func));
  }

  CachedExtentRef peek_extent_viewable_by_trans(
    Transaction &t,
    CachedExtentRef extent) final
  {
    assert(extent);
    auto ext = extent->maybe_get_transactional_view(t);
    ceph_assert(ext);
    return ext;
  }

  get_extent_iertr::future<> maybe_wait_accessible(
    Transaction &t,
    CachedExtent &extent) final {
    // as of now, only lba tree nodes can go in here,
    // so it must be fully loaded.
    assert(extent.is_valid());
    assert(extent.is_fully_loaded());
    const auto t_src = t.get_src();
    auto ext_type = extent.get_type();
    assert(!is_retired_placeholder_type(ext_type));
    cache_access_stats_t& access_stats = get_by_ext(
      get_by_src(stats.access_by_src_ext, t_src),
      ext_type);
    if (extent.is_stable()) {
      // stable from trans-view
      bool needs_touch = false, needs_step_2 = false;
      assert(!extent.is_pending_in_trans(t.get_trans_id()));
      auto ret = t.maybe_add_to_read_set(&extent);
      if (ret.added) {
	if (extent.is_stable_dirty()) {
	  ++access_stats.cache_dirty;
	  ++stats.access.cache_dirty;
	} else {
	  ++access_stats.cache_lru;
	  ++stats.access.cache_lru;
	}
	if (ret.is_paddr_known) {
	  touch_extent_fully(extent, &t_src, t.get_cache_hint());
	} else {
	  needs_touch = true;
	}
      } else {
	// already exists
	if (extent.is_stable_dirty()) {
	  ++access_stats.trans_dirty;
	  ++stats.access.trans_dirty;
	} else {
	  ++access_stats.trans_lru;
	  ++stats.access.trans_lru;
	}
      }
      // step 2 maybe reordered after wait_io(),
      // always try step 2 if paddr unknown
      needs_step_2 = !ret.is_paddr_known;

      auto target_extent = CachedExtentRef(&extent);
      return trans_intr::make_interruptible(
	extent.wait_io()
      ).then_interruptible([target_extent, needs_touch,
			    needs_step_2, &t, this, t_src] {
	if (needs_step_2) {
	  t.maybe_add_to_read_set_step_2(target_extent.get());
	}
	if (needs_touch) {
	  touch_extent_fully(*target_extent, &t_src, t.get_cache_hint());
	}
	return get_extent_iertr::now();
      });
    } else {
      assert(extent.is_mutable());
      assert(!extent.is_pending_io());
      assert(extent.is_pending_in_trans(t.get_trans_id()));
      ++access_stats.trans_pending;
      ++stats.access.trans_pending;
      return get_extent_iertr::now();
    }
  }

  get_extent_iertr::future<CachedExtentRef>
  get_extent_viewable_by_trans(
    Transaction &t,
    CachedExtentRef extent) final
  {
    assert(extent->is_valid());

    const auto t_src = t.get_src();
    auto ext_type = extent->get_type();
    // FIXME: retired-placeholder isn't linked in the lba tree yet.
    //   We think it's still working because:
    //   1. A retired-placeholder must be an ObjectDataBlock
    //   2. Per rados object, no read is possible during write,
    //      and write cannot be parallel
    assert(!is_retired_placeholder_type(ext_type));
    cache_access_stats_t& access_stats = get_by_ext(
      get_by_src(stats.access_by_src_ext, t_src),
      ext_type);

    CachedExtent* p_extent;
    bool needs_step_2 = false;
    bool needs_touch = false;
    if (extent->is_stable()) {
      p_extent = extent->maybe_get_transactional_view(t);
      ceph_assert(p_extent);
      if (p_extent != extent.get()) {
        assert(!extent->is_pending_io());
        assert(p_extent->is_pending_in_trans(t.get_trans_id()));
        assert(!p_extent->is_pending_io());
        ++access_stats.trans_pending;
        ++stats.access.trans_pending;
        if (p_extent->is_mutable()) {
          assert(p_extent->is_fully_loaded());
          assert(!p_extent->is_pending_io());
          return get_extent_iertr::make_ready_future<CachedExtentRef>(
            CachedExtentRef(p_extent));
        } else {
          assert(p_extent->is_exist_clean());
        }
      } else {
        // stable from trans-view
        assert(!p_extent->is_pending_in_trans(t.get_trans_id()));
        auto ret = t.maybe_add_to_read_set(p_extent);
        if (ret.added) {
          if (p_extent->is_stable_dirty()) {
            ++access_stats.cache_dirty;
            ++stats.access.cache_dirty;
          } else {
            assert(p_extent->is_stable_clean());
            ++access_stats.cache_lru;
            ++stats.access.cache_lru;
          }
          if (ret.is_paddr_known) {
            touch_extent_fully(*p_extent, &t_src, t.get_cache_hint());
          } else {
            needs_touch = true;
          }
        } else {
          // already exists
          if (p_extent->is_stable_dirty()) {
            ++access_stats.trans_dirty;
            ++stats.access.trans_dirty;
          } else {
            assert(p_extent->is_stable_clean());
            ++access_stats.trans_lru;
            ++stats.access.trans_lru;
          }
        }
        // step 2 maybe reordered after wait_io(),
        // always try step 2 if paddr unknown
        needs_step_2 = !ret.is_paddr_known;
      }
    } else {
      assert(!extent->is_pending_io());
      assert(extent->is_pending_in_trans(t.get_trans_id()));
      ++access_stats.trans_pending;
      ++stats.access.trans_pending;
      if (extent->is_mutable()) {
        assert(extent->is_fully_loaded());
        assert(!extent->is_pending_io());
        return get_extent_iertr::make_ready_future<CachedExtentRef>(extent);
      } else {
        assert(extent->is_exist_clean());
        p_extent = extent.get();
      }
    }

    // user should not see RETIRED_PLACEHOLDER extents
    ceph_assert(!is_retired_placeholder_type(p_extent->get_type()));
    // for logical extents, handle partial load in TM::read_pin(),
    // also see read_extent_maybe_partial() and get_absent_extent()
    assert(is_logical_type(p_extent->get_type()) ||
           p_extent->is_fully_loaded());

    auto target_extent = CachedExtentRef(p_extent);
    return trans_intr::make_interruptible(
      p_extent->wait_io()
    ).then_interruptible([target_extent, needs_touch, needs_step_2, &t, this, t_src] {
      auto p_extent = target_extent.get();
      if (needs_step_2) {
	t.maybe_add_to_read_set_step_2(p_extent);
      }
      if (needs_touch) {
	touch_extent_fully(*p_extent, &t_src, t.get_cache_hint());
      }
      return get_extent_iertr::make_ready_future<CachedExtentRef>(
        CachedExtentRef(p_extent));
    });
  }

  // wait extent io or do partial reads
  template <typename T>
  get_extent_iertr::future<TCachedExtentRef<T>>
  read_extent_maybe_partial(
    Transaction &t,
    TCachedExtentRef<T> extent,
    extent_len_t partial_off,
    extent_len_t partial_len) {
    assert(is_logical_type(extent->get_type()));
    if (!extent->is_range_loaded(partial_off, partial_len)) {
      LOG_PREFIX(Cache::read_extent_maybe_partial);
      SUBDEBUGT(seastore_cache,
        "{} {}~0x{:x} is present on t without range 0x{:x}~0x{:x}, reading ... -- {}",
        t, extent->get_type(), extent->get_paddr(), extent->get_length(),
        partial_off, partial_len, *extent);
      const auto t_src = t.get_src();
      cache_access_stats_t& access_stats = get_by_ext(
        get_by_src(stats.access_by_src_ext, t_src),
        extent->get_type());
      ++access_stats.load_present;
      ++stats.access.load_present;
      return trans_intr::make_interruptible(
        do_read_extent_maybe_partial(
          std::move(extent), partial_off, partial_len, &t_src));
    } else {
      // TODO(implement fine-grained-wait):
      // the range might be already loaded, but we don't know
      return trans_intr::make_interruptible(
        extent->wait_io()
      ).then_interruptible([extent] {
        return get_extent_iertr::make_ready_future<TCachedExtentRef<T>>(extent);
      });
    }
  }

  extent_len_t get_block_size() const {
    return epm.get_block_size();
  }

// Interfaces only for tests.
public:
  CachedExtentRef test_query_cache(paddr_t offset) {
    assert(offset.is_absolute());
    return query_cache(offset);
  }

private:
  using get_extent_ertr = base_ertr;
  template <typename T>
  using read_extent_ret = get_extent_ertr::future<TCachedExtentRef<T>>;
  /// Implements exclusive call to read_extent() for the extent
  template <typename T>
  read_extent_ret<T> do_read_extent_maybe_partial(
    TCachedExtentRef<T>&& extent,
    extent_len_t partial_off,
    extent_len_t partial_len,
    const Transaction::src_t* p_src)
  {
    LOG_PREFIX(Cache::do_read_extent_maybe_partial);
    // They must be atomic:
    // 1. checking missing range and wait io
    // 2. checking missing range and read
    // because the extents in Caches can be accessed concurrently
    //
    // TODO(implement fine-grained-wait)
    assert(!extent->is_range_loaded(partial_off, partial_len));
    assert(extent->is_data_stable());
    if (extent->is_pending_io()) {
      std::optional<Transaction::src_t> src;
      if (p_src) {
        src = *p_src;
      }
      auto* p_extent = extent.get();
      return p_extent->wait_io(
      ).then([extent=std::move(extent), partial_off, partial_len, this, FNAME, src]() mutable
             -> read_extent_ret<T> {
        if (extent->is_range_loaded(partial_off, partial_len)) {
          SUBDEBUG(seastore_cache,
            "{} {}~0x{:x} got range 0x{:x}~0x{:x} ... -- {}",
            extent->get_type(), extent->get_paddr(), extent->get_length(),
            partial_off, partial_len, *extent);
          // we don't know whether the target range is loading or not
          if (extent->is_pending_io()) {
            auto* p_extent = extent.get();
            return p_extent->wait_io(
            ).then([extent=std::move(extent)]() mutable {
              return seastar::make_ready_future<TCachedExtentRef<T>>(std::move(extent));
            });
          } else {
            return seastar::make_ready_future<TCachedExtentRef<T>>(std::move(extent));
          }
        } else { // range not loaded
          SUBDEBUG(seastore_cache,
            "{} {}~0x{:x} without range 0x{:x}~0x{:x} ... -- {}",
            extent->get_type(), extent->get_paddr(), extent->get_length(),
            partial_off, partial_len, *extent);
          Transaction::src_t* p_src = (src.has_value() ? &src.value() : nullptr);
          return do_read_extent_maybe_partial(
              std::move(extent), partial_off, partial_len, p_src);
        }
      });
    } else {
      SUBDEBUG(seastore_cache,
        "{} {}~0x{:x} is not pending without range 0x{:x}~0x{:x}, reading ... -- {}",
        extent->get_type(), extent->get_paddr(), extent->get_length(),
        partial_off, partial_len, *extent);
      return read_extent<T>(
        std::move(extent), partial_off, partial_len, p_src);
    }
  }

  /**
   * do_get_caching_extent
   *
   * returns ref to extent at offset~length of type T either from
   * - extent_set if already in cache
   * - disk
   * only load partial_off~partial_len
   */
  using src_ext_t = std::pair<Transaction::src_t, extent_types_t>;
  template <typename T, typename Func, typename OnCache>
  read_extent_ret<T> do_get_caching_extent(
    paddr_t offset,                ///< [in] starting addr
    extent_len_t length,           ///< [in] length
    extent_len_t partial_off,      ///< [in] offset of piece in extent
    extent_len_t partial_len,      ///< [in] length of piece in extent
    Func &&extent_init_func,       ///< [in] init func for extent
    OnCache &&on_cache,
    const Transaction::src_t* p_src
  ) {
    LOG_PREFIX(Cache::do_get_caching_extent);
    assert(offset.is_absolute());
    auto cached = query_cache(offset);
    if (!cached) {
      // partial read
      TCachedExtentRef<T> ret = CachedExtent::make_cached_extent_ref<T>(length);
      ret->init(CachedExtent::extent_state_t::CLEAN,
                offset,
                PLACEMENT_HINT_NULL,
                NULL_GENERATION,
		TRANS_ID_NULL);
      SUBDEBUG(seastore_cache,
          "{} {}~0x{:x} is absent, add extent and reading range 0x{:x}~0x{:x} ... -- {}",
          T::TYPE, offset, length, partial_off, partial_len, *ret);
      add_extent(ret);
      extent_init_func(*ret);
      // touch_extent() should be included in on_cache,
      // required by add_extent()
      on_cache(*ret);
      return read_extent<T>(
	std::move(ret), partial_off, partial_len, p_src);
    }

    // extent PRESENT in cache
    if (is_retired_placeholder_type(cached->get_type())) {
      // partial read
      TCachedExtentRef<T> ret = CachedExtent::make_cached_extent_ref<T>(length);
      ret->init(CachedExtent::extent_state_t::CLEAN,
                offset,
                PLACEMENT_HINT_NULL,
                NULL_GENERATION,
		TRANS_ID_NULL);
      SUBDEBUG(seastore_cache,
          "{} {}~0x{:x} is absent(placeholder), add extent and reading range 0x{:x}~0x{:x} ... -- {}",
          T::TYPE, offset, length, partial_off, partial_len, *ret);
      extent_init_func(*ret);
      on_cache(*ret);
      extents_index.replace(*ret, *cached);

      // replace placeholder in transactions
      while (!cached->read_transactions.empty()) {
        auto t = cached->read_transactions.begin()->t;
        t->replace_placeholder(*cached, *ret);
      }

      cached->state = CachedExtent::extent_state_t::INVALID;
      return read_extent<T>(
	std::move(ret), partial_off, partial_len, p_src);
    }

    auto ret = TCachedExtentRef<T>(static_cast<T*>(cached.get()));
    on_cache(*ret);
    if (ret->is_range_loaded(partial_off, partial_len)) {
      SUBTRACE(seastore_cache,
          "{} {}~0x{:x} is present with range 0x{:x}~0x{:x} ... -- {}",
          T::TYPE, offset, length, partial_off, partial_len, *ret);
      return ret->wait_io().then([ret] {
        // ret may be invalid, caller must check
        return seastar::make_ready_future<TCachedExtentRef<T>>(ret);
      });
    } else {
      SUBDEBUG(seastore_cache,
          "{} {}~0x{:x} is present without range 0x{:x}~0x{:x}, reading ... -- {}",
          T::TYPE, offset, length, partial_off, partial_len, *ret);
      return do_read_extent_maybe_partial(
          std::move(ret), partial_off, partial_len, p_src);
    }
  }

  template <typename T, typename Func, typename OnCache>
  read_extent_ret<T> do_get_caching_extent(
    paddr_t offset,                ///< [in] starting addr
    extent_len_t length,           ///< [in] length
    Func &&extent_init_func,       ///< [in] init func for extent
    OnCache &&on_cache,
    const Transaction::src_t* p_src
  ) {
    return do_get_caching_extent<T>(offset, length, 0, length,
      std::forward<Func>(extent_init_func),
      std::forward<OnCache>(on_cache),
      p_src);
  }

  // This is a workaround std::move_only_function not being available,
  // not really worth generalizing at this time.
  class extent_init_func_t {
    struct callable_i {
      virtual void operator()(CachedExtent &extent) = 0;
      virtual ~callable_i() = default;
    };
    template <typename Func>
    struct callable_wrapper final : callable_i {
      Func func;
      callable_wrapper(Func &&func) : func(std::forward<Func>(func)) {}
      void operator()(CachedExtent &extent) final {
	return func(extent);
      }
      ~callable_wrapper() final = default;
    };
  public:
    std::unique_ptr<callable_i> wrapped;
    template <typename Func>
    extent_init_func_t(Func &&func) : wrapped(
      std::make_unique<callable_wrapper<Func>>(std::forward<Func>(func)))
    {}
    void operator()(CachedExtent &extent) {
      return (*wrapped)(extent);
    }
  };

  get_extent_ertr::future<CachedExtentRef>
  do_get_caching_extent_by_type(
    extent_types_t type,
    paddr_t offset,
    laddr_t laddr,
    extent_len_t length,
    extent_init_func_t &&extent_init_func,
    extent_init_func_t &&on_cache,
    const Transaction::src_t* p_src);

  /**
   * get_caching_extent_by_type
   *
   * Note, the current implementation leverages parent-child
   * pointers in LBA instead, so it should only be called in tests.
   *
   * This path won't be accounted by the cache_access_stats_t.
   */
  using get_extent_by_type_iertr = get_extent_iertr;
  using get_extent_by_type_ret = get_extent_by_type_iertr::future<
    CachedExtentRef>;
  get_extent_by_type_ret get_caching_extent_by_type(
    Transaction &t,
    extent_types_t type,
    paddr_t offset,
    laddr_t laddr,
    extent_len_t length,
    extent_init_func_t &&extent_init_func
  ) {
    LOG_PREFIX(Cache::get_caching_extent_by_type);
    const auto t_src = t.get_src();
    CachedExtentRef ret;
    auto status = t.get_extent(offset, &ret);
    if (status == Transaction::get_extent_ret::RETIRED) {
      SUBERRORT(seastore_cache, "{} {}~0x{:x} {} is retired on t -- {}",
                t, type, offset, length, laddr, *ret);
      ceph_abort_msg("impossible");
    } else if (status == Transaction::get_extent_ret::PRESENT) {
      assert(ret->get_length() == length);
      if (ret->is_fully_loaded()) {
        SUBTRACET(seastore_cache, "{} {}~0x{:x} {} is present on t -- {}",
                  t, type, offset, length, laddr, *ret);
        return ret->wait_io().then([ret] {
	  return seastar::make_ready_future<CachedExtentRef>(ret);
        });
      } else {
        SUBDEBUGT(seastore_cache,
            "{} {}~0x{:x} {} is present on t without fully loaded, reading ... -- {}",
            t, type, offset, length, laddr, *ret);
        return do_read_extent_maybe_partial<CachedExtent>(
          std::move(ret), 0, length, &t_src);
      }
    } else {
      SUBTRACET(seastore_cache, "{} {}~0x{:x} {} is absent on t, query cache ...",
                t, type, offset, length, laddr);
      auto f = [&t, this, t_src](CachedExtent &ext) {
	t.add_to_read_set(CachedExtentRef(&ext));
	touch_extent_fully(ext, &t_src, t.get_cache_hint());
      };
      return trans_intr::make_interruptible(
	do_get_caching_extent_by_type(
	  type, offset, laddr, length,
	  std::move(extent_init_func), std::move(f), &t_src)
      );
    }
  }

  get_extent_by_type_ret _get_absent_extent_by_type(
    Transaction &t,
    extent_types_t type,
    paddr_t offset,
    laddr_t laddr,
    extent_len_t length,
    extent_init_func_t &&extent_init_func);

  backref_entryrefs_by_seq_t backref_entryrefs_by_seq;
  backref_entry_mset_t backref_entry_mset;

  using backref_entry_query_mset_t = std::multiset<
      backref_entry_t, backref_entry_t::cmp_t>;
  backref_entry_query_mset_t get_backref_entries_in_range(
    paddr_t start,
    paddr_t end) {
    auto start_iter = backref_entry_mset.lower_bound(
      start,
      backref_entry_t::cmp_t());
    auto end_iter = backref_entry_mset.lower_bound(
      end,
      backref_entry_t::cmp_t());
    backref_entry_query_mset_t res;
    for (auto it = start_iter;
	 it != end_iter;
	 it++) {
      res.emplace(it->paddr, it->laddr, it->len, it->type);
    }
    return res;
  }

  const backref_entry_mset_t& get_backref_entry_mset() {
    return backref_entry_mset;
  }

  backref_entryrefs_by_seq_t& get_backref_entryrefs_by_seq() {
    return backref_entryrefs_by_seq;
  }

  const segment_info_t* get_segment_info(segment_id_t sid) {
    auto provider = segment_providers_by_device_id[sid.device_id()];
    if (provider) {
      return &provider->get_seg_info(sid);
    } else {
      return nullptr;
    }
  }

public:
  /*
   * get_absent_extent_by_type
   *
   * Based on type, instantiate the correct concrete type
   * and read in the extent at location offset~length.
   *
   * The extent in query is supposed to be absent in Cache.
   *
   * User is responsible to call get_extent_viewable_by_trans()
   * *atomically* prior to call this method.
   */
  template <typename Func>
  get_extent_by_type_ret get_absent_extent_by_type(
    Transaction &t,         ///< [in] transaction
    extent_types_t type,    ///< [in] type tag
    paddr_t offset,         ///< [in] starting addr
    laddr_t laddr,          ///< [in] logical address if logical
    extent_len_t length,    ///< [in] length
    Func &&extent_init_func ///< [in] extent init func
  ) {
    return _get_absent_extent_by_type(
      t,
      type,
      offset,
      laddr,
      length,
      extent_init_func_t(std::forward<Func>(extent_init_func)));
  }

  get_extent_by_type_ret get_absent_extent_by_type(
    Transaction &t,
    extent_types_t type,
    paddr_t offset,
    laddr_t laddr,
    extent_len_t length
  ) {
    return get_absent_extent_by_type(
      t, type, offset, laddr, length, [](CachedExtent &) {});
  }

  void trim_backref_bufs(const journal_seq_t &trim_to) {
    LOG_PREFIX(Cache::trim_backref_bufs);
    SUBDEBUG(seastore_cache, "trimming to {}", trim_to);
    if (!backref_entryrefs_by_seq.empty()) {
      SUBDEBUG(seastore_cache, "backref_entryrefs_by_seq {} ~ {}, size={}",
               backref_entryrefs_by_seq.rbegin()->first,
               backref_entryrefs_by_seq.begin()->first,
               backref_entryrefs_by_seq.size());
      assert(backref_entryrefs_by_seq.rbegin()->first >= trim_to);
      auto iter = backref_entryrefs_by_seq.upper_bound(trim_to);
      backref_entryrefs_by_seq.erase(backref_entryrefs_by_seq.begin(), iter);
    }
    if (backref_entryrefs_by_seq.empty()) {
      SUBDEBUG(seastore_cache, "backref_entryrefs_by_seq all trimmed");
    }
  }

  /**
   * alloc_new_non_data_extent
   *
   * Allocates a fresh extent. if delayed is true, addr will be alloc'd later.
   * Note that epaddr can only be fed by the btree lba unittest for now
   */
  template <typename T>
  TCachedExtentRef<T> alloc_new_non_data_extent(
    Transaction &t,         ///< [in, out] current transaction
    extent_len_t length,    ///< [in] length
    placement_hint_t hint,  ///< [in] user hint
#ifdef UNIT_TESTS_BUILT
    rewrite_gen_t gen,      ///< [in] rewrite generation
    std::optional<paddr_t> epaddr = std::nullopt ///< [in] paddr fed by callers
#else
    rewrite_gen_t gen
#endif
  ) {
    LOG_PREFIX(Cache::alloc_new_non_data_extent);
    SUBTRACET(seastore_cache, "allocate {} 0x{:x}B, hint={}, gen={}",
              t, T::TYPE, length, hint, rewrite_gen_printer_t{gen});
#ifdef UNIT_TESTS_BUILT
    auto result = epm.alloc_new_non_data_extent(t, T::TYPE, length, hint, gen, epaddr);
#else
    auto result = epm.alloc_new_non_data_extent(t, T::TYPE, length, hint, gen);
#endif
    if (!result) {
      SUBERRORT(seastore_cache, "insufficient space", t);
      std::rethrow_exception(crimson::ct_error::enospc::exception_ptr());
    }
    auto ret = CachedExtent::make_cached_extent_ref<T>(std::move(result->bp));
    assert(is_rewrite_generation(
      result->gen,
      epm.dynamic_max_rewrite_generation));
    ret->init(CachedExtent::extent_state_t::INITIAL_WRITE_PENDING,
              result->paddr,
              hint,
              result->gen,
	      t.get_trans_id());
    t.add_fresh_extent(ret);
    SUBDEBUGT(seastore_cache,
              "allocated {} 0x{:x}B extent at {}, hint={}, gen={} -- {}",
              t, T::TYPE, length, result->paddr,
              hint, rewrite_gen_printer_t{result->gen}, *ret);
    return ret;
  }
  /**
   * alloc_new_data_extents
   *
   * Allocates a fresh extent. if delayed is true, addr will be alloc'd later.
   * Note that epaddr can only be fed by the btree lba unittest for now
   */
  template <typename T>
  std::vector<TCachedExtentRef<T>> alloc_new_data_extents(
    Transaction &t,         ///< [in, out] current transaction
    extent_len_t length,    ///< [in] length
    placement_hint_t hint,  ///< [in] user hint
#ifdef UNIT_TESTS_BUILT
    rewrite_gen_t gen,      ///< [in] rewrite generation
    std::optional<paddr_t> epaddr = std::nullopt ///< [in] paddr fed by callers
#else
    rewrite_gen_t gen
#endif
  ) {
    LOG_PREFIX(Cache::alloc_new_data_extents);
    SUBTRACET(seastore_cache, "allocate {} 0x{:x}B, hint={}, gen={}",
              t, T::TYPE, length, hint, rewrite_gen_printer_t{gen});
#ifdef UNIT_TESTS_BUILT
    auto results = epm.alloc_new_data_extents(t, T::TYPE, length, hint, gen, epaddr);
#else
    auto results = epm.alloc_new_data_extents(t, T::TYPE, length, hint, gen);
#endif
    if (results.empty()) {
      SUBERRORT(seastore_cache, "insufficient space", t);
      std::rethrow_exception(crimson::ct_error::enospc::exception_ptr());
    }
    std::vector<TCachedExtentRef<T>> extents;
    for (auto &result : results) {
      auto ret = CachedExtent::make_cached_extent_ref<T>(std::move(result.bp));
      assert(is_rewrite_generation(
	result.gen,
	epm.dynamic_max_rewrite_generation));
      ret->init(CachedExtent::extent_state_t::INITIAL_WRITE_PENDING,
                result.paddr,
                hint,
                result.gen,
                t.get_trans_id());
      t.add_fresh_extent(ret);
      SUBDEBUGT(seastore_cache,
                "allocated {} 0x{:x}B extent at {}, hint={}, gen={} -- {}",
                t, T::TYPE, length, result.paddr,
                hint, rewrite_gen_printer_t{result.gen}, *ret);
      extents.emplace_back(std::move(ret));
    }
    return extents;
  }

  /**
   * alloc_remapped_extent
   *
   * Allocates an EXIST_CLEAN extent. Use the buffer to fill the new extent
   * if buffer exists.
   */
  template <typename T>
  TCachedExtentRef<T> alloc_remapped_extent(
    Transaction &t,
    laddr_t remap_laddr,
    paddr_t remap_paddr,
    extent_len_t remap_offset,
    extent_len_t remap_length,
    const std::optional<ceph::bufferptr> &original_bptr) {
    LOG_PREFIX(Cache::alloc_remapped_extent);
    TCachedExtentRef<T> ext;
    if (original_bptr.has_value()) {
      // shallow copy the buffer from original extent
      auto nbp = ceph::bufferptr(*original_bptr, remap_offset, remap_length);
      // ExtentPlacementManager::alloc_new_extent will make a new
      // (relative/temp) paddr, so make extent directly
      ext = CachedExtent::make_cached_extent_ref<T>(std::move(nbp));
    } else {
      ext = CachedExtent::make_cached_extent_ref<T>(remap_length);
    }

    ext->init(CachedExtent::extent_state_t::EXIST_CLEAN,
	      remap_paddr,
	      PLACEMENT_HINT_NULL,
	      NULL_GENERATION,
              t.get_trans_id());

    auto extent = ext->template cast<T>();
    extent->set_laddr(remap_laddr);
    t.add_fresh_extent(ext);
    SUBTRACET(seastore_cache, "allocated {} 0x{:x}B, hint={}, has ptr? {} -- {}",
      t, T::TYPE, remap_length, remap_laddr, original_bptr.has_value(), *extent);
    return extent;
  }

  /**
   * alloc_new_non_data_extent_by_type
   *
   * Allocates a fresh non data extent.  addr will be relative until commit.
   */
  CachedExtentRef alloc_new_non_data_extent_by_type(
    Transaction &t,        ///< [in, out] current transaction
    extent_types_t type,   ///< [in] type tag
    extent_len_t length,   ///< [in] length
    placement_hint_t hint, ///< [in] user hint
    rewrite_gen_t gen      ///< [in] rewrite generation
    );

  /**
   * alloc_new_data_extents_by_type
   *
   * Allocates fresh data extents.  addr will be relative until commit.
   */
  std::vector<CachedExtentRef> alloc_new_data_extents_by_type(
    Transaction &t,        ///< [in, out] current transaction
    extent_types_t type,   ///< [in] type tag
    extent_len_t length,   ///< [in] length
    placement_hint_t hint, ///< [in] user hint
    rewrite_gen_t gen      ///< [in] rewrite generation
    );

  /**
   * Allocates mutable buffer from extent_set on offset~len
   *
   * TODO: Note, currently all implementations literally copy the
   * buffer.  This needn't be true, CachedExtent implementations could
   * choose to refer to the same buffer unmodified until commit and just
   * buffer the mutations in an ancillary data structure.
   *
   * @param current transaction
   * @param extent to duplicate
   * @return mutable extent
   */
  CachedExtentRef duplicate_for_write(
    Transaction &t,    ///< [in, out] current transaction
    CachedExtentRef i  ///< [in] ref to existing extent
  );

  /**
   * set_segment_provider
   *
   * Set to provide segment information to help identify out-dated delta.
   *
   * FIXME: This is specific to the segmented implementation
   */
  void set_segment_providers(std::vector<SegmentProvider*> &&providers) {
    segment_providers_by_device_id = std::move(providers);
  }

  /**
   * prepare_record
   *
   * Construct the record for Journal from transaction.
   */
  record_t prepare_record(
    Transaction &t, ///< [in, out] current transaction
    const journal_seq_t &journal_head,
    const journal_seq_t &journal_dirty_tail
  );

  /**
   * complete_commit
   *
   * Must be called upon completion of write.  Releases blocks on mutating
   * extents, fills in addresses, and calls relevant callbacks on fresh
   * and mutated exents.
   */
  void complete_commit(
    Transaction &t,            ///< [in, out] current transaction
    paddr_t final_block_start, ///< [in] offset of initial block
    journal_seq_t seq          ///< [in] journal commit seq
  );

  /**
   * init
   */
  void init();

  /**
   * mkfs
   *
   * Alloc initial root node and add to t.  The intention is for other
   * components to use t to adjust the resulting root ref prior to commit.
   */
  using mkfs_iertr = base_iertr;
  mkfs_iertr::future<> mkfs(Transaction &t);

  /**
   * close
   *
   * TODO: should flush dirty blocks
   */
  using close_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  close_ertr::future<> close();

  /**
   * replay_delta
   *
   * Intended for use in Journal::delta. For each delta, should decode delta,
   * read relevant block from disk or cache (using correct type), and call
   * CachedExtent::apply_delta marking the extent dirty.
   *
   * Returns whether the delta is applied.
   */
  using replay_delta_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using replay_delta_ret = replay_delta_ertr::future<
    std::pair<bool, CachedExtentRef>>;
  replay_delta_ret replay_delta(
    journal_seq_t seq,
    paddr_t record_block_base,
    const delta_info_t &delta,
    const journal_seq_t &dirty_tail,
    const journal_seq_t &alloc_tail,
    sea_time_point modify_time);

  /**
   * init_cached_extents
   *
   * Calls passed lambda for each dirty cached block.  Intended for use
   * after replay to allow lba_manager (or w/e) to read in any ancestor
   * blocks.
   */
  using init_cached_extents_iertr = base_iertr;
  using init_cached_extents_ret = init_cached_extents_iertr::future<>;
  template <typename F>
  init_cached_extents_ret init_cached_extents(
    Transaction &t,
    F &&f)
  {
    LOG_PREFIX(Cache::init_cached_extents);
    SUBINFOT(seastore_cache,
        "start with {}(0x{:x}B) extents, {} dirty, dirty_from={}, alloc_from={}",
        t,
        extents_index.size(),
        extents_index.get_bytes(),
        dirty.size(),
        get_oldest_dirty_from().value_or(JOURNAL_SEQ_NULL),
        get_oldest_backref_dirty_from().value_or(JOURNAL_SEQ_NULL));

    // journal replay should has been finished at this point,
    // Cache::root should have been inserted to the dirty list
    assert(root->is_stable_dirty());
    std::vector<CachedExtentRef> _dirty;
    for (auto &e : extents_index) {
      _dirty.push_back(CachedExtentRef(&e));
    }
    return seastar::do_with(
      std::forward<F>(f),
      std::move(_dirty),
      [this, FNAME, &t](auto &f, auto &refs) mutable
    {
      return trans_intr::do_for_each(
        refs,
        [this, FNAME, &t, &f](auto &e)
      {
        SUBTRACET(seastore_cache, "inspecting extent ... -- {}", t, *e);
        return f(t, e
        ).si_then([this, FNAME, &t, e](bool is_alive) {
          if (!is_alive) {
            SUBDEBUGT(seastore_cache, "extent is not alive, remove extent -- {}", t, *e);
            remove_extent(e, nullptr);
	    e->set_invalid(t);
          } else {
            SUBDEBUGT(seastore_cache, "extent is alive -- {}", t, *e);
          }
        });
      });
    }).handle_error_interruptible(
      init_cached_extents_iertr::pass_further{},
      crimson::ct_error::assert_all{
        "Invalid error in Cache::init_cached_extents"
      }
    ).si_then([this, FNAME, &t] {
      SUBINFOT(seastore_cache,
          "finish with {}(0x{:x}B) extents, {} dirty, dirty_from={}, alloc_from={}",
          t,
          extents_index.size(),
          extents_index.get_bytes(),
          dirty.size(),
          get_oldest_dirty_from().value_or(JOURNAL_SEQ_NULL),
          get_oldest_backref_dirty_from().value_or(JOURNAL_SEQ_NULL));
    });
  }

  /**
   * update_extent_from_transaction
   *
   * Updates passed extent based on t.  If extent has been retired,
   * a null result will be returned.
   */
  CachedExtentRef update_extent_from_transaction(
    Transaction &t,
    CachedExtentRef extent) {
    if (extent->get_type() == extent_types_t::ROOT) {
      if (t.root) {
	return t.root;
      } else {
	t.add_to_read_set(extent);
	t.root = extent->cast<RootBlock>();
	return extent;
      }
    } else {
      if (extent->is_stable()) {
	std::ignore = t.maybe_add_to_read_set(extent);
      }
      return extent->maybe_get_transactional_view(t);
    }
  }

  /**
   * print
   *
   * Dump summary of contents (TODO)
   */
  std::ostream &print(
    std::ostream &out) const {
    return out;
  }

  /**
   * get_next_dirty_extents
   *
   * Returns extents with get_dirty_from() < seq and adds to read set of
   * t.
   */
  using get_next_dirty_extents_iertr = base_iertr;
  using get_next_dirty_extents_ret = get_next_dirty_extents_iertr::future<
    std::vector<CachedExtentRef>>;
  get_next_dirty_extents_ret get_next_dirty_extents(
    Transaction &t,
    journal_seq_t seq,
    size_t max_bytes);

  /// returns std::nullopt if no pending alloc-infos
  std::optional<journal_seq_t> get_oldest_backref_dirty_from() const {
    LOG_PREFIX(Cache::get_oldest_backref_dirty_from);
    if (backref_entryrefs_by_seq.empty()) {
      SUBDEBUG(seastore_cache, "backref_oldest: null");
      return std::nullopt;
    }
    auto oldest = backref_entryrefs_by_seq.begin()->first;
    SUBDEBUG(seastore_cache, "backref_oldest: {}", oldest);
    ceph_assert(oldest != JOURNAL_SEQ_NULL);
    return oldest;
  }

  /// returns std::nullopt if no dirty extents
  /// returns JOURNAL_SEQ_NULL if the oldest dirty extent is still pending
  std::optional<journal_seq_t> get_oldest_dirty_from() const {
    LOG_PREFIX(Cache::get_oldest_dirty_from);
    if (dirty.empty()) {
      SUBDEBUG(seastore_cache, "dirty_oldest: null");
      return std::nullopt;
    } else {
      auto oldest = dirty.begin()->get_dirty_from();
      if (oldest == JOURNAL_SEQ_NULL) {
	SUBDEBUG(seastore_cache, "dirty_oldest: pending");
      } else {
	SUBDEBUG(seastore_cache, "dirty_oldest: {}", oldest);
      }
      return oldest;
    }
  }

  /// Dump live extents
  void dump_contents();

  /**
   * backref_extent_entry_t
   *
   * All the backref extent entries have to be indexed by paddr in memory,
   * so they can be retrived by range during cleaning.
   *
   * See BtreeBackrefManager::retrieve_backref_extents_in_range()
   */
  struct backref_extent_entry_t {
    backref_extent_entry_t(
      paddr_t paddr,
      paddr_t key,
      extent_types_t type)
      : paddr(paddr), key(key), type(type) {}
    paddr_t paddr = P_ADDR_NULL;
    paddr_t key = P_ADDR_NULL;
    extent_types_t type = extent_types_t::ROOT;
    struct cmp_t {
      using is_transparent = paddr_t;
      bool operator()(
	const backref_extent_entry_t &l,
	const backref_extent_entry_t &r) const {
	return l.paddr < r.paddr;
      }
      bool operator()(
	const paddr_t &l,
	const backref_extent_entry_t &r) const {
	return l < r.paddr;
      }
      bool operator()(
	const backref_extent_entry_t &l,
	const paddr_t &r) const {
	return l.paddr < r;
      }
    };
  };

  void update_tree_extents_num(extent_types_t type, int64_t delta) {
    switch (type) {
    case extent_types_t::LADDR_INTERNAL:
      [[fallthrough]];
    case extent_types_t::DINK_LADDR_LEAF:
      [[fallthrough]];
    case extent_types_t::LADDR_LEAF:
      stats.lba_tree_extents_num += delta;
      ceph_assert(stats.lba_tree_extents_num >= 0);
      return;
    case extent_types_t::OMAP_INNER:
      [[fallthrough]];
    case extent_types_t::OMAP_LEAF:
      stats.omap_tree_extents_num += delta;
      ceph_assert(stats.lba_tree_extents_num >= 0);
      return;
    case extent_types_t::ONODE_BLOCK_STAGED:
      stats.onode_tree_extents_num += delta;
      ceph_assert(stats.onode_tree_extents_num >= 0);
      return;
    case extent_types_t::BACKREF_INTERNAL:
      [[fallthrough]];
    case extent_types_t::BACKREF_LEAF:
      stats.backref_tree_extents_num += delta;
      ceph_assert(stats.backref_tree_extents_num >= 0);
      return;
    default:
      return;
    }
  }

  uint64_t get_omap_tree_depth() {
    return stats.omap_tree_depth;
  }

  void boot_done() {
    booting = false;
  }
private:
  void touch_extent_fully(
      CachedExtent &ext,
      const Transaction::src_t* p_src,
      cache_hint_t hint)
  {
    touch_extent_by_range(ext, p_src, hint, 0, ext.get_length());
  }

  /// Update extent pinboard for access to ref
  ///
  /// The extent must be touched before read_extent(), otherwise it is
  /// incorrect to detect sequential read using load_start and load_length.
  void touch_extent_by_range(
      CachedExtent &ext,
      const Transaction::src_t* p_src,
      cache_hint_t hint,
      extent_len_t load_start,
      extent_len_t load_length)
  {
    assert(ext.get_paddr().is_absolute());
    if (hint == CACHE_HINT_NOCACHE && is_logical_type(ext.get_type())) {
      return;
    }
    if (ext.is_stable_clean() && !ext.is_placeholder()) {
      pinboard->move_to_top(ext, p_src, load_start, load_length);
    }
  }

  ExtentPlacementManager& epm;
  RootBlockRef root;               ///< ref to current root
  ExtentIndex extents_index;             ///< set of live extents

  journal_seq_t last_commit = JOURNAL_SEQ_MIN;

  // FIXME: This is specific to the segmented implementation
  std::vector<SegmentProvider*> segment_providers_by_device_id;

  transaction_id_t next_id = 0;

  /**
   * dirty
   *
   * holds refs to dirty extents.  Ordered by CachedExtent::get_dirty_from().
   */
  CachedExtent::primary_ref_list dirty;

  using backref_extent_entry_query_set_t =
    std::set<
      backref_extent_entry_t,
      backref_extent_entry_t::cmp_t>;
  backref_extent_entry_query_set_t backref_extents;

  void add_backref_extent(
    paddr_t paddr,
    paddr_t key,
    extent_types_t type) {
    assert(paddr.is_absolute());
    auto [iter, inserted] = backref_extents.emplace(paddr, key, type);
    boost::ignore_unused(inserted);
    assert(inserted);
  }

  void remove_backref_extent(paddr_t paddr) {
    auto iter = backref_extents.find(paddr);
    if (iter != backref_extents.end())
      backref_extents.erase(iter);
  }

  backref_extent_entry_query_set_t get_backref_extents_in_range(
    paddr_t start,
    paddr_t end) {
    auto start_iter = backref_extents.lower_bound(start);
    auto end_iter = backref_extents.upper_bound(end);
    backref_extent_entry_query_set_t res;
    res.insert(start_iter, end_iter);
    return res;
  }

  friend class crimson::os::seastore::backref::BtreeBackrefManager;
  friend class crimson::os::seastore::BackrefManager;

  ExtentPinboardRef pinboard;

  struct query_counters_t {
    uint64_t access = 0;
    uint64_t hit = 0;
  };

  btree_cursor_stats_t cursor_stats;
  struct invalid_trans_efforts_t {
    io_stat_t read;
    io_stat_t mutate;
    uint64_t mutate_delta_bytes = 0;
    io_stat_t retire;
    io_stat_t fresh;
    io_stat_t fresh_ool_written;
    counter_by_extent_t<uint64_t> num_trans_invalidated;
    uint64_t total_trans_invalidated = 0;
    uint64_t num_ool_records = 0;
    uint64_t ool_record_bytes = 0;
  };

  struct commit_trans_efforts_t {
    counter_by_extent_t<io_stat_t> read_by_ext;
    counter_by_extent_t<io_stat_t> mutate_by_ext;
    counter_by_extent_t<uint64_t> delta_bytes_by_ext;
    counter_by_extent_t<io_stat_t> retire_by_ext;
    counter_by_extent_t<io_stat_t> fresh_invalid_by_ext; // inline but is already invalid (retired)
    counter_by_extent_t<io_stat_t> fresh_inline_by_ext;
    counter_by_extent_t<io_stat_t> fresh_ool_by_ext;
    uint64_t num_trans = 0; // the number of inline records
    uint64_t num_ool_records = 0;
    uint64_t ool_record_metadata_bytes = 0;
    uint64_t ool_record_data_bytes = 0;
    uint64_t inline_record_metadata_bytes = 0; // metadata exclude the delta bytes
  };

  struct success_read_trans_efforts_t {
    io_stat_t read;
    uint64_t num_trans = 0;
  };

  struct tree_efforts_t {
    uint64_t num_inserts = 0;
    uint64_t num_erases = 0;
    uint64_t num_updates = 0;

    void increment(const Transaction::tree_stats_t& incremental) {
      num_inserts += incremental.num_inserts;
      num_erases += incremental.num_erases;
      num_updates += incremental.num_updates;
    }
  };

  static constexpr std::size_t NUM_SRC_COMB =
      TRANSACTION_TYPE_MAX * (TRANSACTION_TYPE_MAX + 1) / 2;

  struct {
    counter_by_src_t<uint64_t> trans_created_by_src;
    counter_by_src_t<commit_trans_efforts_t> committed_efforts_by_src;
    counter_by_src_t<invalid_trans_efforts_t> invalidated_efforts_by_src;
    success_read_trans_efforts_t success_read_efforts;

    uint64_t dirty_bytes = 0;
    counter_by_extent_t<cache_size_stats_t> dirty_sizes_by_ext;
    dirty_io_stats_t dirty_io;
    counter_by_src_t<counter_by_extent_t<dirty_io_stats_t> >
      dirty_io_by_src_ext;

    cache_access_stats_t access;
    counter_by_src_t<uint64_t> cache_absent_by_src;
    counter_by_src_t<counter_by_extent_t<cache_access_stats_t> >
      access_by_src_ext;

    uint64_t onode_tree_depth = 0;
    int64_t onode_tree_extents_num = 0;
    counter_by_src_t<tree_efforts_t> committed_onode_tree_efforts;
    counter_by_src_t<tree_efforts_t> invalidated_onode_tree_efforts;

    uint64_t omap_tree_depth = 0;
    int64_t omap_tree_extents_num = 0;
    counter_by_src_t<tree_efforts_t> committed_omap_tree_efforts;
    counter_by_src_t<tree_efforts_t> invalidated_omap_tree_efforts;

    uint64_t lba_tree_depth = 0;
    int64_t lba_tree_extents_num = 0;
    counter_by_src_t<tree_efforts_t> committed_lba_tree_efforts;
    counter_by_src_t<tree_efforts_t> invalidated_lba_tree_efforts;

    uint64_t backref_tree_depth = 0;
    int64_t backref_tree_extents_num = 0;
    counter_by_src_t<tree_efforts_t> committed_backref_tree_efforts;
    counter_by_src_t<tree_efforts_t> invalidated_backref_tree_efforts;

    std::array<uint64_t, NUM_SRC_COMB> trans_conflicts_by_srcs;
    counter_by_src_t<uint64_t> trans_conflicts_by_unknown;

    rewrite_stats_t trim_rewrites;
    rewrite_stats_t reclaim_rewrites;
  } stats;

  mutable dirty_io_stats_t last_dirty_io;
  mutable counter_by_src_t<counter_by_extent_t<dirty_io_stats_t> >
    last_dirty_io_by_src_ext;
  mutable rewrite_stats_t last_trim_rewrites;
  mutable rewrite_stats_t last_reclaim_rewrites;
  mutable cache_access_stats_t last_access;
  mutable counter_by_src_t<uint64_t> last_cache_absent_by_src;
  mutable counter_by_src_t<counter_by_extent_t<cache_access_stats_t> >
    last_access_by_src_ext;

  void account_conflict(Transaction::src_t src1, Transaction::src_t src2) {
    assert(src1 < Transaction::src_t::MAX);
    assert(src2 < Transaction::src_t::MAX);
    if (src1 > src2) {
      std::swap(src1, src2);
    }
    // impossible combinations
    // should be consistent with trans_srcs_invalidated in register_metrics()
    assert(!(src1 == Transaction::src_t::READ &&
             src2 == Transaction::src_t::READ));
    assert(!(src1 == Transaction::src_t::TRIM_DIRTY &&
             src2 == Transaction::src_t::TRIM_DIRTY));
    assert(!(src1 == Transaction::src_t::CLEANER_MAIN &&
	     src2 == Transaction::src_t::CLEANER_MAIN));
    assert(!(src1 == Transaction::src_t::CLEANER_COLD &&
	     src2 == Transaction::src_t::CLEANER_COLD));
    assert(!(src1 == Transaction::src_t::TRIM_ALLOC &&
             src2 == Transaction::src_t::TRIM_ALLOC));

    auto src1_value = static_cast<std::size_t>(src1);
    auto src2_value = static_cast<std::size_t>(src2);
    auto num_srcs = static_cast<std::size_t>(Transaction::src_t::MAX);
    auto conflict_index = num_srcs * src1_value + src2_value -
        src1_value * (src1_value + 1) / 2;
    assert(conflict_index < NUM_SRC_COMB);
    ++stats.trans_conflicts_by_srcs[conflict_index];
  }

  seastar::metrics::metric_group metrics;
  void register_metrics();

  void apply_backref_mset(
      backref_entry_refs_t& backref_entries) {
    for (auto& entry : backref_entries) {
      backref_entry_mset.insert(*entry);
    }
  }

  void apply_backref_byseq(
      backref_entry_refs_t&& backref_entries,
      const journal_seq_t& seq);

  void commit_backref_entries(
      backref_entry_refs_t&& backref_entries,
      const journal_seq_t& seq) {
    apply_backref_mset(backref_entries);
    apply_backref_byseq(std::move(backref_entries), seq);
  }

  /// Add extent to extents handling dirty and refcounting
  ///
  /// Note, it must follows with add_to_dirty() or touch_extent().
  /// The only exception is RetiredExtentPlaceholder.
  void add_extent(CachedExtentRef ref);

  /// Mark exising extent ref dirty -- mainly for replay
  void mark_dirty(CachedExtentRef ref);

  /// Add dirty extent to dirty list
  void add_to_dirty(
      CachedExtentRef ref,
      const Transaction::src_t* p_src);

  /// Replace the prev dirty extent by next
  void replace_dirty(
      CachedExtentRef next,
      CachedExtentRef prev,
      const Transaction::src_t& src);

  /// Remove from dirty list
  void remove_from_dirty(
      CachedExtentRef ref,
      const Transaction::src_t* p_src);

  void clear_dirty();

  /// Remove extent from extents handling dirty and refcounting
  void remove_extent(
      CachedExtentRef ref,
      const Transaction::src_t* p_src);

  /// Retire extent
  void commit_retire_extent(Transaction& t, CachedExtentRef ref);

  /// Replace prev with next
  void commit_replace_extent(Transaction& t, CachedExtentRef next, CachedExtentRef prev);

  /// Invalidate extent and mark affected transactions
  void invalidate_extent(Transaction& t, CachedExtent& extent);

  /// Mark a valid transaction as conflicted
  void mark_transaction_conflicted(
    Transaction& t, CachedExtent& conflicting_extent);

  /// Introspect transaction when it is being destructed
  void on_transaction_destruct(Transaction& t);

  /// Read the extent in range offset~length,
  /// must be called exclusively for an extent,
  /// also see do_read_extent_maybe_partial().
  ///
  /// May return an invalid extent due to transaction conflict.
  get_extent_ertr::future<CachedExtentRef> _read_extent(
    CachedExtentRef &&extent,
    extent_len_t offset,
    extent_len_t length,
    const Transaction::src_t *p_src)
  {
    LOG_PREFIX(Cache::read_extent);
    assert(extent->state == CachedExtent::extent_state_t::EXIST_CLEAN ||
           extent->state == CachedExtent::extent_state_t::CLEAN);
    assert(!extent->is_range_loaded(offset, length));
    assert(is_aligned(offset, get_block_size()));
    assert(is_aligned(length, get_block_size()));
    assert(extent->get_paddr().is_absolute());
    extent->set_io_wait(extent->state);
    auto old_length = extent->get_loaded_length();
    load_ranges_t to_read = extent->load_ranges(offset, length);
    auto new_length = extent->get_loaded_length();
    assert(new_length > old_length);
    pinboard->increase_cached_size(*extent, new_length - old_length, p_src);
    return seastar::do_with(to_read.ranges, [extent, this, FNAME](auto &read_ranges) {
      return ExtentPlacementManager::read_ertr::parallel_for_each(
          read_ranges, [extent, this, FNAME](auto &read_range) {
        SUBDEBUG(seastore_cache, "reading extent {} 0x{:x}~0x{:x} ...",
                 extent->get_paddr(), read_range.offset, read_range.get_length());
        assert(is_aligned(read_range.offset, get_block_size()));
        assert(is_aligned(read_range.get_length(), get_block_size()));
        return epm.read(
          extent->get_paddr() + read_range.offset,
          read_range.get_length(),
          read_range.ptr);
      });
    }).safe_then(
      [this, FNAME, extent=std::move(extent), offset, length]() mutable {
        ceph_assert(extent->state == CachedExtent::extent_state_t::EXIST_CLEAN
          || extent->state == CachedExtent::extent_state_t::CLEAN
          || !extent->is_valid());
        if (extent->is_valid()) {
          if (extent->is_fully_loaded()) {
            // crc will be checked against LBA leaf entry for logical extents,
            // or check against in-extent crc for physical extents.
            if (epm.get_checksum_needed(extent->get_paddr())) {
              extent->last_committed_crc = extent->calc_crc32c();
            } else {
              extent->last_committed_crc = CRC_NULL;
            }
            // on_clean_read() may change the content, call after calc_crc32c()
            extent->on_clean_read();
            SUBDEBUG(seastore_cache, "read extent 0x{:x}~0x{:x} done -- {}",
              offset, length, *extent);
          } else {
            extent->last_committed_crc = CRC_NULL;
            SUBDEBUG(seastore_cache, "read extent 0x{:x}~0x{:x} done (partial) -- {}",
              offset, length, *extent);
          }
        } else {
          SUBDEBUG(seastore_cache, "read extent 0x{:x}~0x{:x} done (invalidated) -- {}",
            offset, length, *extent);
        }
        extent->complete_io();
        return get_extent_ertr::make_ready_future<CachedExtentRef>(
          std::move(extent));
      },
      get_extent_ertr::pass_further{},
      crimson::ct_error::assert_all{
        "Cache::read_extent: invalid error"
      }
    );
  }

  template <typename T>
  read_extent_ret<T> read_extent(
    TCachedExtentRef<T>&& extent,
    extent_len_t offset,
    extent_len_t length,
    const Transaction::src_t* p_src
  ) {
    return _read_extent(std::move(extent), offset, length, p_src
    ).safe_then([](auto extent) {
      return extent->template cast<T>();
    });
  }

  // Extents in cache may contain placeholders
  CachedExtentRef query_cache(paddr_t offset) {
    if (auto iter = extents_index.find_offset(offset);
        iter != extents_index.end()) {
      assert(iter->is_stable());
      return CachedExtentRef(&*iter);
    } else {
      return CachedExtentRef();
    }
  }

  bool booting = true;
};
using CacheRef = std::unique_ptr<Cache>;

}
