// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include "seastar/core/shared_future.hh"

#include "include/buffer.h"

#include "crimson/common/errorator.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/extent_placement_manager.h"
#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/random_block_manager.h"
#include "crimson/os/seastore/root_block.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/transaction.h"

namespace crimson::os::seastore::backref {
class BtreeBackrefManager;
}

namespace crimson::os::seastore {

template <
  typename node_key_t,
  typename node_val_t,
  typename internal_node_t,
  typename leaf_node_t,
  typename pin_t,
  size_t node_size,
  bool leaf_has_children>
class FixedKVBtree;
class BackrefManager;
class SegmentProvider;

struct backref_entry_t {
  backref_entry_t(
    const paddr_t paddr,
    const laddr_t laddr,
    const extent_len_t len,
    const extent_types_t type,
    const journal_seq_t seq)
    : paddr(paddr),
      laddr(laddr),
      len(len),
      type(type),
      seq(seq)
  {}
  backref_entry_t(alloc_blk_t alloc_blk)
    : paddr(alloc_blk.paddr),
      laddr(alloc_blk.laddr),
      len(alloc_blk.len),
      type(alloc_blk.type)
  {}
  paddr_t paddr = P_ADDR_NULL;
  laddr_t laddr = L_ADDR_NULL;
  extent_len_t len = 0;
  extent_types_t type =
    extent_types_t::ROOT;
  journal_seq_t seq;
  friend bool operator< (
    const backref_entry_t &l,
    const backref_entry_t &r) {
    return l.paddr < r.paddr;
  }
  friend bool operator> (
    const backref_entry_t &l,
    const backref_entry_t &r) {
    return l.paddr > r.paddr;
  }
  friend bool operator== (
    const backref_entry_t &l,
    const backref_entry_t &r) {
    return l.paddr == r.paddr;
  }

  using set_hook_t =
    boost::intrusive::set_member_hook<
      boost::intrusive::link_mode<
	boost::intrusive::auto_unlink>>;
  set_hook_t backref_set_hook;
  using backref_set_member_options = boost::intrusive::member_hook<
    backref_entry_t,
    set_hook_t,
    &backref_entry_t::backref_set_hook>;
  using multiset_t = boost::intrusive::multiset<
    backref_entry_t,
    backref_set_member_options,
    boost::intrusive::constant_time_size<false>>;

  struct cmp_t {
    using is_transparent = paddr_t;
    bool operator()(
      const backref_entry_t &l,
      const backref_entry_t &r) const {
      return l.paddr < r.paddr;
    }
    bool operator()(const paddr_t l, const backref_entry_t &r) const {
      return l < r.paddr;
    }
    bool operator()(const backref_entry_t &l, const paddr_t r) const {
      return l.paddr < r;
    }
  };
};

std::ostream &operator<<(std::ostream &out, const backref_entry_t &ent);

using backref_entry_ref = std::unique_ptr<backref_entry_t>;
using backref_entry_mset_t = backref_entry_t::multiset_t;
using backref_entry_refs_t = std::vector<backref_entry_ref>;
using backref_entryrefs_by_seq_t = std::map<journal_seq_t, backref_entry_refs_t>;
using backref_entryrefs_by_trans_id_t = std::map<
  transaction_id_t, backref_entry_refs_t>;
using backref_entry_query_set_t = std::set<
    backref_entry_t, backref_entry_t::cmp_t>;

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
class Cache {
public:
  using base_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using base_iertr = trans_iertr<base_ertr>;

  Cache(ExtentPlacementManager &epm);
  ~Cache();

  cache_stats_t get_stats(bool report_detail, double seconds) const;

  /// Creates empty transaction by source
  TransactionRef create_transaction(
      Transaction::src_t src,
      const char* name,
      bool is_weak) {
    LOG_PREFIX(Cache::create_transaction);

    ++(get_by_src(stats.trans_created_by_src, src));

    auto ret = std::make_unique<Transaction>(
      get_dummy_ordering_handle(),
      is_weak,
      src,
      last_commit,
      [this](Transaction& t) {
        return on_transaction_destruct(t);
      },
      ++next_id
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
    t.reset_preserve_handle(last_commit);
  }

  /// Declare ref retired in t
  void retire_extent(Transaction &t, CachedExtentRef ref) {
    LOG_PREFIX(Cache::retire_extent);
    SUBDEBUGT(seastore_cache, "retire extent -- {}", t, *ref);
    t.add_to_retired_set(ref);
  }

  /// Declare paddr retired in t
  using retire_extent_iertr = base_iertr;
  using retire_extent_ret = base_iertr::future<>;
  retire_extent_ret retire_extent_addr(
    Transaction &t, paddr_t addr, extent_len_t length);

  void retire_absent_extent_addr(
    Transaction &t, paddr_t addr, extent_len_t length);

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

  void account_absent_access(Transaction::src_t src) {
    ++(get_by_src(stats.cache_absent_by_src, src));
    ++stats.access.cache_absent;
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
    paddr_t offset,
    extent_types_t type) {
    CachedExtentRef ret;
    LOG_PREFIX(Cache::get_extent_if_cached);
    auto result = t.get_extent(offset, &ret);
    const auto t_src = t.get_src();
    extent_access_stats_t& access_stats = get_by_ext(
      get_by_src(stats.access_by_src_ext, t_src),
      type);
    if (result == Transaction::get_extent_ret::RETIRED) {
      SUBDEBUGT(seastore_cache, "{} {} is retired on t -- {}",
                t, type, offset, *ret);
      return get_extent_if_cached_iertr::make_ready_future<
        CachedExtentRef>(ret);
    } else if (result == Transaction::get_extent_ret::PRESENT) {
      if (ret->is_stable()) {
        if (ret->is_dirty()) {
          ++access_stats.trans_dirty;
          ++stats.access.s.trans_dirty;
        } else {
          ++access_stats.trans_lru;
          ++stats.access.s.trans_lru;
        }
      } else {
        ++access_stats.trans_pending;
        ++stats.access.s.trans_pending;
      }

      if (ret->is_fully_loaded()) {
        SUBTRACET(seastore_cache, "{} {} is present on t -- {}",
                  t, type, offset, *ret);
        return ret->wait_io().then([ret] {
	  return get_extent_if_cached_iertr::make_ready_future<
	    CachedExtentRef>(ret);
        });
      } else {
        SUBDEBUGT(seastore_cache, "{} {} is present on t -- {}"
                  " without being fully loaded", t, type, offset, *ret);
        return get_extent_if_cached_iertr::make_ready_future<
          CachedExtentRef>();
      }
    }

    // get_extent_ret::ABSENT from transaction
    ret = query_cache(offset);
    if (!ret) {
      SUBDEBUGT(seastore_cache, "{} {} is absent", t, type, offset);
      account_absent_access(t_src);
      return get_extent_if_cached_iertr::make_ready_future<CachedExtentRef>();
    } else if (is_retired_placeholder_type(ret->get_type())) {
      // retired_placeholder is not really cached yet
      SUBDEBUGT(seastore_cache, "{} {} is absent(placeholder)",
                t, type, offset);
      account_absent_access(t_src);
      return get_extent_if_cached_iertr::make_ready_future<CachedExtentRef>();
    }

    if (ret->is_dirty()) {
      ++access_stats.cache_dirty;
      ++stats.access.s.cache_dirty;
    } else {
      ++access_stats.cache_lru;
      ++stats.access.s.cache_lru;
    }

    if (!ret->is_fully_loaded()) {
      // ignore non-full extent
      SUBDEBUGT(seastore_cache, "{} {} is present without "
                "being fully loaded", t, type, offset);
      return get_extent_if_cached_iertr::make_ready_future<CachedExtentRef>();
    }

    // present in cache(fully loaded) and is not a retired_placeholder
    SUBDEBUGT(seastore_cache, "{} {} is present in cache -- {}",
              t, type, offset, *ret);
    t.add_to_read_set(ret);
    touch_extent(*ret, &t_src);
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
    auto result = t.get_extent(offset, &ret);
    if (result == Transaction::get_extent_ret::RETIRED) {
      SUBERRORT(seastore_cache, "{} {}~{} is retired on t -- {}",
                t, T::TYPE, offset, length, *ret);
      ceph_abort("impossible");
    } else if (result == Transaction::get_extent_ret::PRESENT) {
      if (ret->is_fully_loaded()) {
        SUBTRACET(seastore_cache, "{} {}~{} is present on t -- {}",
                  t, T::TYPE, offset, length, *ret);
        return ret->wait_io().then([ret] {
	  return seastar::make_ready_future<TCachedExtentRef<T>>(
            ret->cast<T>());
        });
      } else {
	assert(!ret->is_mutable());
        SUBDEBUGT(seastore_cache, "{} {}~{} is present on t without been \
          fully loaded, reading ... {}", t, T::TYPE, offset, length, *ret);
        auto bp = alloc_cache_buf(ret->get_length());
        ret->set_bptr(std::move(bp));
        return read_extent<T>(
          ret->cast<T>());
      }
    } else {
      SUBTRACET(seastore_cache, "{} {}~{} is absent on t, query cache ...",
                t, T::TYPE, offset, length);
      auto f = [&t, this](CachedExtent &ext) {
        t.add_to_read_set(CachedExtentRef(&ext));
        const auto t_src = t.get_src();
        touch_extent(ext, &t_src);
      };
      return trans_intr::make_interruptible(
        do_get_caching_extent<T>(
          offset, length, [](T &){}, std::move(f))
      );
    }
  }

  /*
   * get_absent_extent
   *
   * The extent in query is supposed to be absent in Cache.
   */
  template <typename T, typename Func>
  get_extent_iertr::future<TCachedExtentRef<T>> get_absent_extent(
    Transaction &t,
    paddr_t offset,
    extent_len_t length,
    Func &&extent_init_func) {
    CachedExtentRef ret;
    LOG_PREFIX(Cache::get_absent_extent);

#ifndef NDEBUG
    auto r = t.get_extent(offset, &ret);
    if (r != Transaction::get_extent_ret::ABSENT) {
      SUBERRORT(seastore_cache, "unexpected non-absent extent {}", t, *ret);
      ceph_abort();
    }
#endif

    SUBTRACET(seastore_cache, "{} {}~{} is absent on t, query cache ...",
	      t, T::TYPE, offset, length);
    auto f = [&t, this](CachedExtent &ext) {
      // FIXME: assert(ext.is_stable_clean());
      assert(ext.is_stable());
      assert(T::TYPE == ext.get_type());
      const auto t_src = t.get_src();
      extent_access_stats_t& access_stats = get_by_ext(
        get_by_src(stats.access_by_src_ext, t_src),
        T::TYPE);
      ++access_stats.load_absent;
      ++stats.access.s.load_absent;

      t.add_to_read_set(CachedExtentRef(&ext));
      touch_extent(ext, &t_src);
    };
    return trans_intr::make_interruptible(
      do_get_caching_extent<T>(
	offset, length, std::forward<Func>(extent_init_func), std::move(f))
    );
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

  bool is_viewable_extent_stable(
    Transaction &t,
    CachedExtentRef extent)
  {
    assert(extent);
    auto view = extent->get_transactional_view(t);
    return view->is_stable();
  }

  bool is_viewable_extent_data_stable(
    Transaction &t,
    CachedExtentRef extent)
  {
    assert(extent);
    auto view = extent->get_transactional_view(t);
    return view->is_data_stable();
  }

  using get_extent_ertr = base_ertr;
  get_extent_ertr::future<CachedExtentRef>
  get_extent_viewable_by_trans(
    Transaction &t,
    CachedExtentRef extent)
  {
    assert(extent->is_valid());

    const auto t_src = t.get_src();
    auto ext_type = extent->get_type();
    extent_access_stats_t& access_stats = get_by_ext(
      get_by_src(stats.access_by_src_ext, t_src),
      ext_type);

    CachedExtent* p_extent;
    if (extent->is_stable()) {
      p_extent = extent->get_transactional_view(t);
      if (p_extent != extent.get()) {
        assert(!extent->is_stable_writting());
        assert(p_extent->is_pending_in_trans(t.get_trans_id()));
        assert(!p_extent->is_stable_writting());
        ++access_stats.trans_pending;
        ++stats.access.s.trans_pending;
        if (p_extent->is_mutable()) {
          assert(p_extent->is_fully_loaded());
          assert(!p_extent->is_pending_io());
          return get_extent_ertr::make_ready_future<CachedExtentRef>(
            CachedExtentRef(p_extent));
        } else {
          assert(p_extent->is_exist_clean());
        }
      } else {
        // stable from trans-view
        assert(!p_extent->is_pending_in_trans(t.get_trans_id()));
        if (t.maybe_add_to_read_set(p_extent)) {
          if (p_extent->is_dirty()) {
            ++access_stats.cache_dirty;
            ++stats.access.s.cache_dirty;
          } else {
            ++access_stats.cache_lru;
            ++stats.access.s.cache_lru;
          }
          touch_extent(*p_extent, &t_src);
        } else {
          if (p_extent->is_dirty()) {
            ++access_stats.trans_dirty;
            ++stats.access.s.trans_dirty;
          } else {
            ++access_stats.trans_lru;
            ++stats.access.s.trans_lru;
          }
        }
      }
    } else {
      assert(!extent->is_stable_writting());
      assert(extent->is_pending_in_trans(t.get_trans_id()));
      ++access_stats.trans_pending;
      ++stats.access.s.trans_pending;
      if (extent->is_mutable()) {
        assert(extent->is_fully_loaded());
        assert(!extent->is_pending_io());
        return get_extent_ertr::make_ready_future<CachedExtentRef>(extent);
      } else {
        assert(extent->is_exist_clean());
        p_extent = extent.get();
      }
    }

    // user should not see RETIRED_PLACEHOLDER extents
    ceph_assert(!is_retired_placeholder_type(p_extent->get_type()));
    if (!p_extent->is_fully_loaded()) {
      assert(!p_extent->is_mutable());
      ++access_stats.load_present;
      ++stats.access.s.load_present;
      LOG_PREFIX(Cache::get_extent_viewable_by_trans);
      SUBDEBUG(seastore_cache,
        "{} {}~{} is present without been fully loaded, reading ... -- {}",
        p_extent->get_type(), p_extent->get_paddr(),p_extent->get_length(),
        *p_extent);
      auto bp = alloc_cache_buf(p_extent->get_length());
      p_extent->set_bptr(std::move(bp));
      return read_extent<CachedExtent>(CachedExtentRef(p_extent));
    }
    return p_extent->wait_io(
    ).then([p_extent] {
      return get_extent_ertr::make_ready_future<CachedExtentRef>(
        CachedExtentRef(p_extent));
    });
  }

  template <typename T>
  using read_extent_ret = get_extent_ertr::future<TCachedExtentRef<T>>;

  template <typename T>
  read_extent_ret<T> get_extent_viewable_by_trans(
    Transaction &t,
    TCachedExtentRef<T> extent)
  {
    return get_extent_viewable_by_trans(t, CachedExtentRef(extent.get())
    ).safe_then([](auto p_extent) {
      return p_extent->template cast<T>();
    });
  }

  extent_len_t get_block_size() const {
    return epm.get_block_size();
  }

// Interfaces only for tests.
public:
  CachedExtentRef test_query_cache(paddr_t offset) {
    return query_cache(offset);
  }

private:
  /**
   * do_get_caching_extent
   *
   * returns ref to extent at offset~length of type T either from
   * - extent_set if already in cache
   * - disk
   */
  using src_ext_t = std::pair<Transaction::src_t, extent_types_t>;
  template <typename T, typename Func, typename OnCache>
  read_extent_ret<T> do_get_caching_extent(
    paddr_t offset,                ///< [in] starting addr
    extent_len_t length,           ///< [in] length
    Func &&extent_init_func,       ///< [in] init func for extent
    OnCache &&on_cache
  ) {
    LOG_PREFIX(Cache::do_get_caching_extent);
    auto cached = query_cache(offset);
    if (!cached) {
      auto ret = CachedExtent::make_cached_extent_ref<T>(
        alloc_cache_buf(length));
      ret->init(CachedExtent::extent_state_t::CLEAN_PENDING,
                offset,
                PLACEMENT_HINT_NULL,
                NULL_GENERATION,
		TRANS_ID_NULL);
      SUBDEBUG(seastore_cache,
          "{} {}~{} is absent, add extent and reading ... -- {}",
          T::TYPE, offset, length, *ret);
      add_extent(ret);
      // touch_extent() should be included in on_cache
      on_cache(*ret);
      extent_init_func(*ret);
      return read_extent<T>(
	std::move(ret));
    }

    // extent PRESENT in cache
    if (is_retired_placeholder_type(cached->get_type())) {
      auto ret = CachedExtent::make_cached_extent_ref<T>(
        alloc_cache_buf(length));
      ret->init(CachedExtent::extent_state_t::CLEAN_PENDING,
                offset,
                PLACEMENT_HINT_NULL,
                NULL_GENERATION,
		TRANS_ID_NULL);
      SUBDEBUG(seastore_cache,
          "{} {}~{} is absent(placeholder), reading ... -- {}",
          T::TYPE, offset, length, *ret);
      extents_index.replace(*ret, *cached);
      on_cache(*ret);

      // replace placeholder in transactions
      while (!cached->transactions.empty()) {
        auto t = cached->transactions.begin()->t;
        t->replace_placeholder(*cached, *ret);
      }

      cached->state = CachedExtent::extent_state_t::INVALID;
      extent_init_func(*ret);
      return read_extent<T>(
	std::move(ret));
    } else if (!cached->is_fully_loaded()) {
      auto ret = TCachedExtentRef<T>(static_cast<T*>(cached.get()));
      on_cache(*ret);
      SUBDEBUG(seastore_cache,
        "{} {}~{} is present without been fully loaded, reading ... -- {}",
        T::TYPE, offset, length, *ret);
      auto bp = alloc_cache_buf(length);
      ret->set_bptr(std::move(bp));
      return read_extent<T>(
        std::move(ret));
    } else {
      SUBTRACE(seastore_cache,
          "{} {}~{} is present in cache -- {}",
          T::TYPE, offset, length, *cached);
      auto ret = TCachedExtentRef<T>(static_cast<T*>(cached.get()));
      on_cache(*ret);
      return ret->wait_io(
      ).then([ret=std::move(ret)]() mutable
	     -> read_extent_ret<T> {
        // ret may be invalid, caller must check
        return read_extent_ret<T>(
          get_extent_ertr::ready_future_marker{},
          std::move(ret));
      });
    }
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
    extent_init_func_t &&on_cache
  );

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
    CachedExtentRef ret;
    auto status = t.get_extent(offset, &ret);
    if (status == Transaction::get_extent_ret::RETIRED) {
      SUBERRORT(seastore_cache, "{} {}~{} {} is retired on t -- {}",
                t, type, offset, length, laddr, *ret);
      ceph_abort("impossible");
    } else if (status == Transaction::get_extent_ret::PRESENT) {
      if (ret->is_fully_loaded()) {
        SUBTRACET(seastore_cache, "{} {}~{} {} is present on t -- {}",
                  t, type, offset, length, laddr, *ret);
        return ret->wait_io().then([ret] {
	  return seastar::make_ready_future<CachedExtentRef>(ret);
        });
      } else {
	assert(!ret->is_mutable());
        SUBDEBUGT(seastore_cache, "{} {}~{} {} is present on t without been \
                  fully loaded, reading ...", t, type, offset, length, laddr);
        auto bp = alloc_cache_buf(ret->get_length());
        ret->set_bptr(std::move(bp));
        return read_extent<CachedExtent>(
          std::move(ret));
      }
    } else {
      SUBTRACET(seastore_cache, "{} {}~{} {} is absent on t, query cache ...",
                t, type, offset, length, laddr);
      auto f = [&t, this](CachedExtent &ext) {
	t.add_to_read_set(CachedExtentRef(&ext));
	const auto t_src = t.get_src();
	touch_extent(ext, &t_src);
      };
      return trans_intr::make_interruptible(
	do_get_caching_extent_by_type(
	  type, offset, laddr, length,
	  std::move(extent_init_func), std::move(f))
      );
    }
  }

  get_extent_by_type_ret _get_absent_extent_by_type(
    Transaction &t,
    extent_types_t type,
    paddr_t offset,
    laddr_t laddr,
    extent_len_t length,
    extent_init_func_t &&extent_init_func
  ) {
    LOG_PREFIX(Cache::_get_absent_extent_by_type);

#ifndef NDEBUG
    CachedExtentRef ret;
    auto r = t.get_extent(offset, &ret);
    if (r != Transaction::get_extent_ret::ABSENT) {
      SUBERRORT(seastore_cache, "unexpected non-absent extent {}", t, *ret);
      ceph_abort();
    }
#endif

    SUBTRACET(seastore_cache, "{} {}~{} {} is absent on t, query cache ...",
	      t, type, offset, length, laddr);
    auto f = [&t, this](CachedExtent &ext) {
      // FIXME: assert(ext.is_stable_clean());
      assert(ext.is_stable());
      const auto t_src = t.get_src();
      extent_access_stats_t& access_stats = get_by_ext(
        get_by_src(stats.access_by_src_ext, t_src),
        ext.get_type());
      ++access_stats.load_absent;
      ++stats.access.s.load_absent;

      t.add_to_read_set(CachedExtentRef(&ext));
      touch_extent(ext, &t_src);
    };
    return trans_intr::make_interruptible(
      do_get_caching_extent_by_type(
	type, offset, laddr, length,
	std::move(extent_init_func), std::move(f))
    );
  }

  // the backref entries indexed by trans_id only store the reampped
  // entries to ensure the gc transaction could retrieve the latest
  // changes. These entries are merged into backref_entryrefs_by_seq
  // in Cache::complete_commit
  backref_entryrefs_by_trans_id_t backref_entryrefs_by_trans_id;
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
      res.emplace(it->paddr, it->laddr, it->len, it->type, it->seq);
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
    SUBTRACET(seastore_cache, "allocate {} {}B, hint={}, gen={}",
              t, T::TYPE, length, hint, rewrite_gen_printer_t{gen});
#ifdef UNIT_TESTS_BUILT
    auto result = epm.alloc_new_non_data_extent(t, T::TYPE, length, hint, gen, epaddr);
#else
    auto result = epm.alloc_new_non_data_extent(t, T::TYPE, length, hint, gen);
#endif
    if (!result) {
      return nullptr;
    }
    auto ret = CachedExtent::make_cached_extent_ref<T>(std::move(result->bp));
    ret->init(CachedExtent::extent_state_t::INITIAL_WRITE_PENDING,
              result->paddr,
              hint,
              result->gen,
	      t.get_trans_id());
    t.add_fresh_extent(ret);
    SUBDEBUGT(seastore_cache,
              "allocated {} {}B extent at {}, hint={}, gen={} -- {}",
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
    SUBTRACET(seastore_cache, "allocate {} {}B, hint={}, gen={}",
              t, T::TYPE, length, hint, rewrite_gen_printer_t{gen});
#ifdef UNIT_TESTS_BUILT
    auto results = epm.alloc_new_data_extents(t, T::TYPE, length, hint, gen, epaddr);
#else
    auto results = epm.alloc_new_data_extents(t, T::TYPE, length, hint, gen);
#endif
    std::vector<TCachedExtentRef<T>> extents;
    for (auto &result : results) {
      auto ret = CachedExtent::make_cached_extent_ref<T>(std::move(result.bp));
      ret->init(CachedExtent::extent_state_t::INITIAL_WRITE_PENDING,
                result.paddr,
                hint,
                result.gen,
                t.get_trans_id());
      t.add_fresh_extent(ret);
      SUBDEBUGT(seastore_cache,
                "allocated {} {}B extent at {}, hint={}, gen={} -- {}",
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
    extent_len_t remap_length,
    laddr_t original_laddr,
    std::optional<ceph::bufferptr> &original_bptr) {
    LOG_PREFIX(Cache::alloc_remapped_extent);
    assert(remap_laddr >= original_laddr);
    TCachedExtentRef<T> ext;
    if (original_bptr.has_value()) {
      // shallow copy the buffer from original extent
      auto remap_offset = remap_laddr.get_byte_distance<
	extent_len_t>(original_laddr);
      auto nbp = ceph::bufferptr(*original_bptr, remap_offset, remap_length);
      // ExtentPlacementManager::alloc_new_extent will make a new
      // (relative/temp) paddr, so make extent directly
      ext = CachedExtent::make_cached_extent_ref<T>(std::move(nbp));
    } else {
      ext = CachedExtent::make_placeholder_cached_extent_ref<T>(remap_length);
    }

    ext->init(CachedExtent::extent_state_t::EXIST_CLEAN,
	      remap_paddr,
	      PLACEMENT_HINT_NULL,
	      NULL_GENERATION,
              t.get_trans_id());

    auto extent = ext->template cast<T>();
    extent->set_laddr(remap_laddr);
    t.add_fresh_extent(ext);
    SUBTRACET(seastore_cache, "allocated {} {}B, hint={}, has ptr? {} -- {}",
      t, T::TYPE, remap_length, remap_laddr, original_bptr.has_value(), *extent);
    return extent;
  }

  /**
   * alloc_new_extent
   *
   * Allocates a fresh extent.  addr will be relative until commit.
   */
  CachedExtentRef alloc_new_extent_by_type(
    Transaction &t,        ///< [in, out] current transaction
    extent_types_t type,   ///< [in] type tag
    extent_len_t length,   ///< [in] length
    placement_hint_t hint, ///< [in] user hint
    rewrite_gen_t gen      ///< [in] rewrite generation
    );

  /**
   * alloc_new_extent
   *
   * Allocates a fresh extent.  addr will be relative until commit.
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
        "start with {}({}B) extents, {} dirty, dirty_from={}, alloc_from={}",
        t,
        extents_index.size(),
        extents_index.get_bytes(),
        dirty.size(),
        get_oldest_dirty_from().value_or(JOURNAL_SEQ_NULL),
        get_oldest_backref_dirty_from().value_or(JOURNAL_SEQ_NULL));

    // journal replay should has been finished at this point,
    // Cache::root should have been inserted to the dirty list
    assert(root->is_dirty());
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
          "finish with {}({}B) extents, {} dirty, dirty_from={}, alloc_from={}",
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
      auto result = t.get_extent(extent->get_paddr(), &extent);
      if (result == Transaction::get_extent_ret::RETIRED) {
	return CachedExtentRef();
      } else {
	if (result == Transaction::get_extent_ret::ABSENT) {
	  t.add_to_read_set(extent);
	}
	return extent;
      }
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

private:
  /// Update lru for access to ref
  void touch_extent(
      CachedExtent &ext,
      const Transaction::src_t* p_src)
  {
    if (p_src &&
	is_background_transaction(*p_src) &&
	is_logical_type(ext.get_type())) {
      return;
    }
    if (ext.is_stable_clean() && !ext.is_placeholder()) {
      lru.move_to_top(ext, p_src);
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
    assert(!paddr.is_relative());
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

  /**
   * lru
   *
   * holds references to recently used extents
   */
  class LRU {
    // max size (bytes)
    const size_t capacity = 0;

    // current size (bytes)
    size_t current_size = 0;

    counter_by_extent_t<cache_size_stats_t> sizes_by_ext;
    cache_io_stats_t overall_io;
    counter_by_src_t<counter_by_extent_t<cache_io_stats_t> >
      trans_io_by_src_ext;

    mutable cache_io_stats_t last_overall_io;
    mutable cache_io_stats_t last_trans_io;
    mutable counter_by_src_t<counter_by_extent_t<cache_io_stats_t> >
      last_trans_io_by_src_ext;

    CachedExtent::primary_ref_list lru;

    void do_remove_from_lru(
        CachedExtent &extent,
        const Transaction::src_t* p_src) {
      assert(extent.is_stable_clean() && !extent.is_placeholder());
      assert(extent.primary_ref_list_hook.is_linked());
      assert(lru.size() > 0);
      auto extent_length = extent.get_length();
      assert(current_size >= extent_length);

      lru.erase(lru.s_iterator_to(extent));
      current_size -= extent_length;
      get_by_ext(sizes_by_ext, extent.get_type()).account_out(extent_length);
      overall_io.out_sizes.account_in(extent_length);
      if (p_src) {
        get_by_ext(
          get_by_src(trans_io_by_src_ext, *p_src),
          extent.get_type()
        ).out_sizes.account_in(extent_length);
      }
      intrusive_ptr_release(&extent);
    }

  public:
    LRU(size_t capacity) : capacity(capacity) {}

    size_t get_capacity_bytes() const {
      return capacity;
    }

    size_t get_current_size_bytes() const {
      return current_size;
    }

    size_t get_current_num_extents() const {
      return lru.size();
    }

    void get_stats(
        cache_stats_t &stats,
        bool report_detail,
        double seconds) const;

    void remove_from_lru(CachedExtent &extent) {
      assert(extent.is_stable_clean() && !extent.is_placeholder());

      if (extent.primary_ref_list_hook.is_linked()) {
        do_remove_from_lru(extent, nullptr);
      }
    }

    void move_to_top(
        CachedExtent &extent,
        const Transaction::src_t* p_src) {
      assert(extent.is_stable_clean() && !extent.is_placeholder());

      auto extent_length = extent.get_length();
      if (extent.primary_ref_list_hook.is_linked()) {
        // present, move to top (back)
        assert(lru.size() > 0);
        assert(current_size >= extent_length);
        lru.erase(lru.s_iterator_to(extent));
        lru.push_back(extent);
      } else {
        // absent, add to top (back)
        current_size += extent_length;
        get_by_ext(sizes_by_ext, extent.get_type()).account_in(extent_length);
        overall_io.in_sizes.account_in(extent_length);
        if (p_src) {
          get_by_ext(
            get_by_src(trans_io_by_src_ext, *p_src),
            extent.get_type()
          ).in_sizes.account_in(extent_length);
        }
        intrusive_ptr_add_ref(&extent);
        lru.push_back(extent);

        // trim to capacity
        while (current_size > capacity) {
          do_remove_from_lru(lru.front(), p_src);
        }
      }
    }

    void clear() {
      LOG_PREFIX(Cache::LRU::clear);
      for (auto iter = lru.begin(); iter != lru.end();) {
	SUBDEBUG(seastore_cache, "clearing {}", *iter);
	do_remove_from_lru(*(iter++), nullptr);
      }
    }

    ~LRU() {
      clear();
    }
  } lru;

  struct query_counters_t {
    uint64_t access = 0;
    uint64_t hit = 0;
  };

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
    counter_by_src_t<counter_by_extent_t<extent_access_stats_t> >
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
  mutable counter_by_src_t<counter_by_extent_t<extent_access_stats_t> >
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

  /// alloc buffer for cached extent
  bufferptr alloc_cache_buf(size_t size) {
    // TODO: memory pooling etc
    auto bp = ceph::bufferptr(
      buffer::create_page_aligned(size));
    bp.zero();
    return bp;
  }

  void prepare_remapped_backref_entryrefs(
    transaction_id_t trans_id,
    backref_entry_refs_t backref_entries);

  void backref_batch_update(
    transaction_id_t,
    std::vector<backref_entry_ref> &&,
    const journal_seq_t &);

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

  template <typename T>
  read_extent_ret<T> read_extent(
    TCachedExtentRef<T>&& extent
  ) {
    assert(extent->state == CachedExtent::extent_state_t::CLEAN_PENDING ||
      extent->state == CachedExtent::extent_state_t::EXIST_CLEAN ||
      extent->state == CachedExtent::extent_state_t::CLEAN);
    extent->set_io_wait();
    return epm.read(
      extent->get_paddr(),
      extent->get_length(),
      extent->get_bptr()
    ).safe_then(
      [extent=std::move(extent), this]() mutable {
        LOG_PREFIX(Cache::read_extent);
	if (likely(extent->state == CachedExtent::extent_state_t::CLEAN_PENDING)) {
	  extent->state = CachedExtent::extent_state_t::CLEAN;
	}
	ceph_assert(extent->state == CachedExtent::extent_state_t::EXIST_CLEAN
	  || extent->state == CachedExtent::extent_state_t::CLEAN
	  || !extent->is_valid());
	if (extent->is_valid()) {
	  // crc will be checked against LBA leaf entry for logical extents,
	  // or check against in-extent crc for physical extents.
	  if (epm.get_checksum_needed(extent->get_paddr())) {
	    extent->last_committed_crc = extent->calc_crc32c();
	  } else {
	    extent->last_committed_crc = CRC_NULL;
	  }
	  extent->on_clean_read();
	}
        extent->complete_io();
        SUBDEBUG(seastore_cache, "read extent done -- {}", *extent);
        return get_extent_ertr::make_ready_future<TCachedExtentRef<T>>(
          std::move(extent));
      },
      get_extent_ertr::pass_further{},
      crimson::ct_error::assert_all{
        "Cache::read_extent: invalid error"
      }
    );
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
};
using CacheRef = std::unique_ptr<Cache>;

}
