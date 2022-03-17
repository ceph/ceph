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
#include "crimson/os/seastore/segment_cleaner.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/transaction.h"

namespace crimson::os::seastore {

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
 * - Merge Transaction::write_set into Cache::extents
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

  Cache(ExtentReader &reader, ExtentPlacementManager &epm);
  ~Cache();

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
      }
    );
    SUBDEBUGT(seastore_t, "created name={}, source={}, is_weak={}",
             *ret, name, src, is_weak);
    return ret;
  }

  /// Resets transaction preserving
  void reset_transaction_preserve_handle(Transaction &t) {
    LOG_PREFIX(Cache::reset_transaction_preserve_handle);
    if (t.did_reset()) {
      SUBTRACET(seastore_t, "reset", t);
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
   * get_extent
   *
   * returns ref to extent at offset~length of type T either from
   * - extent_set if already in cache
   * - disk
   */
  using src_ext_t = std::pair<Transaction::src_t, extent_types_t>;
  using get_extent_ertr = base_ertr;
  template <typename T>
  using get_extent_ret = get_extent_ertr::future<TCachedExtentRef<T>>;
  template <typename T, typename Func>
  get_extent_ret<T> get_extent(
    paddr_t offset,                ///< [in] starting addr
    seastore_off_t length,          ///< [in] length
    const src_ext_t* p_metric_key, ///< [in] cache query metric key
    Func &&extent_init_func        ///< [in] init func for extent
  ) {
    LOG_PREFIX(Cache::get_extent);
    auto cached = query_cache(offset, p_metric_key);
    if (!cached) {
      auto ret = CachedExtent::make_cached_extent_ref<T>(
        alloc_cache_buf(length));
      ret->set_paddr(offset);
      ret->state = CachedExtent::extent_state_t::CLEAN_PENDING;
      SUBDEBUG(seastore_cache,
          "{} {}~{} is absent, add extent and reading ... -- {}",
          T::TYPE, offset, length, *ret);
      add_extent(ret);
      extent_init_func(*ret);
      return read_extent<T>(
	std::move(ret));
    }

    // extent PRESENT in cache
    if (cached->get_type() == extent_types_t::RETIRED_PLACEHOLDER) {
      auto ret = CachedExtent::make_cached_extent_ref<T>(
        alloc_cache_buf(length));
      ret->set_paddr(offset);
      ret->state = CachedExtent::extent_state_t::CLEAN_PENDING;
      SUBDEBUG(seastore_cache,
          "{} {}~{} is absent(placeholder), reading ... -- {}",
          T::TYPE, offset, length, *ret);
      extents.replace(*ret, *cached);

      // replace placeholder in transactions
      while (!cached->transactions.empty()) {
        auto t = cached->transactions.begin()->t;
        t->replace_placeholder(*cached, *ret);
      }

      cached->state = CachedExtent::extent_state_t::INVALID;
      extent_init_func(*ret);
      return read_extent<T>(
	std::move(ret));
    } else {
      SUBTRACE(seastore_cache,
          "{} {}~{} is present in cache -- {}",
          T::TYPE, offset, length, *cached);
      auto ret = TCachedExtentRef<T>(static_cast<T*>(cached.get()));
      return ret->wait_io(
      ).then([ret=std::move(ret)]() mutable
	     -> get_extent_ret<T> {
        // ret may be invalid, caller must check
        return get_extent_ret<T>(
          get_extent_ertr::ready_future_marker{},
          std::move(ret));
      });
    }
  }
  template <typename T>
  get_extent_ret<T> get_extent(
    paddr_t offset,                ///< [in] starting addr
    seastore_off_t length,          ///< [in] length
    const src_ext_t* p_metric_key  ///< [in] cache query metric key
  ) {
    return get_extent<T>(
      offset, length, p_metric_key,
      [](T &){});
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
    if (result == Transaction::get_extent_ret::RETIRED) {
      SUBDEBUGT(seastore_cache, "{} {} is retired on t -- {}",
          t, type, offset, *ret);
      return get_extent_if_cached_iertr::make_ready_future<
        CachedExtentRef>(ret);
    } else if (result == Transaction::get_extent_ret::PRESENT) {
      SUBTRACET(seastore_cache, "{} {} is present on t -- {}",
          t, type, offset, *ret);
      return get_extent_if_cached_iertr::make_ready_future<
        CachedExtentRef>(ret);
    }

    // get_extent_ret::ABSENT from transaction
    auto metric_key = std::make_pair(t.get_src(), type);
    ret = query_cache(offset, &metric_key);
    if (!ret ||
        // retired_placeholder is not really cached yet
        ret->get_type() == extent_types_t::RETIRED_PLACEHOLDER) {
      SUBDEBUGT(seastore_cache, "{} {} is absent{}",
                t, type, offset, !!ret ? "(placeholder)" : "");
      return get_extent_if_cached_iertr::make_ready_future<
        CachedExtentRef>();
    }

    // present in cache and is not a retired_placeholder
    SUBDEBUGT(seastore_cache, "{} {} is present in cache -- {}",
              t, type, offset, *ret);
    t.add_to_read_set(ret);
    touch_extent(*ret);
    return ret->wait_io().then([ret] {
      return get_extent_if_cached_iertr::make_ready_future<
        CachedExtentRef>(ret);
    });
  }

  /**
   * get_extent
   *
   * returns ref to extent at offset~length of type T either from
   * - t if modified by t
   * - extent_set if already in cache
   * - disk
   *
   * t *must not* have retired offset
   */
  using get_extent_iertr = base_iertr;
  template <typename T, typename Func>
  get_extent_iertr::future<TCachedExtentRef<T>> get_extent(
    Transaction &t,
    paddr_t offset,
    seastore_off_t length,
    Func &&extent_init_func) {
    CachedExtentRef ret;
    LOG_PREFIX(Cache::get_extent);
    auto result = t.get_extent(offset, &ret);
    if (result != Transaction::get_extent_ret::ABSENT) {
      SUBTRACET(seastore_cache, "{} {}~{} is {} on t -- {}",
          t,
          T::TYPE,
          offset,
          length,
          result == Transaction::get_extent_ret::PRESENT ? "present" : "retired",
          *ret);
      assert(result != Transaction::get_extent_ret::RETIRED);
      return seastar::make_ready_future<TCachedExtentRef<T>>(
	ret->cast<T>());
    } else {
      SUBTRACET(seastore_cache, "{} {}~{} is absent on t, query cache ...",
                t, T::TYPE, offset, length);
      auto metric_key = std::make_pair(t.get_src(), T::TYPE);
      return trans_intr::make_interruptible(
	get_extent<T>(
	  offset, length, &metric_key,
	  std::forward<Func>(extent_init_func))
      ).si_then([this, FNAME, offset, length, &t](auto ref) {
	(void)this; // silence incorrect clang warning about capture
	if (!ref->is_valid()) {
	  SUBDEBUGT(seastore_cache, "{} {}~{} is invalid -- {}",
	            t, T::TYPE, offset, length, *ref);
	  ++(get_by_src(stats.trans_conflicts_by_unknown, t.get_src()));
	  mark_transaction_conflicted(t, *ref);
	  return get_extent_iertr::make_ready_future<TCachedExtentRef<T>>();
	} else {
	  touch_extent(*ref);
	  t.add_to_read_set(ref);
	  return get_extent_iertr::make_ready_future<TCachedExtentRef<T>>(
	    std::move(ref));
	}
      });
    }
  }
  template <typename T>
  get_extent_iertr::future<TCachedExtentRef<T>> get_extent(
    Transaction &t,
    paddr_t offset,
    seastore_off_t length) {
    return get_extent<T>(t, offset, length, [](T &){});
  }

private:
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
  get_extent_ertr::future<CachedExtentRef> _get_extent_by_type(
    extent_types_t type,
    paddr_t offset,
    laddr_t laddr,
    seastore_off_t length,
    const Transaction::src_t* p_src,
    extent_init_func_t &&extent_init_func
  );

  using get_extent_by_type_iertr = get_extent_iertr;
  using get_extent_by_type_ret = get_extent_by_type_iertr::future<
    CachedExtentRef>;
  get_extent_by_type_ret _get_extent_by_type(
    Transaction &t,
    extent_types_t type,
    paddr_t offset,
    laddr_t laddr,
    seastore_off_t length,
    extent_init_func_t &&extent_init_func
  ) {
    LOG_PREFIX(Cache::get_extent_by_type);
    CachedExtentRef ret;
    auto status = t.get_extent(offset, &ret);
    if (status == Transaction::get_extent_ret::RETIRED) {
      SUBDEBUGT(seastore_cache, "{} {}~{} {} is retired on t -- {}",
                t, type, offset, length, laddr, *ret);
      return seastar::make_ready_future<CachedExtentRef>();
    } else if (status == Transaction::get_extent_ret::PRESENT) {
      SUBTRACET(seastore_cache, "{} {}~{} {} is present on t -- {}",
                t, type, offset, length, laddr, *ret);
      return seastar::make_ready_future<CachedExtentRef>(ret);
    } else {
      SUBTRACET(seastore_cache, "{} {}~{} {} is absent on t, query cache ...",
                t, type, offset, length, laddr);
      auto src = t.get_src();
      return trans_intr::make_interruptible(
	_get_extent_by_type(
	  type, offset, laddr, length, &src,
	  std::move(extent_init_func))
      ).si_then([=, &t](CachedExtentRef ret) {
        if (!ret->is_valid()) {
          SUBDEBUGT(seastore_cache, "{} {}~{} {} is invalid -- {}",
                    t, type, offset, length, laddr, *ret);
          ++(get_by_src(stats.trans_conflicts_by_unknown, t.get_src()));
          mark_transaction_conflicted(t, *ret.get());
          return get_extent_ertr::make_ready_future<CachedExtentRef>();
        } else {
	  touch_extent(*ret);
          t.add_to_read_set(ret);
          return get_extent_ertr::make_ready_future<CachedExtentRef>(
            std::move(ret));
        }
      });
    }
  }

public:
  /**
   * get_extent_by_type
   *
   * Based on type, instantiate the correct concrete type
   * and read in the extent at location offset~length.
   */
  template <typename Func>
  get_extent_by_type_ret get_extent_by_type(
    Transaction &t,         ///< [in] transaction
    extent_types_t type,    ///< [in] type tag
    paddr_t offset,         ///< [in] starting addr
    laddr_t laddr,          ///< [in] logical address if logical
    seastore_off_t length,   ///< [in] length
    Func &&extent_init_func ///< [in] extent init func
  ) {
    return _get_extent_by_type(
      t,
      type,
      offset,
      laddr,
      length,
      extent_init_func_t(std::forward<Func>(extent_init_func)));
  }
  get_extent_by_type_ret get_extent_by_type(
    Transaction &t,
    extent_types_t type,
    paddr_t offset,
    laddr_t laddr,
    seastore_off_t length
  ) {
    return get_extent_by_type(
      t, type, offset, laddr, length, [](CachedExtent &) {});
  }


  /**
   * alloc_new_extent
   *
   * Allocates a fresh extent. if delayed is true, addr will be alloc'd later
   */
  template <typename T>
  TCachedExtentRef<T> alloc_new_extent(
    Transaction &t,         ///< [in, out] current transaction
    seastore_off_t length,  ///< [in] length
    placement_hint_t hint = placement_hint_t::HOT
  ) {
    LOG_PREFIX(Cache::alloc_new_extent);
    SUBTRACET(seastore_cache, "allocate {} {}B, hint={}",
              t, T::TYPE, length, hint);
    auto result = epm.alloc_new_extent(t, T::TYPE, length, hint);
    auto ret = CachedExtent::make_cached_extent_ref<T>(std::move(result.bp));
    ret->set_paddr(result.paddr);
    ret->hint = hint;
    t.add_fresh_extent(ret);
    ret->state = CachedExtent::extent_state_t::INITIAL_WRITE_PENDING;
    SUBDEBUGT(seastore_cache, "allocated {} {}B extent at {}, hint={} -- {}",
              t, T::TYPE, length, result.paddr, hint, *ret);
    return ret;
  }

  /**
   * alloc_new_extent
   *
   * Allocates a fresh extent.  addr will be relative until commit.
   */
  CachedExtentRef alloc_new_extent_by_type(
    Transaction &t,       ///< [in, out] current transaction
    extent_types_t type,  ///< [in] type tag
    seastore_off_t length, ///< [in] length
    placement_hint_t hint = placement_hint_t::HOT
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
   * prepare_record
   *
   * Construct the record for Journal from transaction.
   */
  record_t prepare_record(
    Transaction &t ///< [in, out] current transaction
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
    journal_seq_t seq,         ///< [in] journal commit seq
    SegmentCleaner *cleaner=nullptr ///< [out] optional segment stat listener
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
   */
  using replay_delta_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using replay_delta_ret = replay_delta_ertr::future<>;
  replay_delta_ret replay_delta(
    journal_seq_t seq,
    paddr_t record_block_base,
    const delta_info_t &delta,
    seastar::lowres_system_clock::time_point& last_modified);

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
        "start with {}({}B) extents, {} dirty, from {}",
        t,
        extents.size(),
        extents.get_bytes(),
        dirty.size(),
        get_oldest_dirty_from().value_or(JOURNAL_SEQ_NULL));

    // journal replay should has been finished at this point,
    // Cache::root should have been inserted to the dirty list
    assert(root->is_dirty());
    std::vector<CachedExtentRef> _dirty;
    for (auto &e : extents) {
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
            remove_extent(e);
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
          "finish with {}({}B) extents, {} dirty, from {}",
          t,
          extents.size(),
          extents.get_bytes(),
          dirty.size(),
          get_oldest_dirty_from().value_or(JOURNAL_SEQ_NULL));
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

  /// returns std::nullopt if no dirty extents or get_dirty_from() for oldest
  std::optional<journal_seq_t> get_oldest_dirty_from() const {
    if (dirty.empty()) {
      return std::nullopt;
    } else {
      auto oldest = dirty.begin()->get_dirty_from();
      if (oldest == JOURNAL_SEQ_NULL) {
	return std::nullopt;
      } else {
	return oldest;
      }
    }
  }

  /// Dump live extents
  void dump_contents();

private:
  ExtentReader &reader;	   	   ///< ref to extent reader
  ExtentPlacementManager& epm;
  RootBlockRef root;               ///< ref to current root
  ExtentIndex extents;             ///< set of live extents

  journal_seq_t last_commit = JOURNAL_SEQ_MIN;

  /**
   * dirty
   *
   * holds refs to dirty extents.  Ordered by CachedExtent::get_dirty_from().
   */
  CachedExtent::list dirty;

  /**
   * lru
   *
   * holds references to recently used extents
   */
  class LRU {
    // max size (bytes)
    const size_t capacity = 0;

    // current size (bytes)
    size_t contents = 0;

    CachedExtent::list lru;

    void trim_to_capacity() {
      while (contents > capacity) {
	assert(lru.size() > 0);
	remove_from_lru(lru.front());
      }
    }

    void add_to_lru(CachedExtent &extent) {
      assert(
	extent.is_clean() &&
	!extent.is_pending() &&
	!extent.is_placeholder());
      
      if (!extent.primary_ref_list_hook.is_linked()) {
	contents += extent.get_length();
	intrusive_ptr_add_ref(&extent);
	lru.push_back(extent);
      }
      trim_to_capacity();
    }

  public:
    LRU(size_t capacity) : capacity(capacity) {}

    size_t get_capacity() const {
      return capacity;
    }

    size_t get_current_contents_bytes() const {
      return contents;
    }

    size_t get_current_contents_extents() const {
      return lru.size();
    }

    void remove_from_lru(CachedExtent &extent) {
      assert(extent.is_clean());
      assert(!extent.is_pending());
      assert(!extent.is_placeholder());

      if (extent.primary_ref_list_hook.is_linked()) {
	lru.erase(lru.s_iterator_to(extent));
	assert(contents >= extent.get_length());
	contents -= extent.get_length();
	intrusive_ptr_release(&extent);
      }
    }

    void move_to_top(CachedExtent &extent) {
      assert(
	extent.is_clean() &&
	!extent.is_pending() &&
	!extent.is_placeholder());

      if (extent.primary_ref_list_hook.is_linked()) {
	lru.erase(lru.s_iterator_to(extent));
	intrusive_ptr_release(&extent);
	assert(contents >= extent.get_length());
	contents -= extent.get_length();
      }
      add_to_lru(extent);
    }

    void clear() {
      LOG_PREFIX(Cache::LRU::clear);
      for (auto iter = lru.begin(); iter != lru.end();) {
	SUBDEBUG(seastore_cache, "clearing {}", *iter);
	remove_from_lru(*(iter++));
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

  template <typename CounterT>
  using counter_by_extent_t = std::array<CounterT, EXTENT_TYPES_MAX>;

  struct invalid_trans_efforts_t {
    io_stat_t read;
    io_stat_t mutate;
    uint64_t mutate_delta_bytes = 0;
    io_stat_t retire;
    io_stat_t fresh;
    io_stat_t fresh_ool_written;
    counter_by_extent_t<uint64_t> num_trans_invalidated;
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

    void increment(const Transaction::tree_stats_t& incremental) {
      num_inserts += incremental.num_inserts;
      num_erases += incremental.num_erases;
    }
  };

  template <typename CounterT>
  using counter_by_src_t = std::array<CounterT, Transaction::SRC_MAX>;

  static constexpr std::size_t NUM_SRC_COMB =
      Transaction::SRC_MAX * (Transaction::SRC_MAX + 1) / 2;

  struct {
    counter_by_src_t<uint64_t> trans_created_by_src;
    counter_by_src_t<commit_trans_efforts_t> committed_efforts_by_src;
    counter_by_src_t<invalid_trans_efforts_t> invalidated_efforts_by_src;
    counter_by_src_t<query_counters_t> cache_query_by_src;
    success_read_trans_efforts_t success_read_efforts;
    uint64_t dirty_bytes = 0;

    uint64_t onode_tree_depth = 0;
    counter_by_src_t<tree_efforts_t> committed_onode_tree_efforts;
    counter_by_src_t<tree_efforts_t> invalidated_onode_tree_efforts;

    uint64_t lba_tree_depth = 0;
    counter_by_src_t<tree_efforts_t> committed_lba_tree_efforts;
    counter_by_src_t<tree_efforts_t> invalidated_lba_tree_efforts;

    std::array<uint64_t, NUM_SRC_COMB> trans_conflicts_by_srcs;
    counter_by_src_t<uint64_t> trans_conflicts_by_unknown;
  } stats;

  template <typename CounterT>
  CounterT& get_by_src(
      counter_by_src_t<CounterT>& counters_by_src,
      Transaction::src_t src) {
    assert(static_cast<std::size_t>(src) < counters_by_src.size());
    return counters_by_src[static_cast<std::size_t>(src)];
  }

  template <typename CounterT>
  CounterT& get_by_ext(
      counter_by_extent_t<CounterT>& counters_by_ext,
      extent_types_t ext) {
    auto index = static_cast<uint8_t>(ext);
    assert(index < EXTENT_TYPES_MAX);
    return counters_by_ext[index];
  }

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
    assert(!(src1 == Transaction::src_t::CLEANER_TRIM &&
             src2 == Transaction::src_t::CLEANER_TRIM));
    assert(!(src1 == Transaction::src_t::CLEANER_RECLAIM &&
             src2 == Transaction::src_t::CLEANER_RECLAIM));
    assert(!(src1 == Transaction::src_t::CLEANER_TRIM &&
             src2 == Transaction::src_t::CLEANER_RECLAIM));

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

  /// Update lru for access to ref
  void touch_extent(CachedExtent &ext) {
    assert(!ext.is_pending());
    if (ext.is_clean() && !ext.is_placeholder()) {
      lru.move_to_top(ext);
    }
  }

  /// Add extent to extents handling dirty and refcounting
  void add_extent(CachedExtentRef ref);

  /// Mark exising extent ref dirty -- mainly for replay
  void mark_dirty(CachedExtentRef ref);

  /// Add dirty extent to dirty list
  void add_to_dirty(CachedExtentRef ref);

  /// Remove from dirty list
  void remove_from_dirty(CachedExtentRef ref);

  /// Remove extent from extents handling dirty and refcounting
  void remove_extent(CachedExtentRef ref);

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
  get_extent_ret<T> read_extent(
    TCachedExtentRef<T>&& extent
  ) {
    assert(extent->state == CachedExtent::extent_state_t::CLEAN_PENDING);
    extent->set_io_wait();
    return reader.read(
      extent->get_paddr(),
      extent->get_length(),
      extent->get_bptr()
    ).safe_then(
      [extent=std::move(extent)]() mutable {
        LOG_PREFIX(Cache::read_extent);
        extent->state = CachedExtent::extent_state_t::CLEAN;
        /* TODO: crc should be checked against LBA manager */
        extent->last_committed_crc = extent->get_crc32c();

        extent->on_clean_read();
        extent->complete_io();
        SUBDEBUG(seastore_cache, "read extent done -- {}", *extent);
        return get_extent_ertr::make_ready_future<TCachedExtentRef<T>>(
          std::move(extent));
      },
      get_extent_ertr::pass_further{},
      crimson::ct_error::assert_all{
        "Cache::get_extent: invalid error"
      }
    );
  }

  // Extents in cache may contain placeholders
  CachedExtentRef query_cache(
      paddr_t offset,
      const src_ext_t* p_metric_key) {
    query_counters_t* p_counters = nullptr;
    if (p_metric_key) {
      p_counters = &get_by_src(stats.cache_query_by_src, p_metric_key->first);
      ++p_counters->access;
    }
    if (auto iter = extents.find_offset(offset);
        iter != extents.end()) {
      if (p_metric_key &&
          // retired_placeholder is not really cached yet
          iter->get_type() != extent_types_t::RETIRED_PLACEHOLDER) {
        ++p_counters->hit;
      }
      return CachedExtentRef(&*iter);
    } else {
      return CachedExtentRef();
    }
  }

};
using CacheRef = std::unique_ptr<Cache>;

}
