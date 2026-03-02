// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <iostream>
#include <optional>
#include <vector>
#include <utility>
#include <functional>

#include <boost/intrusive_ptr.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <seastar/core/future.hh>
#include <seastar/util/defer.hh>

#include "include/ceph_assert.h"
#include "include/buffer.h"

#include "crimson/common/coroutine.h"

#include "crimson/osd/exceptions.h"

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/root_meta.h"
#include "crimson/os/seastore/lba_manager.h"
#include "crimson/os/seastore/backref_manager.h"
#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/extent_placement_manager.h"
#include "crimson/os/seastore/device.h"

class transaction_manager_test_t;
class object_data_handler_test_t;

namespace crimson::os::seastore {
class Journal;

template <typename F>
auto repeat_eagain(F &&f) {
  return seastar::do_with(
    std::forward<F>(f),
    [](auto &f)
  {
    return crimson::repeat([&f] {
      return std::invoke(f
      ).safe_then([] {
        return seastar::stop_iteration::yes;
      }).handle_error(
        [](const crimson::ct_error::eagain &e) {
          return seastar::stop_iteration::no;
        },
        crimson::ct_error::pass_further_all{}
      );
    });
  });
}

/**
 * TransactionManager
 *
 * Abstraction hiding reading and writing to persistence.
 * Exposes transaction based interface with read isolation.
 */
class TransactionManager : public ExtentCallbackInterface {
public:
  TransactionManager(
    JournalRef journal,
    CacheRef cache,
    LBAManagerRef lba_manager,
    ExtentPlacementManagerRef &&epm,
    BackrefManagerRef&& backref_manager,
    shard_stats_t& shard_stats);

  /// Writes initial metadata to disk
  using mkfs_ertr = base_ertr::extend<
    crimson::ct_error::enospc>;
  mkfs_ertr::future<> mkfs();

  /// Reads initial metadata from disk
  using mount_ertr = base_ertr::extend<
    crimson::ct_error::enospc>;
  mount_ertr::future<> mount();

  /// Closes transaction_manager
  using close_ertr = base_ertr;
  close_ertr::future<> close();

  device_stats_t get_device_stats(
      bool report_detail, double seconds) const {
    writer_stats_t journal_stats = journal->get_writer_stats();
    return epm->get_device_stats(journal_stats, report_detail, seconds);
  }

  cache_stats_t get_cache_stats(bool report_detail, double seconds) const {
    return cache->get_stats(report_detail, seconds);
  }

  /// Resets transaction
  void reset_transaction_preserve_handle(Transaction &t) {
    return cache->reset_transaction_preserve_handle(t);
  }

  /**
   * get_pin
   *
   * Get the logical pin at offset
   */
  using get_pin_iertr = LBAManager::get_mapping_iertr;
  using get_pin_ret = LBAManager::get_mapping_iertr::future<LBAMapping>;
  get_pin_ret get_pin(
    Transaction &t,
    laddr_t offset) {
    LOG_PREFIX(TransactionManager::get_pin);
    SUBDEBUGT(seastore_tm, "{} ...", t, offset);
    return lba_manager->get_mapping(t, offset, false
    ).si_then([FNAME, &t](LBAMapping pin) {
      SUBDEBUGT(seastore_tm, "got {}", t, pin);
      return pin;
    });
  }

  /**
   * get_containing_pin
   *
   * Get the logical pin containing laddr
   */
  get_pin_ret get_containing_pin(
    Transaction &t,
    laddr_t laddr) {
    LOG_PREFIX(TransactionManager::get_containing_pin);
    SUBDEBUGT(seastore_tm, "{} ...", t, laddr);
    return lba_manager->get_mapping(t, laddr, true
    ).si_then([FNAME, &t](LBAMapping pin) {
      SUBDEBUGT(seastore_tm, "got {}", t, pin);
      return pin;
    });
  }

  get_pin_ret get_pin(Transaction &t, LogicalChildNode &extent) {
    LOG_PREFIX(TransactionManager::get_pin);
    SUBDEBUGT(seastore_tm, "{} ...", t, extent);
    return lba_manager->get_mapping(t, extent
    ).si_then([FNAME, &t](LBAMapping pin) {
      SUBDEBUGT(seastore_tm, "got {}", t, pin);
      return pin;
    });
  }

  /**
   * get_pins
   *
   * Get logical pins overlapping offset~length
   */
  using get_pins_iertr = LBAManager::get_mappings_iertr;
  using get_pins_ret = get_pins_iertr::future<lba_mapping_list_t>;
  get_pins_ret get_pins(
    Transaction &t,
    laddr_t offset,
    extent_len_t length) {
    LOG_PREFIX(TransactionManager::get_pins);
    SUBDEBUGT(seastore_tm, "{}~0x{:x} ...", t, offset, length);
    return lba_manager->get_mappings(
      t, offset, length
    ).si_then([FNAME, &t](lba_mapping_list_t pins) {
      SUBDEBUGT(seastore_tm, "got {} pins", t, pins.size());
      return pins;
    });
  }

  /**
   * maybe_indirect_extent_t
   *
   * Contains necessary information in case the extent is loaded from an
   * indirect pin.
   */
  struct indirect_info_t {
    extent_len_t intermediate_offset = 0;
    extent_len_t length = 0;
  };
  template <typename T>
  struct maybe_indirect_extent_t {
    TCachedExtentRef<T> extent;
    std::optional<indirect_info_t> maybe_indirect_info;
    bool is_clone = false;

    bool is_indirect() const {
      return maybe_indirect_info.has_value();
    }

    ceph::bufferlist get_bl() const {
      if (is_indirect()) {
        return do_get_indirect_range(0, maybe_indirect_info->length);
      } else {
        assert(extent->is_fully_loaded());
        bufferlist bl;
        bl.append(extent->get_bptr());
        return bl;
      }
    }

    ceph::bufferlist get_range(
        extent_len_t offset, extent_len_t length) const {
      if (is_indirect()) {
        return do_get_indirect_range(offset, length);
      } else {
        return extent->get_range(offset, length);
      }
    }
  private:
    ceph::bufferlist do_get_indirect_range(
        extent_len_t offset, extent_len_t length) const {
      assert(is_indirect());
      assert(maybe_indirect_info->intermediate_offset + offset + length <=
             extent->get_length());
      assert(offset + length <= maybe_indirect_info->length);
      return extent->get_range(
          maybe_indirect_info->intermediate_offset + offset,
          length);
    }
  };

  template <typename T>
  using lextent_init_func_t = std::function<void (T&)>;
  /**
   * read_extent
   *
   * Read extent of type T at offset~length
   */
  using read_extent_iertr = get_pin_iertr;
  template <typename T>
  using read_extent_ret =
    read_extent_iertr::future<maybe_indirect_extent_t<T>>;
  template <typename T>
  read_extent_ret<T> read_extent(
    Transaction &t,
    laddr_t offset,
    extent_len_t length,
    lextent_init_func_t<T> maybe_init = [](T&) {}) {
    LOG_PREFIX(TransactionManager::read_extent);
    SUBDEBUGT(seastore_tm, "{}~0x{:x} {} ...",
              t, offset, length, T::TYPE);
    return get_pin(
      t, offset
    ).si_then([this, FNAME, &t, offset, length,
	      maybe_init=std::move(maybe_init)] (auto pin) mutable
      -> read_extent_ret<T> {
      if (length != pin.get_length() || !pin.get_val().is_real_location()) {
        SUBERRORT(seastore_tm, "{}~0x{:x} {} got wrong pin {}",
                  t, offset, length, T::TYPE, pin);
        ceph_abort_msg("Impossible");
      }
      return this->read_pin<T>(t, std::move(pin), std::move(maybe_init));
    });
  }

  /**
   * read_extent
   *
   * Read extent of type T at offset
   */
  template <typename T>
  read_extent_ret<T> read_extent(
    Transaction &t,
    laddr_t offset,
    lextent_init_func_t<T> maybe_init = [](T&) {}) {
    LOG_PREFIX(TransactionManager::read_extent);
    SUBDEBUGT(seastore_tm, "{} {} ...",
              t, offset, T::TYPE);
    return get_pin(
      t, offset
    ).si_then([this, FNAME, &t, offset,
	      maybe_init=std::move(maybe_init)] (auto pin) mutable
      -> read_extent_ret<T> {
      if (!pin.get_val().is_real_location()) {
        SUBERRORT(seastore_tm, "{} {} got wrong pin {}",
                  t, offset, T::TYPE, pin);
        ceph_abort_msg("Impossible");
      }
      return this->read_pin<T>(t, std::move(pin), std::move(maybe_init));
    });
  }

  template <typename T>
  base_iertr::future<maybe_indirect_extent_t<T>> read_pin(
    Transaction &t,
    LBAMapping pin,
    extent_len_t partial_off,
    extent_len_t partial_len,
    lextent_init_func_t<T> maybe_init = [](T&) {})
  {
    LOG_PREFIX(TransactionManager::read_pin);
    static_assert(is_logical_type(T::TYPE));
    assert(is_aligned(partial_off, get_block_size()));
    assert(is_aligned(partial_len, get_block_size()));
    // must be user-oriented required by maybe_init
    assert(is_user_transaction(t.get_src()));

    co_await pin.co_refresh();

    if (pin.is_indirect()) {
      pin = co_await lba_manager->complete_indirect_lba_mapping(
	t, std::move(pin));
    }

    extent_len_t direct_partial_off = partial_off;
    bool is_clone = pin.is_clone();
    std::optional<indirect_info_t> maybe_indirect_info;
    if (pin.is_indirect()) {
      auto intermediate_offset = pin.get_intermediate_offset();
      direct_partial_off = intermediate_offset + partial_off;
      maybe_indirect_info = indirect_info_t{
        intermediate_offset, pin.get_length()};
    }

    SUBDEBUGT(seastore_tm, "{} {} 0x{:x}~0x{:x} direct_off=0x{:x} ...",
              t, T::TYPE, pin, partial_off, partial_len, direct_partial_off);


    // checking the lba child must be atomic with creating
    // and linking the absent child
    auto ret = get_extent_if_linked<T>(t, std::move(pin));
    TCachedExtentRef<T> extent;
    if (ret.index() == 1) {
      extent = co_await std::move(std::get<1>(ret));
      extent = co_await cache->read_extent_maybe_partial(
	t, std::move(extent), direct_partial_off, partial_len);
      if (!extent->is_seen_by_users()) {
	maybe_init(*extent);
	extent->set_seen_by_users();
      }
    } else {
      auto &r = std::get<0>(ret);
      extent = co_await this->pin_to_extent<T>(
	t, std::move(r.mapping), std::move(r.child_pos),
	direct_partial_off, partial_len,
	std::move(maybe_init));
    }
    if (maybe_indirect_info.has_value()) {
      SUBDEBUGT(seastore_tm, "got indirect +0x{:x}~0x{:x} is_clone={} {}",
		t, maybe_indirect_info->intermediate_offset,
		maybe_indirect_info->length, is_clone, *extent);
    } else {
      SUBDEBUGT(seastore_tm, "got direct is_clone={} {}",
		t, is_clone, *extent);
    }
    co_return maybe_indirect_extent_t<T>{extent, maybe_indirect_info, is_clone};
  }

  template <typename T>
  base_iertr::future<maybe_indirect_extent_t<T>> read_pin(
    Transaction &t,
    LBAMapping pin,
    lextent_init_func_t<T> maybe_init = [](T&) {})
  {
    auto len = pin.get_length();
    return read_pin<T>(
      t, std::move(pin), 0, len,
      std::move(maybe_init));
  }

  /// Obtain mutable copy of extent
  LogicalChildNodeRef get_mutable_extent(Transaction &t, LogicalChildNodeRef ref) {
    return cache->duplicate_for_write(t, ref)->cast<LogicalChildNode>();
  }

  using ref_iertr = LBAManager::ref_iertr;
  using ref_ret = ref_iertr::future<extent_ref_count_t>;

  /** 
   * remove
   *
   * Remove the extent and the corresponding lba mapping,
   * users must make sure that lba mapping's refcount > 1
   */
  ref_ret remove(
    Transaction &t,
    LogicalChildNodeRef &ref);

  ref_ret remove(
    Transaction &t,
    laddr_t offset);

  ref_iertr::future<LBAMapping> remove(
    Transaction &t,
    LBAMapping mapping);

  /// remove refcount for list of offset
  using refs_ret = ref_iertr::future<std::vector<unsigned>>;
  refs_ret remove(
    Transaction &t,
    std::vector<laddr_t> offsets);

  /**
   * alloc_non_data_extent
   *
   * Allocates a new block of type T with the minimum lba range of size len
   * greater than laddr_hint.
   */
  using alloc_extent_ertr = base_ertr::extend<
    crimson::ct_error::enospc>;
  using alloc_extent_iertr = base_iertr::extend<
    crimson::ct_error::enospc>;
  template <typename T>
  using alloc_extent_ret = alloc_extent_iertr::future<TCachedExtentRef<T>>;
  template <typename T>
  alloc_extent_ret<T> alloc_non_data_extent(
    Transaction &t,
    laddr_t laddr_hint,
    extent_len_t len,
    placement_hint_t placement_hint = placement_hint_t::HOT) {
    static_assert(is_logical_metadata_type(T::TYPE));
    LOG_PREFIX(TransactionManager::alloc_non_data_extent);
    SUBDEBUGT(seastore_tm, "{} hint {}~0x{:x} phint={} ...",
              t, T::TYPE, laddr_hint, len, placement_hint);
    auto ext = cache->alloc_new_non_data_extent<T>(
      t,
      len,
      placement_hint,
      INIT_GENERATION);
    // user must initialize the logical extent themselves.
    assert(is_user_transaction(t.get_src()));
    ext->set_seen_by_users();
    return lba_manager->alloc_extent(
      t,
      laddr_hint,
      *ext,
      EXTENT_DEFAULT_REF_COUNT
    ).si_then([ext=std::move(ext), &t, FNAME](auto &&) mutable {
      SUBDEBUGT(seastore_tm, "allocated {}", t, *ext);
      return alloc_extent_iertr::make_ready_future<TCachedExtentRef<T>>(
	std::move(ext));
    });
  }

  /**
   * alloc_data_extents
   *
   * Allocates a new block of type T with the minimum lba range of size len
   * greater than laddr_hint.
   */
  using alloc_extents_iertr = alloc_extent_iertr;
  template <typename T>
  using alloc_extents_ret = alloc_extents_iertr::future<
    std::vector<TCachedExtentRef<T>>>;
  template <typename T>
  alloc_extents_ret<T> alloc_data_extents(
    Transaction &t,
    laddr_t laddr_hint,
    extent_len_t len,
    std::optional<LBAMapping> pos = std::nullopt,
    placement_hint_t placement_hint = placement_hint_t::HOT) {
    static_assert(is_data_type(T::TYPE));
    LOG_PREFIX(TransactionManager::alloc_data_extents);
    SUBDEBUGT(seastore_tm, "{} hint {}~0x{:x} phint={} ...",
              t, T::TYPE, laddr_hint, len, placement_hint);
    return seastar::do_with(
      cache->alloc_new_data_extents<T>(
	t,
	len,
	placement_hint,
	INIT_GENERATION),
      [pos=std::move(pos), this, &t,
      FNAME, laddr_hint](auto &exts) mutable {
      // user must initialize the logical extent themselves
      assert(is_user_transaction(t.get_src()));
      for (auto& ext : exts) {
	ext->set_seen_by_users();
      }
      if (pos) {
	// laddr_hint is determined
	auto off = laddr_hint;
	for (auto &extent : exts) {
	  extent->set_laddr(off);
	  off = (off + extent->get_length()).checked_to_laddr();
	}
      }
      auto fut = alloc_extents_iertr::make_ready_future<
	std::vector<LBAMapping>>();
      if (pos) {
	fut = pos->refresh(
	).si_then([&t, &exts, this](auto pos) {
	  return lba_manager->alloc_extents(
	    t,
	    std::move(pos),
	    std::vector<LogicalChildNodeRef>(
	      exts.begin(), exts.end()));
	});
      } else {
	fut = lba_manager->alloc_extents(
	  t,
	  laddr_hint,
	  std::vector<LogicalChildNodeRef>(
	    exts.begin(), exts.end()),
	  EXTENT_DEFAULT_REF_COUNT);
      }
      return fut.si_then([&exts, &t, FNAME](auto &&) mutable {
	for (auto &ext : exts) {
	  SUBDEBUGT(seastore_tm, "allocated {}", t, *ext);
	}
	return alloc_extent_iertr::make_ready_future<
	  std::vector<TCachedExtentRef<T>>>(std::move(exts));
      });
    });
  }

  template <typename T>
  get_pin_iertr::future<TCachedExtentRef<T>>
  get_mutable_extent_by_laddr(
      Transaction &t,
      laddr_t laddr,
      extent_len_t len) {
    LOG_PREFIX(TransactionManager::get_mutable_extent_by_laddr);
    SUBDEBUGT(seastore_tm, "{}~0x{:x} ...", t, laddr, len);
    return get_pin(t, laddr
    ).si_then([this, &t, len](auto pin) {
      ceph_assert(pin.is_data_stable() && !pin.is_zero_reserved());
      ceph_assert(!pin.is_clone());
      ceph_assert(pin.get_length() == len);
      return this->read_pin<T>(t, std::move(pin));
    }).si_then([this, &t, FNAME](auto maybe_indirect_extent) {
      assert(!maybe_indirect_extent.is_indirect());
      assert(!maybe_indirect_extent.is_clone);
      auto ext = get_mutable_extent(
          t, maybe_indirect_extent.extent)->template cast<T>();
      SUBDEBUGT(seastore_tm, "got mutable {}", t, *ext);
      return read_extent_iertr::make_ready_future<TCachedExtentRef<T>>(
	std::move(ext));
    });
  }

  using reserve_extent_iertr = alloc_extent_iertr;
  using reserve_extent_ret = reserve_extent_iertr::future<LBAMapping>;
  reserve_extent_ret reserve_region(
    Transaction &t,
    laddr_t hint,
    extent_len_t len) {
    LOG_PREFIX(TransactionManager::reserve_region);
    SUBDEBUGT(seastore_tm, "hint {}~0x{:x} ...", t, hint, len);
    return lba_manager->reserve_region(
      t,
      hint,
      len
    ).si_then([FNAME, &t](auto pin) {
      SUBDEBUGT(seastore_tm, "reserved {}", t, pin);
      return pin;
    });
  }

  reserve_extent_ret reserve_region(
    Transaction &t,
    LBAMapping pos,
    laddr_t hint,
    extent_len_t len) {
    LOG_PREFIX(TransactionManager::reserve_region);
    SUBDEBUGT(seastore_tm, "hint {}~0x{:x} ...", t, hint, len);
    return pos.refresh(
    ).si_then([FNAME, this, &t, hint, len](auto pos) {
      return lba_manager->reserve_region(
	t,
	std::move(pos),
	hint,
	len
      ).si_then([FNAME, &t](auto pin) {
	SUBDEBUGT(seastore_tm, "reserved {}", t, pin);
	return pin;
      });
    });
  }

  /*
   * clone_mapping
   *
   * create an indirect lba mapping pointing to the direct
   * lba mapping whose key is intermediate_key. Resort to btree_lba_manager.h
   * for the definition of "indirect lba mapping" and "direct lba mapping".
   * Note that the cloned extent must be stable
   */
  using clone_extent_iertr = LBAManager::clone_mapping_iertr;
  using clone_extent_ret = LBAManager::clone_mapping_ret;
  clone_extent_ret clone_pin(
    Transaction &t,
    LBAMapping pos,
    LBAMapping mapping,
    laddr_t hint,
    extent_len_t offset,
    extent_len_t len,
    bool updateref) {
    LOG_PREFIX(TransactionManager::clone_pin);
    SUBDEBUGT(seastore_tm, "{} clone to hint {} ... pos={}, updateref={}",
      t, mapping, hint, pos, updateref);
    return seastar::do_with(
      std::move(pos),
      std::move(mapping),
      [offset, len, FNAME, this, &t, hint, updateref](auto &pos, auto &mapping) {
      return pos.refresh(
      ).si_then([&pos, &mapping](auto m) {
	pos = std::move(m);
	return mapping.refresh();
      }).si_then([offset, len, FNAME, this, &pos,
		  &t, hint, updateref](auto mapping) {
	return lba_manager->clone_mapping(
	  t,
	  std::move(pos),
	  std::move(mapping),
	  hint,
	  offset,
	  len,
	  updateref
	).si_then([FNAME, &t](auto ret) {
	  SUBDEBUGT(seastore_tm, "cloned as {}", t, ret.cloned_mapping);
	  return ret;
	});
      });
    });
  }

  struct clone_range_ret_t {
    bool shared_direct_mapping = false;
    LBAMapping next_mapping;
  };
  // clone the mappings in range base~len, returns true if there exists
  // direct mappings that are cloned.
  using clone_iertr = base_iertr;
  using clone_ret = clone_iertr::future<clone_range_ret_t>;
  clone_ret clone_range(
    Transaction &t,
    laddr_t src_base,
    laddr_t dst_base,
    extent_len_t offset,
    extent_len_t len,
    LBAMapping pos,
    LBAMapping mapping,
    bool updateref)
  {
    LOG_PREFIX(TransactionManager::clone_range);
    SUBDEBUGT(seastore_tm,
      "src_base={}, dst_base={}, {}~{}, mapping={}, pos={}, updateref={}",
      t, src_base, dst_base, offset, len, mapping, pos, updateref);
    co_await pos.co_refresh();
    mapping = co_await mapping.refresh();
    auto left = len;
    bool shared_direct = false;
    auto cloned_to = offset;
    while (left != 0) {
      auto src_offset = src_base.template get_byte_distance<
	extent_len_t>(mapping.get_key());
      ceph_assert(cloned_to >= src_offset);
      extent_len_t clone_offset = cloned_to - src_offset;
      extent_len_t clone_len = mapping.get_length() - clone_offset;
      clone_len = std::min(clone_len, left);
      left -= clone_len;
      if (!mapping.is_indirect() && mapping.get_val().is_zero()) {
	auto r = co_await reserve_region(
	  t,
	  std::move(pos),
	  (dst_base + cloned_to).checked_to_laddr(),
	  clone_len
	).handle_error_interruptible(
	  clone_iertr::pass_further{},
	  crimson::ct_error::assert_all{"unexpected error"}
	);
	assert((dst_base + cloned_to).checked_to_laddr() == r.get_key());
	cloned_to += clone_len;
	pos = co_await r.next();
	mapping = co_await mapping.next();
	continue;
      }
      if (mapping.is_real()) {
	shared_direct = true;
      }
      auto ret = co_await clone_pin(
	t, std::move(pos), std::move(mapping),
	(dst_base + cloned_to).checked_to_laddr(),
	clone_offset, clone_len, updateref);
      cloned_to += clone_len;
      pos = co_await ret.cloned_mapping.next();
      mapping = co_await ret.orig_mapping.next();
    }
    co_return clone_range_ret_t{shared_direct, std::move(pos)};
  }

  /* alloc_extents
   *
   * allocates more than one new blocks of type T.
   */
   template<class T>
   alloc_extents_iertr::future<std::vector<TCachedExtentRef<T>>>
   alloc_extents(
     Transaction &t,
     laddr_t hint,
     extent_len_t len,
     int num) {
     LOG_PREFIX(TransactionManager::alloc_extents);
     SUBDEBUGT(seastore_tm, "hint {}~({} * 0x{:x}) ...",
               t, hint, num, len);
     return seastar::do_with(std::vector<TCachedExtentRef<T>>(),
       [this, &t, hint, len, num, FNAME](auto &extents) {
       return trans_intr::do_for_each(
                       boost::make_counting_iterator(0),
                       boost::make_counting_iterator(num),
         [this, &t, len, hint, &extents] (auto i) {
         return alloc_non_data_extent<T>(t, hint, len).si_then(
           [&extents](auto &&node) {
           extents.push_back(node);
         });
       }).si_then([&extents, &t, FNAME] {
         SUBDEBUGT(seastore_tm, "allocated {} extents", t, extents.size());
         return alloc_extents_iertr::make_ready_future
                <std::vector<TCachedExtentRef<T>>>(std::move(extents));
       });
     });
  }

  /**
   * submit_transaction
   *
   * Atomically submits transaction to persistence
   */
  using submit_transaction_iertr = base_iertr;
  submit_transaction_iertr::future<> submit_transaction(Transaction &);

  /**
   * flush
   *
   * Block until all outstanding IOs on handle are committed.
   * Note, flush() machinery must go through the same pipeline
   * stages and locks as submit_transaction.
   */
  seastar::future<> flush(OrderingHandle &handle);

  /*
   * ExtentCallbackInterface
   */

  shard_stats_t& get_shard_stats() {
    return shard_stats;
  }

  /// weak transaction should be type READ
  TransactionRef create_transaction(
      Transaction::src_t src,
      const char* name,
      cache_hint_t cache_hint = CACHE_HINT_TOUCH,
      bool is_weak=false) final {
    return cache->create_transaction(src, name, cache_hint, is_weak);
  }

  using ExtentCallbackInterface::submit_transaction_direct_ret;
  submit_transaction_direct_ret submit_transaction_direct(
    Transaction &t,
    std::optional<journal_seq_t> seq_to_trim = std::nullopt) final;

  using ExtentCallbackInterface::get_next_dirty_extents_ret;
  get_next_dirty_extents_ret get_next_dirty_extents(
    Transaction &t,
    journal_seq_t seq,
    size_t max_bytes) final;

  using ExtentCallbackInterface::rewrite_extent_ret;
  rewrite_extent_ret rewrite_extent(
    Transaction &t,
    CachedExtentRef extent,
    rewrite_gen_t target_generation,
    sea_time_point modify_time) final;

  using ExtentCallbackInterface::get_extents_if_live_ret;
  get_extents_if_live_ret get_extents_if_live(
    Transaction &t,
    extent_types_t type,
    paddr_t paddr,
    laddr_t laddr,
    extent_len_t len) final;

  /**
   * read_root_meta
   *
   * Read root block meta entry for key.
   */
  using read_root_meta_iertr = base_iertr;
  using read_root_meta_bare = std::optional<std::string>;
  using read_root_meta_ret = read_root_meta_iertr::future<
    read_root_meta_bare>;
  read_root_meta_ret read_root_meta(
    Transaction &t,
    const std::string &key) {
    return cache->get_root(
      t
    ).si_then([&t, this](auto root) {
      return read_extent<RootMetaBlock>(t, root->root.meta);
    }).si_then([key, &t](auto maybe_indirect_extent) {
      LOG_PREFIX(TransactionManager::read_root_meta);
      assert(!maybe_indirect_extent.is_indirect());
      assert(!maybe_indirect_extent.is_clone);
      auto& mblock = maybe_indirect_extent.extent;
      auto meta = mblock->get_meta();
      auto iter = meta.find(key);
      if (iter == meta.end()) {
        SUBDEBUGT(seastore_tm, "{} -> nullopt", t, key);
	return seastar::make_ready_future<read_root_meta_bare>(std::nullopt);
      } else {
        SUBDEBUGT(seastore_tm, "{} -> {}", t, key, iter->second);
	return seastar::make_ready_future<read_root_meta_bare>(iter->second);
      }
    }).handle_error_interruptible(
      crimson::ct_error::input_output_error::pass_further{},
      crimson::ct_error::assert_all{"unexpected error!"}
    );
  }

  /**
   * init_root_meta
   *
   * create the root meta block
   */
  using init_root_meta_iertr = base_iertr;
  using init_root_meta_ret = init_root_meta_iertr::future<>;
  init_root_meta_ret init_root_meta(Transaction &t) {
    return alloc_non_data_extent<RootMetaBlock>(
      t, L_ADDR_MIN, RootMetaBlock::SIZE
    ).si_then([this, &t](auto meta) {
      meta->set_meta(RootMetaBlock::meta_t{});
      return cache->get_root(t
      ).si_then([this, &t, meta](auto root) {
	auto mroot = cache->duplicate_for_write(
	  t, root)->template cast<RootBlock>();
	mroot->root.meta = meta->get_laddr();
	return seastar::now();
      });
    }).handle_error_interruptible(
      crimson::ct_error::input_output_error::pass_further{},
      crimson::ct_error::assert_all{"unexpected error!"}
    );
  }

  /**
   * update_root_meta
   *
   * Update root block meta entry for key to value.
   */
  using update_root_meta_iertr = base_iertr;
  using update_root_meta_ret = update_root_meta_iertr::future<>;
  update_root_meta_ret update_root_meta(
    Transaction& t,
    const std::string& key,
    const std::string& value) {
    LOG_PREFIX(TransactionManager::update_root_meta);
    SUBDEBUGT(seastore_tm, "seastore_tm, {} -> {} ...", t, key, value);
    return cache->get_root(
      t
    ).si_then([this, &t](RootBlockRef root) {
      return read_extent<RootMetaBlock>(t, root->root.meta);
    }).si_then([this, key, value, &t](auto maybe_indirect_extent) {
      assert(!maybe_indirect_extent.is_indirect());
      assert(!maybe_indirect_extent.is_clone);
      auto& mblock = maybe_indirect_extent.extent;
      mblock = get_mutable_extent(t, mblock
	)->template cast<RootMetaBlock>();

      auto meta = mblock->get_meta();
      meta[key] = value;

      mblock->set_meta(meta);
      return seastar::now();
    }).handle_error_interruptible(
      crimson::ct_error::input_output_error::pass_further{},
      crimson::ct_error::assert_all{"unexpected error!"}
    );
  }

  /**
   * read_onode_root
   *
   * Get onode-tree root logical address
   */
  using read_onode_root_iertr = base_iertr;
  using read_onode_root_ret = read_onode_root_iertr::future<laddr_t>;
  read_onode_root_ret read_onode_root(Transaction &t) {
    return cache->get_root(t).si_then([&t](auto croot) {
      LOG_PREFIX(TransactionManager::read_onode_root);
      laddr_t ret = croot->get_root().onode_root;
      SUBTRACET(seastore_tm, "{}", t, ret);
      return ret;
    });
  }

  /**
   * write_onode_root
   *
   * Write onode-tree root logical address, must be called after read.
   */
  void write_onode_root(Transaction &t, laddr_t addr) {
    LOG_PREFIX(TransactionManager::write_onode_root);
    SUBDEBUGT(seastore_tm, "{}", t, addr);
    auto croot = cache->get_root_fast(t);
    croot = cache->duplicate_for_write(t, croot)->cast<RootBlock>();
    croot->get_root().onode_root = addr;
  }

  /**
   * read_collection_root
   *
   * Get collection root addr
   */
  using read_collection_root_iertr = base_iertr;
  using read_collection_root_ret = read_collection_root_iertr::future<
    coll_root_t>;
  read_collection_root_ret read_collection_root(Transaction &t) {
    return cache->get_root(t).si_then([&t](auto croot) {
      LOG_PREFIX(TransactionManager::read_collection_root);
      auto ret = croot->get_root().collection_root.get();
      SUBTRACET(seastore_tm, "{}~0x{:x}",
                t, ret.get_location(), ret.get_size());
      return ret;
    });
  }

  /**
   * write_collection_root
   *
   * Update collection root addr
   */
  void write_collection_root(Transaction &t, coll_root_t cmroot) {
    LOG_PREFIX(TransactionManager::write_collection_root);
    SUBDEBUGT(seastore_tm, "{}~0x{:x}",
              t, cmroot.get_location(), cmroot.get_size());
    auto croot = cache->get_root_fast(t);
    croot = cache->duplicate_for_write(t, croot)->cast<RootBlock>();
    croot->get_root().collection_root.update(cmroot);
  }

  extent_len_t get_block_size() const {
    return epm->get_block_size();
  }

  store_statfs_t store_stat() const {
    return epm->get_stat();
  }

  ExtentTransViewRetriever& get_etvr() {
    return *cache;
  }

  using remap_entry_t = LBAManager::remap_entry_t;
  using remap_mappings_iertr = base_iertr;
  using remap_mappings_ret = remap_mappings_iertr::future<
    std::vector<LBAMapping>>;
  template <typename T, std::size_t N>
  remap_mappings_ret remap_mappings(
    Transaction &t,
    LBAMapping mapping,
    std::array<TransactionManager::remap_entry_t, N> remaps)
  {
    if (!mapping.is_indirect() && mapping.is_zero_reserved()) {
      return seastar::do_with(
	std::vector<TransactionManager::remap_entry_t>(
	  remaps.begin(), remaps.end()),
	std::vector<LBAMapping>(),
	[&t, mapping=std::move(mapping), this]
	(auto &remaps, auto &mappings) mutable {
	auto orig_laddr = mapping.get_key();
	return remove(t, std::move(mapping)
	).si_then([&remaps, &t, &mappings, orig_laddr,
		  this](auto pos) {
	  return seastar::do_with(
	    std::move(pos),
	    [this, &t, &remaps, orig_laddr, &mappings](auto &pos) {
	    return trans_intr::do_for_each(
	      remaps.begin(),
	      remaps.end(),
	      [&t, &pos, orig_laddr, &mappings, this]
	      (const auto &remap) mutable {
	      auto laddr = (orig_laddr + remap.offset).checked_to_laddr();
	      return this->reserve_region(
		t,
		std::move(pos),
		laddr,
		remap.len
	      ).si_then([&mappings](auto new_mapping) {
		mappings.emplace_back(new_mapping);
		return new_mapping.next();
	      }).si_then([&pos](auto new_mapping) {
		pos = std::move(new_mapping);
		return seastar::now();
	      });
	    });
	  });
	}).si_then([&mappings] { return std::move(mappings); });
      }).handle_error_interruptible(
	remap_mappings_iertr::pass_further{},
	crimson::ct_error::assert_all{
	  "remap_mappings hit invalid error"
	}
      );
    } else {
      return remap_pin<T, N>(
	t, std::move(mapping), std::move(remaps));
    }
  }

  /*
   * punch_hole_in_mapping
   *
   * punch an lba hole inside a single mapping, this requires laddr~len
   * is within the mapping.
   *
   * Return: the position for later inserts, e.g. the mapping next to
   * 	     the hole
   */
  using punch_mappings_iertr = base_iertr;
  using punch_mappings_ret = punch_mappings_iertr::future<LBAMapping>;
  template <typename T>
  punch_mappings_ret punch_hole_in_mapping(
    Transaction &t,
    laddr_t laddr,
    objaddr_t aligned_len,
    LBAMapping mapping)
  {
    LOG_PREFIX(TransactionManager::punch_hole_in_mapping);
    SUBDEBUGT(seastore_tm, "{}~{} {}", t, laddr, aligned_len, mapping);
    assert(!mapping.is_pending());
    assert(laddr >= mapping.get_key() &&
	laddr + aligned_len <= mapping.get_key() + mapping.get_length());
    if (laddr > mapping.get_key()) {
      if (laddr + aligned_len < mapping.get_key() + mapping.get_length()) {
	auto offset1 = laddr.template get_byte_distance<
	  extent_len_t>(mapping.get_key());
	auto offset2 = (laddr + aligned_len).template get_byte_distance<
	  extent_len_t>(mapping.get_key());
	auto len2 = mapping.get_length() - offset2;
	return remap_mappings<T, 2>(
	  t,
	  std::move(mapping),
	  std::array{
	    remap_entry_t{0, offset1},
	    remap_entry_t{offset2, len2}}
	).si_then([](auto ret) {
	  assert(ret.size() == 2);
	  return std::move(ret.back());
	});
      } else {
	return cut_mapping<T>(t, laddr, std::move(mapping), true
	).si_then([](auto mapping) {
	  return mapping.next();
	});
      }
    } else if (laddr + aligned_len < mapping.get_key() + mapping.get_length()) {
      return cut_mapping<T>(
	t, (laddr + aligned_len).checked_to_laddr(), std::move(mapping), false);
    } else {
      return remove(t, std::move(mapping)
      ).handle_error_interruptible(
	punch_mappings_iertr::pass_further{},
	crimson::ct_error::assert_all{"impossible"}
      );
    }
  }

  /*
   * cut_mapping
   *
   * remove the left/right part of the mapping
   *
   * Return: the remaining part of the mapping
   */
  using cut_mapping_iertr = punch_mappings_ret;
  using cut_mapping_ret = punch_mappings_ret;
  template <typename T>
  cut_mapping_ret cut_mapping(
    Transaction &t,
    laddr_t pivot,
    LBAMapping mapping,
    bool keep_left)
  {
    LOG_PREFIX(TransactionManager::cut_mapping);
    SUBDEBUGT(seastore_tm, "{} {} {}",
      t, pivot, mapping, keep_left ? "LEFT" : "RIGHT");
    assert(mapping.is_indirect() || mapping.is_data_stable());
    assert(pivot > mapping.get_key() &&
      pivot < mapping.get_key() + mapping.get_length());
    auto offset = keep_left
      ? 0
      : pivot.template get_byte_distance<extent_len_t>(mapping.get_key());
    auto len = keep_left
      ? pivot.template get_byte_distance<
	extent_len_t>(mapping.get_key())
      : pivot.template get_byte_distance<
	extent_len_t>(mapping.get_key() + mapping.get_length());
    return remap_mappings<T, 1>(
      t,
      std::move(mapping),
      std::array{remap_entry_t{offset, len}}
    ).si_then([] (auto ret) {
      assert(ret.size() == 1);
      return std::move(ret.back());
    });
  }

  struct remove_mappings_param_t {
    bool cascade_remove_on_indirect = true;
    bool skip_direct_mapping = false;
  };
  /*
   * remove_mappings_in_range
   *
   * remove the mappings that are completely inside the range start~unaligned_len
   *
   * Return: the mapping next to the right boundary of the range
   */
  punch_mappings_ret remove_mappings_in_range(
    Transaction &t,
    laddr_t start,
    objaddr_t unaligned_len,
    LBAMapping first_mapping,
    remove_mappings_param_t params)
  {
    LOG_PREFIX(TransactionManager::remove_mappings_in_range);
    SUBDEBUGT(seastore_tm, "{}~{}, first_mapping: {}",
      t, start, unaligned_len, first_mapping);
    auto mapping = co_await first_mapping.refresh();
    while (!mapping.is_end()) {
      assert(mapping.get_key() >= start);
      auto mapping_end = (mapping.get_key() + mapping.get_length()
	).checked_to_laddr();
      if (mapping_end > start + unaligned_len) {
	break;
      }
      if (params.skip_direct_mapping && mapping.is_real()) {
	mapping = co_await mapping.next();
	continue;
      }
      if (params.cascade_remove_on_indirect ||
	  mapping.is_zero_reserved()) {
	mapping = co_await remove(t, std::move(mapping)
	).handle_error_interruptible(
	  punch_mappings_iertr::pass_further{},
	  crimson::ct_error::assert_all{
	    "remove_mappings_in_range hit invalid error"
	  }
	);
      } else {
	mapping = co_await _remove_indirect_mapping_only(
	  t, std::move(mapping)
	).handle_error_interruptible(
	  punch_mappings_iertr::pass_further{},
	  crimson::ct_error::assert_all{
	    "remove_mappings_in_range hit invalid error"
	  }
	);
      }
    }
    co_return mapping;
  }

  ~TransactionManager();

private:
  friend class Transaction;

  CacheRef cache;
  LBAManagerRef lba_manager;
  JournalRef journal;
  ExtentPlacementManagerRef epm;
  BackrefManagerRef backref_manager;

  WritePipeline write_pipeline;

  bool full_extent_integrity_check = true;

  shard_stats_t& shard_stats;

  bool can_drop_backref() const {
    return cache->can_drop_backref();
  }

  using LBALeafNode = lba::LBALeafNode;
  struct unlinked_child_t {
    LBAMapping mapping;
    child_pos_t<LBALeafNode> child_pos;
  };
  template <typename T>
  std::variant<unlinked_child_t, get_child_ifut<T>>
  get_extent_if_linked(
    Transaction &t,
    LBAMapping pin)
  {
    ceph_assert(pin.is_viewable());
    // checking the lba child must be atomic with creating
    // and linking the absent child
    auto v = pin.get_logical_extent(t);
    if (v.has_child()) {
      return v.get_child_fut(
      ).si_then([pin](auto extent) {
#ifndef NDEBUG
        auto lextent = extent->template cast<LogicalChildNode>();
        auto pin_laddr = pin.get_intermediate_base();
        assert(lextent->get_laddr() == pin_laddr);
#endif
	return extent->template cast<T>();
      });
    } else {
      return unlinked_child_t{
	std::move(const_cast<LBAMapping&>(pin)),
	v.get_child_pos()};
    }
  }

  base_iertr::future<LogicalChildNodeRef> read_pin_by_type(
    Transaction &t,
    LBAMapping pin,
    extent_types_t type)
  {
    ceph_assert(pin.is_viewable());
    assert(!pin.is_indirect());
    // Note: pin might be a clone
    auto v = pin.get_logical_extent(t);
    // checking the lba child must be atomic with creating
    // and linking the absent child
    if (v.has_child()) {
      auto extent = co_await std::move(v.get_child_fut());
      auto len = extent->get_length();
      auto ext = co_await cache->read_extent_maybe_partial(
	t, std::move(extent), 0, len);
      ceph_assert(ext->get_type() == type);
      co_return ext;
    } else {
      auto extent = co_await pin_to_extent_by_type(
	t, pin, v.get_child_pos(), type);
      co_return extent;
    }
  }

  /**
   * remap_pin
   *
   * Remap original extent to new extents.
   * Return the pins of new extent.
   */
  using remap_pin_iertr = base_iertr;
  using remap_pin_ret = remap_pin_iertr::future<std::vector<LBAMapping>>;
  template <typename T, std::size_t N>
  remap_pin_ret remap_pin(
    Transaction &t,
    LBAMapping pin,
    std::array<remap_entry_t, N> remaps) {
    static_assert(std::is_base_of_v<LogicalChildNode, T>);
    // data extents don't need maybe_init yet, currently,
    static_assert(is_data_type(T::TYPE));
    // must be user-oriented required by (the potential) maybe_init
    assert(is_user_transaction(t.get_src()));
    assert(pin.is_indirect() || !pin.is_zero_reserved());

    LOG_PREFIX(TransactionManager::remap_pin);
#ifndef NDEBUG
    std::sort(remaps.begin(), remaps.end(),
      [](remap_entry_t x, remap_entry_t y) {
        return x.offset < y.offset;
    });
    auto original_len = pin.get_length();
    extent_len_t total_remap_len = 0;
    extent_len_t last_offset = 0;
    extent_len_t last_len = 0;

    for (auto &remap : remaps) {
      auto remap_offset = remap.offset;
      auto remap_len = remap.len;
      assert(remap_len > 0);
      total_remap_len += remap.len;
      assert(remap_offset >= (last_offset + last_len));
      last_offset = remap_offset;
      last_len = remap_len;
    }
    if (remaps.size() == 1) {
      assert(total_remap_len < original_len);
    } else {
      assert(total_remap_len <= original_len);
    }
#endif

    if (pin.is_indirect()) {
      SUBDEBUGT(seastore_tm, "{} into {} remaps ...",
        t, pin, remaps.size());
      co_await pin.co_refresh();
      pin = co_await lba_manager->complete_indirect_lba_mapping(t, pin);
    } else {
      laddr_t original_laddr = pin.get_key();
      extent_len_t original_len = pin.get_length();
      paddr_t original_paddr = pin.get_val();
      SUBDEBUGT(seastore_tm, "{}~0x{:x} {} into {} remaps ... {}",
        t, original_laddr, original_len, original_paddr, remaps.size(), pin);
      ceph_assert(!pin.is_clone());

      TCachedExtentRef<T> extent;
      co_await pin.co_refresh();
      if (full_extent_integrity_check) {
        SUBTRACET(seastore_tm, "{} reading pin...", t, pin);
        // read the entire extent from disk (See: pin_to_extent)
        auto maybe_indirect_extent = co_await read_pin<T>(t, pin);
        assert(!maybe_indirect_extent.is_indirect());
        assert(!maybe_indirect_extent.is_clone);
        extent = maybe_indirect_extent.extent;
      } else {
        auto ret = get_extent_if_linked<T>(t, pin);
        if (std::holds_alternative<get_child_ifut<T>>(ret)) {
          SUBTRACET(seastore_tm, "getting linked child...", t);
          extent = co_await std::move(std::get<get_child_ifut<T>>(ret));
          if (!extent->is_seen_by_users()) {
            // Note, no maybe_init available for data extents
            extent->set_seen_by_users();
          }
        } else if (std::holds_alternative<unlinked_child_t>(ret)) {
          SUBTRACET(seastore_tm, "retire extent place holder...", t);
          auto unlinked_child =  std::move(std::get<unlinked_child_t>(ret));
          auto retired_placeholder = cache->retire_absent_extent_addr(
            t, pin.get_key(), original_paddr, original_len
          )->template cast<RetiredExtentPlaceholder>();
          unlinked_child.child_pos.link_child(retired_placeholder.get());
        } else {
          ceph_abort("unexpected varaint in remap_pin");
        }
      }

      if (full_extent_integrity_check) {
        ceph_assert(extent && extent->is_fully_loaded());
        // CRC_NULL shouldn't be possible when full extent
        // integrity checks are enabled.
        assert(extent->calc_crc32c() != CRC_NULL);
      }

      std::optional<ceph::bufferptr> original_bptr;
      // TODO: preserve the bufferspace if partially loaded
      if (extent && extent->is_fully_loaded()) {
        SUBDEBUGT(seastore_tm, "extent fully loaded...", t);
        ceph_assert(extent->is_data_stable());
        ceph_assert(extent->get_length() >= original_len);
        ceph_assert(extent->get_paddr() == original_paddr);
        original_bptr = extent->get_bptr();
      }
      if (extent) {
        SUBTRACET(seastore_tm, "retire extent...", t);
        assert(extent->is_seen_by_users());
        cache->retire_extent(t, extent);
      }
      for (auto &remap : remaps) {
        auto remap_offset = remap.offset;
        auto remap_len = remap.len;
        auto remap_laddr = (original_laddr + remap_offset).checked_to_laddr();
        auto remap_paddr = original_paddr.add_offset(remap_offset);
        SUBDEBUGT(seastore_tm, "remap direct pin into {}~0x{:x} {} ...",
                  t, remap_laddr, remap_len, remap_paddr);
        ceph_assert(remap_len < original_len);
        ceph_assert(remap_offset + remap_len <= original_len);
        ceph_assert(remap_len != 0);
        ceph_assert(remap_offset % cache->get_block_size() == 0);
        ceph_assert(remap_len % cache->get_block_size() == 0);
        auto remapped_extent = cache->alloc_remapped_extent<T>(
          t,
          remap_laddr,
          remap_paddr,
          remap_len,
          original_laddr,
          original_bptr);
        // user must initialize the logical extent themselves.
        remapped_extent->set_seen_by_users();
        remap.extent = remapped_extent.get();
      }
     }

     SUBTRACET(seastore_tm, "remapping pins...", t);
     auto mapping_vec = co_await lba_manager->remap_mappings(
       t,
       pin,
       std::vector<remap_entry_t>(remaps.begin(), remaps.end())
       ).handle_error_interruptible(remap_pin_iertr::pass_further{},
         crimson::ct_error::assert_all{
         "TransactionManager::remap_pin hit invalid error"}
       );
       SUBDEBUGT(seastore_tm, "remapped {} pins", t, mapping_vec.size());
       co_return std::move(mapping_vec);
  }

  using _remove_mapping_result_t = LBAManager::ref_update_result_t;
  ref_iertr::future<_remove_mapping_result_t> _remove(
    Transaction &t,
    LBAMapping mapping);
  ref_iertr::future<_remove_mapping_result_t>
  _remove_indirect_mapping(
    Transaction &t,
    LBAMapping mapping);
  ref_iertr::future<_remove_mapping_result_t>
  _remove_direct_mapping(
    Transaction &t,
    LBAMapping mapping);
  ref_iertr::future<LBAMapping>
  _remove_indirect_mapping_only(
    Transaction &t,
    LBAMapping mapping);

  rewrite_extent_ret rewrite_logical_extent(
    Transaction& t,
    LogicalChildNodeRef extent);

  submit_transaction_direct_ret do_submit_transaction(
    Transaction &t,
    ExtentPlacementManager::dispatch_result_t dispatch_result,
    std::optional<journal_seq_t> seq_to_trim = std::nullopt);

  using update_lba_mappings_ret = LBAManager::update_mappings_ret;
  update_lba_mappings_ret update_lba_mappings(
    Transaction &t,
    std::list<CachedExtentRef> &pre_allocated_extents);

  /**
   * pin_to_extent
   *
   * Get extent mapped at pin.
   * partially load buffer from direct_partial_off~partial_len if not present.
   */
  using pin_to_extent_iertr = base_iertr;
  template <typename T>
  using pin_to_extent_ret = pin_to_extent_iertr::future<
    TCachedExtentRef<T>>;
  template <typename T>
  pin_to_extent_ret<T> pin_to_extent(
    Transaction &t,
    LBAMapping pin,
    child_pos_t<LBALeafNode> child_pos,
    extent_len_t direct_partial_off,
    extent_len_t partial_len,
    lextent_init_func_t<T> &&maybe_init) {
    LOG_PREFIX(TransactionManager::pin_to_extent);
    SUBDEBUGT(seastore_tm, "getting absent extent from pin {}, 0x{:x}~0x{:x} ...",
              t, pin, direct_partial_off, partial_len);
    static_assert(is_logical_type(T::TYPE));
    // must be user-oriented required by maybe_init
    assert(is_user_transaction(t.get_src()));
    assert(pin.is_viewable());
    auto direct_length = pin.get_intermediate_length();
    if (full_extent_integrity_check) {
      direct_partial_off = 0;
      partial_len = direct_length;
    }

    // are we reading the entire extent?
    bool is_full_extent = (direct_partial_off == 0 &&
                           partial_len == direct_length);
    SUBDEBUGT(seastore_tm, "getting absent extent from pin {}, 0x{:x}~0x{:x} full extent: {}...",
              t, pin, direct_partial_off, partial_len, is_full_extent);

    auto ref = co_await cache->get_absent_extent<T>(
      t,
      pin.get_val(),
      direct_length,
      direct_partial_off,
      partial_len,
      // extent_init_func
      seastar::coroutine::lambda(
        [laddr=pin.get_intermediate_base(),
        maybe_init=std::move(maybe_init),
        child_pos=std::move(child_pos),
        &t, this] (T &extent) mutable {
          assert(extent.is_logical());
          assert(!extent.has_laddr());
          assert(!extent.has_been_invalidated());
          child_pos.link_child(&extent);
          child_pos.invalidate_retired_placeholder(t, *cache, extent);
          extent.set_laddr(laddr);
          maybe_init(extent);
          extent.set_seen_by_users();
      }),
      pin.get_checksum()
    );

    SUBDEBUGT(seastore_tm, "got extent -- {} fully_loaded: {}",
              t, *ref, ref->is_fully_loaded());

    co_return std::move(ref);
  }

  /**
   * pin_to_extent_by_type
   *
   * Get extent mapped at pin.
   */
  using pin_to_extent_by_type_ret = pin_to_extent_iertr::future<
    LogicalChildNodeRef>;
  pin_to_extent_by_type_ret pin_to_extent_by_type(
      Transaction &t,
      LBAMapping pin,
      child_pos_t<LBALeafNode> child_pos,
      extent_types_t type)
  {
    LOG_PREFIX(TransactionManager::pin_to_extent_by_type);
    SUBTRACET(seastore_tm, "getting absent extent from pin {} type {} ...",
              t, pin, type);
    assert(pin.is_viewable());
    assert(is_logical_type(type));
    assert(is_background_transaction(t.get_src()));
    laddr_t direct_key = pin.get_intermediate_base();
    extent_len_t direct_length = pin.get_intermediate_length();
    auto ref = co_await cache->get_absent_extent_by_type(
      t,
      type,
      pin.get_val(),
      direct_key,
      direct_length,
      // extent_init_func
      seastar::coroutine::lambda(
      [direct_key, child_pos=std::move(child_pos),
      &t, this](CachedExtent &extent) mutable {
        assert(extent.is_logical());
        auto &lextent = static_cast<LogicalChildNode&>(extent);
        assert(!lextent.has_laddr());
        assert(!lextent.has_been_invalidated());
        child_pos.link_child(&lextent);
        child_pos.invalidate_retired_placeholder(t, *cache, lextent);
        lextent.set_laddr(direct_key);
        // No change to extent::seen_by_user because this path is only
        // for background cleaning.
      }
    ),
    pin.get_checksum());
    SUBDEBUGT(seastore_tm, "got extent -- {} fully_loaded: {}",
              t, *ref, ref->is_fully_loaded());
    assert(ref->is_fully_loaded());
    co_return ref->template cast<LogicalChildNode>();
  }

  bool get_checksum_needed(paddr_t paddr) {
    if (paddr.is_record_relative()) {
      return journal->is_checksum_needed();
    }
    return epm->get_checksum_needed(paddr);
  }

  friend class ::transaction_manager_test_t;
  friend class ::object_data_handler_test_t;
public:
  // Testing interfaces
  auto get_epm() {
    return epm.get();
  }

  auto get_lba_manager() {
    return lba_manager.get();
  }

  auto get_backref_manager() {
    return backref_manager.get();
  }

  auto get_cache() {
    return cache.get();
  }
  auto get_journal() {
    return journal.get();
  }
};
using TransactionManagerRef = std::unique_ptr<TransactionManager>;

TransactionManagerRef make_transaction_manager(
    Device *primary_device,
    const std::vector<Device*> &secondary_devices,
    shard_stats_t& shard_stats,
    bool is_test);
}
