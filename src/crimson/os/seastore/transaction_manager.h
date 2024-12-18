// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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

#include "include/ceph_assert.h"
#include "include/buffer.h"

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
  using mkfs_ertr = base_ertr;
  mkfs_ertr::future<> mkfs();

  /// Reads initial metadata from disk
  using mount_ertr = base_ertr;
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
  using get_pin_ret = LBAManager::get_mapping_iertr::future<LBAMappingRef>;
  get_pin_ret get_pin(
    Transaction &t,
    laddr_t offset) {
    LOG_PREFIX(TransactionManager::get_pin);
    SUBDEBUGT(seastore_tm, "{} ...", t, offset);
    return lba_manager->get_mapping(t, offset
    ).si_then([FNAME, &t](LBAMappingRef pin) {
      SUBDEBUGT(seastore_tm, "got {}", t, *pin);
      return pin;
    });
  }

  /**
   * get_pins
   *
   * Get logical pins overlapping offset~length
   */
  using get_pins_iertr = LBAManager::get_mappings_iertr;
  using get_pins_ret = get_pins_iertr::future<lba_pin_list_t>;
  get_pins_ret get_pins(
    Transaction &t,
    laddr_t offset,
    extent_len_t length) {
    LOG_PREFIX(TransactionManager::get_pins);
    SUBDEBUGT(seastore_tm, "{}~0x{:x} ...", t, offset, length);
    return lba_manager->get_mappings(
      t, offset, length
    ).si_then([FNAME, &t](lba_pin_list_t pins) {
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
    extent_len_t length) {
    LOG_PREFIX(TransactionManager::read_extent);
    SUBDEBUGT(seastore_tm, "{}~0x{:x} {} ...",
              t, offset, length, T::TYPE);
    return get_pin(
      t, offset
    ).si_then([this, FNAME, &t, offset, length] (auto pin)
      -> read_extent_ret<T> {
      if (length != pin->get_length() || !pin->get_val().is_real()) {
        SUBERRORT(seastore_tm, "{}~0x{:x} {} got wrong {}",
                  t, offset, length, T::TYPE, *pin);
        ceph_assert(0 == "Should be impossible");
      }
      return this->read_pin<T>(t, std::move(pin));
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
    laddr_t offset) {
    LOG_PREFIX(TransactionManager::read_extent);
    SUBDEBUGT(seastore_tm, "{} {} ...",
              t, offset, T::TYPE);
    return get_pin(
      t, offset
    ).si_then([this, FNAME, &t, offset] (auto pin)
      -> read_extent_ret<T> {
      if (!pin->get_val().is_real()) {
        SUBERRORT(seastore_tm, "{} {} got wrong {}",
                  t, offset, T::TYPE, *pin);
        ceph_assert(0 == "Should be impossible");
      }
      return this->read_pin<T>(t, std::move(pin));
    });
  }

  template <typename T>
  base_iertr::future<maybe_indirect_extent_t<T>> read_pin(
    Transaction &t,
    LBAMappingRef pin,
    extent_len_t partial_off,
    extent_len_t partial_len)
  {
    static_assert(is_logical_type(T::TYPE));
    assert(is_aligned(partial_off, get_block_size()));
    assert(is_aligned(partial_len, get_block_size()));

    extent_len_t direct_partial_off = partial_off;
    bool is_clone = pin->is_clone();
    std::optional<indirect_info_t> maybe_indirect_info;
    if (pin->is_indirect()) {
      auto intermediate_offset = pin->get_intermediate_offset();
      direct_partial_off = intermediate_offset + partial_off;
      maybe_indirect_info = indirect_info_t{
        intermediate_offset, pin->get_length()};
    }

    LOG_PREFIX(TransactionManager::read_pin);
    SUBDEBUGT(seastore_tm, "{} {} 0x{:x}~0x{:x} direct_off=0x{:x} ...",
              t, T::TYPE, *pin, partial_off, partial_len, direct_partial_off);

    auto fut = base_iertr::make_ready_future<LBAMappingRef>();
    if (!pin->is_parent_viewable()) {
      if (pin->is_parent_valid()) {
	pin = pin->refresh_with_pending_parent();
	fut = base_iertr::make_ready_future<LBAMappingRef>(std::move(pin));
      } else {
	fut = get_pin(t, pin->get_key()
	).handle_error_interruptible(
	  crimson::ct_error::enoent::assert_failure{"unexpected enoent"},
	  crimson::ct_error::input_output_error::pass_further{}
	);
      }
    } else {
      pin->maybe_fix_pos();
      fut = base_iertr::make_ready_future<LBAMappingRef>(std::move(pin));
    }
    return fut.si_then([&t, this, direct_partial_off, partial_len](auto npin) {
      // checking the lba child must be atomic with creating
      // and linking the absent child
      auto ret = get_extent_if_linked<T>(t, std::move(npin));
      if (ret.index() == 1) {
	return std::get<1>(ret
        ).si_then([direct_partial_off, partial_len, this, &t](auto extent) {
          return cache->read_extent_maybe_partial(
            t, std::move(extent), direct_partial_off, partial_len);
        });
      } else {
	return this->pin_to_extent<T>(
          t, std::move(std::get<0>(ret)), direct_partial_off, partial_len);
      }
    }).si_then([FNAME, maybe_indirect_info, is_clone, &t](TCachedExtentRef<T> ext) {
      if (maybe_indirect_info.has_value()) {
        SUBDEBUGT(seastore_tm, "got indirect +0x{:x}~0x{:x} is_clone={} {}",
                  t, maybe_indirect_info->intermediate_offset,
                  maybe_indirect_info->length, is_clone, *ext);
      } else {
        SUBDEBUGT(seastore_tm, "got direct is_clone={} {}",
                  t, is_clone, *ext);
      }
      return maybe_indirect_extent_t<T>{ext, maybe_indirect_info, is_clone};
    });
  }

  template <typename T>
  base_iertr::future<maybe_indirect_extent_t<T>> read_pin(
    Transaction &t,
    LBAMappingRef pin)
  {
    auto& pin_ref = *pin;
    return read_pin<T>(t, std::move(pin), 0, pin_ref.get_length());
  }

  /// Obtain mutable copy of extent
  LogicalCachedExtentRef get_mutable_extent(Transaction &t, LogicalCachedExtentRef ref) {
    LOG_PREFIX(TransactionManager::get_mutable_extent);
    auto ret = cache->duplicate_for_write(
      t,
      ref)->cast<LogicalCachedExtent>();
    if (!ret->has_laddr()) {
      SUBDEBUGT(seastore_tm, "duplicate from {}", t, *ref);
      ret->set_laddr(ref->get_laddr());
    } else {
      assert(ref->is_mutable());
      assert(&*ref == &*ret);
    }
    return ret;
  }

  using ref_iertr = LBAManager::ref_iertr;
  using ref_ret = ref_iertr::future<extent_ref_count_t>;

#ifdef UNIT_TESTS_BUILT
  /// Add refcount for ref
  ref_ret inc_ref(
    Transaction &t,
    LogicalCachedExtentRef &ref);

  /// Add refcount for offset
  ref_ret inc_ref(
    Transaction &t,
    laddr_t offset);
#endif

  /** 
   * remove
   *
   * Remove the extent and the corresponding lba mapping,
   * users must make sure that lba mapping's refcount > 1
   */
  ref_ret remove(
    Transaction &t,
    LogicalCachedExtentRef &ref);

  ref_ret remove(
    Transaction &t,
    laddr_t offset);

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
  using alloc_extent_iertr = LBAManager::alloc_extent_iertr::extend<
    crimson::ct_error::enospc>;
  template <typename T>
  using alloc_extent_ret = alloc_extent_iertr::future<TCachedExtentRef<T>>;
  template <typename T>
  alloc_extent_ret<T> alloc_non_data_extent(
    Transaction &t,
    laddr_t laddr_hint,
    extent_len_t len,
    placement_hint_t placement_hint = placement_hint_t::HOT) {
    LOG_PREFIX(TransactionManager::alloc_non_data_extent);
    SUBDEBUGT(seastore_tm, "{} hint {}~0x{:x} phint={} ...",
              t, T::TYPE, laddr_hint, len, placement_hint);
    auto ext = cache->alloc_new_non_data_extent<T>(
      t,
      len,
      placement_hint,
      INIT_GENERATION);
    return lba_manager->alloc_extent(
      t,
      laddr_hint,
      *ext
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
    placement_hint_t placement_hint = placement_hint_t::HOT) {
    LOG_PREFIX(TransactionManager::alloc_data_extents);
    SUBDEBUGT(seastore_tm, "{} hint {}~0x{:x} phint={} ...",
              t, T::TYPE, laddr_hint, len, placement_hint);
    auto exts = cache->alloc_new_data_extents<T>(
      t,
      len,
      placement_hint,
      INIT_GENERATION);
    return lba_manager->alloc_extents(
      t,
      laddr_hint,
      std::vector<LogicalCachedExtentRef>(
	exts.begin(), exts.end()),
      EXTENT_DEFAULT_REF_COUNT
    ).si_then([exts=std::move(exts), &t, FNAME](auto &&) mutable {
      for (auto &ext : exts) {
	SUBDEBUGT(seastore_tm, "allocated {}", t, *ext);
      }
      return alloc_extent_iertr::make_ready_future<
	std::vector<TCachedExtentRef<T>>>(std::move(exts));
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
      ceph_assert(pin->is_data_stable() && !pin->is_zero_reserved());
      ceph_assert(!pin->is_clone());
      ceph_assert(pin->get_length() == len);
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

  /**
   * remap_pin
   *
   * Remap original extent to new extents.
   * Return the pins of new extent.
   */
  using remap_entry = LBAManager::remap_entry;
  using remap_pin_iertr = base_iertr;
  using remap_pin_ret = remap_pin_iertr::future<std::vector<LBAMappingRef>>;
  template <typename T, std::size_t N>
  remap_pin_ret remap_pin(
    Transaction &t,
    LBAMappingRef &&pin,
    std::array<remap_entry, N> remaps) {
    static_assert(std::is_base_of_v<LogicalCachedExtent, T>);

#ifndef NDEBUG
    std::sort(remaps.begin(), remaps.end(),
      [](remap_entry x, remap_entry y) {
        return x.offset < y.offset;
    });
    auto original_len = pin->get_length();
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

    return seastar::do_with(
      std::vector<LogicalCachedExtentRef>(),
      std::move(pin),
      std::move(remaps),
      [&t, this](auto &extents, auto &pin, auto &remaps) {
      laddr_t original_laddr = pin->get_key();
      extent_len_t original_len = pin->get_length();
      paddr_t original_paddr = pin->get_val();
      LOG_PREFIX(TransactionManager::remap_pin);
      SUBDEBUGT(seastore_tm, "{}~0x{:x} {} into {} remaps ... {}",
                t, original_laddr, original_len, original_paddr, remaps.size(), *pin);
      // The according extent might be stable or pending.
      auto fut = base_iertr::now();
      if (!pin->is_indirect()) {
        ceph_assert(!pin->is_clone());
	if (!pin->is_parent_viewable()) {
	  if (pin->is_parent_valid()) {
	    pin = pin->refresh_with_pending_parent();
	  } else {
	    fut = get_pin(t, pin->get_key()
	    ).si_then([&pin](auto npin) {
	      assert(npin);
	      pin = std::move(npin);
	      return seastar::now();
	    }).handle_error_interruptible(
	      crimson::ct_error::enoent::assert_failure{"unexpected enoent"},
	      crimson::ct_error::input_output_error::pass_further{}
	    );
	  }
	} else {
	  pin->maybe_fix_pos();
	}

	fut = fut.si_then([this, &t, &pin] {
	  if (full_extent_integrity_check) {
	    return read_pin<T>(t, pin->duplicate()
            ).si_then([](auto maybe_indirect_extent) {
              assert(!maybe_indirect_extent.is_indirect());
              assert(!maybe_indirect_extent.is_clone);
              return maybe_indirect_extent.extent;
            });
	  } else {
	    auto ret = get_extent_if_linked<T>(t, pin->duplicate());
	    if (ret.index() == 1) {
	      return std::move(std::get<1>(ret));
	    } else {
	      // absent
	      return base_iertr::make_ready_future<TCachedExtentRef<T>>();
	    }
	  }
	}).si_then([this, &t, &remaps, original_paddr,
			    original_laddr, original_len,
			    &extents, FNAME](auto ext) mutable {
	  ceph_assert(full_extent_integrity_check
	      ? (ext && ext->is_fully_loaded())
	      : true);
	  std::optional<ceph::bufferptr> original_bptr;
	  // TODO: preserve the bufferspace if partially loaded
	  if (ext && ext->is_fully_loaded()) {
	    ceph_assert(!ext->is_mutable());
	    ceph_assert(ext->get_length() >= original_len);
	    ceph_assert(ext->get_paddr() == original_paddr);
	    original_bptr = ext->get_bptr();
	  }
	  if (ext) {
	    cache->retire_extent(t, ext);
	  } else {
	    cache->retire_absent_extent_addr(t, original_paddr, original_len);
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
	    auto extent = cache->alloc_remapped_extent<T>(
	      t,
	      remap_laddr,
	      remap_paddr,
	      remap_len,
	      original_laddr,
	      original_bptr);
	    extents.emplace_back(std::move(extent));
	  }
	});
      }
      return fut.si_then([this, &t, &pin, &remaps, &extents, FNAME] {
	return lba_manager->remap_mappings(
	  t,
	  std::move(pin),
	  std::vector<remap_entry>(remaps.begin(), remaps.end()),
	  std::move(extents)
	).si_then([FNAME, &t](auto ret) {
	  SUBDEBUGT(seastore_tm, "remapped {} pins",
	            t, ret.remapped_mappings.size());
	  return Cache::retire_extent_iertr::make_ready_future<
	    std::vector<LBAMappingRef>>(std::move(ret.remapped_mappings));
	});
      }).handle_error_interruptible(
	remap_pin_iertr::pass_further{},
	crimson::ct_error::assert_all{
	  "TransactionManager::remap_pin hit invalid error"
	}
      );
    });
  }

  using reserve_extent_iertr = alloc_extent_iertr;
  using reserve_extent_ret = reserve_extent_iertr::future<LBAMappingRef>;
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
      SUBDEBUGT(seastore_tm, "reserved {}", t, *pin);
      return pin;
    });
  }

  /*
   * clone_mapping
   *
   * create an indirect lba mapping pointing to the physical
   * lba mapping whose key is intermediate_key. Resort to btree_lba_manager.h
   * for the definition of "indirect lba mapping" and "physical lba mapping".
   * Note that the cloned extent must be stable
   */
  using clone_extent_iertr = alloc_extent_iertr;
  using clone_extent_ret = clone_extent_iertr::future<LBAMappingRef>;
  clone_extent_ret clone_pin(
    Transaction &t,
    laddr_t hint,
    const LBAMapping &mapping) {
    auto intermediate_key =
      mapping.is_indirect()
	? mapping.get_intermediate_key()
	: mapping.get_key();
    auto intermediate_base =
      mapping.is_indirect()
        ? mapping.get_intermediate_base()
        : mapping.get_key();

    LOG_PREFIX(TransactionManager::clone_pin);
    SUBDEBUGT(seastore_tm, "{} clone to hint {} ...", t, mapping, hint);
    return lba_manager->clone_mapping(
      t,
      hint,
      mapping.get_length(),
      intermediate_key,
      intermediate_base
    ).si_then([FNAME, &t](auto pin) {
      SUBDEBUGT(seastore_tm, "cloned as {}", t, *pin);
      return pin;
    });
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
      bool is_weak=false) final {
    return cache->create_transaction(src, name, is_weak);
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

  template <typename T>
  std::variant<LBAMappingRef, base_iertr::future<TCachedExtentRef<T>>>
  get_extent_if_linked(
    Transaction &t,
    LBAMappingRef pin)
  {
    ceph_assert(pin->is_parent_viewable());
    // checking the lba child must be atomic with creating
    // and linking the absent child
    auto v = pin->get_logical_extent(t);
    if (v.has_child()) {
      return v.get_child_fut().safe_then([pin=std::move(pin)](auto extent) {
#ifndef NDEBUG
        auto lextent = extent->template cast<LogicalCachedExtent>();
        auto pin_laddr = pin->get_key();
        if (pin->is_indirect()) {
          pin_laddr = pin->get_intermediate_base();
        }
        assert(lextent->get_laddr() == pin_laddr);
#endif
	return extent->template cast<T>();
      });
    } else {
      return pin;
    }
  }

  base_iertr::future<LogicalCachedExtentRef> read_pin_by_type(
    Transaction &t,
    LBAMappingRef pin,
    extent_types_t type)
  {
    ceph_assert(!pin->parent_modified());
    assert(!pin->is_indirect());
    // Note: pin might be a clone
    auto v = pin->get_logical_extent(t);
    // checking the lba child must be atomic with creating
    // and linking the absent child
    if (v.has_child()) {
      return std::move(v.get_child_fut());
    } else {
      return pin_to_extent_by_type(t, std::move(pin), type);
    }
  }

  rewrite_extent_ret rewrite_logical_extent(
    Transaction& t,
    LogicalCachedExtentRef extent);

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
    LBAMappingRef pin,
    extent_len_t direct_partial_off,
    extent_len_t partial_len) {
    static_assert(is_logical_type(T::TYPE));
    using ret = pin_to_extent_ret<T>;
    auto &pref = *pin;
    auto direct_length = pref.is_indirect() ?
      pref.get_intermediate_length() :
      pref.get_length();
    if (full_extent_integrity_check) {
      direct_partial_off = 0;
      partial_len = direct_length;
    }
    LOG_PREFIX(TransactionManager::pin_to_extent);
    SUBTRACET(seastore_tm, "getting absent extent from pin {}, 0x{:x}~0x{:x} ...",
              t, *pin, direct_partial_off, partial_len);
    return cache->get_absent_extent<T>(
      t,
      pref.get_val(),
      direct_length,
      direct_partial_off,
      partial_len,
      [&pref]
      (T &extent) mutable {
	assert(!extent.has_laddr());
	assert(!extent.has_been_invalidated());
	assert(!pref.has_been_invalidated());
	assert(pref.get_parent());
	pref.link_child(&extent);
	extent.maybe_set_intermediate_laddr(pref);
      }
    ).si_then([FNAME, &t, pin=std::move(pin), this](auto ref) mutable -> ret {
      if (ref->is_fully_loaded()) {
        auto crc = ref->calc_crc32c();
        SUBTRACET(
	  seastore_tm,
	  "got extent -- {}, chksum in the lba tree: {}, actual chksum: {}",
	  t,
	  *ref,
	  pin->get_checksum(),
	  crc);
        bool inconsistent = false;
        if (full_extent_integrity_check) {
	  inconsistent = (pin->get_checksum() != crc);
        } else { // !full_extent_integrity_check: remapped extent may be skipped
	  inconsistent = !(pin->get_checksum() == 0 ||
                           pin->get_checksum() == crc);
        }
        if (unlikely(inconsistent)) {
	  SUBERRORT(seastore_tm,
	    "extent checksum inconsistent, recorded: {}, actual: {}, {}",
	    t,
	    pin->get_checksum(),
	    crc,
	    *ref);
	  ceph_abort();
        }
      } else {
        assert(!full_extent_integrity_check);
      }
      return pin_to_extent_ret<T>(
	interruptible::ready_future_marker{},
	std::move(ref));
    });
  }

  /**
   * pin_to_extent_by_type
   *
   * Get extent mapped at pin.
   */
  using pin_to_extent_by_type_ret = pin_to_extent_iertr::future<
    LogicalCachedExtentRef>;
  pin_to_extent_by_type_ret pin_to_extent_by_type(
      Transaction &t,
      LBAMappingRef pin,
      extent_types_t type)
  {
    LOG_PREFIX(TransactionManager::pin_to_extent_by_type);
    SUBTRACET(seastore_tm, "getting absent extent from pin {} type {} ...",
              t, *pin, type);
    assert(is_logical_type(type));
    auto &pref = *pin;
    laddr_t direct_key;
    extent_len_t direct_length;
    if (pref.is_indirect()) {
      direct_key = pref.get_intermediate_base();
      direct_length = pref.get_intermediate_length();
    } else {
      direct_key = pref.get_key();
      direct_length = pref.get_length();
    }
    return cache->get_absent_extent_by_type(
      t,
      type,
      pref.get_val(),
      direct_key,
      direct_length,
      [&pref](CachedExtent &extent) mutable {
	auto &lextent = static_cast<LogicalCachedExtent&>(extent);
	assert(!lextent.has_laddr());
	assert(!lextent.has_been_invalidated());
	assert(!pref.has_been_invalidated());
	assert(pref.get_parent());
	assert(!pref.get_parent()->is_pending());
	pref.link_child(&lextent);
	lextent.maybe_set_intermediate_laddr(pref);
      }
    ).si_then([FNAME, &t, pin=std::move(pin), this](auto ref) {
      auto crc = ref->calc_crc32c();
      SUBTRACET(
	seastore_tm,
	"got extent -- {}, chksum in the lba tree: {}, actual chksum: {}",
	t,
	*ref,
	pin->get_checksum(),
	crc);
      assert(ref->is_fully_loaded());
      bool inconsistent = false;
      if (full_extent_integrity_check) {
	inconsistent = (pin->get_checksum() != crc);
      } else { // !full_extent_integrity_check: remapped extent may be skipped
	inconsistent = !(pin->get_checksum() == 0 ||
			 pin->get_checksum() == crc);
      }
      if (unlikely(inconsistent)) {
	SUBERRORT(seastore_tm,
	  "extent checksum inconsistent, recorded: {}, actual: {}, {}",
	  t,
	  pin->get_checksum(),
	  crc,
	  *ref);
	ceph_abort();
      }
      return pin_to_extent_by_type_ret(
	interruptible::ready_future_marker{},
	std::move(ref->template cast<LogicalCachedExtent>()));
    });
  }

  bool get_checksum_needed(paddr_t paddr) {
    if (paddr.is_record_relative()) {
      return journal->is_checksum_needed();
    }
    return epm->get_checksum_needed(paddr);
  }

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
