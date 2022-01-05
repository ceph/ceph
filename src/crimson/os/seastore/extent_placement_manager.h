// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include "seastar/core/gate.hh"

#include "crimson/common/condition_variable.h"
#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/lba_manager.h"

namespace crimson::os::seastore {

/**
 * ool_record_t
 *
 * Encapsulates logic for building and encoding an ool record destined for
 * an ool segment.
 *
 * Uses a metadata header to enable scanning the ool segment for gc purposes.
 * Introducing a seperate physical->logical mapping would enable removing the
 * metadata block overhead.
 */
class ool_record_t {
  class OolExtent {
  public:
    OolExtent(LogicalCachedExtentRef& lextent)
      : lextent(lextent) {}
    void set_ool_paddr(paddr_t addr) {
      ool_offset = addr;
    }
    paddr_t get_ool_paddr() const {
      return ool_offset;
    }
    bufferptr& get_bptr() {
      return lextent->get_bptr();
    }
    LogicalCachedExtentRef& get_lextent() {
      return lextent;
    }
  private:
    paddr_t ool_offset;
    LogicalCachedExtentRef lextent;
  };

public:
  ool_record_t(size_t block_size) : block_size(block_size) {}
  record_group_size_t get_encoded_record_length() {
    assert(extents.size() == record.extents.size());
    return record_group_size_t(record.size, block_size);
  }
  size_t get_wouldbe_encoded_record_length(LogicalCachedExtentRef& extent) {
    record_size_t rsize = record.size;
    rsize.account_extent(extent->get_bptr().length());
    return record_group_size_t(rsize, block_size).get_encoded_length();
  }
  ceph::bufferlist encode(segment_id_t segment,
                          segment_nonce_t nonce) {
    assert(extents.size() == record.extents.size());
    assert(!record.deltas.size());
    auto record_group = record_group_t(std::move(record), block_size);
    segment_off_t extent_offset = base + record_group.size.get_mdlength();
    for (auto& extent : extents) {
      extent.set_ool_paddr(
        paddr_t::make_seg_paddr(segment, extent_offset));
      extent_offset += extent.get_bptr().length();
    }
    assert(extent_offset ==
           (segment_off_t)(base + record_group.size.get_encoded_length()));
    return encode_records(record_group, journal_seq_t(), nonce);
  }
  void add_extent(LogicalCachedExtentRef& extent) {
    extents.emplace_back(extent);
    ceph::bufferlist bl;
    bl.append(extent->get_bptr());
    record.push_back(extent_t{
      extent->get_type(),
      extent->get_laddr(),
      std::move(bl)});
  }
  std::vector<OolExtent>& get_extents() {
    return extents;
  }
  void set_base(segment_off_t b) {
    base = b;
  }
  segment_off_t get_base() const {
    return base;
  }
  void clear() {
    record = {};
    extents.clear();
    base = MAX_SEG_OFF;
  }
  uint64_t get_num_extents() const {
    return extents.size();
  }

private:
  std::vector<OolExtent> extents;
  record_t record;
  size_t block_size;
  segment_off_t base = MAX_SEG_OFF;
};

/**
 * ExtentOolWriter
 *
 * Interface through which final write to ool segment is performed.
 */
class ExtentOolWriter {
public:
  using write_iertr = trans_iertr<crimson::errorator<
    crimson::ct_error::input_output_error, // media error or corruption
    crimson::ct_error::invarg,             // if offset is < write pointer or misaligned
    crimson::ct_error::ebadf,              // segment closed
    crimson::ct_error::enospc              // write exceeds segment size
    >>;

  using stop_ertr = Segment::close_ertr;
  virtual stop_ertr::future<> stop() = 0;
  virtual write_iertr::future<> write(
    Transaction& t,
    std::list<LogicalCachedExtentRef>& extent) = 0;
  virtual ~ExtentOolWriter() {}
};

/**
 * ExtentAllocator
 *
 * Handles allocating ool extents from a specific family of targets.
 */
class ExtentAllocator {
public:
  using alloc_paddr_iertr = trans_iertr<crimson::errorator<
    crimson::ct_error::input_output_error, // media error or corruption
    crimson::ct_error::invarg,             // if offset is < write pointer or misaligned
    crimson::ct_error::ebadf,              // segment closed
    crimson::ct_error::enospc              // write exceeds segment size
    >>;

  virtual alloc_paddr_iertr::future<> alloc_ool_extents_paddr(
    Transaction& t,
    std::list<LogicalCachedExtentRef>&) = 0;

  using stop_ertr = ExtentOolWriter::stop_ertr;
  virtual stop_ertr::future<> stop() = 0;
  virtual ~ExtentAllocator() {};
};
using ExtentAllocatorRef = std::unique_ptr<ExtentAllocator>;

struct open_segment_wrapper_t : public boost::intrusive_ref_counter<
  open_segment_wrapper_t,
  boost::thread_unsafe_counter> {
  SegmentRef segment;
  std::list<seastar::future<>> inflight_writes;
  bool outdated = false;
};

using open_segment_wrapper_ref =
  boost::intrusive_ptr<open_segment_wrapper_t>;

/**
 * SegmentedAllocator
 *
 * Handles out-of-line writes to a SegmentManager device (such as a ZNS device
 * or conventional flash device where sequential writes are heavily preferred).
 *
 * Creates <seastore_init_rewrite_segments_per_device> Writer instances
 * internally to round-robin writes.  Later work will partition allocations
 * based on hint (age, presumably) among the created Writers.

 * Each Writer makes use of SegmentProvider to obtain a new segment for writes
 * as needed.
 */
class SegmentedAllocator : public ExtentAllocator {
  class Writer : public ExtentOolWriter {
  public:
    Writer(
      SegmentProvider& sp,
      SegmentManager& sm,
      LBAManager& lba_manager,
      Journal& journal,
      Cache& cache)
      : segment_provider(sp),
        segment_manager(sm),
        lba_manager(lba_manager),
        journal(journal),
        cache(cache)
    {}
    Writer(Writer &&) = default;

    write_iertr::future<> write(
      Transaction& t,
      std::list<LogicalCachedExtentRef>& extent) final;
    stop_ertr::future<> stop() final {
      return writer_guard.close().then([this] {
        return crimson::do_for_each(open_segments, [](auto& seg_wrapper) {
          return seg_wrapper->segment->close();
        });
      });
    }
  private:
    using update_lba_mapping_iertr = LBAManager::update_le_mapping_iertr;
    using finish_record_iertr = update_lba_mapping_iertr;
    using finish_record_ret = finish_record_iertr::future<>;
    finish_record_ret finish_write(
      Transaction& t,
      ool_record_t& record);
    bool _needs_roll(segment_off_t length) const;

    write_iertr::future<> _write(
      Transaction& t,
      ool_record_t& record);

    using roll_segment_ertr = crimson::errorator<
      crimson::ct_error::input_output_error>;
    roll_segment_ertr::future<> roll_segment(bool);

    using init_segment_ertr = crimson::errorator<
      crimson::ct_error::input_output_error>;
    init_segment_ertr::future<> init_segment(Segment& segment);

    void add_extent_to_write(
      ool_record_t&,
      LogicalCachedExtentRef& extent);

    SegmentProvider& segment_provider;
    SegmentManager& segment_manager;
    open_segment_wrapper_ref current_segment;
    std::list<open_segment_wrapper_ref> open_segments;
    segment_off_t allocated_to = 0;
    LBAManager& lba_manager;
    Journal& journal;
    crimson::condition_variable segment_rotation_guard;
    seastar::gate writer_guard;
    bool rolling_segment = false;
    Cache& cache;
  };
public:
  SegmentedAllocator(
    SegmentProvider& sp,
    SegmentManager& sm,
    LBAManager& lba_manager,
    Journal& journal,
    Cache& cache);

  Writer &get_writer(placement_hint_t hint) {
    return writers[std::rand() % writers.size()];
  }

  alloc_paddr_iertr::future<> alloc_ool_extents_paddr(
    Transaction& t,
    std::list<LogicalCachedExtentRef>& extents) final {
    LOG_PREFIX(SegmentedAllocator::alloc_ool_extents_paddr);
    SUBDEBUGT(seastore_tm, "start", t);
    return seastar::do_with(
      std::map<Writer*, std::list<LogicalCachedExtentRef>>(),
      [this, extents=std::move(extents), &t](auto& alloc_map) {
      for (auto& extent : extents) {
        auto writer = &(get_writer(extent->hint));
        alloc_map[writer].emplace_back(extent);
      }
      return trans_intr::do_for_each(alloc_map, [&t](auto& p) {
        auto writer = p.first;
        auto& extents_to_pesist = p.second;
        return writer->write(t, extents_to_pesist);
      });
    });
  }

  stop_ertr::future<> stop() {
    return crimson::do_for_each(writers, [](auto& writer) {
      return writer.stop();
    });
  }
private:
  SegmentProvider& segment_provider;
  SegmentManager& segment_manager;
  std::vector<Writer> writers;
  LBAManager& lba_manager;
  Journal& journal;
  Cache& cache;
};

class ExtentPlacementManager {
public:
  ExtentPlacementManager(
    Cache& cache,
    LBAManager& lba_manager
  ) : cache(cache), lba_manager(lba_manager) {}

  /**
   * alloc_new_extent_by_type
   *
   * Create a new extent, CachedExtent::poffset may not be set
   * if a delayed allocation is needed.
   */
  CachedExtentRef alloc_new_extent_by_type(
    Transaction& t,
    extent_types_t type,
    segment_off_t length,
    placement_hint_t hint) {
    // only logical extents should fall in this path
    assert(is_logical_type(type));
    assert(hint < placement_hint_t::NUM_HINTS);
    auto dtype = get_allocator_type(hint);
    // FIXME: set delay for COLD extent when the record overhead is low
    bool delay = (hint > placement_hint_t::COLD &&
                  can_delay_allocation(dtype));
    CachedExtentRef extent = cache.alloc_new_extent_by_type(
        t, type, length, delay);
    extent->backend_type = dtype;
    extent->hint = hint;
    return extent;
  }

  template<
    typename T,
    std::enable_if_t<std::is_base_of_v<LogicalCachedExtent, T>, int> = 0>
  TCachedExtentRef<T> alloc_new_extent(
    Transaction& t,
    segment_off_t length,
    placement_hint_t hint) {
    // only logical extents should fall in this path
    static_assert(is_logical_type(T::TYPE));
    assert(hint < placement_hint_t::NUM_HINTS);
    auto dtype = get_allocator_type(hint);
    // FIXME: set delay for COLD extent when the record overhead is low
    bool delay = (hint > placement_hint_t::COLD &&
                  can_delay_allocation(dtype));
    TCachedExtentRef<T> extent = cache.alloc_new_extent<T>(
        t, length, delay);
    extent->backend_type = dtype;
    extent->hint = hint;
    return extent;
  }

  /**
   * delayed_alloc_or_ool_write
   *
   * Performs any outstanding ool writes and updates pending lba updates
   * accordingly
   */
  using alloc_paddr_iertr = ExtentOolWriter::write_iertr;
  alloc_paddr_iertr::future<> delayed_alloc_or_ool_write(
    Transaction& t) {
    LOG_PREFIX(ExtentPlacementManager::delayed_alloc_or_ool_write);
    SUBDEBUGT(seastore_tm, "start", t);
    return seastar::do_with(
        std::map<ExtentAllocator*, std::list<LogicalCachedExtentRef>>(),
        [this, &t](auto& alloc_map) {
      LOG_PREFIX(ExtentPlacementManager::delayed_alloc_or_ool_write);
      auto& alloc_list = t.get_delayed_alloc_list();
      uint64_t num_ool_extents = 0;
      for (auto& extent : alloc_list) {
        // extents may be invalidated
        if (!extent->is_valid()) {
          t.increment_delayed_invalid_extents();
          continue;
        }
        // For now, just do ool allocation for any delayed extent
        auto& allocator_ptr = get_allocator(
          extent->backend_type, extent->hint
        );
        alloc_map[allocator_ptr.get()].emplace_back(extent);
        num_ool_extents++;
      }
      SUBDEBUGT(seastore_tm, "{} ool extents", t, num_ool_extents);
      return trans_intr::do_for_each(alloc_map, [&t](auto& p) {
        auto allocator = p.first;
        auto& extents = p.second;
        return allocator->alloc_ool_extents_paddr(t, extents);
      });
    });
  }

  void add_allocator(device_type_t type, ExtentAllocatorRef&& allocator) {
    allocators[type].emplace_back(std::move(allocator));
    LOG_PREFIX(ExtentPlacementManager::add_allocator);
    SUBDEBUG(seastore_tm, "allocators for {}: {}",
      type,
      allocators[type].size());
  }

private:
  device_type_t get_allocator_type(placement_hint_t hint) {
    return device_type_t::SEGMENTED;
  }

  ExtentAllocatorRef& get_allocator(
    device_type_t type,
    placement_hint_t hint) {
    auto& devices = allocators[type];
    return devices[std::rand() % devices.size()];
  }

  Cache& cache;
  LBAManager& lba_manager;
  std::map<device_type_t, std::vector<ExtentAllocatorRef>> allocators;
};
using ExtentPlacementManagerRef = std::unique_ptr<ExtentPlacementManager>;

}
