// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include "seastar/core/gate.hh"
#include "seastar/core/shared_future.hh"

#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/journal/segment_allocator.h"
#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/transaction.h"

namespace crimson::os::seastore {

/**
 * ExtentOolWriter
 *
 * Interface through which final write to ool segment is performed.
 */
class ExtentOolWriter {
  using base_ertr = crimson::errorator<
      crimson::ct_error::input_output_error>;
public:
  virtual ~ExtentOolWriter() {}

  using open_ertr = base_ertr;
  virtual open_ertr::future<> open() = 0;

  using write_iertr = trans_iertr<base_ertr>;
  virtual write_iertr::future<> write(
    Transaction& t,
    std::list<LogicalCachedExtentRef>& extent) = 0;

  using stop_ertr = base_ertr;
  virtual stop_ertr::future<> stop() = 0;
};

/**
 * ExtentAllocator
 *
 * Handles allocating ool extents from a specific family of targets.
 */
class ExtentAllocator {
public:
  using open_ertr = ExtentOolWriter::open_ertr;
  virtual open_ertr::future<> open() = 0;

  using alloc_paddr_iertr = ExtentOolWriter::write_iertr;
  virtual alloc_paddr_iertr::future<> alloc_ool_extents_paddr(
    Transaction& t,
    std::list<LogicalCachedExtentRef>&) = 0;

  using stop_ertr = ExtentOolWriter::stop_ertr;
  virtual stop_ertr::future<> stop() = 0;
  virtual ~ExtentAllocator() {};
};
using ExtentAllocatorRef = std::unique_ptr<ExtentAllocator>;

class SegmentProvider;

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
    Writer(SegmentProvider& sp, SegmentManager& sm)
      : segment_allocator("OOL", segment_type_t::OOL, sp, sm) {}

    Writer(Writer &&) = default;

    open_ertr::future<> open() final;

    write_iertr::future<> write(
      Transaction& t,
      std::list<LogicalCachedExtentRef>& extent) final;

    stop_ertr::future<> stop() final {
      return write_guard.close().then([this] {
        return segment_allocator.close();
      }).safe_then([this] {
        write_guard = seastar::gate();
      });
    }

  private:
    write_iertr::future<> do_write(
      Transaction& t,
      std::list<LogicalCachedExtentRef>& extent);

    write_iertr::future<> _write(
      Transaction& t,
      record_t&& record,
      std::list<LogicalCachedExtentRef>&& extents);

    journal::SegmentAllocator segment_allocator;
    std::optional<seastar::shared_promise<>> roll_promise;
    seastar::gate write_guard;
  };
public:
  SegmentedAllocator(
    SegmentProvider& sp,
    SegmentManager& sm);

  Writer &get_writer(placement_hint_t hint) {
    if (hint == placement_hint_t::REWRITE) {
      return rewriter;
    } else {
      return writers[std::rand() % writers.size()];
    }
  }

  open_ertr::future<> open() {
    return crimson::do_for_each(writers, [](auto& writer) {
      return writer.open();
    });
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
  Writer rewriter;
  std::vector<Writer> writers;
};

class ExtentPlacementManager {
public:
  ExtentPlacementManager() = default;

  void add_allocator(device_type_t type, ExtentAllocatorRef&& allocator) {
    allocators[type].emplace_back(std::move(allocator));
    LOG_PREFIX(ExtentPlacementManager::add_allocator);
    SUBDEBUG(seastore_tm, "allocators for {}: {}",
      type,
      allocators[type].size());
  }

  using open_ertr = ExtentOolWriter::open_ertr;
  open_ertr::future<> open() {
    LOG_PREFIX(ExtentPlacementManager::open);
    SUBINFO(seastore_tm, "started");
    return crimson::do_for_each(allocators, [](auto& allocators_item) {
      return crimson::do_for_each(allocators_item.second, [](auto& allocator) {
        return allocator->open();
      });
    });
  }

  struct alloc_result_t {
    paddr_t paddr;
    bufferptr bp;
  };
  alloc_result_t alloc_new_extent(
    Transaction& t,
    extent_types_t type,
    seastore_off_t length,
    placement_hint_t hint
  ) {
    assert(hint < placement_hint_t::NUM_HINTS);

    // XXX: bp might be extended to point to differnt memory (e.g. PMem)
    // according to the allocator.
    auto bp = ceph::bufferptr(
      buffer::create_page_aligned(length));
    bp.zero();

    if (!is_logical_type(type)) {
      // TODO: implement out-of-line strategy for physical extent.
      return {make_record_relative_paddr(0),
              std::move(bp)};
    }

    // FIXME: set delay for COLD extent when the record overhead is low
    // NOTE: delay means to delay the decision about whether to write the
    // extent as inline or out-of-line extents.
    bool delay = (hint > placement_hint_t::COLD &&
                  can_delay_allocation(get_allocator_type(hint)));
    if (delay) {
      return {make_delayed_temp_paddr(0),
              std::move(bp)};
    } else {
      return {make_record_relative_paddr(0),
              std::move(bp)};
    }
  }

  /**
   * delayed_alloc_or_ool_write
   *
   * Performs delayed allocation and do writes for out-of-line extents.
   */
  using alloc_paddr_iertr = ExtentOolWriter::write_iertr;
  alloc_paddr_iertr::future<> delayed_alloc_or_ool_write(
    Transaction& t,
    const std::list<LogicalCachedExtentRef>& delayed_extents) {
    LOG_PREFIX(ExtentPlacementManager::delayed_alloc_or_ool_write);
    SUBDEBUGT(seastore_tm, "start with {} delayed extents",
              t, delayed_extents.size());
    return seastar::do_with(
        std::map<ExtentAllocator*, std::list<LogicalCachedExtentRef>>(),
        [this, &t, &delayed_extents](auto& alloc_map) {
      for (auto& extent : delayed_extents) {
        // For now, just do ool allocation for any delayed extent
        auto& allocator_ptr = get_allocator(
          get_allocator_type(extent->hint), extent->hint
        );
        alloc_map[allocator_ptr.get()].emplace_back(extent);
      }
      return trans_intr::do_for_each(alloc_map, [&t](auto& p) {
        auto allocator = p.first;
        auto& extents = p.second;
        return allocator->alloc_ool_extents_paddr(t, extents);
      });
    });
  }

  using close_ertr = ExtentOolWriter::stop_ertr;
  close_ertr::future<> close() {
    LOG_PREFIX(ExtentPlacementManager::close);
    SUBINFO(seastore_tm, "started");
    return crimson::do_for_each(allocators, [](auto& allocators_item) {
      return crimson::do_for_each(allocators_item.second, [](auto& allocator) {
        return allocator->stop();
      });
    });
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

  std::map<device_type_t, std::vector<ExtentAllocatorRef>> allocators;
};
using ExtentPlacementManagerRef = std::unique_ptr<ExtentPlacementManager>;

}
