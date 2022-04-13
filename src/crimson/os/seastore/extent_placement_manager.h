// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include "seastar/core/gate.hh"

#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/journal/segment_allocator.h"
#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/transaction.h"

namespace crimson::os::seastore {

/**
 * ExtentOolWriter
 *
 * Write the extents as out-of-line and allocate the physical addresses.
 * Different writers write extents to different locations.
 */
class ExtentOolWriter {
  using base_ertr = crimson::errorator<
      crimson::ct_error::input_output_error>;
public:
  virtual ~ExtentOolWriter() {}

  using open_ertr = base_ertr;
  virtual open_ertr::future<> open() = 0;

  using alloc_write_ertr = base_ertr;
  using alloc_write_iertr = trans_iertr<alloc_write_ertr>;
  virtual alloc_write_iertr::future<> alloc_write_ool_extents(
    Transaction &t,
    std::list<LogicalCachedExtentRef> &extents) = 0;

  using close_ertr = base_ertr;
  virtual close_ertr::future<> close() = 0;
};
using ExtentOolWriterRef = std::unique_ptr<ExtentOolWriter>;

class SegmentProvider;

/**
 * SegmentedOolWriter
 *
 * Different writers write extents to different out-of-line segments provided
 * by the SegmentProvider.
 */
class SegmentedOolWriter : public ExtentOolWriter {
public:
  SegmentedOolWriter(std::string name,
                     SegmentProvider &sp,
                     SegmentSeqAllocator &ssa);

  open_ertr::future<> open() final {
    return record_submitter.open().discard_result();
  }

  alloc_write_iertr::future<> alloc_write_ool_extents(
    Transaction &t,
    std::list<LogicalCachedExtentRef> &extents) final;

  close_ertr::future<> close() final {
    return write_guard.close().then([this] {
      return record_submitter.close();
    }).safe_then([this] {
      write_guard = seastar::gate();
    });
  }

private:
  alloc_write_iertr::future<> do_write(
    Transaction& t,
    std::list<LogicalCachedExtentRef> &extent);

  alloc_write_ertr::future<> write_record(
    Transaction& t,
    record_t&& record,
    std::list<LogicalCachedExtentRef> &&extents);

  journal::SegmentAllocator segment_allocator;
  journal::RecordSubmitter record_submitter;
  seastar::gate write_guard;
};

class ExtentPlacementManager {
public:
  ExtentPlacementManager() {
    devices_by_id.resize(DEVICE_ID_GLOBAL_MAX, nullptr);
  }

  void init_ool_writers(SegmentProvider &sp, SegmentSeqAllocator &ssa) {
    // Currently only one SegmentProvider is supported, so hardcode the
    // writers_by_hint for now.
    writer_seed = 0;
    writer_refs.clear();
    writers_by_hint.resize((std::size_t)placement_hint_t::NUM_HINTS, {});

    // ool writer is not supported for placement_hint_t::HOT
    writer_refs.emplace_back(
        std::make_unique<SegmentedOolWriter>("COLD", sp, ssa));
    writers_by_hint[(std::size_t)placement_hint_t::COLD
                   ].emplace_back(writer_refs.back().get());
    writer_refs.emplace_back(
        std::make_unique<SegmentedOolWriter>("REWRITE", sp, ssa));
    writers_by_hint[(std::size_t)placement_hint_t::REWRITE
                   ].emplace_back(writer_refs.back().get());
  }

  void add_device(Device* device, bool is_primary) {
    auto device_id = device->get_device_id();
    ceph_assert(devices_by_id[device_id] == nullptr);
    devices_by_id[device_id] = device;
    if (is_primary) {
      ceph_assert(primary_device == nullptr);
      primary_device = device;
    }
  }

  seastore_off_t get_block_size() const {
    assert(primary_device != nullptr);
    // assume all the devices have the same block size
    return primary_device->get_block_size();
  }

  Device& get_primary_device() {
    assert(primary_device != nullptr);
    return *primary_device;
  }

  using open_ertr = ExtentOolWriter::open_ertr;
  open_ertr::future<> open() {
    LOG_PREFIX(ExtentPlacementManager::open);
    SUBINFO(seastore_journal, "started");
    return crimson::do_for_each(writers_by_hint, [](auto& writers) {
      return crimson::do_for_each(writers, [](auto& writer) {
        return writer->open();
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

    // FIXME: set delay for COLD extent and improve GC
    // NOTE: delay means to delay the decision about whether to write the
    // extent as inline or out-of-line extents.
    bool delay = (hint > placement_hint_t::COLD);
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
  using alloc_paddr_iertr = ExtentOolWriter::alloc_write_iertr;
  alloc_paddr_iertr::future<> delayed_alloc_or_ool_write(
    Transaction& t,
    const std::list<LogicalCachedExtentRef>& delayed_extents) {
    LOG_PREFIX(ExtentPlacementManager::delayed_alloc_or_ool_write);
    SUBDEBUGT(seastore_journal, "start with {} delayed extents",
              t, delayed_extents.size());
    return seastar::do_with(
        std::map<ExtentOolWriter*, std::list<LogicalCachedExtentRef>>(),
        [this, &t, &delayed_extents](auto& alloc_map) {
      for (auto& extent : delayed_extents) {
        // For now, just do ool allocation for any delayed extent
        auto writer_ptr = get_writer(extent->hint);
        alloc_map[writer_ptr].emplace_back(extent);
      }
      return trans_intr::do_for_each(alloc_map, [&t](auto& p) {
        auto writer = p.first;
        auto& extents = p.second;
        return writer->alloc_write_ool_extents(t, extents);
      });
    });
  }

  using close_ertr = ExtentOolWriter::close_ertr;
  close_ertr::future<> close() {
    LOG_PREFIX(ExtentPlacementManager::close);
    SUBINFO(seastore_journal, "started");
    return crimson::do_for_each(writers_by_hint, [](auto& writers) {
      return crimson::do_for_each(writers, [](auto& writer) {
        return writer->close();
      });
    }).safe_then([this] {
      devices_by_id.clear();
      devices_by_id.resize(DEVICE_ID_GLOBAL_MAX, nullptr);
      primary_device = nullptr;
    });
  }

  using read_ertr = Device::read_ertr;
  read_ertr::future<> read(
    paddr_t addr,
    size_t len,
    ceph::bufferptr &out
  ) {
    assert(devices_by_id[addr.get_device_id()] != nullptr);
    return devices_by_id[addr.get_device_id()]->read(addr, len, out);
  }

private:
  ExtentOolWriter* get_writer(placement_hint_t hint) {
    assert(hint < placement_hint_t::NUM_HINTS);
    auto hint_index = static_cast<std::size_t>(hint);
    assert(hint_index < writers_by_hint.size());
    auto& writers = writers_by_hint[hint_index];
    assert(writers.size() > 0);
    return writers[writer_seed++ % writers.size()];
  }

  std::size_t writer_seed = 0;
  std::vector<ExtentOolWriterRef> writer_refs;
  std::vector<std::vector<ExtentOolWriter*>> writers_by_hint;
  std::vector<Device*> devices_by_id;
  Device* primary_device = nullptr;
};
using ExtentPlacementManagerRef = std::unique_ptr<ExtentPlacementManager>;

}
