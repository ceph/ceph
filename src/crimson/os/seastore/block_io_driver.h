// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <string>
#include <vector>

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>

#include "include/buffer_fwd.h"

#include "crimson/common/errorator.h"
#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore {

/**
 * BlockIODriver
 *
 * Shared low-level raw block I/O primitive used by both SeaStore backends:
 * the segmented backend (BlockSegmentManager) and the random-block backend
 * (NVMeBlockDevice). It abstracts byte-addressed DMA reads and writes against
 * an underlying device so the transport (kernel io_uring vs. SPDK polled NVMe)
 * can be swapped without touching the backends above it.
 *
 * A driver instance is owned by, and used on, a single Seastar shard. All
 * offsets are absolute byte offsets into the device.
 */
class BlockIODriver {
public:
  using write_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using read_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using open_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::permission_denied,
    crimson::ct_error::enoent>;
  using close_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;

  virtual ~BlockIODriver() = default;

  /// Open the underlying device and return its detected {size, block_size}.
  virtual open_ertr::future<seastar::stat_data> open(
    const std::string &path,
    seastar::open_flags flags) = 0;

  /// Close the underlying device.
  virtual close_ertr::future<> close() = 0;

  /// Write a single buffer at the given absolute device offset.
  virtual write_ertr::future<> write(
    device_id_t device_id,
    uint64_t offset,
    bufferptr &bptr) = 0;

  /// Scatter-write a bufferlist at the given absolute device offset.
  /// Buffers are re-aligned to block_size as required by the transport.
  virtual write_ertr::future<> writev(
    device_id_t device_id,
    uint64_t offset,
    bufferlist &&bl,
    size_t block_size) = 0;

  /// Read len bytes at the given absolute device offset into out.
  virtual read_ertr::future<> read(
    device_id_t device_id,
    uint64_t offset,
    size_t len,
    bufferptr &out) = 0;

  /// Gather-read at the given absolute device offset into ptrs.
  virtual read_ertr::future<> readv(
    device_id_t device_id,
    uint64_t offset,
    std::vector<bufferptr> ptrs) = 0;

  /// Allocate a buffer suitable for DMA on this transport. The default is a
  /// plain page-aligned buffer; the SPDK driver overrides this to allocate
  /// from its registered hugepage pool, enabling the zero-copy paths.
  virtual ceph::unique_leakable_ptr<ceph::buffer::raw> alloc_io_buffer(
    size_t len);

  /// Whether alloc_io_buffer() yields DMA-passthrough memory, i.e. a single
  /// sector-aligned buffer can be handed to the device with zero coalescing
  /// copy (SPDK). The kernel transport returns false. Lets the journal encode
  /// record metadata in place into such a buffer instead of bouncing it.
  virtual bool dma_passthrough() const { return false; }
};
using BlockIODriverRef = std::unique_ptr<BlockIODriver>;

/**
 * make_block_io_driver
 *
 * The single point that selects the I/O transport. Returns the SPDK driver
 * when seastore_spdk_transport_id is configured (and supported for dtype),
 * otherwise the kernel/io_uring driver. dtype lets the factory reject
 * transports that a device type cannot use (e.g. SPDK for ZBD/ZNS).
 *
 * Construction is synchronous and non-blocking for both transports (the SPDK
 * driver defers env init / device probe to open()), so the result is the
 * driver directly rather than a future.
 */
BlockIODriverRef make_block_io_driver(
  const std::string &path,
  device_type_t dtype);

}
