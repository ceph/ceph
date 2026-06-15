// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <string>
#include <vector>

#include "crimson/os/seastore/block_io_driver.h"
#include "crimson/os/seastore/spdk_nvme_io.h"

namespace crimson::os::seastore {

/**
 * SPDKBlockIODriver
 *
 * Tier-1 SPDK transport: a BlockIODriver whose data path runs over an SPDK NVMe
 * queue pair (polled from the Seastar reactor) instead of io_uring. Consumed by
 * the segmented backend; the RBM backend uses SPDKNVMeIODriver, which composes
 * the same spdk_nvme_io helper.
 *
 * One instance per shard. The transport id (PCI address) is supplied at
 * construction; open()'s path argument is ignored (the device is the transport
 * id). Buffers are allocated DMA-safe via alloc_io_buffer(); I/O whose buffer is
 * not already DMA-safe is bounced through a temporary hugepage buffer.
 */
class SPDKBlockIODriver final : public BlockIODriver {
public:
  explicit SPDKBlockIODriver(std::string transport_id)
    : transport_id(std::move(transport_id)) {}

  open_ertr::future<seastar::stat_data> open(
    const std::string &path, seastar::open_flags flags) final;
  close_ertr::future<> close() final;

  write_ertr::future<> write(
    device_id_t device_id, uint64_t offset, bufferptr &bptr) final;
  write_ertr::future<> writev(
    device_id_t device_id, uint64_t offset,
    bufferlist &&bl, size_t block_size) final;
  read_ertr::future<> read(
    device_id_t device_id, uint64_t offset, size_t len, bufferptr &out) final;
  read_ertr::future<> readv(
    device_id_t device_id, uint64_t offset, std::vector<bufferptr> ptrs) final;

  ceph::unique_leakable_ptr<ceph::buffer::raw> alloc_io_buffer(size_t len) final;
  bool dma_passthrough() const final { return true; }

private:
  std::string transport_id;
  spdk_nvme_io nvme;
};

}
