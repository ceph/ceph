// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <optional>
#include <string>

#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/internal/poll.hh>   // complete seastar::reactor::poller
#include <seastar/core/timer.hh>
#include <seastar/core/lowres_clock.hh>

#include "include/buffer_fwd.h"
#include "crimson/common/errorator.h"

struct spdk_nvme_ctrlr;
struct spdk_nvme_ns;
struct spdk_nvme_qpair;
struct spdk_nvme_cpl;
struct spdk_nvme_cmd;

namespace crimson::os::seastore {

/**
 * spdk_nvme_io
 *
 * Transport-only SPDK machinery shared by SPDKBlockIODriver (data path) and
 * SPDKNVMeIODriver (NVMe control plane), so neither duplicates it and there is
 * no inheritance diamond. It owns the controller/namespace handles, the
 * per-shard queue pair and its Seastar completion poller, and bridges SPDK
 * completion callbacks to Seastar futures.
 *
 * One instance per shard. probe()/detach() are process/controller scoped;
 * start_shard()/stop_shard() and submit_io() are per-shard and must run on the
 * shard that owns the queue pair.
 */
class spdk_nvme_io {
public:
  spdk_nvme_io() = default;
  ~spdk_nvme_io();

  spdk_nvme_io(const spdk_nvme_io&) = delete;
  spdk_nvme_io& operator=(const spdk_nvme_io&) = delete;

  /// Initialize the SPDK env (once process-wide) and attach to the controller
  /// at transport_id, storing its first active namespace. Blocking probe; call
  /// at open() time.
  seastar::future<> probe(const std::string& transport_id);

  /// Detach from the controller.
  seastar::future<> detach();

  /// Allocate this shard's io queue pair and register the completion poller.
  void start_shard();
  /// Free this shard's queue pair and poller.
  void stop_shard();

  /// errorator shared with BlockIODriver (errorator<input_output_error>).
  using io_ertr = crimson::errorator<crimson::ct_error::input_output_error>;

  /// Submit a read or write of len bytes at absolute byte offset; the future
  /// resolves on completion. buf must stay alive (and, for writes, unchanged)
  /// until then, and must be DMA-safe (see raw_spdk_dma). io_flags carries
  /// NVMe command flags (e.g. PRACT|PRCHK_GUARD for end-to-end protection).
  seastar::future<> submit_io(
    bool is_write, uint64_t offset, void* buf, size_t len,
    uint32_t io_flags = 0);

  /// DMA-safe read/write with an io_uring-equivalent errorator result: bounces
  /// `data` through a hugepage buffer when it is not already DMA-safe, asserts
  /// shard affinity, and maps SPDK errors to input_output_error. Shared by
  /// SPDKBlockIODriver and SPDKNVMeIODriver.
  io_ertr::future<> do_io(
    bool is_write, uint64_t offset, char* data, size_t len,
    uint32_t io_flags = 0);

  /// Write a whole (block-aligned) bufferlist in a single NVMe command via a
  /// sector-aligned SGL. Data extents that are already sector-aligned DMA
  /// buffers travel zero-copy; only the sub-sector encoded-metadata fragments
  /// are coalesced into a sector-aligned hugepage buffer (the small copy).
  /// Relies on seastore's [block-aligned metadata][block-aligned data] record
  /// layout, so it needs no on-disk change. Replaces the per-backend
  /// "rebuild_aligned + per-fragment bounce" path; shared by both
  /// SPDKBlockIODriver and SPDKNVMeIODriver.
  io_ertr::future<> do_writev(
    uint64_t offset, ceph::bufferlist&& bl, uint32_t io_flags = 0);

  /// Submit a raw NVMe admin command; returns 0 on success, -1 on error. The
  /// admin queue is pumped (process_admin_completions) until completion.
  seastar::future<int> admin_raw(spdk_nvme_cmd& cmd, void* buf, size_t len);
  /// Submit a raw NVMe I/O command on this shard's qpair (completed by the
  /// existing poller); returns 0 on success, -1 on error.
  seastar::future<int> io_raw(spdk_nvme_cmd& cmd, void* buf, size_t len);

  uint64_t size_bytes() const;
  uint32_t block_size() const;
  uint32_t get_nsid() const { return nsid; }

  spdk_nvme_ctrlr* get_ctrlr() const { return ctrlr; }
  spdk_nvme_ns* get_ns() const { return ns; }
  bool attached() const { return ctrlr != nullptr && ns != nullptr; }

private:
  static void io_complete(void* arg, const spdk_nvme_cpl* cpl);

  /// Submit a prepared SGL write context (heap-owned; freed in its completion
  /// callback), with the same ENOMEM drain-and-retry flow as submit_io().
  seastar::future<> submit_writev(
    void* writev_ctx, uint64_t lba, uint32_t lba_count, uint32_t io_flags);


  std::string transport_id;
  spdk_nvme_ctrlr* ctrlr = nullptr;
  spdk_nvme_ns* ns = nullptr;
  uint32_t sector_size = 0;
  uint32_t nsid = 0;

  spdk_nvme_qpair* qpair = nullptr;
  std::optional<seastar::reactor::poller> poller;
  std::optional<seastar::timer<seastar::lowres_clock>> admin_timer;
  unsigned owning_shard = 0;
};

}
