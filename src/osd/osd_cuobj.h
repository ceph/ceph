// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "include/buffer.h"
#include "include/common_fwd.h"
#include "osd/oob_placement.h"

namespace ceph { class Formatter; }

struct rdma_buffer;
class cuObjServer;

/**
 * Per-OSD cuObject RDMA endpoint backing CEPH_OSD_OP_READ_RDMA.
 *
 * Owns one cuObjServer (a DC initiator bound to the OSD's RDMA NIC)
 * and a pool of pre-registered host buffers that stripe data is staged
 * through on its way into client memory. Instantiated by OSD::init()
 * when osd_cuobj_enabled is set; PrimaryLogPG reaches it through
 * OSDService::cuobj.
 *
 * Thread safety: rdma_write() may be called concurrently from any
 * number of op worker threads. Each thread lazily allocates its own
 * cuObject channel (DCI); buffer-pool slots are claimed with atomic
 * compare-exchange.
 */
class OSDCuObj {
public:
  OSDCuObj(CephContext *cct, const std::string& rdma_ip, uint16_t rdma_port);
  ~OSDCuObj();

  OSDCuObj(const OSDCuObj&) = delete;
  OSDCuObj& operator=(const OSDCuObj&) = delete;

  /// true once the local RDMA session started successfully
  bool is_available() const;

  /**
   * RDMA-write bl into the client memory window described by the
   * opaque descriptor token, at the token's base address plus
   * client_offset. key is only used for telemetry. Blocks until the
   * transfer completes (bounded by the transport's timeout/retry
   * budget). Returns bytes written or a negative errno.
   */
  ssize_t rdma_write(const std::string& key, const ceph::buffer::list& bl,
		     const std::string& token, uint64_t client_offset);

  /**
   * Execute a placement plan: stage data once into a registered
   * buffer, then RDMA-write each triple's byte range to
   * token_base + triple.client_ofs, batching asynchronous submissions
   * on this thread's channel and polling them to completion. All or
   * nothing: returns total bytes pushed only if every triple
   * completed, else a negative errno (and the caller must deliver
   * inline instead). Blocks until the batch drains.
   */
  ssize_t execute_plan(const std::string& key,
		       const std::string& token,
		       const ceph::buffer::list& data,
		       const ceph::osd::oob::placement_plan& plan);

  /// asok/debug counters
  void dump_stats(ceph::Formatter* f) const;

private:
  struct BufEntry {
    void* ptr = nullptr;
    size_t size = 0;
    struct rdma_buffer* handle = nullptr;
    std::atomic<bool> in_use{false};
  };

  int do_init(const std::string& rdma_ip, uint16_t rdma_port);
  void do_shutdown();

  /// claim a pooled buffer of at least needed bytes, or register a
  /// transient one when the pool is exhausted or too small
  BufEntry* acquire_buffer(size_t needed, bool* transient);
  void release_buffer(BufEntry* buf, bool transient);

  /// lazily allocated per-thread channel (DCI); returns
  /// invalid_channel on allocation failure
  uint16_t get_channel_id();
  static constexpr uint16_t invalid_channel = UINT16_MAX;

  CephContext* m_cct;
  std::unique_ptr<cuObjServer> m_server;
  std::unique_ptr<BufEntry[]> m_pool;
  size_t m_pool_count = 0;
  size_t m_buf_size = 0;

  std::atomic<uint64_t> m_plans_started{0};
  std::atomic<uint64_t> m_plans_completed{0};
  std::atomic<uint64_t> m_plans_failed{0};
  std::atomic<uint64_t> m_bytes_pushed{0};
  std::atomic<uint32_t> m_writes_inflight{0};
  std::atomic<uint64_t> m_buffers_leaked{0};

  static thread_local uint16_t tls_channel_id;
  static thread_local bool tls_channel_valid;
};
