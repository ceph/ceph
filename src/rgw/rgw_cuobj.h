// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "acconfig.h"
#include "include/common_fwd.h"

struct rdma_buffer;
class cuObjServer;
class cuObjClient;

class RGWCuObjServer {
public:
  struct RDMABufEntry {
    void* ptr = nullptr;
    size_t size = 0;
    struct rdma_buffer* handle = nullptr;
    std::atomic<bool> in_use{false};
    /// descriptor of this buffer as an RDMA target (rgw_cuobj_osd_push):
    /// carried on stripe reads so OSDs write object data straight in
    std::string token;
    char* token_raw = nullptr; ///< owned by the client library
    /// a buffer an OSD might still write into (a resent op whose first
    /// attempt we lost track of) stays out of circulation until then
    std::atomic<int64_t> quarantine_until_ns{0};
  };

  ~RGWCuObjServer();

  static int init(CephContext* cct);
  static void shutdown();
  static RGWCuObjServer* get_instance();

  bool is_available() const;
  /// true when pool buffers carry tokens OSDs can push into
  bool push_target_available() const { return m_push_target; }

  RDMABufEntry* acquire_buffer(size_t needed_size);
  /**
   * Return a buffer. quarantine_ms > 0 keeps it unavailable that long,
   * for a buffer some OSD may still RDMA-write into (the pool's
   * delivery lease plus the transport drain bound).
   */
  void release_buffer(RDMABufEntry* buf, uint64_t quarantine_ms = 0);

  static size_t parse_rdma_descriptor_size(const std::string& rdma_descr);

  ssize_t rdma_read_from_client(const std::string& key,
                                RDMABufEntry* buf,
                                uint64_t remote_offset,
                                size_t size,
                                const std::string& rdma_descr);

  ssize_t rdma_write_to_client(const std::string& key,
                               RDMABufEntry* buf,
                               uint64_t remote_offset,
                               size_t size,
                               const std::string& rdma_descr);

private:
  RGWCuObjServer() = default;

  int do_init(CephContext* cct);
  void do_shutdown();

  /// the library's allocation-failure value
  static constexpr uint16_t invalid_channel = UINT16_MAX;
  /// a channel may not be shared by concurrent callers, and the library
  /// has only rgw_cuobj_num_dcis of them while the frontend may run far
  /// more threads; so they are pooled and held for one RDMA transfer.
  /// acquire_channel() waits for a free one
  uint16_t acquire_channel();
  void release_channel(uint16_t channel);

  /// register the pool with the client library and mint tokens
  int init_push_target(CephContext* cct);

  std::unique_ptr<cuObjServer> m_server;
#ifdef WITH_RADOSGW_CUOBJ_TARGET
  std::unique_ptr<cuObjClient> m_client;
#endif
  bool m_push_target = false;
  size_t m_buf_count = 0;
  std::unique_ptr<RDMABufEntry[]> m_buffer_pool;
  CephContext* m_cct = nullptr;

  std::mutex m_channel_lock;
  std::condition_variable m_channel_cond;
  std::vector<uint16_t> m_free_channels;

  static std::unique_ptr<RGWCuObjServer> s_instance;
};
