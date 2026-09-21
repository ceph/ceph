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

#include "include/common_fwd.h"

struct rdma_buffer;
class cuObjServer;

class RGWCuObjServer {
public:
  struct RDMABufEntry {
    void* ptr = nullptr;
    size_t size = 0;
    struct rdma_buffer* handle = nullptr;
    std::atomic<bool> in_use{false};
  };

  ~RGWCuObjServer();

  static int init(CephContext* cct);
  static void shutdown();
  static RGWCuObjServer* get_instance();

  bool is_available() const;

  RDMABufEntry* acquire_buffer(size_t needed_size);
  void release_buffer(RDMABufEntry* buf);

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

  std::unique_ptr<cuObjServer> m_server;
  size_t m_buf_count = 0;
  std::unique_ptr<RDMABufEntry[]> m_buffer_pool;
  CephContext* m_cct = nullptr;

  std::mutex m_channel_lock;
  std::condition_variable m_channel_cond;
  std::vector<uint16_t> m_free_channels;

  static std::unique_ptr<RGWCuObjServer> s_instance;
};
