// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

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

  uint16_t get_channel_id();
  void release_channel_id();

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

  std::unique_ptr<cuObjServer> m_server;
  size_t m_buf_count = 0;
  std::unique_ptr<RDMABufEntry[]> m_buffer_pool;
  CephContext* m_cct = nullptr;

  static std::unique_ptr<RGWCuObjServer> s_instance;
  static thread_local uint16_t tls_channel_id;
  static thread_local bool tls_channel_valid;
};
