// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "common/dout.h"

struct rdma_buffer;
namespace ceph::oob { class Arena; }
class cuObjServer;

class RGWCuObjServer : public DoutPrefixProvider {
public:
  /// a staging buffer: a buffer of the arena, which the library knows
  /// as offset in the arena's registration, handle
  struct RDMABufEntry {
    void* ptr = nullptr;
    size_t size = 0;
    uint64_t offset = 0;
    struct rdma_buffer* handle = nullptr;
  };

  ~RGWCuObjServer() override;

  std::ostream& gen_prefix(std::ostream& out) const override {
    return out << "rgw_cuobj: ";
  }
  CephContext* get_cct() const override { return m_cct; }
  unsigned get_subsys() const override { return ceph_subsys_rgw; }

  /// arena: the gateway's out-of-band memory, which the server registers
  /// once and stages each transfer in; it must outlive the server.
  /// Called once the frontends dropped privileges, when they take
  /// requests already
  static int init(CephContext* cct, ceph::oob::Arena* arena);
  /// once the frontends are joined, so that no request holds the server
  static void shutdown();
  /// the server once init() has set it up, else null
  static RGWCuObjServer* get_instance();

  /// the server is up, and the calling thread has a channel, which this
  /// allocates on the thread's first call
  bool is_available();

  /// the calling thread's channel (DC initiator), allocated on first
  /// use; invalid_channel when the library has none left
  uint16_t get_channel_id();
  void release_channel_id();
  static constexpr uint16_t invalid_channel = UINT16_MAX;

  /// a buffer of the arena of at least needed_size bytes, or null when
  /// the arena has none free, none that large, or the calling thread no
  /// channel
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
  friend class RGWCuObjServerTest;

  RGWCuObjServer() = default;

  int do_init(CephContext* cct);
  void do_shutdown();

  std::unique_ptr<cuObjServer> m_server;
  ceph::oob::Arena* m_arena = nullptr;
  /// the arena, registered with the library once
  struct rdma_buffer* m_arena_handle = nullptr;
  CephContext* m_cct = nullptr;
  /// DC initiators: one for each frontend thread
  int m_num_dcis = 0;

  /// set once do_init() succeeded: the frontends take requests already
  static std::atomic<RGWCuObjServer*> s_instance;
  static thread_local uint16_t tls_channel_id;
  static thread_local bool tls_channel_valid;
  /// the library had no channel left for this thread; reported once
  static thread_local bool tls_channel_refused;
};
