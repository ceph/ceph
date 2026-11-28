// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <memory>
#include <string>

#include <boost/optional.hpp>

#include "common/dout.h"
#include "rgw_common.h"

// Opaque handle owned by the cuObject library. RGW never dereferences it.
struct rdma_buffer;

// Thin abstraction over the cuObject server library, bound at runtime by
// the ABI shim in rgw_cuobj_dl.cc without the NVIDIA SDK.
class RGWCuObjTransport {
public:
  virtual ~RGWCuObjTransport() = default;

  virtual bool is_connected() = 0;
  virtual void* alloc_host_buffer(size_t size) = 0;
  virtual rdma_buffer* register_buffer(void* ptr, size_t size) = 0;
  virtual void deregister_buffer(rdma_buffer* handle) = 0;
  virtual uint16_t allocate_channel_id() = 0;
  virtual void free_channel_id(uint16_t channel) = 0;

  // RDMA_READ from the client into the registered buffer.
  virtual ssize_t put_object(const std::string& key, rdma_buffer* handle,
                             uint64_t remote_addr, size_t size,
                             const std::string& rdma_descr, uint16_t channel,
                             uint64_t local_offset, int* wc_status) = 0;
  // RDMA_WRITE from the registered buffer to the client.
  virtual ssize_t get_object(const std::string& key, rdma_buffer* handle,
                             uint64_t remote_addr, size_t size,
                             const std::string& rdma_descr, uint16_t channel,
                             uint64_t local_offset) = 0;

  struct Params {
    std::string rdma_ip;
    unsigned short rdma_port = 0;
    int num_dcis = 0;
    std::string log_level; // off, error, info, debug
  };

  // Construct the transport. Returns nullptr and sets 'err' (negative errno)
  // on failure, including an absent or incompatible library.
  // The caller must check is_connected() before using the transport.
  static std::unique_ptr<RGWCuObjTransport> create(
      const DoutPrefixProvider* dpp, const Params& params, int& err);
};

class RGWCuObjServer : public DoutPrefixProvider {
public:
  struct RDMABufEntry {
    void* ptr = nullptr;
    size_t size = 0;
    struct rdma_buffer* handle = nullptr;
    std::atomic<bool> in_use{false};
  };

  ~RGWCuObjServer() override;

  std::ostream& gen_prefix(std::ostream& out) const override {
    return out << "rgw_cuobj: ";
  }
  CephContext* get_cct() const override { return m_cct; }
  unsigned get_subsys() const override { return ceph_subsys_rgw; }

  static int init(CephContext* cct);
  static void shutdown();
  static RGWCuObjServer* get_instance();

  // True when RDMA transfers can be served: the transport was created and
  // the server is connected. S3 requests carrying an RDMA token fail with
  // 500 when this is false.
  static bool available() {
    auto* server = get_instance();
    return server && server->is_available();
  }

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
  friend class RGWCuObjServerTest;

  RGWCuObjServer() = default;

  int do_init(CephContext* cct);
  void do_shutdown();

  std::unique_ptr<RGWCuObjTransport> m_server;
  size_t m_buf_count = 0;
  std::unique_ptr<RDMABufEntry[]> m_buffer_pool;
  CephContext* m_cct = nullptr;

  static std::unique_ptr<RGWCuObjServer> s_instance;
  static thread_local uint16_t tls_channel_id;
  static thread_local bool tls_channel_valid;
};

namespace rgw::cuobj {

// NIXL's Dell engine sends the same raw cuObject descriptor as
// x-amz-rdma-token, but names the header x-rdma-info. Keep selection identical
// in request validation and the transfer path. Never treat an empty token as
// an ordinary HTTP request or choose between conflicting descriptors.
inline int get_request_token(const RGWEnv& env,
                             boost::optional<const std::string&>& token)
{
  token = boost::none;
  const auto amz_token = env.get_optional("HTTP_X_AMZ_RDMA_TOKEN");
  const auto rdma_info = env.get_optional("HTTP_X_RDMA_INFO");
  if ((amz_token && amz_token->empty()) ||
      (rdma_info && rdma_info->empty()) ||
      (amz_token && rdma_info && *amz_token != *rdma_info)) {
    return -EINVAL;
  }
  token = amz_token ? amz_token : rdma_info;
  return 0;
}

} // namespace rgw::cuobj
