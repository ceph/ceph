// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

// Direct binding to the cuObject SDK, selected with WITH_RADOSGW_CUOBJ_SHIM=OFF.
#include "rgw_cuobj.h"

#include <cuobjserver.h>

#include <cerrno>
#include <exception>
#include <iostream>

#include "common/dout_fmt.h"

namespace {

unsigned telem_flags(const std::string& level)
{
  if (level == "debug") {
    return CUOBJ_LOG_PATH_ERROR | CUOBJ_LOG_PATH_INFO | CUOBJ_LOG_PATH_DEBUG;
  }
  if (level == "info") {
    return CUOBJ_LOG_PATH_ERROR | CUOBJ_LOG_PATH_INFO;
  }
  if (level == "off") {
    return 0;
  }
  return CUOBJ_LOG_PATH_ERROR;
}

class DirectTransport final : public RGWCuObjTransport {
  cuObjServer server;

public:
  DirectTransport(const Params& params, const cuObjRDMATunable& tunable)
    : server(params.rdma_ip.c_str(), params.rdma_port,
             CUOBJ_PROTO_RDMA_DC_V1, tunable) {}

  bool is_connected() override { return server.isConnected(); }
  void* alloc_host_buffer(size_t size) override {
    return server.allocHostBuffer(size);
  }
  rdma_buffer* register_buffer(void* ptr, size_t size) override {
    return server.registerBuffer(ptr, size);
  }
  void deregister_buffer(rdma_buffer* handle) override {
    server.deRegisterBuffer(handle);
  }
  uint16_t allocate_channel_id() override { return server.allocateChannelId(); }
  void free_channel_id(uint16_t channel) override {
    server.freeChannelId(channel);
  }
  ssize_t put_object(const std::string& key, rdma_buffer* handle,
                     uint64_t remote_addr, size_t size,
                     const std::string& rdma_descr, uint16_t channel,
                     uint64_t local_offset, int* wc_status) override {
    ibv_wc_status status = IBV_WC_SUCCESS;
    const auto ret = server.handlePutObject(
        key, handle, remote_addr, size, rdma_descr, channel, local_offset,
        wc_status ? &status : nullptr, nullptr);
    if (wc_status) {
      *wc_status = static_cast<int>(status);
    }
    return ret;
  }
  ssize_t get_object(const std::string& key, rdma_buffer* handle,
                     uint64_t remote_addr, size_t size,
                     const std::string& rdma_descr, uint16_t channel,
                     uint64_t local_offset) override {
    return server.handleGetObject(key, handle, remote_addr, size, rdma_descr,
                                  channel, local_offset, nullptr, nullptr);
  }
};

} // anonymous namespace

std::unique_ptr<RGWCuObjTransport> RGWCuObjTransport::create(
    const DoutPrefixProvider* dpp, const Params& params, int& err)
{
  ldpp_dout_fmt(dpp, 1, "using directly linked cuObject server library");
  try {
    cuObjRDMATunable tunable;
    tunable.setNumDcis(params.num_dcis);
    auto transport = std::make_unique<DirectTransport>(params, tunable);

    cuObjServer::setupTelemetry(false, &std::cerr);
    // The second argument selects which ops emit telemetry spans; leave at 0.
    cuObjServer::setTelemFlags(telem_flags(params.log_level), 0);

    err = 0;
    return transport;
  } catch (const std::exception& e) {
    ldpp_dout_fmt(dpp, -1, "ERROR: cuObjServer construction failed: {}",
                  e.what());
    err = -EIO;
    return nullptr;
  }
}
