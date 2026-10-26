// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

// Runtime binding to NVIDIA's libcuobjserver. No NVIDIA header is included:
// the library is opened with dlopen() and the C++ entry points of the public
// cuObjServer / cuObjRDMATunable classes are resolved by their Itanium-ABI
// mangled names. Both classes are pimpl wrappers around one pointer and have
// no virtual functions, so a block of suitably aligned storage can stand in for
// an instance as long as it is at least sizeof(Impl*) bytes (the storage
// below is generously oversized).
//
// This must be kept in sync with cuobjserver.h of the supported library
// release (major version 2). A library whose symbols do not match is rejected
// at load time, never half-used.

#include "rgw_cuobj.h"

#include <dlfcn.h>

#include <cerrno>
#include <exception>
#include <iostream>

#include "common/dout_fmt.h"

namespace {

// Values copied from the cuObject headers (cuobjserver.h / cuobjtelem.h).
constexpr unsigned CUOBJ_PROTO_RDMA_DC_V1 = 1001;
constexpr unsigned CUOBJ_LOG_PATH_INFO = 0x0001;
constexpr unsigned CUOBJ_LOG_PATH_DEBUG = 0x0002;
constexpr unsigned CUOBJ_LOG_PATH_ERROR = 0x0004;

// Oversized, aligned stand-in for a library-owned pimpl object.
struct alignas(16) ObjectStorage {
  unsigned char bytes[64];
};

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

// Mangled names of the cuObject server 2.x entry points we use.
using tunable_ctor_fn = void (*)(void*);
using tunable_dtor_fn = void (*)(void*);
using tunable_set_dcis_fn = void (*)(void*, int);
using server_ctor_fn = void (*)(void*, const char*, unsigned short, unsigned,
                                const void*);
using server_dtor_fn = void (*)(void*);
using is_connected_fn = bool (*)(void*);
using alloc_host_fn = void* (*)(void*, size_t);
using register_fn = rdma_buffer* (*)(void*, void*, size_t);
using deregister_fn = void (*)(void*, rdma_buffer*);
using alloc_channel_fn = uint16_t (*)(void*);
using free_channel_fn = void (*)(void*, uint16_t);
using transfer_fn = ssize_t (*)(void*, const std::string&, rdma_buffer*,
                                uint64_t, size_t, const std::string&, uint16_t,
                                uint64_t, int*, void*);
using setup_telemetry_fn = void (*)(bool, std::ostream*);
using set_telem_flags_fn = void (*)(unsigned, unsigned);

struct Symbols {
  tunable_ctor_fn tunable_ctor = nullptr;
  tunable_dtor_fn tunable_dtor = nullptr;
  tunable_set_dcis_fn tunable_set_dcis = nullptr;
  server_ctor_fn server_ctor = nullptr;
  server_dtor_fn server_dtor = nullptr;
  is_connected_fn is_connected = nullptr;
  alloc_host_fn alloc_host = nullptr;
  register_fn register_buffer = nullptr;
  deregister_fn deregister_buffer = nullptr;
  alloc_channel_fn alloc_channel = nullptr;
  free_channel_fn free_channel = nullptr;
  transfer_fn handle_put = nullptr;
  transfer_fn handle_get = nullptr;
  setup_telemetry_fn setup_telemetry = nullptr;
  set_telem_flags_fn set_telem_flags = nullptr;

  // Returns the name of the first symbol that is missing, or nullptr.
  const char* load(void* lib) {
    const char* missing = nullptr;
    auto bind = [&](auto& fn, const char* name) {
      if (missing) {
        return;
      }
      fn = reinterpret_cast<std::remove_reference_t<decltype(fn)>>(
          dlsym(lib, name));
      if (!fn) {
        missing = name;
      }
    };
    bind(tunable_ctor, "_ZN16cuObjRDMATunableC1Ev");
    bind(tunable_dtor, "_ZN16cuObjRDMATunableD1Ev");
    bind(tunable_set_dcis, "_ZN16cuObjRDMATunable10setNumDcisEi");
    bind(server_ctor, "_ZN11cuObjServerC1EPKctjRK16cuObjRDMATunable");
    bind(server_dtor, "_ZN11cuObjServerD1Ev");
    bind(is_connected, "_ZN11cuObjServer11isConnectedEv");
    bind(alloc_host, "_ZN11cuObjServer15allocHostBufferEm");
    bind(register_buffer, "_ZN11cuObjServer14registerBufferEPvm");
    bind(deregister_buffer, "_ZN11cuObjServer16deRegisterBufferEP11rdma_buffer");
    bind(alloc_channel, "_ZN11cuObjServer17allocateChannelIdEv");
    bind(free_channel, "_ZN11cuObjServer13freeChannelIdEt");
    bind(handle_put,
         "_ZN11cuObjServer15handlePutObjectERKNSt7__cxx1112basic_stringIcSt11"
         "char_traitsIcESaIcEEEP11rdma_buffermmS7_tmP13ibv_wc_statusPv");
    bind(handle_get,
         "_ZN11cuObjServer15handleGetObjectERKNSt7__cxx1112basic_stringIcSt11"
         "char_traitsIcESaIcEEEP11rdma_buffermmS7_tmP13ibv_wc_statusPv");
    bind(setup_telemetry, "_ZN11cuObjServer14setupTelemetryEbPSo");
    bind(set_telem_flags, "_ZN11cuObjServer13setTelemFlagsEjj");
    return missing;
  }
};

class DlTransport final : public RGWCuObjTransport {
public:
  Symbols sym;
  ObjectStorage storage{};
  bool constructed = false;

  ~DlTransport() override {
    if (constructed) {
      sym.server_dtor(&storage);
    }
    // The library is intentionally never dlclose()d: it owns process-wide
    // telemetry state and may have started threads.
  }

  bool is_connected() override { return sym.is_connected(&storage); }
  void* alloc_host_buffer(size_t size) override {
    return sym.alloc_host(&storage, size);
  }
  rdma_buffer* register_buffer(void* ptr, size_t size) override {
    return sym.register_buffer(&storage, ptr, size);
  }
  void deregister_buffer(rdma_buffer* handle) override {
    sym.deregister_buffer(&storage, handle);
  }
  uint16_t allocate_channel_id() override { return sym.alloc_channel(&storage); }
  void free_channel_id(uint16_t channel) override {
    sym.free_channel(&storage, channel);
  }
  ssize_t put_object(const std::string& key, rdma_buffer* handle,
                     uint64_t remote_addr, size_t size,
                     const std::string& rdma_descr, uint16_t channel,
                     uint64_t local_offset, int* wc_status) override {
    return sym.handle_put(&storage, key, handle, remote_addr, size, rdma_descr,
                          channel, local_offset, wc_status, nullptr);
  }
  ssize_t get_object(const std::string& key, rdma_buffer* handle,
                     uint64_t remote_addr, size_t size,
                     const std::string& rdma_descr, uint16_t channel,
                     uint64_t local_offset) override {
    return sym.handle_get(&storage, key, handle, remote_addr, size, rdma_descr,
                          channel, local_offset, nullptr, nullptr);
  }
};

} // anonymous namespace

std::unique_ptr<RGWCuObjTransport> RGWCuObjTransport::create(
    const DoutPrefixProvider* dpp, const Params& params, int& err)
{
  ldpp_dout_fmt(dpp, 1, "loading cuObject library {} through the ABI shim",
                params.library);
  void* lib = dlopen(params.library.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (!lib) {
    ldpp_dout_fmt(dpp, -1,
                  "ERROR: cannot load cuObject library {}: {} "
                  "(S3 RDMA requests will fail)",
                  params.library, dlerror());
    err = -ENOENT;
    return nullptr;
  }

  auto transport = std::make_unique<DlTransport>();
  if (const char* missing = transport->sym.load(lib)) {
    ldpp_dout_fmt(dpp, -1,
                  "ERROR: cuObject library {} lacks symbol {}: unsupported "
                  "library version",
                  params.library, missing);
    err = -ENOSYS;
    return nullptr;
  }

  ObjectStorage tunable{};
  transport->sym.tunable_ctor(&tunable);
  transport->sym.tunable_set_dcis(&tunable, params.num_dcis);
  try {
    transport->sym.server_ctor(&transport->storage, params.rdma_ip.c_str(),
                               params.rdma_port, CUOBJ_PROTO_RDMA_DC_V1,
                               &tunable);
    transport->constructed = true;
  } catch (const std::exception& e) {
    ldpp_dout_fmt(dpp, -1, "ERROR: cuObjServer construction failed: {}",
                  e.what());
    transport->sym.tunable_dtor(&tunable);
    err = -EIO;
    return nullptr;
  }
  transport->sym.tunable_dtor(&tunable);

  transport->sym.setup_telemetry(false, &std::cerr);
  // The second argument selects which ops emit telemetry spans; leave it at 0.
  transport->sym.set_telem_flags(telem_flags(params.log_level), 0);

  err = 0;
  return transport;
}
