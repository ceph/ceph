// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab
//
// No-op stub. Always reports psp_supported=false.
// Follow-on work replaces make_real_backend() with a libmnl/libnl-genl
// implementation gated on HAVE_PSP.

#include "PSPNetlink.h"

#include <cerrno>

namespace ceph::msgr::psp {

namespace {

class StubBackend : public NetlinkBackend {
 public:
  std::optional<DeviceCaps> get_dev_caps(int /*sock_fd*/) override {
    return DeviceCaps{};   // psp_supported=false (the field default)
  }
  std::optional<std::vector<uint8_t>>
    alloc_rx_assoc(int /*sock_fd*/) override {
    return std::nullopt;
  }
  int install_tx_assoc(int /*sock_fd*/,
                       std::span<const uint8_t> /*wrapped*/) override {
    return -ENOSYS;
  }
  int teardown(int /*sock_fd*/) override {
    return 0;
  }
};

}  // namespace

std::unique_ptr<NetlinkBackend> make_real_backend() {
  return std::make_unique<StubBackend>();
}

// make_mock_backend() is intentionally undefined here; it lands
// in PSPNetlinkMock.cc. Linking a unit test that references it
// will fail until then.

}  // namespace ceph::msgr::psp
