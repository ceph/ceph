// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab
//
// Linux PSP Security Protocol netlink wrapper.
//
// This ships the interface + a no-op default backend that always
// reports psp_supported=false. The mock backend for CI tests lands
// in the next commit; the real netlink-backed implementation (gated
// on HAVE_PSP) comes in follow-on work.

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace ceph::msgr::psp {

struct DeviceCaps {
  std::string ifname;
  bool psp_supported = false;
  uint32_t versions_mask = 0;          // AEAD-version bitmask (NIC-defined)
  uint32_t free_tx_assoc_slots = 0;    // capacity hint; 0 if not exposed
  uint32_t free_rx_assoc_slots = 0;
};

// Backend abstraction. CI swaps in a mock; production will swap in
// a real libmnl/libnl-genl impl. Today only the stub exists, which
// reports psp_supported=false from every call so the rest of the
// messenger can develop against the interface without committing
// to behavior.
class NetlinkBackend {
 public:
  virtual ~NetlinkBackend() = default;

  // Query PSP capability for the interface backing this socket.
  // A backend that does not support PSP returns DeviceCaps with
  // psp_supported=false (NOT std::nullopt). std::nullopt is
  // reserved for hard error.
  virtual std::optional<DeviceCaps> get_dev_caps(int sock_fd) = 0;

  // Allocate a rx-direction PSP key for sock_fd; returns the
  // wrapped-key blob the peer will install via install_tx_assoc.
  // The blob is opaque to Ceph (kernel/NIC-defined size + layout)
  // and must round-trip verbatim through the in-band msgr2
  // handshake.
  virtual std::optional<std::vector<uint8_t>>
    alloc_rx_assoc(int sock_fd) = 0;

  // Install a peer-supplied wrapped-key blob as the tx-direction
  // PSP key for sock_fd. Returns 0 on success, -errno on failure;
  // -ENOSYS indicates the backend does not support PSP.
  virtual int install_tx_assoc(int sock_fd,
                               std::span<const uint8_t> wrapped) = 0;

  // Best-effort cleanup of PSP state bound to sock_fd. Always
  // succeeds at the API level (the kernel may still need to GC).
  virtual int teardown(int sock_fd) = 0;
};

// Production backend factory. Currently returns a stub that reports
// unsupported; the real implementation will make netlink calls when
// HAVE_PSP is defined.
std::unique_ptr<NetlinkBackend> make_real_backend();

// Forward decl for the CI mock backend.
struct MockConfig;
std::unique_ptr<NetlinkBackend> make_mock_backend(const MockConfig&);

}  // namespace ceph::msgr::psp
