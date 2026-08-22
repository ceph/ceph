// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab
//
// Deterministic mock backend for CI.
//
// Each MockBackend instance maintains its own slot accounting and
// is NOT thread-safe (single-threaded unit-test consumption only).
// Two MockBackend instances act as independent peers in the
// two-peer rendezvous test: peer A's alloc_rx_assoc blob is
// passed by the test harness to peer B's install_tx_assoc.

#include "PSPNetlink.h"

#include <cerrno>
#include <cstring>
#include <map>
#include <set>

namespace ceph::msgr::psp {

namespace {

// Wrapped-key blob layout the mock emits and validates.
// 16 bytes, fully deterministic: magic + version + seq + fd + pad.
// Real NICs emit opaque blobs of vendor-defined size; this is
// pure bookkeeping for the mock.
constexpr size_t kBlobSize = 16;

class MockBackend : public NetlinkBackend {
 public:
  explicit MockBackend(MockConfig cfg) : cfg_(std::move(cfg)) {}

  std::optional<DeviceCaps> get_dev_caps(int /*sock_fd*/) override {
    if (cfg_.fail_get_dev_caps) return std::nullopt;
    return DeviceCaps{
      .ifname = cfg_.ifname,
      .psp_supported = cfg_.psp_supported,
      .versions_mask = cfg_.versions_mask,
      .free_tx_assoc_slots = cfg_.tx_capacity - tx_used(),
      .free_rx_assoc_slots = cfg_.rx_capacity - rx_used(),
    };
  }

  std::optional<std::vector<uint8_t>>
  alloc_rx_assoc(int sock_fd) override {
    if (!cfg_.psp_supported) return std::nullopt;
    if (cfg_.fail_next_alloc_rx_assoc) {
      --cfg_.fail_next_alloc_rx_assoc;
      return std::nullopt;
    }
    if (rx_used() >= cfg_.rx_capacity) return std::nullopt;
    rx_fds_.insert(sock_fd);
    return make_blob(sock_fd);
  }

  int install_tx_assoc(int sock_fd,
                       std::span<const uint8_t> wrapped) override {
    if (!cfg_.psp_supported) return -ENOSYS;
    if (cfg_.fail_next_install_tx_assoc) {
      --cfg_.fail_next_install_tx_assoc;
      // Distinct from -ENOSPC so tests can tell an injected kernel
      // rejection from genuine capacity exhaustion.
      return -EIO;
    }
    if (!validate_blob(wrapped)) return -EINVAL;
    // A peer must install the *other* side's rx key. Installing a blob
    // this instance issued means the handshake crossed its wires, so
    // reject it rather than silently modelling a working association.
    if (blob_issuer(wrapped) == instance_id_) return -EINVAL;
    if (tx_used() >= cfg_.tx_capacity) return -ENOSPC;
    installed_tx_[sock_fd] =
      std::vector<uint8_t>(wrapped.begin(), wrapped.end());
    return 0;
  }

  int teardown(int sock_fd) override {
    installed_tx_.erase(sock_fd);
    rx_fds_.erase(sock_fd);
    return 0;
  }

 private:
  uint32_t tx_used() const {
    return static_cast<uint32_t>(installed_tx_.size());
  }
  uint32_t rx_used() const {
    return static_cast<uint32_t>(rx_fds_.size());
  }

  std::vector<uint8_t> make_blob(int sock_fd) {
    // magic(4) version(2) issuer(2) seq(4) fd(4) == 16
    std::vector<uint8_t> b(kBlobSize, 0);
    b[0] = 'P'; b[1] = 'S'; b[2] = 'P'; b[3] = 'M';
    const uint16_t ver = 1;
    const uint16_t issuer = instance_id_;
    const uint32_t seq = ++seq_;
    const uint32_t fd  = static_cast<uint32_t>(sock_fd);
    std::memcpy(&b[4],  &ver, sizeof(ver));
    std::memcpy(&b[6],  &issuer, sizeof(issuer));
    std::memcpy(&b[8],  &seq, sizeof(seq));
    std::memcpy(&b[12], &fd,  sizeof(fd));
    return b;
  }
  static uint16_t blob_issuer(std::span<const uint8_t> w) {
    uint16_t issuer = 0;
    std::memcpy(&issuer, w.data() + 6, sizeof(issuer));
    return issuer;
  }
  static bool validate_blob(std::span<const uint8_t> w) {
    if (w.size() != kBlobSize)
      return false;
    if (!(w[0] == 'P' && w[1] == 'S' && w[2] == 'P' && w[3] == 'M'))
      return false;
    uint16_t ver = 0;
    std::memcpy(&ver, w.data() + 4, sizeof(ver));
    return ver == 1;
  }

  MockConfig cfg_;
  // Identifies which mock instance issued a blob, so a peer can tell
  // its own rx key from the far side's. Deterministic: instances are
  // numbered in construction order (single-threaded test use only).
  static inline uint16_t next_instance_id_ = 0;
  const uint16_t instance_id_ = ++next_instance_id_;
  uint32_t seq_ = 0;
  std::set<int> rx_fds_;
  std::map<int, std::vector<uint8_t>> installed_tx_;
};

}  // namespace

std::unique_ptr<NetlinkBackend>
make_mock_backend(const MockConfig& cfg) {
  return std::make_unique<MockBackend>(cfg);
}

}  // namespace ceph::msgr::psp
