// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab
//
// Unit tests for the PSP netlink mock backend.
//
// These tests exercise the NetlinkBackend interface contract
// using only the in-memory mock - no kernel, no hardware, no
// real netlink. The two-peer rendezvous test models the shape
// of the planned in-band PSP handshake without requiring the
// protocol code to exist yet.

#include "msg/async/PSPNetlink.h"

#include <gtest/gtest.h>

using namespace ceph::msgr::psp;

TEST(PSPNetlinkMock, GetDevCapsReportsSupported) {
  auto backend = make_mock_backend(MockConfig{});
  auto caps = backend->get_dev_caps(/*sock_fd*/ 42);
  ASSERT_TRUE(caps.has_value());
  EXPECT_TRUE(caps->psp_supported);
  EXPECT_EQ(caps->ifname, "mock0");
  EXPECT_EQ(caps->free_tx_assoc_slots, 64u);
  EXPECT_EQ(caps->free_rx_assoc_slots, 64u);
}

TEST(PSPNetlinkMock, GetDevCapsHonorsUnsupported) {
  MockConfig cfg;
  cfg.psp_supported = false;
  auto backend = make_mock_backend(cfg);
  auto caps = backend->get_dev_caps(42);
  ASSERT_TRUE(caps.has_value());
  EXPECT_FALSE(caps->psp_supported);
}

TEST(PSPNetlinkMock, GetDevCapsHardError) {
  MockConfig cfg;
  cfg.fail_get_dev_caps = true;
  auto backend = make_mock_backend(cfg);
  EXPECT_FALSE(backend->get_dev_caps(42).has_value());
}

// Models the in-band PSP handshake shape: each peer allocs its
// own rx-key locally, the wrapped blob round-trips over the
// (mocked) wire, and each peer installs the other's blob as its
// tx-key. After this, both peers consider the connection PSP-up.
TEST(PSPNetlinkMock, TwoPeerRendezvous) {
  auto peer_a = make_mock_backend(MockConfig{});
  auto peer_b = make_mock_backend(MockConfig{});

  auto blob_a = peer_a->alloc_rx_assoc(/*fd*/ 100);
  ASSERT_TRUE(blob_a.has_value());
  auto blob_b = peer_b->alloc_rx_assoc(/*fd*/ 200);
  ASSERT_TRUE(blob_b.has_value());

  EXPECT_EQ(0, peer_a->install_tx_assoc(100, *blob_b));
  EXPECT_EQ(0, peer_b->install_tx_assoc(200, *blob_a));

  auto caps_a = peer_a->get_dev_caps(0);
  ASSERT_TRUE(caps_a.has_value());
  EXPECT_EQ(caps_a->free_tx_assoc_slots, 64u - 1);
  EXPECT_EQ(caps_a->free_rx_assoc_slots, 64u - 1);
}

// A tx key must come from the peer. Installing a blob this instance
// issued is a crossed handshake, not a working association - without
// this check the rendezvous test would pass even if the harness never
// exchanged anything.
TEST(PSPNetlinkMock, RejectsOwnBlobAsTxKey) {
  auto peer = make_mock_backend(MockConfig{});
  auto own = peer->alloc_rx_assoc(/*fd*/ 10);
  ASSERT_TRUE(own.has_value());
  EXPECT_EQ(-EINVAL, peer->install_tx_assoc(10, *own));
}

// Exercises the tear-down-on-key-error path: alloc succeeds and the
// peer's blob arrives, but the kernel-side install rejects it. The
// local rx association must then be released, not leaked.
TEST(PSPNetlinkMock, InjectedInstallFailure) {
  MockConfig cfg;
  cfg.fail_next_install_tx_assoc = 1;
  auto local = make_mock_backend(cfg);
  auto remote = make_mock_backend(MockConfig{});

  auto local_rx = local->alloc_rx_assoc(/*fd*/ 50);
  ASSERT_TRUE(local_rx.has_value());
  auto remote_rx = remote->alloc_rx_assoc(/*fd*/ 250);
  ASSERT_TRUE(remote_rx.has_value());

  // -EIO, not -ENOSPC: an injected rejection is not capacity pressure.
  EXPECT_EQ(-EIO, local->install_tx_assoc(50, *remote_rx));

  // The connection is torn down after a failed key install; the rx
  // slot taken by alloc_rx_assoc must come back.
  EXPECT_EQ(0, local->teardown(50));
  auto caps = local->get_dev_caps(0);
  ASSERT_TRUE(caps.has_value());
  EXPECT_EQ(caps->free_rx_assoc_slots, 64u);
  EXPECT_EQ(caps->free_tx_assoc_slots, 64u);

  // Budget consumed; a subsequent handshake succeeds.
  auto remote_rx2 = remote->alloc_rx_assoc(/*fd*/ 251);
  ASSERT_TRUE(remote_rx2.has_value());
  ASSERT_TRUE(local->alloc_rx_assoc(51).has_value());
  EXPECT_EQ(0, local->install_tx_assoc(51, *remote_rx2));
}

TEST(PSPNetlinkMock, InjectedAllocFailure) {
  MockConfig cfg;
  cfg.fail_next_alloc_rx_assoc = 2;
  auto backend = make_mock_backend(cfg);

  EXPECT_FALSE(backend->alloc_rx_assoc(60).has_value());
  EXPECT_FALSE(backend->alloc_rx_assoc(61).has_value());
  // Budget consumed.
  EXPECT_TRUE(backend->alloc_rx_assoc(62).has_value());

  // Failed allocs must not have consumed rx slots.
  auto caps = backend->get_dev_caps(0);
  ASSERT_TRUE(caps.has_value());
  EXPECT_EQ(caps->free_rx_assoc_slots, 64u - 1);
}

TEST(PSPNetlinkMock, TxCapacityExhaustion) {
  MockConfig cfg;
  cfg.tx_capacity = 2;
  auto local = make_mock_backend(cfg);
  auto remote = make_mock_backend(MockConfig{});

  for (int i = 0; i < 2; ++i) {
    auto peer_blob = remote->alloc_rx_assoc(200 + i);
    ASSERT_TRUE(peer_blob.has_value());
    EXPECT_EQ(0, local->install_tx_assoc(100 + i, *peer_blob));
  }
  auto peer_blob3 = remote->alloc_rx_assoc(202);
  ASSERT_TRUE(peer_blob3.has_value());
  EXPECT_EQ(-ENOSPC, local->install_tx_assoc(102, *peer_blob3));
}

TEST(PSPNetlinkMock, RxCapacityExhaustion) {
  MockConfig cfg;
  cfg.rx_capacity = 2;
  auto backend = make_mock_backend(cfg);

  EXPECT_TRUE(backend->alloc_rx_assoc(1).has_value());
  EXPECT_TRUE(backend->alloc_rx_assoc(2).has_value());
  EXPECT_FALSE(backend->alloc_rx_assoc(3).has_value());
}

TEST(PSPNetlinkMock, RejectsMalformedBlob) {
  auto backend = make_mock_backend(MockConfig{});
  std::vector<uint8_t> garbage(16, 0xff);
  EXPECT_EQ(-EINVAL, backend->install_tx_assoc(50, garbage));

  std::vector<uint8_t> wrong_size{'P', 'S', 'P', 'M'};
  EXPECT_EQ(-EINVAL, backend->install_tx_assoc(50, wrong_size));

  // Right magic and size, unsupported version.
  std::vector<uint8_t> bad_ver(16, 0);
  bad_ver[0] = 'P'; bad_ver[1] = 'S'; bad_ver[2] = 'P'; bad_ver[3] = 'M';
  bad_ver[4] = 0xff; bad_ver[5] = 0xff;
  EXPECT_EQ(-EINVAL, backend->install_tx_assoc(50, bad_ver));
}

TEST(PSPNetlinkMock, TeardownReleasesSlots) {
  auto local = make_mock_backend(MockConfig{});
  auto remote = make_mock_backend(MockConfig{});
  auto peer_blob = remote->alloc_rx_assoc(/*fd*/ 275);
  ASSERT_TRUE(peer_blob.has_value());
  ASSERT_TRUE(local->alloc_rx_assoc(/*fd*/ 75).has_value());
  ASSERT_EQ(0, local->install_tx_assoc(75, *peer_blob));

  EXPECT_EQ(0, local->teardown(75));

  auto caps = local->get_dev_caps(0);
  ASSERT_TRUE(caps.has_value());
  EXPECT_EQ(caps->free_tx_assoc_slots, 64u);
  EXPECT_EQ(caps->free_rx_assoc_slots, 64u);
}
