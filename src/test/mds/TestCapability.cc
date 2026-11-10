// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/ceph_fs.h"
#include "include/elist.h"
#include "mds/Capability.h"
#include "mds/SessionMap.h"

#include "gtest/gtest.h"

/*
 * Tests for revoking-list bookkeeping in Capability. Locker links a cap on
 * revoking_caps and revoking_caps_by_client when it sends a revoke; the cap
 * must leave both lists once nothing is being revoked any more, or
 * Locker::caps_tick() reports a late revoke the client cannot ack.
 * See https://tracker.ceph.com/issues/82984.
 */

static constexpr int CAPS_FULL = CEPH_CAP_PIN | CEPH_CAP_AUTH_SHARED |
  CEPH_CAP_LINK_SHARED | CEPH_CAP_XATTR_SHARED | CEPH_CAP_FILE_SHARED |
  CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_RD;
static constexpr int CAPS_REVOKED = CEPH_CAP_FILE_SHARED | CEPH_CAP_FILE_RD;
static constexpr int CAPS_REDUCED = CAPS_FULL & ~CAPS_REVOKED;

class CapabilityRevoke : public ::testing::Test {
protected:
  ceph::ref_t<Session> session = ceph::make_ref<Session>(nullptr);
  elist<Capability*> revoking_caps{member_offset(Capability, item_revoking_caps)};
  elist<Capability*> client_revoking_caps{
    member_offset(Capability, item_client_revoking_caps)};
  std::unique_ptr<Capability> cap;

  void SetUp() override {
    cap = std::make_unique<Capability>(nullptr, session.get(), 1);
    cap->issue(CAPS_FULL);
  }

  void TearDown() override {
    cap->item_revoking_caps.remove_myself();
    cap->item_client_revoking_caps.remove_myself();
    cap->item_session_caps.remove_myself();
    cap.reset();
  }

  // what Locker::issue_caps() does for a CEPH_CAP_OP_REVOKE
  ceph_seq_t revoke(int caps) {
    ceph_seq_t seq = cap->issue(caps);
    EXPECT_NE(0, cap->revoking());
    revoking_caps.push_back(&cap->item_revoking_caps);
    client_revoking_caps.push_back(&cap->item_client_revoking_caps);
    return seq;
  }

  bool on_revoking_lists() const {
    return cap->item_revoking_caps.is_on_list() ||
           cap->item_client_revoking_caps.is_on_list();
  }
};

// baseline: the client acks the revoke before anything is re-issued
TEST_F(CapabilityRevoke, AckUnlinks)
{
  ceph_seq_t seq = revoke(CAPS_REDUCED);
  EXPECT_TRUE(on_revoking_lists());

  cap->confirm_receipt(seq, CAPS_REDUCED);
  EXPECT_EQ(cap->pending(), cap->issued());
  EXPECT_FALSE(on_revoking_lists());
}

// Locker::issue_caps() re-grants the revoked bits (issue() "adding bits
// only" branch) before the client's ack arrives
TEST_F(CapabilityRevoke, RegrantBeforeAckUnlinks)
{
  ceph_seq_t seq = revoke(CAPS_REDUCED);

  cap->issue(CAPS_FULL);
  EXPECT_EQ(CAPS_FULL, cap->pending());
  EXPECT_EQ(CAPS_FULL, cap->issued());
  EXPECT_FALSE(on_revoking_lists());

  // the late ack for the revoke must not change that
  cap->confirm_receipt(seq, CAPS_REDUCED);
  EXPECT_EQ(CAPS_FULL, cap->pending());
  EXPECT_EQ(CAPS_FULL, cap->issued());
  EXPECT_FALSE(on_revoking_lists());
}

// CInode::encode_inodestat() re-grants the revoked bits in a reply
// (issue_norevoke()) before the client's ack arrives
TEST_F(CapabilityRevoke, NorevokeRegrantBeforeAckUnlinks)
{
  ceph_seq_t seq = revoke(CAPS_REDUCED);

  cap->issue_norevoke(CAPS_FULL);
  EXPECT_EQ(CAPS_FULL, cap->pending());
  EXPECT_EQ(CAPS_FULL, cap->issued());
  EXPECT_FALSE(on_revoking_lists());

  cap->confirm_receipt(seq, CAPS_REDUCED);
  EXPECT_EQ(CAPS_FULL, cap->pending());
  EXPECT_EQ(CAPS_FULL, cap->issued());
  EXPECT_FALSE(on_revoking_lists());
}

// re-granting only part of the revoked bits leaves the revoke in progress
TEST_F(CapabilityRevoke, PartialRegrantStaysRevoking)
{
  ceph_seq_t seq = revoke(CAPS_REDUCED);

  cap->issue(CAPS_REDUCED | CEPH_CAP_FILE_SHARED);
  EXPECT_EQ(CEPH_CAP_FILE_RD, cap->revoking());
  EXPECT_TRUE(on_revoking_lists());

  cap->issue_norevoke(CAPS_REDUCED | CEPH_CAP_FILE_SHARED);
  EXPECT_EQ(CEPH_CAP_FILE_RD, cap->revoking());
  EXPECT_TRUE(on_revoking_lists());

  // the client acks the original revoke: Fr is gone, nothing left to revoke
  cap->confirm_receipt(seq, CAPS_REDUCED);
  EXPECT_EQ(cap->pending(), cap->issued());
  EXPECT_FALSE(on_revoking_lists());
}
