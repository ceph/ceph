// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockOperations.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "librbd/mirror/snapshot/UnlinkPeerRequest.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public MockImageCtx {
  explicit MockTestImageCtx(librbd::ImageCtx& image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace
} // namespace librbd

// template definitions
#include "librbd/mirror/snapshot/UnlinkPeerRequest.cc"
template class librbd::mirror::snapshot::UnlinkPeerRequest<librbd::MockTestImageCtx>;

namespace librbd {
namespace mirror {
namespace snapshot {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;

class TestMockMirrorSnapshotUnlinkPeerRequest : public TestMockFixture {
public:
  typedef UnlinkPeerRequest<MockTestImageCtx> MockUnlinkPeerRequest;

  uint64_t m_snap_seq = 0;

  uint64_t snap_create(MockTestImageCtx &mock_image_ctx,
                   const cls::rbd::SnapshotNamespace &ns,
                   const std::string& snap_name) {
    EXPECT_TRUE(mock_image_ctx.snap_info.insert(
                  {++m_snap_seq,
                   SnapInfo{snap_name, ns, 0, {}, 0, 0, {}}}).second);
    return m_snap_seq;
  }

  void expect_get_snap_info(MockTestImageCtx &mock_image_ctx,
                            librados::snap_t snap_id) {
    EXPECT_CALL(mock_image_ctx, get_snap_info(snap_id))
      .WillRepeatedly(Invoke([&mock_image_ctx](
                                 librados::snap_t snap_id) -> librbd::SnapInfo * {
                               auto it = mock_image_ctx.snap_info.find(snap_id);
                               if (it == mock_image_ctx.snap_info.end()) {
                                 return nullptr;
                               }
                               return &it->second;
                             }));
  }

  void expect_is_refresh_required(MockTestImageCtx &mock_image_ctx,
                                  bool refresh_required) {
    EXPECT_CALL(*mock_image_ctx.state, is_refresh_required())
      .WillOnce(Return(refresh_required));
  }

  void expect_refresh_image(MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.state, refresh(_))
      .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_unlink_peer(MockTestImageCtx &mock_image_ctx, uint64_t snap_id,
                          const std::string &peer_uuid, int r) {
    using ceph::encode;
    bufferlist bl;
    encode(snapid_t{snap_id}, bl);
    encode(peer_uuid, bl);

    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                     StrEq("mirror_image_snapshot_unlink_peer"),
                     ContentsEqual(bl), _, _))
      .WillOnce(Invoke([&mock_image_ctx, snap_id, peer_uuid, r](auto&&... args) -> int {
                         if (r == 0) {
                           auto it = mock_image_ctx.snap_info.find(snap_id);
                           EXPECT_NE(it, mock_image_ctx.snap_info.end());
                           auto info =
                             boost::get<cls::rbd::MirrorSnapshotNamespace>(
                               &it->second.snap_namespace);
                           EXPECT_NE(nullptr, info);
                           EXPECT_NE(0, info->mirror_peer_uuids.erase(
                                       peer_uuid));
                         }
                         return r;
                       }));
  }

  void expect_notify_update(MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(mock_image_ctx, notify_update(_))
      .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_remove_snapshot(MockTestImageCtx &mock_image_ctx,
                              uint64_t snap_id, int r) {
    EXPECT_CALL(*mock_image_ctx.operations, snap_remove(_, _, _))
      .WillOnce(Invoke([&mock_image_ctx, snap_id, r](
                           const cls::rbd::SnapshotNamespace &snap_namespace,
                           const std::string &snap_name, Context *on_finish) {
                         if (r == 0) {
                           auto it = mock_image_ctx.snap_info.find(snap_id);
                           EXPECT_NE(it, mock_image_ctx.snap_info.end());
                           EXPECT_EQ(it->second.snap_namespace, snap_namespace);
                           EXPECT_EQ(it->second.name, snap_name);
                           mock_image_ctx.snap_info.erase(it);
                         }
                         mock_image_ctx.image_ctx->op_work_queue->queue(
                           on_finish, r);
                       }));
  }
};

TEST_F(TestMockMirrorSnapshotUnlinkPeerRequest, Success) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  cls::rbd::MirrorSnapshotNamespace ns{
    cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, {"peer1_uuid", "peer2_uuid"},
    "", CEPH_NOSNAP};
  auto snap_id = snap_create(mock_image_ctx, ns, "mirror_snap");

  expect_get_snap_info(mock_image_ctx, snap_id);

  InSequence seq;

  expect_is_refresh_required(mock_image_ctx, true);
  expect_refresh_image(mock_image_ctx, 0);
  expect_unlink_peer(mock_image_ctx, snap_id, "peer1_uuid", 0);
  expect_notify_update(mock_image_ctx, 0);
  expect_refresh_image(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto req = new MockUnlinkPeerRequest(&mock_image_ctx, snap_id, "peer1_uuid",
                                       &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotUnlinkPeerRequest, RemoveSnapshot) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  cls::rbd::MirrorSnapshotNamespace ns{
    cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, {"peer_uuid"},
    "", CEPH_NOSNAP};
  auto snap_id = snap_create(mock_image_ctx, ns, "mirror_snap");
  snap_create(mock_image_ctx, ns, "mirror_snap2");

  expect_get_snap_info(mock_image_ctx, snap_id);

  InSequence seq;

  expect_is_refresh_required(mock_image_ctx, true);
  expect_refresh_image(mock_image_ctx, 0);
  expect_remove_snapshot(mock_image_ctx, snap_id, 0);

  C_SaferCond ctx;
  auto req = new MockUnlinkPeerRequest(&mock_image_ctx, snap_id, "peer_uuid",
                                       &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotUnlinkPeerRequest, SnapshotRemoveEmptyPeers) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  cls::rbd::MirrorSnapshotNamespace ns{
    cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, {},
    "", CEPH_NOSNAP};
  auto snap_id = snap_create(mock_image_ctx, ns, "mirror_snap");
  ns.mirror_peer_uuids = {"peer_uuid"};
  snap_create(mock_image_ctx, ns, "mirror_snap2");

  expect_get_snap_info(mock_image_ctx, snap_id);

  InSequence seq;

  expect_is_refresh_required(mock_image_ctx, true);
  expect_refresh_image(mock_image_ctx, 0);
  expect_remove_snapshot(mock_image_ctx, snap_id, 0);

  C_SaferCond ctx;
  auto req = new MockUnlinkPeerRequest(&mock_image_ctx, snap_id, "peer_uuid",
                                       &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotUnlinkPeerRequest, SnapshotDNE) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  expect_get_snap_info(mock_image_ctx, 123);

  InSequence seq;

  expect_is_refresh_required(mock_image_ctx, true);
  expect_refresh_image(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto req = new MockUnlinkPeerRequest(&mock_image_ctx, 123, "peer_uuid", &ctx);
  req->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotUnlinkPeerRequest, PeerDNE) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  cls::rbd::MirrorSnapshotNamespace ns{
    cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, {"peer_uuid"},
    "", CEPH_NOSNAP};
  auto snap_id = snap_create(mock_image_ctx, ns, "mirror_snap");

  expect_get_snap_info(mock_image_ctx, snap_id);

  InSequence seq;

  expect_is_refresh_required(mock_image_ctx, true);
  expect_refresh_image(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto req = new MockUnlinkPeerRequest(&mock_image_ctx, snap_id, "unknown_peer",
                                       &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotUnlinkPeerRequest, InvalidSnapshot) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  cls::rbd::UserSnapshotNamespace ns;
  auto snap_id = snap_create(mock_image_ctx, ns, "user_snap");

  expect_get_snap_info(mock_image_ctx, snap_id);

  InSequence seq;

  expect_is_refresh_required(mock_image_ctx, false);

  C_SaferCond ctx;
  auto req = new MockUnlinkPeerRequest(&mock_image_ctx, snap_id, "peer_uuid",
                                       &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotUnlinkPeerRequest, RefreshError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;

  expect_is_refresh_required(mock_image_ctx, true);
  expect_refresh_image(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  auto req = new MockUnlinkPeerRequest(&mock_image_ctx, 123, "peer_uuid", &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotUnlinkPeerRequest, UnlinkError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  cls::rbd::MirrorSnapshotNamespace ns{
    cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, {"peer1_uuid", "peer2_uuid"},
    "", CEPH_NOSNAP};
  auto snap_id = snap_create(mock_image_ctx, ns, "mirror_snap");

  expect_get_snap_info(mock_image_ctx, snap_id);

  InSequence seq;

  expect_is_refresh_required(mock_image_ctx, false);
  expect_unlink_peer(mock_image_ctx, snap_id, "peer1_uuid", -EINVAL);

  C_SaferCond ctx;
  auto req = new MockUnlinkPeerRequest(&mock_image_ctx, snap_id, "peer1_uuid",
                                       &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotUnlinkPeerRequest, NotifyError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  cls::rbd::MirrorSnapshotNamespace ns{
    cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, {"peer1_uuid", "peer2_uuid"},
    "", CEPH_NOSNAP};
  auto snap_id = snap_create(mock_image_ctx, ns, "mirror_snap");

  expect_get_snap_info(mock_image_ctx, snap_id);

  InSequence seq;

  expect_is_refresh_required(mock_image_ctx, false);
  expect_unlink_peer(mock_image_ctx, snap_id, "peer1_uuid", 0);
  expect_notify_update(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  auto req = new MockUnlinkPeerRequest(&mock_image_ctx, snap_id, "peer1_uuid",
                                       &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotUnlinkPeerRequest, RemoveSnapshotError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  cls::rbd::MirrorSnapshotNamespace ns{
    cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, {"peer_uuid"},
    "", CEPH_NOSNAP};
  auto snap_id = snap_create(mock_image_ctx, ns, "mirror_snap");
  snap_create(mock_image_ctx, ns, "mirror_snap2");

  expect_get_snap_info(mock_image_ctx, snap_id);

  InSequence seq;

  expect_is_refresh_required(mock_image_ctx, false);
  expect_remove_snapshot(mock_image_ctx, snap_id, -EINVAL);

  C_SaferCond ctx;
  auto req = new MockUnlinkPeerRequest(&mock_image_ctx, snap_id, "peer_uuid",
                                       &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd
