// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/stringify.h"
#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockOperations.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "librbd/mirror/snapshot/CreatePrimaryRequest.h"
#include "librbd/mirror/snapshot/UnlinkPeerRequest.h"
#include "librbd/mirror/snapshot/Utils.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public MockImageCtx {
  explicit MockTestImageCtx(librbd::ImageCtx& image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace mirror {
namespace snapshot {
namespace util {

namespace {

struct Mock {
  static Mock* s_instance;

  Mock() {
    s_instance = this;
  }

  MOCK_METHOD4(can_create_primary_snapshot,
               bool(librbd::MockTestImageCtx *, bool, bool, uint64_t *));
};

Mock *Mock::s_instance = nullptr;

} // anonymous namespace

template<> bool can_create_primary_snapshot(librbd::MockTestImageCtx *image_ctx,
                                            bool demoted, bool force,
                                            bool* requires_orphan,
                                            uint64_t *rollback_snap_id) {
  return Mock::s_instance->can_create_primary_snapshot(image_ctx, demoted,
                                                       force, rollback_snap_id);
}

} // namespace util

template <>
struct UnlinkPeerRequest<MockTestImageCtx> {
  uint64_t snap_id = CEPH_NOSNAP;
  std::string mirror_peer_uuid;
  Context* on_finish = nullptr;
  static UnlinkPeerRequest* s_instance;
  static UnlinkPeerRequest *create(MockTestImageCtx *image_ctx,
                                   uint64_t snap_id,
                                   const std::string &mirror_peer_uuid,
                                   Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->snap_id = snap_id;
    s_instance->mirror_peer_uuid = mirror_peer_uuid;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  UnlinkPeerRequest() {
    s_instance = this;
  }
};

UnlinkPeerRequest<MockTestImageCtx>* UnlinkPeerRequest<MockTestImageCtx>::s_instance = nullptr;

} // namespace snapshot
} // namespace mirror
} // namespace librbd

// template definitions
#include "librbd/mirror/snapshot/CreatePrimaryRequest.cc"
template class librbd::mirror::snapshot::CreatePrimaryRequest<librbd::MockTestImageCtx>;

namespace librbd {
namespace mirror {
namespace snapshot {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockMirrorSnapshotCreatePrimaryRequest : public TestMockFixture {
public:
  typedef CreatePrimaryRequest<MockTestImageCtx> MockCreatePrimaryRequest;
  typedef UnlinkPeerRequest<MockTestImageCtx> MockUnlinkPeerRequest;
  typedef util::Mock MockUtils;

  uint64_t m_snap_seq = 0;

  void snap_create(MockTestImageCtx &mock_image_ctx,
                   const cls::rbd::SnapshotNamespace &ns,
                   const std::string& snap_name) {
    ASSERT_TRUE(mock_image_ctx.snap_info.insert(
                  {m_snap_seq++,
                   SnapInfo{snap_name, ns, 0, {}, 0, 0, {}}}).second);
  }

  void expect_clone_md_ctx(MockTestImageCtx &mock_image_ctx) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx), clone())
      .WillOnce(Invoke([&mock_image_ctx]() {
                         get_mock_io_ctx(mock_image_ctx.md_ctx).get();
                         return &get_mock_io_ctx(mock_image_ctx.md_ctx);
                       }));
  }

  void expect_can_create_primary_snapshot(MockUtils &mock_utils, bool demoted,
                                          bool force, bool result) {
    EXPECT_CALL(mock_utils,
                can_create_primary_snapshot(_, demoted, force, nullptr))
      .WillOnce(Return(result));
  }

  void expect_get_mirror_peers(MockTestImageCtx &mock_image_ctx,
                               const std::vector<cls::rbd::MirrorPeer> &peers,
                               int r) {
    using ceph::encode;
    bufferlist bl;
    encode(peers, bl);

    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(RBD_MIRRORING, _, StrEq("rbd"), StrEq("mirror_peer_list"),
                     _, _, _))
      .WillOnce(DoAll(WithArg<5>(CopyInBufferlist(bl)),
                      Return(r)));
  }

  void expect_create_snapshot(MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.operations, snap_create(_, _, _, _, _))
      .WillOnce(DoAll(
                  Invoke([this, &mock_image_ctx, r](
                             const cls::rbd::SnapshotNamespace &ns,
                             const std::string& snap_name,
                             uint64_t flags,
                             ProgressContext &prog_ctx,
                             Context *on_finish) {
                           if (r != 0) {
                             return;
                           }
                           snap_create(mock_image_ctx, ns, snap_name);
                         }),
                  WithArg<4>(CompleteContext(
                               r, mock_image_ctx.image_ctx->op_work_queue))
                  ));
  }

  void expect_unlink_peer(MockTestImageCtx &mock_image_ctx,
                          MockUnlinkPeerRequest &mock_unlink_peer_request,
                          uint64_t snap_id, const std::string &peer_uuid,
                          int r) {
    EXPECT_CALL(mock_unlink_peer_request, send())
      .WillOnce(Invoke([&mock_image_ctx, &mock_unlink_peer_request, snap_id,
                        peer_uuid, r]() {
                         ASSERT_EQ(mock_unlink_peer_request.mirror_peer_uuid,
                                   peer_uuid);
                         ASSERT_EQ(mock_unlink_peer_request.snap_id, snap_id);
                         if (r == 0) {
                           auto it = mock_image_ctx.snap_info.find(snap_id);
                           ASSERT_NE(it, mock_image_ctx.snap_info.end());
                           auto info =
                             boost::get<cls::rbd::MirrorSnapshotNamespace>(
                               &it->second.snap_namespace);
                           ASSERT_NE(nullptr, info);
                           ASSERT_NE(0, info->mirror_peer_uuids.erase(
                                       peer_uuid));
                           if (info->mirror_peer_uuids.empty()) {
                             mock_image_ctx.snap_info.erase(it);
                           }
                         }
                         mock_image_ctx.image_ctx->op_work_queue->queue(
                           mock_unlink_peer_request.on_finish, r);
                       }));
  }
};

TEST_F(TestMockMirrorSnapshotCreatePrimaryRequest, Success) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;

  expect_clone_md_ctx(mock_image_ctx);
  MockUtils mock_utils;
  expect_can_create_primary_snapshot(mock_utils, false, false, true);
  expect_get_mirror_peers(mock_image_ctx,
                          {{"uuid", cls::rbd::MIRROR_PEER_DIRECTION_TX, "ceph",
                            "mirror", "mirror uuid"}}, 0);
  expect_create_snapshot(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto req = new MockCreatePrimaryRequest(&mock_image_ctx, "gid", CEPH_NOSNAP,
                                          0U, 0U, nullptr, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotCreatePrimaryRequest, CanNotError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;

  expect_clone_md_ctx(mock_image_ctx);
  MockUtils mock_utils;
  expect_can_create_primary_snapshot(mock_utils, false, false, false);

  C_SaferCond ctx;
  auto req = new MockCreatePrimaryRequest(&mock_image_ctx, "gid", CEPH_NOSNAP,
                                          0U, 0U, nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotCreatePrimaryRequest, GetMirrorPeersError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;

  expect_clone_md_ctx(mock_image_ctx);
  MockUtils mock_utils;
  expect_can_create_primary_snapshot(mock_utils, false, false, true);
  expect_get_mirror_peers(mock_image_ctx,
                          {{"uuid", cls::rbd::MIRROR_PEER_DIRECTION_TX, "ceph",
                            "mirror", "mirror uuid"}}, -EINVAL);

  C_SaferCond ctx;
  auto req = new MockCreatePrimaryRequest(&mock_image_ctx, "gid", CEPH_NOSNAP,
                                          0U, 0U, nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotCreatePrimaryRequest, CreateSnapshotError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;

  expect_clone_md_ctx(mock_image_ctx);
  MockUtils mock_utils;
  expect_can_create_primary_snapshot(mock_utils, false, false, true);
  expect_get_mirror_peers(mock_image_ctx,
                          {{"uuid", cls::rbd::MIRROR_PEER_DIRECTION_TX, "ceph",
                            "mirror", "mirror uuid"}}, 0);
  expect_create_snapshot(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  auto req = new MockCreatePrimaryRequest(&mock_image_ctx, "gid", CEPH_NOSNAP,
                                          0U, 0U, nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotCreatePrimaryRequest, SuccessUnlinkPeer) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ictx->config.set_val("conf_rbd_mirroring_max_mirroring_snapshots", "3");

  MockTestImageCtx mock_image_ctx(*ictx);
  for (int i = 0; i < 3; i++) {
    cls::rbd::MirrorSnapshotNamespace ns{
      cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, {"uuid"}, "", CEPH_NOSNAP};
    snap_create(mock_image_ctx, ns, "mirror_snap");
  }

  InSequence seq;

  expect_clone_md_ctx(mock_image_ctx);
  MockUtils mock_utils;
  expect_can_create_primary_snapshot(mock_utils, false, false, true);
  expect_get_mirror_peers(mock_image_ctx,
                          {{"uuid", cls::rbd::MIRROR_PEER_DIRECTION_TX, "ceph",
                            "mirror", "mirror uuid"}}, 0);
  expect_create_snapshot(mock_image_ctx, 0);
  MockUnlinkPeerRequest mock_unlink_peer_request;
  auto it = mock_image_ctx.snap_info.rbegin();
  auto snap_id = (++it)->first;
  expect_unlink_peer(mock_image_ctx, mock_unlink_peer_request, snap_id, "uuid",
                     0);
  C_SaferCond ctx;
  auto req = new MockCreatePrimaryRequest(&mock_image_ctx, "gid", CEPH_NOSNAP,
                                          0U, 0U, nullptr, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

