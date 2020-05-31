// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockOperations.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "librbd/mirror/snapshot/CreateNonPrimaryRequest.h"
#include "librbd/mirror/snapshot/Utils.h"
#include "librbd/mirror/snapshot/WriteImageStateRequest.h"

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

  MOCK_METHOD1(can_create_non_primary_snapshot,
               bool(librbd::MockTestImageCtx *));
};

Mock *Mock::s_instance = nullptr;

} // anonymous namespace

template<> bool can_create_non_primary_snapshot(
    librbd::MockTestImageCtx *image_ctx) {
  return Mock::s_instance->can_create_non_primary_snapshot(image_ctx);
}

} // namespace util

template <>
struct WriteImageStateRequest<MockTestImageCtx> {
  uint64_t snap_id = CEPH_NOSNAP;
  ImageState image_state;
  Context* on_finish = nullptr;
  static WriteImageStateRequest* s_instance;
  static WriteImageStateRequest *create(MockTestImageCtx *image_ctx,
                                        uint64_t snap_id,
                                        const ImageState &image_state,
                                        Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->snap_id = snap_id;
    s_instance->image_state = image_state;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  WriteImageStateRequest() {
    s_instance = this;
  }
};

WriteImageStateRequest<MockTestImageCtx>* WriteImageStateRequest<MockTestImageCtx>::s_instance = nullptr;

} // namespace snapshot
} // namespace mirror
} // namespace librbd

// template definitions
#include "librbd/mirror/snapshot/CreateNonPrimaryRequest.cc"
template class librbd::mirror::snapshot::CreateNonPrimaryRequest<librbd::MockTestImageCtx>;

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

class TestMockMirrorSnapshotCreateNonPrimaryRequest : public TestMockFixture {
public:
  typedef CreateNonPrimaryRequest<MockTestImageCtx> MockCreateNonPrimaryRequest;
  typedef WriteImageStateRequest<MockTestImageCtx> MockWriteImageStateRequest;
  typedef util::Mock MockUtils;

  void expect_clone_md_ctx(MockTestImageCtx &mock_image_ctx) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx), clone())
      .WillOnce(Invoke([&mock_image_ctx]() {
                         get_mock_io_ctx(mock_image_ctx.md_ctx).get();
                         return &get_mock_io_ctx(mock_image_ctx.md_ctx);
                       }));
  }

  void expect_refresh_image(MockTestImageCtx &mock_image_ctx,
                            bool refresh_required, int r) {
    EXPECT_CALL(*mock_image_ctx.state, is_refresh_required())
      .WillOnce(Return(refresh_required));
    if (refresh_required) {
      EXPECT_CALL(*mock_image_ctx.state, refresh(_))
        .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
    }
  }

  void expect_get_mirror_image(MockTestImageCtx &mock_image_ctx,
                               const cls::rbd::MirrorImage &mirror_image,
                               int r) {
    using ceph::encode;
    bufferlist bl;
    encode(mirror_image, bl);

    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(RBD_MIRRORING, _, StrEq("rbd"), StrEq("mirror_image_get"),
                     _, _, _))
      .WillOnce(DoAll(WithArg<5>(CopyInBufferlist(bl)),
                      Return(r)));
  }

  void expect_can_create_non_primary_snapshot(MockUtils &mock_utils,
                                              bool result) {
    EXPECT_CALL(mock_utils, can_create_non_primary_snapshot(_))
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
      .WillOnce(WithArg<4>(CompleteContext(
                             r, mock_image_ctx.image_ctx->op_work_queue)));
  }

  void expect_write_image_state(
      MockTestImageCtx &mock_image_ctx,
      MockWriteImageStateRequest &mock_write_image_state_request, int r) {
    EXPECT_CALL(mock_image_ctx, get_snap_id(_, _))
      .WillOnce(Return(123));
    EXPECT_CALL(mock_write_image_state_request, send())
      .WillOnce(Invoke([&mock_image_ctx, &mock_write_image_state_request, r]() {
                         mock_image_ctx.image_ctx->op_work_queue->queue(
                           mock_write_image_state_request.on_finish, r);
                       }));
  }
};

TEST_F(TestMockMirrorSnapshotCreateNonPrimaryRequest, Success) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;

  expect_refresh_image(mock_image_ctx, true, 0);
  expect_get_mirror_image(
    mock_image_ctx, {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, "gid",
                     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, 0);
  MockUtils mock_utils;
  expect_can_create_non_primary_snapshot(mock_utils, true);
  expect_create_snapshot(mock_image_ctx, 0);
  MockWriteImageStateRequest mock_write_image_state_request;
  expect_write_image_state(mock_image_ctx, mock_write_image_state_request, 0);

  C_SaferCond ctx;
  auto req = new MockCreateNonPrimaryRequest(&mock_image_ctx, false,
                                             "mirror_uuid", 123, {{1, 2}}, {},
                                             nullptr, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotCreateNonPrimaryRequest, SuccessDemoted) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;

  expect_clone_md_ctx(mock_image_ctx);
  expect_refresh_image(mock_image_ctx, true, 0);
  expect_get_mirror_image(
    mock_image_ctx, {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, "gid",
                     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, 0);
  MockUtils mock_utils;
  expect_can_create_non_primary_snapshot(mock_utils, true);
  expect_get_mirror_peers(mock_image_ctx,
                          {{"uuid", cls::rbd::MIRROR_PEER_DIRECTION_TX, "ceph",
                            "mirror", "mirror uuid"}}, 0);
  expect_create_snapshot(mock_image_ctx, 0);
  MockWriteImageStateRequest mock_write_image_state_request;
  expect_write_image_state(mock_image_ctx, mock_write_image_state_request, 0);

  C_SaferCond ctx;
  auto req = new MockCreateNonPrimaryRequest(&mock_image_ctx, true,
                                             "mirror_uuid", 123, {{1, 2}}, {},
                                             nullptr, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotCreateNonPrimaryRequest, RefreshError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;

  expect_refresh_image(mock_image_ctx, true, -EINVAL);

  C_SaferCond ctx;
  auto req = new MockCreateNonPrimaryRequest(&mock_image_ctx, false,
                                             "mirror_uuid", 123, {{1, 2}}, {},
                                             nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotCreateNonPrimaryRequest, GetMirrorImageError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;

  expect_refresh_image(mock_image_ctx, false, 0);
  expect_get_mirror_image(
    mock_image_ctx, {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, "gid",
                     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, -EINVAL);

  C_SaferCond ctx;
  auto req = new MockCreateNonPrimaryRequest(&mock_image_ctx, false,
                                             "mirror_uuid", 123, {{1, 2}}, {},
                                             nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotCreateNonPrimaryRequest, CanNotError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;

  expect_refresh_image(mock_image_ctx, false, 0);
  expect_get_mirror_image(
    mock_image_ctx, {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, "gid",
                     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, 0);
  MockUtils mock_utils;
  expect_can_create_non_primary_snapshot(mock_utils, false);

  C_SaferCond ctx;
  auto req = new MockCreateNonPrimaryRequest(&mock_image_ctx, false,
                                             "mirror_uuid", 123, {{1, 2}}, {},
                                             nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotCreateNonPrimaryRequest, GetMirrorPeersError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;

  expect_clone_md_ctx(mock_image_ctx);
  expect_refresh_image(mock_image_ctx, true, 0);
  expect_get_mirror_image(
    mock_image_ctx, {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, "gid",
                     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, 0);
  MockUtils mock_utils;
  expect_can_create_non_primary_snapshot(mock_utils, true);
  expect_get_mirror_peers(mock_image_ctx, {}, -EPERM);

  C_SaferCond ctx;
  auto req = new MockCreateNonPrimaryRequest(&mock_image_ctx, true,
                                             "mirror_uuid", 123, {{1, 2}}, {},
                                             nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotCreateNonPrimaryRequest, CreateSnapshotError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;

  expect_refresh_image(mock_image_ctx, true, 0);
  expect_get_mirror_image(
    mock_image_ctx, {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, "gid",
                     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, 0);
  MockUtils mock_utils;
  expect_can_create_non_primary_snapshot(mock_utils, true);
  expect_create_snapshot(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  auto req = new MockCreateNonPrimaryRequest(&mock_image_ctx, false,
                                             "mirror_uuid", 123, {{1, 2}}, {},
                                             nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotCreateNonPrimaryRequest, WriteImageStateError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;

  expect_refresh_image(mock_image_ctx, true, 0);
  expect_get_mirror_image(
    mock_image_ctx, {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, "gid",
                     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, 0);
  MockUtils mock_utils;
  expect_can_create_non_primary_snapshot(mock_utils, true);
  expect_create_snapshot(mock_image_ctx, 0);
  MockWriteImageStateRequest mock_write_image_state_request;
  expect_write_image_state(mock_image_ctx, mock_write_image_state_request,
                           -EINVAL);

  C_SaferCond ctx;
  auto req = new MockCreateNonPrimaryRequest(&mock_image_ctx, false,
                                             "mirror_uuid", 123, {{1, 2}}, {},
                                             nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd
