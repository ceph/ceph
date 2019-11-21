// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockOperations.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "librbd/mirror/snapshot/CreateNonPrimaryRequest.h"
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

  uint64_t m_snap_seq = 0;

  void snap_create(MockTestImageCtx &mock_image_ctx,
                   const cls::rbd::SnapshotNamespace &ns,
                   const std::string& snap_name) {
    ASSERT_TRUE(mock_image_ctx.snap_info.insert(
                  {m_snap_seq++,
                   SnapInfo{snap_name, ns, 0, {}, 0, 0, {}}}).second);
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

  void expect_create_snapshot(MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.operations, snap_create(_, _, _))
      .WillOnce(WithArg<2>(CompleteContext(
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
  expect_create_snapshot(mock_image_ctx, 0);
  MockWriteImageStateRequest mock_write_image_state_request;
  expect_write_image_state(mock_image_ctx, mock_write_image_state_request, 0);

  C_SaferCond ctx;
  auto req = new MockCreateNonPrimaryRequest(&mock_image_ctx, "mirror_uuid",
                                             123, {}, nullptr, &ctx);
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
  auto req = new MockCreateNonPrimaryRequest(&mock_image_ctx, "mirror_uuid",
                                             123, {}, nullptr, &ctx);
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
  auto req = new MockCreateNonPrimaryRequest(&mock_image_ctx, "mirror_uuid",
                                             123, {}, nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
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
  expect_create_snapshot(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  auto req = new MockCreateNonPrimaryRequest(&mock_image_ctx, "mirror_uuid",
                                             123, {}, nullptr, &ctx);
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
  expect_create_snapshot(mock_image_ctx, 0);
  MockWriteImageStateRequest mock_write_image_state_request;
  expect_write_image_state(mock_image_ctx, mock_write_image_state_request,
                           -EINVAL);

  C_SaferCond ctx;
  auto req = new MockCreateNonPrimaryRequest(&mock_image_ctx, "mirror_uuid",
                                             123, {}, nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotCreateNonPrimaryRequest, ValidateErrorPrimary) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  cls::rbd::MirrorPrimarySnapshotNamespace ns{false, {"peer_uuid"}};
  snap_create(mock_image_ctx, ns, "mirror_snap");

  InSequence seq;

  expect_refresh_image(mock_image_ctx, false, 0);
  expect_get_mirror_image(
    mock_image_ctx, {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, "gid",
                     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, 0);

  C_SaferCond ctx;
  auto req = new MockCreateNonPrimaryRequest(&mock_image_ctx, "mirror_uuid",
                                             123, {}, nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotCreateNonPrimaryRequest, ValidatePrimaryDemoted) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  cls::rbd::MirrorPrimarySnapshotNamespace ns{true, {"peer_uuid"}};
  snap_create(mock_image_ctx, ns, "mirror_snap");

  InSequence seq;

  expect_refresh_image(mock_image_ctx, false, 0);
  expect_get_mirror_image(
    mock_image_ctx, {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, "gid",
                     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, 0);
  expect_create_snapshot(mock_image_ctx, 0);
  MockWriteImageStateRequest mock_write_image_state_request;
  expect_write_image_state(mock_image_ctx, mock_write_image_state_request, 0);

  C_SaferCond ctx;
  auto req = new MockCreateNonPrimaryRequest(&mock_image_ctx, "mirror_uuid",
                                             123, {}, nullptr, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotCreateNonPrimaryRequest, ValidateErrorNonPrimaryNotCopied) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  cls::rbd::MirrorNonPrimarySnapshotNamespace ns{"mirror_uuid", 111};
  snap_create(mock_image_ctx, ns, "mirror_snap");

  InSequence seq;

  expect_refresh_image(mock_image_ctx, false, 0);
  expect_get_mirror_image(
    mock_image_ctx, {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, "gid",
                     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, 0);

  C_SaferCond ctx;
  auto req = new MockCreateNonPrimaryRequest(&mock_image_ctx, "mirror_uuid",
                                             123, {}, nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotCreateNonPrimaryRequest, ValidateNonPrimaryCopied) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  cls::rbd::MirrorNonPrimarySnapshotNamespace ns{"mirror_uuid", 111};
  ns.copied = true;
  snap_create(mock_image_ctx, ns, "mirror_snap");

  InSequence seq;

  expect_refresh_image(mock_image_ctx, false, 0);
  expect_get_mirror_image(
    mock_image_ctx, {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, "gid",
                     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, 0);
  expect_create_snapshot(mock_image_ctx, 0);
  MockWriteImageStateRequest mock_write_image_state_request;
  expect_write_image_state(mock_image_ctx, mock_write_image_state_request, 0);

  C_SaferCond ctx;
  auto req = new MockCreateNonPrimaryRequest(&mock_image_ctx, "mirror_uuid",
                                             123, {}, nullptr, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd
