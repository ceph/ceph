// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/image/RemoveRequest.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_deleter/RemoveRequest.h"
#include "tools/rbd_mirror/image_deleter/SnapshotPurgeRequest.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace image {

template <>
struct RemoveRequest<librbd::MockTestImageCtx> {
  static RemoveRequest *s_instance;
  Context *on_finish = nullptr;

  static RemoveRequest *create(librados::IoCtx &io_ctx,
                               const std::string &image_name,
                               const std::string &image_id,
                               bool force,
                               bool remove_from_trash,
                               librbd::ProgressContext &progress_ctx,
                               ContextWQ *work_queue,
                               Context *on_finish) {
    assert(s_instance != nullptr);
    EXPECT_TRUE(image_name.empty());
    EXPECT_TRUE(force);
    EXPECT_TRUE(remove_from_trash);
    s_instance->construct(image_id);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD1(construct, void(const std::string&));
  MOCK_METHOD0(send, void());

  RemoveRequest() {
    s_instance = this;
  }
};

RemoveRequest<librbd::MockTestImageCtx>* RemoveRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace image
} // namespace librbd

namespace rbd {
namespace mirror {
namespace image_deleter {

template <>
struct SnapshotPurgeRequest<librbd::MockTestImageCtx> {
  static SnapshotPurgeRequest *s_instance;
  Context *on_finish = nullptr;

  static SnapshotPurgeRequest *create(librados::IoCtx &io_ctx,
                                      const std::string &image_id,
                                      Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->construct(image_id);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD1(construct, void(const std::string&));
  MOCK_METHOD0(send, void());

  SnapshotPurgeRequest() {
    s_instance = this;
  }
};

SnapshotPurgeRequest<librbd::MockTestImageCtx>* SnapshotPurgeRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace image_deleter
} // namespace mirror
} // namespace rbd

#include "tools/rbd_mirror/image_deleter/RemoveRequest.cc"

namespace rbd {
namespace mirror {
namespace image_deleter {

using ::testing::_;
using ::testing::Invoke;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;
using ::testing::WithArgs;

class TestMockImageDeleterRemoveRequest : public TestMockFixture {
public:
  typedef RemoveRequest<librbd::MockTestImageCtx> MockRemoveRequest;
  typedef SnapshotPurgeRequest<librbd::MockTestImageCtx> MockSnapshotPurgeRequest;
  typedef librbd::image::RemoveRequest<librbd::MockTestImageCtx> MockImageRemoveRequest;

  void expect_get_snapcontext(const std::string& image_id,
                              const ::SnapContext &snapc, int r) {
    bufferlist bl;
    encode(snapc, bl);

    EXPECT_CALL(get_mock_io_ctx(m_local_io_ctx),
                exec(librbd::util::header_name(image_id), _, StrEq("rbd"),
                     StrEq("get_snapcontext"), _, _, _))
      .WillOnce(DoAll(WithArg<5>(Invoke([bl](bufferlist *out_bl) {
                                          *out_bl = bl;
                                        })),
                      Return(r)));
  }

  void expect_snapshot_purge(MockSnapshotPurgeRequest &snapshot_purge_request,
                             const std::string &image_id, int r) {
    EXPECT_CALL(snapshot_purge_request, construct(image_id));
    EXPECT_CALL(snapshot_purge_request, send())
      .WillOnce(Invoke([this, &snapshot_purge_request, r]() {
                  m_threads->work_queue->queue(snapshot_purge_request.on_finish, r);
                }));
  }

  void expect_image_remove(MockImageRemoveRequest &image_remove_request,
                           const std::string &image_id, int r) {
    EXPECT_CALL(image_remove_request, construct(image_id));
    EXPECT_CALL(image_remove_request, send())
      .WillOnce(Invoke([this, &image_remove_request, r]() {
                  m_threads->work_queue->queue(image_remove_request.on_finish, r);
                }));
  }
};

TEST_F(TestMockImageDeleterRemoveRequest, Success) {
  InSequence seq;
  expect_get_snapcontext("image id", {1, {1}}, 0);

  MockSnapshotPurgeRequest mock_snapshot_purge_request;
  expect_snapshot_purge(mock_snapshot_purge_request, "image id", 0);

  MockImageRemoveRequest mock_image_remove_request;
  expect_image_remove(mock_image_remove_request, "image id", 0);

  C_SaferCond ctx;
  ErrorResult error_result;
  auto req = MockRemoveRequest::create(m_local_io_ctx, "image id",
                                       &error_result, m_threads->work_queue,
                                       &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDeleterRemoveRequest, GetSnapContextDNE) {
  InSequence seq;
  expect_get_snapcontext("image id", {1, {1}}, -ENOENT);

  MockImageRemoveRequest mock_image_remove_request;
  expect_image_remove(mock_image_remove_request, "image id", 0);

  C_SaferCond ctx;
  ErrorResult error_result;
  auto req = MockRemoveRequest::create(m_local_io_ctx, "image id",
                                       &error_result, m_threads->work_queue,
                                       &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDeleterRemoveRequest, GetSnapContextError) {
  InSequence seq;
  expect_get_snapcontext("image id", {1, {1}}, -EINVAL);

  C_SaferCond ctx;
  ErrorResult error_result;
  auto req = MockRemoveRequest::create(m_local_io_ctx, "image id",
                                       &error_result, m_threads->work_queue,
                                       &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageDeleterRemoveRequest, PurgeSnapshotBusy) {
  InSequence seq;
  expect_get_snapcontext("image id", {1, {1}}, 0);

  MockSnapshotPurgeRequest mock_snapshot_purge_request;
  expect_snapshot_purge(mock_snapshot_purge_request, "image id", -EBUSY);

  C_SaferCond ctx;
  ErrorResult error_result;
  auto req = MockRemoveRequest::create(m_local_io_ctx, "image id",
                                       &error_result, m_threads->work_queue,
                                       &ctx);
  req->send();
  ASSERT_EQ(-EBUSY, ctx.wait());
  ASSERT_EQ(ERROR_RESULT_RETRY_IMMEDIATELY, error_result);
}

TEST_F(TestMockImageDeleterRemoveRequest, PurgeSnapshotError) {
  InSequence seq;
  expect_get_snapcontext("image id", {1, {1}}, 0);

  MockSnapshotPurgeRequest mock_snapshot_purge_request;
  expect_snapshot_purge(mock_snapshot_purge_request, "image id", -EINVAL);

  C_SaferCond ctx;
  ErrorResult error_result;
  auto req = MockRemoveRequest::create(m_local_io_ctx, "image id",
                                       &error_result, m_threads->work_queue,
                                       &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageDeleterRemoveRequest, RemoveError) {
  InSequence seq;
  expect_get_snapcontext("image id", {1, {1}}, 0);

  MockSnapshotPurgeRequest mock_snapshot_purge_request;
  expect_snapshot_purge(mock_snapshot_purge_request, "image id", 0);

  MockImageRemoveRequest mock_image_remove_request;
  expect_image_remove(mock_image_remove_request, "image id", -EINVAL);

  C_SaferCond ctx;
  ErrorResult error_result;
  auto req = MockRemoveRequest::create(m_local_io_ctx, "image id",
                                       &error_result, m_threads->work_queue,
                                       &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace image_deleter
} // namespace mirror
} // namespace rbd
