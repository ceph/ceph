// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/TrashWatcher.h"
#include "librbd/Utils.h"
#include "librbd/trash/RemoveRequest.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_deleter/SnapshotPurgeRequest.h"
#include "tools/rbd_mirror/image_deleter/TrashRemoveRequest.h"
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

template<>
struct TrashWatcher<MockTestImageCtx> {
  static TrashWatcher* s_instance;
  static void notify_image_removed(librados::IoCtx&,
                                   const std::string& image_id, Context *ctx) {
    ceph_assert(s_instance != nullptr);
    s_instance->notify_image_removed(image_id, ctx);
  }

  MOCK_METHOD2(notify_image_removed, void(const std::string&, Context*));

  TrashWatcher() {
    s_instance = this;
  }
};

TrashWatcher<MockTestImageCtx>* TrashWatcher<MockTestImageCtx>::s_instance = nullptr;

namespace trash {

template <>
struct RemoveRequest<librbd::MockTestImageCtx> {
  static RemoveRequest *s_instance;
  Context *on_finish = nullptr;

  static RemoveRequest *create(librados::IoCtx &io_ctx,
                               const std::string &image_id,
                               librbd::asio::ContextWQ *work_queue,
                               bool force,
                               librbd::ProgressContext &progress_ctx,
                               Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    EXPECT_TRUE(force);
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

} // namespace trash
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
    ceph_assert(s_instance != nullptr);
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

#include "tools/rbd_mirror/image_deleter/TrashRemoveRequest.cc"

namespace rbd {
namespace mirror {
namespace image_deleter {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;
using ::testing::WithArgs;

class TestMockImageDeleterTrashRemoveRequest : public TestMockFixture {
public:
  typedef TrashRemoveRequest<librbd::MockTestImageCtx> MockTrashRemoveRequest;
  typedef SnapshotPurgeRequest<librbd::MockTestImageCtx> MockSnapshotPurgeRequest;
  typedef librbd::TrashWatcher<librbd::MockTestImageCtx> MockTrashWatcher;
  typedef librbd::trash::RemoveRequest<librbd::MockTestImageCtx> MockLibrbdTrashRemoveRequest;

  void expect_trash_get(const cls::rbd::TrashImageSpec& trash_spec, int r) {
    using ceph::encode;
    EXPECT_CALL(get_mock_io_ctx(m_local_io_ctx),
                exec(StrEq(RBD_TRASH), _, StrEq("rbd"),
                     StrEq("trash_get"), _, _, _, _))
      .WillOnce(WithArg<5>(Invoke([trash_spec, r](bufferlist* bl) {
                             encode(trash_spec, *bl);
                             return r;
                           })));
  }

  void expect_trash_state_set(const std::string& image_id, int r) {
    bufferlist in_bl;
    encode(image_id, in_bl);
    encode(cls::rbd::TRASH_IMAGE_STATE_REMOVING, in_bl);
    encode(cls::rbd::TRASH_IMAGE_STATE_NORMAL, in_bl);

    EXPECT_CALL(get_mock_io_ctx(m_local_io_ctx),
                exec(StrEq(RBD_TRASH), _, StrEq("rbd"),
                     StrEq("trash_state_set"),
                     ContentsEqual(in_bl), _, _, _))
      .WillOnce(Return(r));
  }

  void expect_get_snapcontext(const std::string& image_id,
                              const ::SnapContext &snapc, int r) {
    bufferlist bl;
    encode(snapc, bl);

    EXPECT_CALL(get_mock_io_ctx(m_local_io_ctx),
                exec(librbd::util::header_name(image_id), _, StrEq("rbd"),
                     StrEq("get_snapcontext"), _, _, _, _))
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
                  m_threads->work_queue->queue(
                    snapshot_purge_request.on_finish, r);
                }));
  }

  void expect_image_remove(MockLibrbdTrashRemoveRequest &image_remove_request,
                           const std::string &image_id, int r) {
    EXPECT_CALL(image_remove_request, construct(image_id));
    EXPECT_CALL(image_remove_request, send())
      .WillOnce(Invoke([this, &image_remove_request, r]() {
                  m_threads->work_queue->queue(
                    image_remove_request.on_finish, r);
                }));
  }

  void expect_notify_image_removed(MockTrashWatcher& mock_trash_watcher,
                                   const std::string& image_id) {
    EXPECT_CALL(mock_trash_watcher, notify_image_removed(image_id, _))
      .WillOnce(WithArg<1>(Invoke([this](Context *ctx) {
                             m_threads->work_queue->queue(ctx, 0);
                           })));
  }

};

TEST_F(TestMockImageDeleterTrashRemoveRequest, Success) {
  InSequence seq;

  cls::rbd::TrashImageSpec trash_image_spec{
    cls::rbd::TRASH_IMAGE_SOURCE_MIRRORING, "image name", {}, {}};
  expect_trash_get(trash_image_spec, 0);

  expect_trash_state_set("image id", 0);

  expect_get_snapcontext("image id", {1, {1}}, 0);

  MockSnapshotPurgeRequest mock_snapshot_purge_request;
  expect_snapshot_purge(mock_snapshot_purge_request, "image id", 0);

  MockLibrbdTrashRemoveRequest mock_image_remove_request;
  expect_image_remove(mock_image_remove_request, "image id", 0);

  MockTrashWatcher mock_trash_watcher;
  expect_notify_image_removed(mock_trash_watcher, "image id");

  C_SaferCond ctx;
  ErrorResult error_result;
  auto req = MockTrashRemoveRequest::create(m_local_io_ctx, "image id",
                                            &error_result,
                                            m_threads->work_queue, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashRemoveRequest, TrashDNE) {
  InSequence seq;

  cls::rbd::TrashImageSpec trash_image_spec{
    cls::rbd::TRASH_IMAGE_SOURCE_MIRRORING, "image name", {}, {}};
  expect_trash_get(trash_image_spec, -ENOENT);

  C_SaferCond ctx;
  ErrorResult error_result;
  auto req = MockTrashRemoveRequest::create(m_local_io_ctx, "image id",
                                            &error_result,
                                            m_threads->work_queue, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashRemoveRequest, TrashError) {
  InSequence seq;

  cls::rbd::TrashImageSpec trash_image_spec{
    cls::rbd::TRASH_IMAGE_SOURCE_MIRRORING, "image name", {}, {}};
  expect_trash_get(trash_image_spec, -EPERM);

  C_SaferCond ctx;
  ErrorResult error_result;
  auto req = MockTrashRemoveRequest::create(m_local_io_ctx, "image id",
                                            &error_result,
                                            m_threads->work_queue, &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashRemoveRequest, TrashSourceIncorrect) {
  InSequence seq;

  cls::rbd::TrashImageSpec trash_image_spec{
    cls::rbd::TRASH_IMAGE_SOURCE_USER, "image name", {}, {}};
  expect_trash_get(trash_image_spec, 0);

  C_SaferCond ctx;
  ErrorResult error_result;
  auto req = MockTrashRemoveRequest::create(m_local_io_ctx, "image id",
                                            &error_result,
                                            m_threads->work_queue, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashRemoveRequest, TrashStateIncorrect) {
  InSequence seq;

  cls::rbd::TrashImageSpec trash_image_spec{
    cls::rbd::TRASH_IMAGE_SOURCE_MIRRORING, "image name", {}, {}};
  trash_image_spec.state = cls::rbd::TRASH_IMAGE_STATE_RESTORING;
  expect_trash_get(trash_image_spec, 0);

  C_SaferCond ctx;
  ErrorResult error_result;
  auto req = MockTrashRemoveRequest::create(m_local_io_ctx, "image id",
                                            &error_result,
                                            m_threads->work_queue, &ctx);
  req->send();
  ASSERT_EQ(-EBUSY, ctx.wait());
  ASSERT_EQ(ERROR_RESULT_RETRY_IMMEDIATELY, error_result);
}

TEST_F(TestMockImageDeleterTrashRemoveRequest, TrashSetStateDNE) {
  InSequence seq;

  cls::rbd::TrashImageSpec trash_image_spec{
    cls::rbd::TRASH_IMAGE_SOURCE_MIRRORING, "image name", {}, {}};
  expect_trash_get(trash_image_spec, 0);

  expect_trash_state_set("image id", -ENOENT);

  C_SaferCond ctx;
  ErrorResult error_result;
  auto req = MockTrashRemoveRequest::create(m_local_io_ctx, "image id",
                                            &error_result,
                                            m_threads->work_queue, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashRemoveRequest, TrashSetStateError) {
  InSequence seq;

  cls::rbd::TrashImageSpec trash_image_spec{
    cls::rbd::TRASH_IMAGE_SOURCE_MIRRORING, "image name", {}, {}};
  expect_trash_get(trash_image_spec, 0);

  expect_trash_state_set("image id", -EPERM);

  C_SaferCond ctx;
  ErrorResult error_result;
  auto req = MockTrashRemoveRequest::create(m_local_io_ctx, "image id",
                                            &error_result,
                                            m_threads->work_queue, &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashRemoveRequest, GetSnapContextDNE) {
  InSequence seq;

  cls::rbd::TrashImageSpec trash_image_spec{
    cls::rbd::TRASH_IMAGE_SOURCE_MIRRORING, "image name", {}, {}};
  expect_trash_get(trash_image_spec, 0);

  expect_trash_state_set("image id", 0);

  expect_get_snapcontext("image id", {1, {1}}, -ENOENT);

  MockLibrbdTrashRemoveRequest mock_image_remove_request;
  expect_image_remove(mock_image_remove_request, "image id", 0);

  MockTrashWatcher mock_trash_watcher;
  expect_notify_image_removed(mock_trash_watcher, "image id");

  C_SaferCond ctx;
  ErrorResult error_result;
  auto req = MockTrashRemoveRequest::create(m_local_io_ctx, "image id",
                                            &error_result,
                                            m_threads->work_queue, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashRemoveRequest, GetSnapContextError) {
  InSequence seq;

  cls::rbd::TrashImageSpec trash_image_spec{
    cls::rbd::TRASH_IMAGE_SOURCE_MIRRORING, "image name", {}, {}};
  expect_trash_get(trash_image_spec, 0);

  expect_trash_state_set("image id", 0);

  expect_get_snapcontext("image id", {1, {1}}, -EINVAL);

  C_SaferCond ctx;
  ErrorResult error_result;
  auto req = MockTrashRemoveRequest::create(m_local_io_ctx, "image id",
                                            &error_result,
                                            m_threads->work_queue, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashRemoveRequest, PurgeSnapshotBusy) {
  InSequence seq;

  cls::rbd::TrashImageSpec trash_image_spec{
    cls::rbd::TRASH_IMAGE_SOURCE_MIRRORING, "image name", {}, {}};
  expect_trash_get(trash_image_spec, 0);

  expect_trash_state_set("image id", 0);

  expect_get_snapcontext("image id", {1, {1}}, 0);

  MockSnapshotPurgeRequest mock_snapshot_purge_request;
  expect_snapshot_purge(mock_snapshot_purge_request, "image id", -EBUSY);

  C_SaferCond ctx;
  ErrorResult error_result;
  auto req = MockTrashRemoveRequest::create(m_local_io_ctx, "image id",
                                            &error_result,
                                            m_threads->work_queue, &ctx);
  req->send();
  ASSERT_EQ(-EBUSY, ctx.wait());
  ASSERT_EQ(ERROR_RESULT_RETRY_IMMEDIATELY, error_result);
}

TEST_F(TestMockImageDeleterTrashRemoveRequest, PurgeSnapshotError) {
  InSequence seq;

  cls::rbd::TrashImageSpec trash_image_spec{
    cls::rbd::TRASH_IMAGE_SOURCE_MIRRORING, "image name", {}, {}};
  expect_trash_get(trash_image_spec, 0);

  expect_trash_state_set("image id", 0);

  expect_get_snapcontext("image id", {1, {1}}, 0);

  MockSnapshotPurgeRequest mock_snapshot_purge_request;
  expect_snapshot_purge(mock_snapshot_purge_request, "image id", -EINVAL);

  C_SaferCond ctx;
  ErrorResult error_result;
  auto req = MockTrashRemoveRequest::create(m_local_io_ctx, "image id",
                                            &error_result,
                                            m_threads->work_queue, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashRemoveRequest, RemoveError) {
  InSequence seq;

  cls::rbd::TrashImageSpec trash_image_spec{
    cls::rbd::TRASH_IMAGE_SOURCE_MIRRORING, "image name", {}, {}};
  expect_trash_get(trash_image_spec, 0);

  expect_trash_state_set("image id", 0);

  expect_get_snapcontext("image id", {1, {1}}, 0);

  MockSnapshotPurgeRequest mock_snapshot_purge_request;
  expect_snapshot_purge(mock_snapshot_purge_request, "image id", 0);

  MockLibrbdTrashRemoveRequest mock_image_remove_request;
  expect_image_remove(mock_image_remove_request, "image id", -EINVAL);

  C_SaferCond ctx;
  ErrorResult error_result;
  auto req = MockTrashRemoveRequest::create(m_local_io_ctx, "image id",
                                            &error_result,
                                            m_threads->work_queue, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace image_deleter
} // namespace mirror
} // namespace rbd
