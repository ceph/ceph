// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/stringify.h"
#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockOperations.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "librbd/image/ListWatchersRequest.h"
#include "librbd/mirror/snapshot/CreateNonPrimaryRequest.h"
#include "librbd/mirror/snapshot/CreatePrimaryRequest.h"
#include "librbd/mirror/snapshot/PromoteRequest.h"
#include "librbd/mirror/snapshot/Utils.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public MockImageCtx {
  explicit MockTestImageCtx(librbd::ImageCtx& image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace image {

template <>
struct ListWatchersRequest<MockTestImageCtx> {
  std::list<obj_watch_t> *watchers;
  Context* on_finish = nullptr;
  static ListWatchersRequest* s_instance;
  static ListWatchersRequest *create(MockTestImageCtx &image_ctx, int flags,
                                     std::list<obj_watch_t> *watchers,
                                     Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->watchers = watchers;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  ListWatchersRequest() {
    s_instance = this;
  }
};

ListWatchersRequest<MockTestImageCtx>* ListWatchersRequest<MockTestImageCtx>::s_instance = nullptr;

} // namespace image

namespace mirror {
namespace snapshot {
namespace util {

namespace {

struct Mock {
  static Mock* s_instance;

  Mock() {
    s_instance = this;
  }

  MOCK_METHOD5(can_create_primary_snapshot,
               bool(librbd::MockTestImageCtx *, bool, bool, bool*, uint64_t *));
};

Mock *Mock::s_instance = nullptr;

} // anonymous namespace

template<> bool can_create_primary_snapshot(librbd::MockTestImageCtx *image_ctx,
                                            bool demoted, bool force,
                                            bool* requires_orphan,
                                            uint64_t *rollback_snap_id) {
  return Mock::s_instance->can_create_primary_snapshot(image_ctx, demoted,
                                                       force, requires_orphan,
                                                       rollback_snap_id);
}

} // namespace util

template <>
struct CreateNonPrimaryRequest<MockTestImageCtx> {
  std::string primary_mirror_uuid;
  uint64_t primary_snap_id = CEPH_NOSNAP;
  Context* on_finish = nullptr;
  static CreateNonPrimaryRequest* s_instance;
  static CreateNonPrimaryRequest *create(MockTestImageCtx *image_ctx,
                                         bool demoted,
                                         const std::string &primary_mirror_uuid,
                                         uint64_t primary_snap_id,
                                         SnapSeqs snap_seqs,
                                         const ImageState &image_state,
                                         uint64_t *snap_id,
                                         Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->primary_mirror_uuid = primary_mirror_uuid;
    s_instance->primary_snap_id = primary_snap_id;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  CreateNonPrimaryRequest() {
    s_instance = this;
  }
};

CreateNonPrimaryRequest<MockTestImageCtx>* CreateNonPrimaryRequest<MockTestImageCtx>::s_instance = nullptr;

template <>
struct CreatePrimaryRequest<MockTestImageCtx> {
  bool demoted = false;
  bool force = false;
  Context* on_finish = nullptr;
  static CreatePrimaryRequest* s_instance;
  static CreatePrimaryRequest *create(MockTestImageCtx *image_ctx,
                                      const std::string& global_image_id,
                                      uint64_t clean_since_snap_id,
                                      uint64_t snap_create_flags,
                                      uint32_t flags, uint64_t *snap_id,
                                      Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->demoted = ((flags & CREATE_PRIMARY_FLAG_DEMOTED) != 0);
    s_instance->force = ((flags & CREATE_PRIMARY_FLAG_FORCE) != 0);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  CreatePrimaryRequest() {
    s_instance = this;
  }
};

CreatePrimaryRequest<MockTestImageCtx>* CreatePrimaryRequest<MockTestImageCtx>::s_instance = nullptr;

} // namespace snapshot
} // namespace mirror
} // namespace librbd

// template definitions
#include "librbd/mirror/snapshot/PromoteRequest.cc"
template class librbd::mirror::snapshot::PromoteRequest<librbd::MockTestImageCtx>;

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
using ::testing::WithArgs;

class TestMockMirrorSnapshotPromoteRequest : public TestMockFixture {
public:
  typedef librbd::image::ListWatchersRequest<MockTestImageCtx> MockListWatchersRequest;
  typedef PromoteRequest<MockTestImageCtx> MockPromoteRequest;
  typedef CreateNonPrimaryRequest<MockTestImageCtx> MockCreateNonPrimaryRequest;
  typedef CreatePrimaryRequest<MockTestImageCtx> MockCreatePrimaryRequest;
  typedef util::Mock MockUtils;

  void expect_can_create_primary_snapshot(MockUtils &mock_utils, bool force,
                                          bool requires_orphan,
                                          uint64_t rollback_snap_id,
                                          bool result) {
    EXPECT_CALL(mock_utils,
                can_create_primary_snapshot(_, false, force, _, _))
      .WillOnce(DoAll(
                  WithArgs<3,4 >(Invoke(
                    [requires_orphan, rollback_snap_id]
                    (bool* orphan, uint64_t *snap_id) {
                      *orphan = requires_orphan;
                      *snap_id = rollback_snap_id;
                    })),
                  Return(result)));
  }

  void expect_create_orphan_snapshot(
      MockTestImageCtx &mock_image_ctx,
      MockCreateNonPrimaryRequest &mock_create_non_primary_request, int r) {
    EXPECT_CALL(mock_create_non_primary_request, send())
      .WillOnce(
        Invoke([&mock_image_ctx, &mock_create_non_primary_request, r]() {
                 mock_image_ctx.image_ctx->op_work_queue->queue(
                   mock_create_non_primary_request.on_finish, r);
               }));
  }

  void expect_list_watchers(
      MockTestImageCtx &mock_image_ctx,
      MockListWatchersRequest &mock_list_watchers_request,
      const std::list<obj_watch_t> &watchers, int r) {
    EXPECT_CALL(mock_list_watchers_request, send())
      .WillOnce(
        Invoke([&mock_image_ctx, &mock_list_watchers_request, watchers, r]() {
                 *mock_list_watchers_request.watchers = watchers;
                 mock_image_ctx.image_ctx->op_work_queue->queue(
                   mock_list_watchers_request.on_finish, r);
               }));
  }

  void expect_acquire_lock(MockTestImageCtx &mock_image_ctx, int r) {
    if (mock_image_ctx.exclusive_lock == nullptr) {
      return;
    }
    EXPECT_CALL(*mock_image_ctx.exclusive_lock, is_lock_owner())
      .WillOnce(Return(false));
    EXPECT_CALL(*mock_image_ctx.exclusive_lock, block_requests(_));
    EXPECT_CALL(*mock_image_ctx.exclusive_lock, acquire_lock(_))
      .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
    if (r == 0) {
      EXPECT_CALL(*mock_image_ctx.exclusive_lock, is_lock_owner())
        .WillOnce(Return(true));
    }
  }

  void expect_get_snap_info(MockTestImageCtx &mock_image_ctx,
                            uint64_t snap_id, const SnapInfo* snap_info) {
    EXPECT_CALL(mock_image_ctx, get_snap_info(snap_id))
      .WillOnce(Return(snap_info));
  }

  void expect_rollback(MockTestImageCtx &mock_image_ctx, uint64_t snap_id,
                       const SnapInfo* snap_info, int r) {
    expect_get_snap_info(mock_image_ctx, snap_id, snap_info);
    EXPECT_CALL(*mock_image_ctx.operations,
                execute_snap_rollback(snap_info->snap_namespace,
                                      snap_info->name, _, _))
      .WillOnce(WithArg<3>(CompleteContext(
                             r, mock_image_ctx.image_ctx->op_work_queue)));
  }

  void expect_create_promote_snapshot(
      MockTestImageCtx &mock_image_ctx,
      MockCreatePrimaryRequest &mock_create_primary_request, int r) {
    EXPECT_CALL(mock_create_primary_request, send())
      .WillOnce(
        Invoke([&mock_image_ctx, &mock_create_primary_request, r]() {
                 mock_image_ctx.image_ctx->op_work_queue->queue(
                   mock_create_primary_request.on_finish, r);
               }));
  }

  void expect_release_lock(MockTestImageCtx &mock_image_ctx, int r) {
    if (mock_image_ctx.exclusive_lock == nullptr) {
      return;
    }
    EXPECT_CALL(*mock_image_ctx.exclusive_lock, unblock_requests());
    EXPECT_CALL(*mock_image_ctx.exclusive_lock, release_lock(_))
      .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }
};

TEST_F(TestMockMirrorSnapshotPromoteRequest, Success) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  InSequence seq;

  MockUtils mock_utils;
  expect_can_create_primary_snapshot(mock_utils, true, false, CEPH_NOSNAP,
                                     true);
  MockCreatePrimaryRequest mock_create_primary_request;
  expect_create_promote_snapshot(mock_image_ctx, mock_create_primary_request,
                                 0);
  C_SaferCond ctx;
  auto req = new MockPromoteRequest(&mock_image_ctx, "gid", &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotPromoteRequest, SuccessForce) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  InSequence seq;

  MockUtils mock_utils;
  expect_can_create_primary_snapshot(mock_utils, true, true, CEPH_NOSNAP, true);
  MockCreateNonPrimaryRequest mock_create_non_primary_request;
  expect_create_orphan_snapshot(mock_image_ctx, mock_create_non_primary_request,
                                0);
  MockListWatchersRequest mock_list_watchers_request;
  expect_list_watchers(mock_image_ctx, mock_list_watchers_request, {}, 0);
  expect_acquire_lock(mock_image_ctx, 0);

  SnapInfo snap_info = {"snap", cls::rbd::MirrorSnapshotNamespace{}, 0,
                        {}, 0, 0, {}};
  MockCreatePrimaryRequest mock_create_primary_request;
  expect_create_promote_snapshot(mock_image_ctx, mock_create_primary_request,
                                 0);
  expect_release_lock(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto req = new MockPromoteRequest(&mock_image_ctx, "gid", &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotPromoteRequest, SuccessRollback) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  InSequence seq;

  MockUtils mock_utils;
  expect_can_create_primary_snapshot(mock_utils, true, false, 123, true);
  MockCreateNonPrimaryRequest mock_create_non_primary_request;
  expect_create_orphan_snapshot(mock_image_ctx, mock_create_non_primary_request,
                                0);
  MockListWatchersRequest mock_list_watchers_request;
  expect_list_watchers(mock_image_ctx, mock_list_watchers_request, {}, 0);
  expect_acquire_lock(mock_image_ctx, 0);

  SnapInfo snap_info = {"snap", cls::rbd::MirrorSnapshotNamespace{}, 0,
                        {}, 0, 0, {}};
  expect_rollback(mock_image_ctx, 123, &snap_info, 0);
  MockCreatePrimaryRequest mock_create_primary_request;
  expect_create_promote_snapshot(mock_image_ctx, mock_create_primary_request,
                                 0);
  expect_release_lock(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto req = new MockPromoteRequest(&mock_image_ctx, "gid", &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotPromoteRequest, ErrorCannotRollback) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  MockUtils mock_utils;
  expect_can_create_primary_snapshot(mock_utils, true, false, CEPH_NOSNAP,
                                     false);

  C_SaferCond ctx;
  auto req = new MockPromoteRequest(&mock_image_ctx, "gid", &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

