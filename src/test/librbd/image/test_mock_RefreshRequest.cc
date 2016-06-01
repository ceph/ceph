// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockJournal.h"
#include "test/librbd/mock/MockObjectMap.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Operations.h"
#include "librbd/image/RefreshRequest.h"
#include "librbd/image/RefreshParentRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <arpa/inet.h>
#include <list>
#include <boost/scope_exit.hpp>

namespace librbd {

namespace {

struct MockRefreshImageCtx : public MockImageCtx {
  MockRefreshImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace image {

template <>
struct RefreshParentRequest<MockRefreshImageCtx> {
  static RefreshParentRequest* s_instance;
  static RefreshParentRequest* create(MockRefreshImageCtx &mock_image_ctx,
                                      const parent_info& parent_md,
                                      Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }
  static bool is_refresh_required(MockRefreshImageCtx &mock_image_ctx,
                                  const parent_info& parent_md) {
    assert(s_instance != nullptr);
    return s_instance->is_refresh_required();
  }

  Context *on_finish = nullptr;

  RefreshParentRequest() {
    s_instance = this;
  }

  MOCK_CONST_METHOD0(is_refresh_required, bool());
  MOCK_METHOD0(send, void());
  MOCK_METHOD0(apply, void());
  MOCK_METHOD1(finalize, void(Context *));
};

RefreshParentRequest<MockRefreshImageCtx>* RefreshParentRequest<MockRefreshImageCtx>::s_instance = nullptr;

} // namespace image
} // namespace librbd

// template definitions
#include "librbd/image/RefreshRequest.cc"
template class librbd::image::RefreshRequest<librbd::MockRefreshImageCtx>;

ACTION_P(TestFeatures, image_ctx) {
  return ((image_ctx->features & arg0) != 0);
}

ACTION_P(ShutDownExclusiveLock, image_ctx) {
  // shutting down exclusive lock will close object map and journal
  image_ctx->exclusive_lock = nullptr;
  image_ctx->object_map = nullptr;
  image_ctx->journal = nullptr;
}

namespace librbd {
namespace image {

using ::testing::_;
using ::testing::DoAll;
using ::testing::DoDefault;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::WithArg;
using ::testing::StrEq;

class TestMockImageRefreshRequest : public TestMockFixture {
public:
  typedef RefreshRequest<MockRefreshImageCtx> MockRefreshRequest;
  typedef RefreshParentRequest<MockRefreshImageCtx> MockRefreshParentRequest;

  void expect_set_require_lock_on_read(MockRefreshImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.aio_work_queue, set_require_lock_on_read());
  }

  void expect_clear_require_lock_on_read(MockRefreshImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.aio_work_queue, clear_require_lock_on_read());
  }

  void expect_v1_read_header(MockRefreshImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               read(mock_image_ctx.header_oid, _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_v1_get_snapshots(MockRefreshImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("rbd"), StrEq("snap_list"), _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_v1_get_locks(MockRefreshImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("lock"), StrEq("get_info"), _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_get_mutable_metadata(MockRefreshImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("rbd"), StrEq("get_size"), _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
      EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                  exec(mock_image_ctx.header_oid, _, StrEq("rbd"), StrEq("get_features"), _, _, _))
                    .WillOnce(DoDefault());
      EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                  exec(mock_image_ctx.header_oid, _, StrEq("rbd"), StrEq("get_snapcontext"), _, _, _))
                    .WillOnce(DoDefault());
      EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                  exec(mock_image_ctx.header_oid, _, StrEq("rbd"), StrEq("get_parent"), _, _, _))
                    .WillOnce(DoDefault());
      EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                  exec(mock_image_ctx.header_oid, _, StrEq("lock"), StrEq("get_info"), _, _, _))
                    .WillOnce(DoDefault());
    }
  }

  void expect_get_flags(MockRefreshImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("rbd"), StrEq("get_flags"), _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_get_snapshots(MockRefreshImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("rbd"), StrEq("get_snapshot_name"), _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
      EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                  exec(mock_image_ctx.header_oid, _, StrEq("rbd"), StrEq("get_size"), _, _, _))
                    .WillOnce(DoDefault());
      EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                  exec(mock_image_ctx.header_oid, _, StrEq("rbd"), StrEq("get_parent"), _, _, _))
                    .WillOnce(DoDefault());
      EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                  exec(mock_image_ctx.header_oid, _, StrEq("rbd"), StrEq("get_protection_status"), _, _, _))
                    .WillOnce(DoDefault());
    }
  }

  void expect_add_snap(MockRefreshImageCtx &mock_image_ctx,
                       const std::string &snap_name, uint64_t snap_id) {
    EXPECT_CALL(mock_image_ctx, add_snap(snap_name, snap_id, _, _, _, _));
  }

  void expect_init_exclusive_lock(MockRefreshImageCtx &mock_image_ctx,
                                  MockExclusiveLock &mock_exclusive_lock,
                                  int r) {
    EXPECT_CALL(mock_image_ctx, create_exclusive_lock())
                  .WillOnce(Return(&mock_exclusive_lock));
    EXPECT_CALL(mock_exclusive_lock, init(mock_image_ctx.features, _))
                  .WillOnce(WithArg<1>(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue)));
  }

  void expect_shut_down_exclusive_lock(MockRefreshImageCtx &mock_image_ctx,
                                       MockExclusiveLock &mock_exclusive_lock,
                                       int r) {
    EXPECT_CALL(mock_exclusive_lock, shut_down(_))
                  .WillOnce(DoAll(ShutDownExclusiveLock(&mock_image_ctx),
                                  CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue)));
  }

  void expect_init_layout(MockRefreshImageCtx &mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, init_layout());
  }

  void expect_test_features(MockRefreshImageCtx &mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, test_features(_, _))
                  .WillRepeatedly(TestFeatures(&mock_image_ctx));
  }

  void expect_refresh_parent_is_required(MockRefreshParentRequest &mock_refresh_parent_request,
                                         bool required) {
    EXPECT_CALL(mock_refresh_parent_request, is_refresh_required())
                  .WillRepeatedly(Return(required));
  }

  void expect_refresh_parent_send(MockRefreshImageCtx &mock_image_ctx,
                                  MockRefreshParentRequest &mock_refresh_parent_request,
                                  int r) {
    EXPECT_CALL(mock_refresh_parent_request, send())
                  .WillOnce(FinishRequest(&mock_refresh_parent_request, r,
                                          &mock_image_ctx));
  }

  void expect_refresh_parent_apply(MockRefreshParentRequest &mock_refresh_parent_request) {
    EXPECT_CALL(mock_refresh_parent_request, apply());
  }

  void expect_refresh_parent_finalize(MockRefreshImageCtx &mock_image_ctx,
                                      MockRefreshParentRequest &mock_refresh_parent_request,
                                      int r) {
    EXPECT_CALL(mock_refresh_parent_request, finalize(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_is_exclusive_lock_owner(MockExclusiveLock &mock_exclusive_lock,
                                      bool is_owner) {
    EXPECT_CALL(mock_exclusive_lock, is_lock_owner()).WillOnce(Return(is_owner));
  }

  void expect_open_journal(MockRefreshImageCtx &mock_image_ctx,
                           MockJournal &mock_journal, int r) {
    EXPECT_CALL(mock_image_ctx, create_journal())
                  .WillOnce(Return(&mock_journal));
    EXPECT_CALL(mock_journal, open(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_close_journal(MockRefreshImageCtx &mock_image_ctx,
                            MockJournal &mock_journal, int r) {
    EXPECT_CALL(mock_journal, close(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_open_object_map(MockRefreshImageCtx &mock_image_ctx,
                              MockObjectMap *mock_object_map, int r) {
    EXPECT_CALL(mock_image_ctx, create_object_map(_))
                  .WillOnce(Return(mock_object_map));
    EXPECT_CALL(*mock_object_map, open(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_close_object_map(MockRefreshImageCtx &mock_image_ctx,
                               MockObjectMap &mock_object_map, int r) {
    EXPECT_CALL(mock_object_map, close(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_get_snap_id(MockRefreshImageCtx &mock_image_ctx,
                          const std::string &snap_name, uint64_t snap_id) {
    EXPECT_CALL(mock_image_ctx, get_snap_id(snap_name)).WillOnce(Return(snap_id));
  }

  void expect_block_writes(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.aio_work_queue, block_writes(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_unblock_writes(MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.aio_work_queue, unblock_writes())
                  .Times(1);
  }

};

TEST_F(TestMockImageRefreshRequest, SuccessV1) {
  REQUIRE_FORMAT_V1();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  InSequence seq;
  expect_v1_read_header(mock_image_ctx, 0);
  expect_v1_get_snapshots(mock_image_ctx, 0);
  expect_v1_get_locks(mock_image_ctx, 0);
  expect_init_layout(mock_image_ctx);

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, SuccessSnapshotV1) {
  REQUIRE_FORMAT_V1();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap"));
  ASSERT_EQ(0, ictx->state->refresh());

  MockRefreshImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  InSequence seq;
  expect_v1_read_header(mock_image_ctx, 0);
  expect_v1_get_snapshots(mock_image_ctx, 0);
  expect_v1_get_locks(mock_image_ctx, 0);
  expect_init_layout(mock_image_ctx);
  expect_add_snap(mock_image_ctx, "snap", ictx->snap_ids.begin()->second);

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, SuccessV2) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;
  MockExclusiveLock mock_exclusive_lock;
  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, 0);
  expect_get_flags(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    expect_init_exclusive_lock(mock_image_ctx, mock_exclusive_lock, 0);
  }

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, SuccessSnapshotV2) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap"));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;
  MockExclusiveLock mock_exclusive_lock;
  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, 0);
  expect_get_flags(mock_image_ctx, 0);
  expect_get_flags(mock_image_ctx, 0);
  expect_get_snapshots(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    expect_init_exclusive_lock(mock_image_ctx, mock_exclusive_lock, 0);
  }
  expect_add_snap(mock_image_ctx, "snap", ictx->snap_ids.begin()->second);

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, SuccessSetSnapshotV2) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap"));
  ASSERT_EQ(0, librbd::snap_set(ictx, "snap"));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;
  MockObjectMap mock_object_map;
  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, 0);
  expect_get_flags(mock_image_ctx, 0);
  expect_get_flags(mock_image_ctx, 0);
  expect_get_snapshots(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    expect_open_object_map(mock_image_ctx, &mock_object_map, 0);
  }
  expect_add_snap(mock_image_ctx, "snap", ictx->snap_ids.begin()->second);
  expect_get_snap_id(mock_image_ctx, "snap", ictx->snap_ids.begin()->second);

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, SuccessChild) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  librbd::ImageCtx *ictx2 = nullptr;
  std::string clone_name = get_temp_image_name();

  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap"));
  ASSERT_EQ(0, snap_protect(*ictx, "snap"));
  BOOST_SCOPE_EXIT_ALL((&)) {
    if (ictx2 != nullptr) {
      close_image(ictx2);
    }

    librbd::NoOpProgressContext no_op;
    ASSERT_EQ(0, librbd::remove(m_ioctx, clone_name.c_str(), no_op));
    ASSERT_EQ(0, ictx->operations->snap_unprotect("snap"));
  };

  int order = ictx->order;
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap", m_ioctx,
                             clone_name.c_str(), ictx->features, &order, 0, 0));

  ASSERT_EQ(0, open_image(clone_name, &ictx2));

  MockRefreshImageCtx mock_image_ctx(*ictx2);
  MockRefreshParentRequest *mock_refresh_parent_request = new MockRefreshParentRequest();
  MockExclusiveLock mock_exclusive_lock;
  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, 0);
  expect_get_flags(mock_image_ctx, 0);
  expect_refresh_parent_is_required(*mock_refresh_parent_request, true);
  expect_refresh_parent_send(mock_image_ctx, *mock_refresh_parent_request, 0);
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    expect_init_exclusive_lock(mock_image_ctx, mock_exclusive_lock, 0);
  }
  expect_refresh_parent_apply(*mock_refresh_parent_request);
  expect_refresh_parent_finalize(mock_image_ctx, *mock_refresh_parent_request, 0);

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, DisableExclusiveLock) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;

  MockExclusiveLock *mock_exclusive_lock = new MockExclusiveLock();
  mock_image_ctx.exclusive_lock = mock_exclusive_lock;

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  MockJournal mock_journal;
  if (ictx->test_features(RBD_FEATURE_JOURNALING)) {
    mock_image_ctx.journal = &mock_journal;
  }

  ASSERT_EQ(0, update_features(ictx,
                               RBD_FEATURE_EXCLUSIVE_LOCK |
                               RBD_FEATURE_OBJECT_MAP |
                               RBD_FEATURE_FAST_DIFF |
                               RBD_FEATURE_JOURNALING, false));

  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  // verify that exclusive lock is properly handled when object map
  // and journaling were never enabled (or active)
  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, 0);
  expect_get_flags(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  expect_shut_down_exclusive_lock(mock_image_ctx, *mock_exclusive_lock, 0);

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, EnableJournalWithExclusiveLock) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, update_features(ictx,
                               RBD_FEATURE_OBJECT_MAP |
                               RBD_FEATURE_FAST_DIFF, false));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  MockJournal mock_journal;

  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);
  expect_is_exclusive_lock_owner(mock_exclusive_lock, true);

  // journal should be immediately opened if exclusive lock owned
  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, 0);
  expect_get_flags(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  expect_open_journal(mock_image_ctx, mock_journal, 0);

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, EnableJournalWithoutExclusiveLock) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, update_features(ictx,
                               RBD_FEATURE_OBJECT_MAP |
                               RBD_FEATURE_FAST_DIFF, false));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);
  expect_is_exclusive_lock_owner(mock_exclusive_lock, false);

  // do not open the journal if exclusive lock is not owned
  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, 0);
  expect_get_flags(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  expect_set_require_lock_on_read(mock_image_ctx);

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, DisableJournal) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  MockJournal *mock_journal = new MockJournal();
  mock_image_ctx.journal = mock_journal;

  ASSERT_EQ(0, update_features(ictx, RBD_FEATURE_JOURNALING, false));

  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  // verify journal is closed if feature disabled
  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, 0);
  expect_get_flags(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  expect_block_writes(mock_image_ctx, 0);
  expect_clear_require_lock_on_read(mock_image_ctx);
  expect_close_journal(mock_image_ctx, *mock_journal, 0);
  expect_unblock_writes(mock_image_ctx);

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, EnableObjectMapWithExclusiveLock) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, update_features(ictx, RBD_FEATURE_JOURNALING, false));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  MockObjectMap mock_object_map;

  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);
  expect_is_exclusive_lock_owner(mock_exclusive_lock, true);

  // object map should be immediately opened if exclusive lock owned
  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, 0);
  expect_get_flags(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  expect_open_object_map(mock_image_ctx, &mock_object_map, 0);

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, EnableObjectMapWithoutExclusiveLock) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, update_features(ictx, RBD_FEATURE_JOURNALING, false));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);
  expect_is_exclusive_lock_owner(mock_exclusive_lock, false);

  // do not open the object map if exclusive lock is not owned
  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, 0);
  expect_get_flags(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, DisableObjectMap) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  MockObjectMap *mock_object_map = new MockObjectMap();
  mock_image_ctx.object_map = mock_object_map;

  MockJournal mock_journal;
  if (ictx->test_features(RBD_FEATURE_JOURNALING)) {
    mock_image_ctx.journal = &mock_journal;
  }

  ASSERT_EQ(0, update_features(ictx,
                               RBD_FEATURE_OBJECT_MAP |
                               RBD_FEATURE_FAST_DIFF, false));

  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  // verify object map is closed if feature disabled
  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, 0);
  expect_get_flags(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  expect_close_object_map(mock_image_ctx, *mock_object_map, 0);

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, OpenObjectMapError) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(0, update_features(ictx, RBD_FEATURE_JOURNALING, false));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  MockObjectMap *mock_object_map = new MockObjectMap();

  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);
  expect_is_exclusive_lock_owner(mock_exclusive_lock, true);

  // object map should be immediately opened if exclusive lock owned
  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, 0);
  expect_get_flags(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  expect_open_object_map(mock_image_ctx, mock_object_map, -EFBIG);

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(nullptr, mock_image_ctx.object_map);
}

} // namespace image
} // namespace librbd
