// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "include/stringify.h"
#include "common/bit_vector.hpp"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/operation/SnapshotRollbackRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace librbd {

namespace {

struct MockOperationImageCtx : public MockImageCtx {
  MockOperationImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace operation {

template <>
struct ResizeRequest<MockOperationImageCtx> {
  static ResizeRequest *s_instance;
  Context *on_finish = nullptr;

  static ResizeRequest* create(MockOperationImageCtx &image_ctx, Context *on_finish,
                               uint64_t new_size, ProgressContext &prog_ctx,
                               uint64_t journal_op_tid, bool disable_journal) {
    assert(s_instance != nullptr);
    assert(journal_op_tid == 0);
    assert(disable_journal);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  ResizeRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

ResizeRequest<MockOperationImageCtx> *ResizeRequest<MockOperationImageCtx>::s_instance = nullptr;

} // namespace operation

template <>
struct AsyncRequest<MockOperationImageCtx> : public AsyncRequest<MockImageCtx> {
  MockOperationImageCtx &m_image_ctx;

  AsyncRequest(MockOperationImageCtx &image_ctx, Context *on_finish)
    : AsyncRequest<MockImageCtx>(image_ctx, on_finish), m_image_ctx(image_ctx) {
  }
};

} // namespace librbd

// template definitions
#include "librbd/AsyncRequest.cc"
#include "librbd/AsyncObjectThrottle.cc"
#include "librbd/operation/Request.cc"
#include "librbd/operation/SnapshotRollbackRequest.cc"

template class librbd::AsyncRequest<librbd::MockImageCtx>;
template class librbd::AsyncObjectThrottle<librbd::MockImageCtx>;
template class librbd::operation::Request<librbd::MockImageCtx>;

namespace librbd {
namespace operation {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::WithArg;

class TestMockOperationSnapshotRollbackRequest : public TestMockFixture {
public:
  typedef SnapshotRollbackRequest<MockOperationImageCtx> MockSnapshotRollbackRequest;
  typedef ResizeRequest<MockOperationImageCtx> MockResizeRequest;

  void expect_block_writes(MockOperationImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.aio_work_queue, block_writes(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_unblock_writes(MockOperationImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.aio_work_queue, unblock_writes())
                  .Times(1);
  }

  void expect_get_image_size(MockOperationImageCtx &mock_image_ctx,
                             uint64_t size) {
    EXPECT_CALL(mock_image_ctx, get_image_size(CEPH_NOSNAP))
                  .WillOnce(Return(size));
  }

  void expect_resize(MockOperationImageCtx &mock_image_ctx,
                     MockResizeRequest &mock_resize_request, int r) {
    expect_get_image_size(mock_image_ctx, 123);
    EXPECT_CALL(mock_resize_request, send())
                  .WillOnce(FinishRequest(&mock_resize_request, r,
                                          &mock_image_ctx));
  }

  void expect_rollback_object_map(MockOperationImageCtx &mock_image_ctx,
                                  MockObjectMap &mock_object_map) {
    if (mock_image_ctx.object_map != nullptr) {
      EXPECT_CALL(mock_object_map, rollback(_, _))
                    .WillOnce(WithArg<1>(CompleteContext(0, mock_image_ctx.image_ctx->op_work_queue)));
    }
  }

  void expect_get_object_name(MockOperationImageCtx &mock_image_ctx,
                              uint64_t object_num) {
    EXPECT_CALL(mock_image_ctx, get_object_name(object_num))
                  .WillOnce(Return("object-name-" + stringify(object_num)));
  }

  void expect_get_current_size(MockOperationImageCtx &mock_image_ctx, uint64_t size) {
    EXPECT_CALL(mock_image_ctx, get_current_size())
                  .WillOnce(Return(size));
  }

  void expect_rollback_snap_id(MockOperationImageCtx &mock_image_ctx,
                               const std::string &oid, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.data_ctx),
                selfmanaged_snap_rollback(oid, _))
                  .WillOnce(Return(r));
  }

  void expect_rollback(MockOperationImageCtx &mock_image_ctx, int r) {
    expect_get_current_size(mock_image_ctx, 1);
    expect_get_object_name(mock_image_ctx, 0);
    expect_rollback_snap_id(mock_image_ctx, "object-name-0", r);
  }

  void expect_create_object_map(MockOperationImageCtx &mock_image_ctx,
                                MockObjectMap *mock_object_map) {
    EXPECT_CALL(mock_image_ctx, create_object_map(_))
                  .WillOnce(Return(mock_object_map));
  }

  void expect_open_object_map(MockOperationImageCtx &mock_image_ctx,
                              MockObjectMap &mock_object_map) {
    EXPECT_CALL(mock_object_map, open(_))
                  .WillOnce(CompleteContext(0, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_refresh_object_map(MockOperationImageCtx &mock_image_ctx,
                                 MockObjectMap &mock_object_map) {
    if (mock_image_ctx.object_map != nullptr) {
      expect_create_object_map(mock_image_ctx, &mock_object_map);
      expect_open_object_map(mock_image_ctx, mock_object_map);
    }
  }

  void expect_invalidate_cache(MockOperationImageCtx &mock_image_ctx, int r) {
    if (mock_image_ctx.object_cacher != nullptr) {
      EXPECT_CALL(mock_image_ctx, invalidate_cache(_))
                    .WillOnce(CompleteContext(r, NULL));
    }
  }

  int when_snap_rollback(MockOperationImageCtx &mock_image_ctx,
                         const std::string &snap_name,
                         uint64_t snap_id, uint64_t snap_size) {
    C_SaferCond cond_ctx;
    librbd::NoOpProgressContext prog_ctx;
    MockSnapshotRollbackRequest *req = new MockSnapshotRollbackRequest(
      mock_image_ctx, &cond_ctx, snap_name, snap_id, snap_size, prog_ctx);
    {
      RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
      req->send();
    }
    return cond_ctx.wait();
  }
};

TEST_F(TestMockOperationSnapshotRollbackRequest, Success) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockOperationImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap *mock_object_map = new MockObjectMap();
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      *mock_object_map);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  MockResizeRequest mock_resize_request;
  expect_append_op_event(mock_image_ctx, 0);
  expect_block_writes(mock_image_ctx, 0);
  expect_resize(mock_image_ctx, mock_resize_request, 0);
  expect_rollback_object_map(mock_image_ctx, *mock_object_map);
  expect_rollback(mock_image_ctx, 0);
  expect_refresh_object_map(mock_image_ctx, *mock_object_map);
  expect_invalidate_cache(mock_image_ctx, 0);
  expect_commit_op_event(mock_image_ctx, 0);
  expect_unblock_writes(mock_image_ctx);
  ASSERT_EQ(0, when_snap_rollback(mock_image_ctx, "snap", 123, 0));
}

TEST_F(TestMockOperationSnapshotRollbackRequest, BlockWritesError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockOperationImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_append_op_event(mock_image_ctx, 0);
  expect_block_writes(mock_image_ctx, -EINVAL);
  expect_commit_op_event(mock_image_ctx, -EINVAL);
  expect_unblock_writes(mock_image_ctx);
  ASSERT_EQ(-EINVAL, when_snap_rollback(mock_image_ctx, "snap", 123, 0));
}

TEST_F(TestMockOperationSnapshotRollbackRequest, SkipResize) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockOperationImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap *mock_object_map = new MockObjectMap();
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      *mock_object_map);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_append_op_event(mock_image_ctx, 0);
  expect_block_writes(mock_image_ctx, 0);
  expect_get_image_size(mock_image_ctx, 345);
  expect_rollback_object_map(mock_image_ctx, *mock_object_map);
  expect_rollback(mock_image_ctx, 0);
  expect_refresh_object_map(mock_image_ctx, *mock_object_map);
  expect_invalidate_cache(mock_image_ctx, 0);
  expect_commit_op_event(mock_image_ctx, 0);
  expect_unblock_writes(mock_image_ctx);
  ASSERT_EQ(0, when_snap_rollback(mock_image_ctx, "snap", 123, 345));
}

TEST_F(TestMockOperationSnapshotRollbackRequest, ResizeError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockOperationImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  MockResizeRequest mock_resize_request;
  expect_append_op_event(mock_image_ctx, 0);
  expect_block_writes(mock_image_ctx, 0);
  expect_resize(mock_image_ctx, mock_resize_request, -EINVAL);
  expect_commit_op_event(mock_image_ctx, -EINVAL);
  expect_unblock_writes(mock_image_ctx);
  ASSERT_EQ(-EINVAL, when_snap_rollback(mock_image_ctx, "snap", 123, 0));
}

TEST_F(TestMockOperationSnapshotRollbackRequest, RollbackObjectsError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockOperationImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  MockResizeRequest mock_resize_request;
  expect_append_op_event(mock_image_ctx, 0);
  expect_block_writes(mock_image_ctx, 0);
  expect_resize(mock_image_ctx, mock_resize_request, 0);
  expect_rollback_object_map(mock_image_ctx, mock_object_map);
  expect_rollback(mock_image_ctx, -EINVAL);
  expect_commit_op_event(mock_image_ctx, -EINVAL);
  expect_unblock_writes(mock_image_ctx);
  ASSERT_EQ(-EINVAL, when_snap_rollback(mock_image_ctx, "snap", 123, 0));
}

TEST_F(TestMockOperationSnapshotRollbackRequest, InvalidateCacheError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockOperationImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap *mock_object_map = new MockObjectMap();
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      *mock_object_map);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  MockResizeRequest mock_resize_request;
  expect_append_op_event(mock_image_ctx, 0);
  expect_block_writes(mock_image_ctx, 0);
  expect_resize(mock_image_ctx, mock_resize_request, 0);
  expect_rollback_object_map(mock_image_ctx, *mock_object_map);
  expect_rollback(mock_image_ctx, 0);
  expect_refresh_object_map(mock_image_ctx, *mock_object_map);
  expect_invalidate_cache(mock_image_ctx, -EINVAL);
  expect_commit_op_event(mock_image_ctx, -EINVAL);
  expect_unblock_writes(mock_image_ctx);
  ASSERT_EQ(-EINVAL, when_snap_rollback(mock_image_ctx, "snap", 123, 0));
}

} // namespace operation
} // namespace librbd
