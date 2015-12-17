// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "common/bit_vector.hpp"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
#include "librbd/operation/ResizeRequest.h"
#include "librbd/operation/TrimRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace librbd {
namespace operation {

template <>
class TrimRequest<MockImageCtx> {
public:
  static TrimRequest *s_instance;
  static TrimRequest *create(MockImageCtx &image_ctx, Context *on_finish,
                             uint64_t original_size, uint64_t new_size,
                             ProgressContext &prog_ctx) {
    assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  Context *on_finish = nullptr;

  TrimRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

TrimRequest<MockImageCtx> *TrimRequest<MockImageCtx>::s_instance = nullptr;

} // namespace operation
} // namespace librbd

// template definitions
#include "librbd/operation/ResizeRequest.cc"
#include "librbd/operation/TrimRequest.cc"

namespace librbd {
namespace operation {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::WithArg;

class TestMockOperationResizeRequest : public TestMockFixture {
public:
  typedef ResizeRequest<MockImageCtx> MockResizeRequest;
  typedef TrimRequest<MockImageCtx> MockTrimRequest;

  void expect_block_writes(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.aio_work_queue, block_writes(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_unblock_writes(MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.aio_work_queue, unblock_writes())
                  .Times(1);
  }

  void expect_is_journal_replaying(MockJournal &mock_journal) {
    EXPECT_CALL(mock_journal, is_journal_replaying()).WillOnce(Return(false));
  }

  void expect_is_journal_ready(MockJournal &mock_journal) {
    EXPECT_CALL(mock_journal, is_journal_ready()).WillOnce(Return(true));
  }

  void expect_allocate_op_tid(MockImageCtx &mock_image_ctx) {
    if (mock_image_ctx.journal != nullptr) {
      EXPECT_CALL(*mock_image_ctx.journal, allocate_op_tid())
                    .WillOnce(Return(1U));
    }
  }

  void expect_append_op_event(MockImageCtx &mock_image_ctx, int r) {
    if (mock_image_ctx.journal != nullptr) {
      expect_is_journal_replaying(*mock_image_ctx.journal);
      expect_allocate_op_tid(mock_image_ctx);
      EXPECT_CALL(*mock_image_ctx.journal, append_op_event_mock(_, _, _))
                    .WillOnce(WithArg<2>(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue)));
    }
  }

  void expect_commit_op_event(MockImageCtx &mock_image_ctx, int r) {
    if (mock_image_ctx.journal != nullptr) {
      expect_is_journal_replaying(*mock_image_ctx.journal);
      expect_is_journal_ready(*mock_image_ctx.journal);
      EXPECT_CALL(*mock_image_ctx.journal, commit_op_event(1U, r));
    }
  }

  void expect_is_lock_owner(MockImageCtx &mock_image_ctx) {
    if (mock_image_ctx.exclusive_lock != nullptr) {
      EXPECT_CALL(*mock_image_ctx.exclusive_lock, is_lock_owner())
                    .WillOnce(Return(true));
    }
  }

  void expect_grow_object_map(MockImageCtx &mock_image_ctx) {
    if (mock_image_ctx.object_map != nullptr) {
      expect_is_lock_owner(mock_image_ctx);
      EXPECT_CALL(*mock_image_ctx.object_map, aio_resize(_, _, _))
                    .WillOnce(WithArg<2>(CompleteContext(0, mock_image_ctx.image_ctx->op_work_queue)));
    }
  }

  void expect_shrink_object_map(MockImageCtx &mock_image_ctx) {
    if (mock_image_ctx.object_map != nullptr) {
      expect_is_lock_owner(mock_image_ctx);
      EXPECT_CALL(*mock_image_ctx.object_map, aio_resize(_, _, _))
                    .WillOnce(WithArg<2>(CompleteContext(0, mock_image_ctx.image_ctx->op_work_queue)));
    }
  }

  void expect_update_header(MockImageCtx &mock_image_ctx, int r) {
    if (mock_image_ctx.old_format) {
      EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                  write(mock_image_ctx.header_oid, _, _, _, _))
                    .WillOnce(Return(r));
    } else {
      expect_is_lock_owner(mock_image_ctx);
      if (mock_image_ctx.exclusive_lock != nullptr) {
        EXPECT_CALL(*mock_image_ctx.exclusive_lock, assert_header_locked(_));
      }
      EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                  exec(mock_image_ctx.header_oid, _, "rbd", "set_size", _, _, _))
                    .WillOnce(Return(r));
    }
  }

  void expect_trim(MockImageCtx &mock_image_ctx,
                   MockTrimRequest &mock_trim_request, int r) {
    EXPECT_CALL(mock_trim_request, send())
                  .WillOnce(FinishRequest(&mock_trim_request, r, &mock_image_ctx));
  }

  void expect_invalidate_cache(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(mock_image_ctx, invalidate_cache(_))
                  .WillOnce(CompleteContext(r, NULL));
    expect_op_work_queue(mock_image_ctx);
  }

  void expect_resize_object_map(MockImageCtx &mock_image_ctx,
                                uint64_t new_size) {
    EXPECT_CALL(*mock_image_ctx.object_map, aio_resize(new_size, _, _))
                  .WillOnce(WithArg<2>(CompleteContext(0, mock_image_ctx.image_ctx->op_work_queue)));
  }

  int when_resize(MockImageCtx &mock_image_ctx, uint64_t new_size) {
    C_SaferCond cond_ctx;
    librbd::NoOpProgressContext prog_ctx;
    MockResizeRequest *req = new MockResizeRequest(
      mock_image_ctx, &cond_ctx, new_size, prog_ctx);
    {
      RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
      req->send();
    }
    return cond_ctx.wait();
  }

  void initialize_features(ImageCtx *ictx, MockImageCtx &mock_image_ctx,
                           MockExclusiveLock &mock_exclusive_lock,
                           MockJournal &mock_journal,
                           MockObjectMap &mock_object_map) {
    if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
      mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
    }
    if (ictx->test_features(RBD_FEATURE_JOURNALING)) {
      mock_image_ctx.journal = &mock_journal;
    }
    if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
      mock_image_ctx.object_map = &mock_object_map;
    }
  }
};

TEST_F(TestMockOperationResizeRequest, NoOpSuccess) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  InSequence seq;
  expect_block_writes(mock_image_ctx, 0);
  expect_append_op_event(mock_image_ctx, 0);
  expect_unblock_writes(mock_image_ctx);
  expect_commit_op_event(mock_image_ctx, 0);
  ASSERT_EQ(0, when_resize(mock_image_ctx, ictx->size));
}

TEST_F(TestMockOperationResizeRequest, GrowSuccess) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  InSequence seq;
  expect_block_writes(mock_image_ctx, 0);
  expect_append_op_event(mock_image_ctx, 0);
  expect_unblock_writes(mock_image_ctx);
  expect_grow_object_map(mock_image_ctx);
  expect_block_writes(mock_image_ctx, 0);
  expect_update_header(mock_image_ctx, 0);
  expect_commit_op_event(mock_image_ctx, 0);
  expect_unblock_writes(mock_image_ctx);
  ASSERT_EQ(0, when_resize(mock_image_ctx, ictx->size * 2));
}

TEST_F(TestMockOperationResizeRequest, ShrinkSuccess) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  InSequence seq;
  expect_block_writes(mock_image_ctx, 0);
  expect_append_op_event(mock_image_ctx, 0);
  expect_unblock_writes(mock_image_ctx);

  MockTrimRequest mock_trim_request;
  expect_trim(mock_image_ctx, mock_trim_request, 0);
  expect_invalidate_cache(mock_image_ctx, 0);
  expect_block_writes(mock_image_ctx, 0);
  expect_update_header(mock_image_ctx, 0);
  expect_commit_op_event(mock_image_ctx, 0);
  expect_shrink_object_map(mock_image_ctx);
  expect_unblock_writes(mock_image_ctx);
  ASSERT_EQ(0, when_resize(mock_image_ctx, ictx->size / 2));
}

TEST_F(TestMockOperationResizeRequest, PreBlockWritesError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  InSequence seq;
  expect_block_writes(mock_image_ctx, -EINVAL);
  expect_unblock_writes(mock_image_ctx);
  ASSERT_EQ(-EINVAL, when_resize(mock_image_ctx, ictx->size));
}

TEST_F(TestMockOperationResizeRequest, TrimError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  InSequence seq;
  expect_block_writes(mock_image_ctx, 0);
  expect_append_op_event(mock_image_ctx, 0);
  expect_unblock_writes(mock_image_ctx);

  MockTrimRequest mock_trim_request;
  expect_trim(mock_image_ctx, mock_trim_request, -EINVAL);
  expect_commit_op_event(mock_image_ctx, -EINVAL);
  ASSERT_EQ(-EINVAL, when_resize(mock_image_ctx, ictx->size / 2));
}

TEST_F(TestMockOperationResizeRequest, InvalidateCacheError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  InSequence seq;
  expect_block_writes(mock_image_ctx, 0);
  expect_append_op_event(mock_image_ctx, 0);
  expect_unblock_writes(mock_image_ctx);

  MockTrimRequest mock_trim_request;
  expect_trim(mock_image_ctx, mock_trim_request, 0);
  expect_invalidate_cache(mock_image_ctx, -EINVAL);
  expect_commit_op_event(mock_image_ctx, -EINVAL);
  ASSERT_EQ(-EINVAL, when_resize(mock_image_ctx, ictx->size / 2));
}

TEST_F(TestMockOperationResizeRequest, PostBlockWritesError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  InSequence seq;
  expect_block_writes(mock_image_ctx, 0);
  expect_append_op_event(mock_image_ctx, 0);
  expect_unblock_writes(mock_image_ctx);
  expect_grow_object_map(mock_image_ctx);
  expect_block_writes(mock_image_ctx, -EINVAL);
  expect_unblock_writes(mock_image_ctx);
  expect_commit_op_event(mock_image_ctx, -EINVAL);
  ASSERT_EQ(-EINVAL, when_resize(mock_image_ctx, ictx->size * 2));
}

TEST_F(TestMockOperationResizeRequest, UpdateHeaderError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  InSequence seq;
  expect_block_writes(mock_image_ctx, 0);
  expect_append_op_event(mock_image_ctx, 0);
  expect_unblock_writes(mock_image_ctx);
  expect_grow_object_map(mock_image_ctx);
  expect_block_writes(mock_image_ctx, 0);
  expect_update_header(mock_image_ctx, -EINVAL);
  expect_unblock_writes(mock_image_ctx);
  expect_commit_op_event(mock_image_ctx, -EINVAL);
  ASSERT_EQ(-EINVAL, when_resize(mock_image_ctx, ictx->size * 2));
}

TEST_F(TestMockOperationResizeRequest, JournalAppendError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  InSequence seq;
  expect_block_writes(mock_image_ctx, 0);
  expect_append_op_event(mock_image_ctx, -EINVAL);
  expect_unblock_writes(mock_image_ctx);
  ASSERT_EQ(-EINVAL, when_resize(mock_image_ctx, ictx->size));
}

} // namespace operation
} // namespace librbd
