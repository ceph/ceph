// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/io/MockObjectDispatch.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "common/bit_vector.hpp"
#include "librbd/AsyncRequest.h"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/operation/TrimRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <boost/variant.hpp>

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

template<>
struct AsyncRequest<librbd::MockTestImageCtx> {
  librbd::MockTestImageCtx& m_image_ctx;
  Context *on_finish;

  AsyncRequest(librbd::MockTestImageCtx& image_ctx, Context* on_finish)
    : m_image_ctx(image_ctx), on_finish(on_finish) {
  }
  virtual ~AsyncRequest() {
  }

  Context* create_callback_context() {
    return util::create_context_callback(this);
  }

  Context* create_async_callback_context() {
    return util::create_context_callback<AsyncRequest,
                                         &AsyncRequest::async_complete>(this);
  }

  void complete(int r) {
    if (should_complete(r)) {
      async_complete(r);
    }
  }

  void async_complete(int r) {
    on_finish->complete(r);
    delete this;
  }

  bool is_canceled() const {
    return false;
  }

  virtual void send() = 0;
  virtual bool should_complete(int r) = 0;
};

namespace io {

struct DiscardVisitor
  : public boost::static_visitor<ObjectDispatchSpec::DiscardRequest*> {
  ObjectDispatchSpec::DiscardRequest*
  operator()(ObjectDispatchSpec::DiscardRequest& discard) const {
    return &discard;
  }

  template <typename T>
  ObjectDispatchSpec::DiscardRequest*
  operator()(T& t) const {
    return nullptr;
  }
};

} // namespace io
} // namespace librbd

// template definitions
#include "librbd/AsyncObjectThrottle.cc"
#include "librbd/operation/TrimRequest.cc"

namespace librbd {
namespace operation {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockOperationTrimRequest : public TestMockFixture {
public:
  typedef TrimRequest<MockTestImageCtx> MockTrimRequest;

  int create_snapshot(const char *snap_name) {
    librbd::ImageCtx *ictx;
    int r = open_image(m_image_name, &ictx);
    if (r < 0) {
      return r;
    }

    r = snap_create(*ictx, snap_name);
    if (r < 0) {
      return r;
    }

    r = snap_protect(*ictx, snap_name);
    if (r < 0) {
      return r;
    }
    close_image(ictx);
    return 0;
  }

  void expect_is_lock_owner(MockTestImageCtx &mock_image_ctx) {
    if (mock_image_ctx.exclusive_lock != nullptr) {
      EXPECT_CALL(*mock_image_ctx.exclusive_lock, is_lock_owner())
                    .WillRepeatedly(Return(true));
    }
  }

  void expect_object_map_update(MockTestImageCtx &mock_image_ctx,
                                uint64_t start_object, uint64_t end_object,
                                uint8_t state, uint8_t current_state,
                                bool updated, int ret_val) {
    if (mock_image_ctx.object_map != nullptr) {
      EXPECT_CALL(*mock_image_ctx.object_map,
                  aio_update(CEPH_NOSNAP, start_object, end_object, state,
                             boost::optional<uint8_t>(current_state), _, false, false, _))
        .WillOnce(WithArg<8>(Invoke([&mock_image_ctx, updated, ret_val](Context *ctx) {
                               if (updated) {
                                 mock_image_ctx.op_work_queue->queue(ctx, ret_val);
                               }
                               return updated;
                             })));
    }
  }

  void expect_get_parent_overlap(MockTestImageCtx &mock_image_ctx,
                                 uint64_t overlap) {
    EXPECT_CALL(mock_image_ctx, get_parent_overlap(CEPH_NOSNAP, _))
      .WillOnce(WithArg<1>(Invoke([overlap](uint64_t *o) {
                             *o = overlap;
                             return 0;
                           })));
  }

  void expect_reduce_parent_overlap(MockTestImageCtx& mock_image_ctx,
                                    uint64_t overlap) {
    EXPECT_CALL(mock_image_ctx, reduce_parent_overlap(overlap, false))
      .WillOnce(Return(std::make_pair(overlap, io::ImageArea::DATA)));
  }

  void expect_get_area_size(MockTestImageCtx& mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, get_area_size(io::ImageArea::CRYPTO_HEADER))
      .WillOnce(Return(0));
  }

  void expect_object_may_exist(MockTestImageCtx &mock_image_ctx,
                               uint64_t object_no, bool exists) {
    if (mock_image_ctx.object_map != nullptr) {
      EXPECT_CALL(*mock_image_ctx.object_map, object_may_exist(object_no))
        .WillOnce(Return(exists));
    }
  }

  void expect_get_object_name(MockTestImageCtx &mock_image_ctx,
                              uint64_t object_no, const std::string& oid) {
    EXPECT_CALL(mock_image_ctx, get_object_name(object_no))
      .WillOnce(Return(oid));
  }

  void expect_aio_remove(MockTestImageCtx &mock_image_ctx,
                         const std::string& oid, int ret_val) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.data_ctx), remove(oid, _))
      .WillOnce(Return(ret_val));
  }

  void expect_object_discard(MockImageCtx &mock_image_ctx,
                             io::MockObjectDispatch& mock_io_object_dispatch,
                             uint64_t offset, uint64_t length,
                             bool update_object_map, int r) {
    EXPECT_CALL(*mock_image_ctx.io_object_dispatcher, send(_))
      .WillOnce(Invoke([&mock_image_ctx, offset, length, update_object_map, r]
                       (io::ObjectDispatchSpec* spec) {
                  auto discard = boost::apply_visitor(io::DiscardVisitor(), spec->request);
                  ASSERT_TRUE(discard != nullptr);
                  ASSERT_EQ(offset, discard->object_off);
                  ASSERT_EQ(length, discard->object_len);
                  int flags = 0;
                  if (!update_object_map) {
                    flags = io::OBJECT_DISCARD_FLAG_DISABLE_OBJECT_MAP_UPDATE;
                  }
                  ASSERT_EQ(flags, discard->discard_flags);

                  spec->dispatch_result = io::DISPATCH_RESULT_COMPLETE;
                  mock_image_ctx.op_work_queue->queue(&spec->dispatcher_ctx, r);
                }));
  }
};

TEST_F(TestMockOperationTrimRequest, SuccessRemove) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);
  expect_op_work_queue(mock_image_ctx);
  expect_is_lock_owner(mock_image_ctx);

  InSequence seq;
  EXPECT_CALL(mock_image_ctx, get_stripe_period()).WillOnce(Return(ictx->get_object_size()));
  EXPECT_CALL(mock_image_ctx, get_stripe_count()).WillOnce(Return(ictx->get_stripe_count()));

  // pre
  expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_PENDING, OBJECT_EXISTS,
                           true, 0);

  // copy-up
  expect_get_area_size(mock_image_ctx);
  expect_get_parent_overlap(mock_image_ctx, 0);
  expect_reduce_parent_overlap(mock_image_ctx, 0);

  // remove
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_get_object_name(mock_image_ctx, 0, "object0");
  expect_aio_remove(mock_image_ctx, "object0", 0);

   // post
  expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_NONEXISTENT,
                           OBJECT_PENDING, true, 0);

  C_SaferCond cond_ctx;
  librbd::NoOpProgressContext progress_ctx;
  MockTrimRequest *req = new MockTrimRequest(
    mock_image_ctx, &cond_ctx, m_image_size, 0, progress_ctx);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationTrimRequest, SuccessCopyUp) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING)
  ASSERT_EQ(0, create_snapshot("snap1"));

  int order = 22;
  uint64_t features;
  ASSERT_TRUE(::get_features(&features));
  std::string clone_name = get_temp_image_name();
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap1", m_ioctx,
                             clone_name.c_str(), features, &order, 0, 0));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(clone_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap"));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);
  expect_op_work_queue(mock_image_ctx);
  expect_is_lock_owner(mock_image_ctx);

  InSequence seq;
  EXPECT_CALL(mock_image_ctx, get_stripe_period()).WillOnce(Return(ictx->get_object_size()));
  EXPECT_CALL(mock_image_ctx, get_stripe_count()).WillOnce(Return(ictx->get_stripe_count()));

  // pre
  expect_object_map_update(mock_image_ctx, 0, 2, OBJECT_PENDING, OBJECT_EXISTS,
                           true, 0);

  // copy-up
  io::MockObjectDispatch mock_io_object_dispatch;
  expect_get_area_size(mock_image_ctx);
  expect_get_parent_overlap(mock_image_ctx, ictx->get_object_size());
  expect_reduce_parent_overlap(mock_image_ctx, ictx->get_object_size());
  expect_get_object_name(mock_image_ctx, 0, "object0");
  expect_object_discard(mock_image_ctx, mock_io_object_dispatch, 0,
                        ictx->get_object_size(), false, 0);

  // remove
  expect_object_may_exist(mock_image_ctx, 1, true);
  expect_get_object_name(mock_image_ctx, 1, "object1");
  expect_aio_remove(mock_image_ctx, "object1", 0);

   // post
  expect_object_map_update(mock_image_ctx, 0, 2, OBJECT_NONEXISTENT,
                           OBJECT_PENDING, true, 0);

  C_SaferCond cond_ctx;
  librbd::NoOpProgressContext progress_ctx;
  MockTrimRequest *req = new MockTrimRequest(
    mock_image_ctx, &cond_ctx, 2 * ictx->get_object_size(), 0, progress_ctx);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationTrimRequest, SuccessBoundary) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);
  expect_op_work_queue(mock_image_ctx);
  expect_is_lock_owner(mock_image_ctx);

  InSequence seq;
  EXPECT_CALL(mock_image_ctx, get_stripe_period()).WillOnce(Return(ictx->get_object_size()));
  EXPECT_CALL(mock_image_ctx, get_stripe_count()).WillOnce(Return(ictx->get_stripe_count()));

  // boundary
  io::MockObjectDispatch mock_io_object_dispatch;
  expect_object_discard(mock_image_ctx, mock_io_object_dispatch, 1,
                        ictx->get_object_size() - 1, true, 0);

  C_SaferCond cond_ctx;
  librbd::NoOpProgressContext progress_ctx;
  MockTrimRequest *req = new MockTrimRequest(
    mock_image_ctx, &cond_ctx, ictx->get_object_size(), 1, progress_ctx);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationTrimRequest, SuccessNoOp) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);
}

TEST_F(TestMockOperationTrimRequest, RemoveError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);
  expect_op_work_queue(mock_image_ctx);
  expect_is_lock_owner(mock_image_ctx);

  InSequence seq;
  EXPECT_CALL(mock_image_ctx, get_stripe_period()).WillOnce(Return(ictx->get_object_size()));
  EXPECT_CALL(mock_image_ctx, get_stripe_count()).WillOnce(Return(ictx->get_stripe_count()));

  // pre
  expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_PENDING, OBJECT_EXISTS,
                           false, 0);

  // copy-up
  expect_get_area_size(mock_image_ctx);
  expect_get_parent_overlap(mock_image_ctx, 0);
  expect_reduce_parent_overlap(mock_image_ctx, 0);

  // remove
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_get_object_name(mock_image_ctx, 0, "object0");
  expect_aio_remove(mock_image_ctx, "object0", -EPERM);

  C_SaferCond cond_ctx;
  librbd::NoOpProgressContext progress_ctx;
  MockTrimRequest *req = new MockTrimRequest(
    mock_image_ctx, &cond_ctx, m_image_size, 0, progress_ctx);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(-EPERM, cond_ctx.wait());
}

TEST_F(TestMockOperationTrimRequest, CopyUpError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING)
  ASSERT_EQ(0, create_snapshot("snap1"));

  int order = 22;
  uint64_t features;
  ASSERT_TRUE(::get_features(&features));
  std::string clone_name = get_temp_image_name();
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap1", m_ioctx,
                             clone_name.c_str(), features, &order, 0, 0));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(clone_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap"));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);
  expect_op_work_queue(mock_image_ctx);
  expect_is_lock_owner(mock_image_ctx);

  InSequence seq;
  EXPECT_CALL(mock_image_ctx, get_stripe_period()).WillOnce(Return(ictx->get_object_size()));
  EXPECT_CALL(mock_image_ctx, get_stripe_count()).WillOnce(Return(ictx->get_stripe_count()));

  // pre
  expect_object_map_update(mock_image_ctx, 0, 2, OBJECT_PENDING, OBJECT_EXISTS,
                           false, 0);

  // copy-up
  io::MockObjectDispatch mock_io_object_dispatch;
  expect_get_area_size(mock_image_ctx);
  expect_get_parent_overlap(mock_image_ctx, ictx->get_object_size());
  expect_reduce_parent_overlap(mock_image_ctx, ictx->get_object_size());
  expect_get_object_name(mock_image_ctx, 0, "object0");
  expect_object_discard(mock_image_ctx, mock_io_object_dispatch, 0,
                        ictx->get_object_size(), false, -EINVAL);

  C_SaferCond cond_ctx;
  librbd::NoOpProgressContext progress_ctx;
  MockTrimRequest *req = new MockTrimRequest(
    mock_image_ctx, &cond_ctx, 2 * ictx->get_object_size(), 0, progress_ctx);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

TEST_F(TestMockOperationTrimRequest, BoundaryError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);
  expect_op_work_queue(mock_image_ctx);
  expect_is_lock_owner(mock_image_ctx);

  InSequence seq;
  EXPECT_CALL(mock_image_ctx, get_stripe_period()).WillOnce(Return(ictx->get_object_size()));
  EXPECT_CALL(mock_image_ctx, get_stripe_count()).WillOnce(Return(ictx->get_stripe_count()));

  // boundary
  io::MockObjectDispatch mock_io_object_dispatch;
  expect_object_discard(mock_image_ctx, mock_io_object_dispatch, 1,
                        ictx->get_object_size() - 1, true, -EINVAL);

  C_SaferCond cond_ctx;
  librbd::NoOpProgressContext progress_ctx;
  MockTrimRequest *req = new MockTrimRequest(
    mock_image_ctx, &cond_ctx, ictx->get_object_size(), 1, progress_ctx);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

} // namespace operation
} // namespace librbd
