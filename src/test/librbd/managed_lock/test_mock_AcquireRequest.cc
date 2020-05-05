// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "cls/lock/cls_lock_ops.h"
#include "librbd/managed_lock/AcquireRequest.h"
#include "librbd/managed_lock/BreakRequest.h"
#include "librbd/managed_lock/GetLockerRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <arpa/inet.h>
#include <list>

namespace librbd {
namespace watcher {
template <>
struct Traits<MockImageCtx> {
  typedef librbd::MockImageWatcher Watcher;
};
}

namespace managed_lock {

template<>
struct BreakRequest<librbd::MockImageCtx> {
  Context *on_finish = nullptr;
  static BreakRequest *s_instance;
  static BreakRequest* create(librados::IoCtx& ioctx,
                              asio::ContextWQ *work_queue,
                              const std::string& oid, const Locker &locker,
                              bool exclusive, bool blacklist_locker,
                              uint32_t blacklist_expire_seconds,
                              bool force_break_lock, Context *on_finish) {
    CephContext *cct = reinterpret_cast<CephContext *>(ioctx.cct());
    EXPECT_EQ(cct->_conf.get_val<bool>("rbd_blacklist_on_break_lock"),
              blacklist_locker);
    EXPECT_EQ(cct->_conf.get_val<uint64_t>("rbd_blacklist_expire_seconds"),
              blacklist_expire_seconds);
    EXPECT_FALSE(force_break_lock);
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  BreakRequest() {
    s_instance = this;
  }
  MOCK_METHOD0(send, void());
};

template <>
struct GetLockerRequest<librbd::MockImageCtx> {
  Locker *locker = nullptr;
  Context *on_finish = nullptr;

  static GetLockerRequest *s_instance;
  static GetLockerRequest* create(librados::IoCtx& ioctx,
                                  const std::string& oid, bool exclusive,
                                  Locker *locker, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->locker = locker;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  GetLockerRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

BreakRequest<librbd::MockImageCtx> *BreakRequest<librbd::MockImageCtx>::s_instance = nullptr;
GetLockerRequest<librbd::MockImageCtx> *GetLockerRequest<librbd::MockImageCtx>::s_instance = nullptr;

} // namespace managed_lock
} // namespace librbd

// template definitions
#include "librbd/managed_lock/AcquireRequest.cc"
template class librbd::managed_lock::AcquireRequest<librbd::MockImageCtx>;

namespace {

MATCHER_P(IsLockType, exclusive, "") {
  cls_lock_lock_op op;
  bufferlist bl;
  bl.share(arg);
  auto iter = bl.cbegin();
  decode(op, iter);
  return op.type == (exclusive ? ClsLockType::EXCLUSIVE : ClsLockType::SHARED);
}

} // anonymous namespace

namespace librbd {
namespace managed_lock {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::StrEq;
using ::testing::WithArg;

static const std::string TEST_COOKIE("auto 123");

class TestMockManagedLockAcquireRequest : public TestMockFixture {
public:
  typedef AcquireRequest<MockImageCtx> MockAcquireRequest;
  typedef BreakRequest<MockImageCtx> MockBreakRequest;
  typedef GetLockerRequest<MockImageCtx> MockGetLockerRequest;

  void expect_lock(MockImageCtx &mock_image_ctx, int r,
                             bool exclusive = true) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("lock"),
                     StrEq("lock"), IsLockType(exclusive), _, _))
                  .WillOnce(Return(r));
  }

  void expect_get_locker(MockImageCtx &mock_image_ctx,
                         MockGetLockerRequest &mock_get_locker_request,
                         const Locker &locker, int r) {
    EXPECT_CALL(mock_get_locker_request, send())
      .WillOnce(Invoke([&mock_image_ctx, &mock_get_locker_request, locker, r]() {
          *mock_get_locker_request.locker = locker;
          mock_image_ctx.image_ctx->op_work_queue->queue(
            mock_get_locker_request.on_finish, r);
        }));
  }

  void expect_break_lock(MockImageCtx &mock_image_ctx,
                         MockBreakRequest &mock_break_request, int r) {
    EXPECT_CALL(mock_break_request, send())
      .WillOnce(FinishRequest(&mock_break_request, r, &mock_image_ctx));
  }
};

TEST_F(TestMockManagedLockAcquireRequest, SuccessExclusive) {

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;

  InSequence seq;
  expect_get_locker(mock_image_ctx, mock_get_locker_request, {}, 0);
  expect_lock(mock_image_ctx, 0);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, true, true, 0, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockManagedLockAcquireRequest, SuccessShared) {

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;

  InSequence seq;
  expect_get_locker(mock_image_ctx, mock_get_locker_request, {}, 0);
  expect_lock(mock_image_ctx, 0, false);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, false, true, 0, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockManagedLockAcquireRequest, LockBusy) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  MockBreakRequest mock_break_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_get_locker(mock_image_ctx, mock_get_locker_request,
                    {entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123},
                    0);
  expect_lock(mock_image_ctx, -EBUSY);
  expect_break_lock(mock_image_ctx, mock_break_request, 0);
  expect_lock(mock_image_ctx, -ENOENT);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, true, true, 0, &ctx);
  req->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockManagedLockAcquireRequest, GetLockInfoError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;

  InSequence seq;
  expect_get_locker(mock_image_ctx, mock_get_locker_request, {}, -EINVAL);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, true, true, 0, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockManagedLockAcquireRequest, GetLockInfoEmpty) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;

  InSequence seq;
  expect_get_locker(mock_image_ctx, mock_get_locker_request, {}, -ENOENT);
  expect_lock(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, true, true, 0, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockManagedLockAcquireRequest, BreakLockError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  MockBreakRequest mock_break_request;

  InSequence seq;
  expect_get_locker(mock_image_ctx, mock_get_locker_request,
                    {entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123},
                    0);
  expect_lock(mock_image_ctx, -EBUSY);
  expect_break_lock(mock_image_ctx, mock_break_request, -EINVAL);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, true, true, 0, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace managed_lock
} // namespace librbd
