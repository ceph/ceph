// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "cls/lock/cls_lock_ops.h"
#include "librbd/managed_lock/BreakRequest.h"
#include "librbd/managed_lock/GetLockerRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <arpa/inet.h>
#include <list>

namespace librbd {
namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace managed_lock {

template <>
struct GetLockerRequest<librbd::MockTestImageCtx> {
  Locker *locker;
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

GetLockerRequest<librbd::MockTestImageCtx> *GetLockerRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace managed_lock
} // namespace librbd

// template definitions
#include "librbd/managed_lock/BreakRequest.cc"

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

class TestMockManagedLockBreakRequest : public TestMockFixture {
public:
  typedef BreakRequest<MockTestImageCtx> MockBreakRequest;
  typedef GetLockerRequest<MockTestImageCtx> MockGetLockerRequest;

  void expect_list_watchers(MockTestImageCtx &mock_image_ctx, int r,
                            const std::string &address, uint64_t watch_handle) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               list_watchers(mock_image_ctx.header_oid, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      obj_watch_t watcher;
      strncpy(watcher.addr, (address + ":0/0").c_str(), sizeof(watcher.addr) - 1);
      watcher.addr[sizeof(watcher.addr) - 1] = '\0';
      watcher.watcher_id = 0;
      watcher.cookie = watch_handle;

      std::list<obj_watch_t> watchers;
      watchers.push_back(watcher);

      expect.WillOnce(DoAll(SetArgPointee<1>(watchers), Return(0)));
    }
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


  void expect_blacklist_add(MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*get_mock_io_ctx(mock_image_ctx.md_ctx).get_mock_rados_client(),
                blacklist_add(_, _))
                  .WillOnce(Return(r));
  }

  void expect_break_lock(MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("lock"),
                     StrEq("break_lock"), _, _, _, _))
                  .WillOnce(Return(r));
  }

  void expect_get_instance_id(MockTestImageCtx &mock_image_ctx, uint64_t id) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx), get_instance_id())
      .WillOnce(Return(id));
  }
};

TEST_F(TestMockManagedLockBreakRequest, DeadLockOwner) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_list_watchers(mock_image_ctx, 0, "dead client", 123);

  MockGetLockerRequest mock_get_locker_request;
  expect_get_locker(mock_image_ctx, mock_get_locker_request,
                    {entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123},
                    0);

  expect_blacklist_add(mock_image_ctx, 0);
  expect_break_lock(mock_image_ctx, 0);

  C_SaferCond ctx;
  Locker locker{entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123};
  MockBreakRequest *req = MockBreakRequest::create(
      mock_image_ctx.md_ctx, *ictx->asio_engine, mock_image_ctx.header_oid,
      locker, true, true, 0, false, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockManagedLockBreakRequest, ForceBreak) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_list_watchers(mock_image_ctx, 0, "1.2.3.4", 123);

  MockGetLockerRequest mock_get_locker_request;
  expect_get_locker(mock_image_ctx, mock_get_locker_request,
                    {entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123},
                    0);

  expect_blacklist_add(mock_image_ctx, 0);
  expect_break_lock(mock_image_ctx, 0);

  C_SaferCond ctx;
  Locker locker{entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123};
  MockBreakRequest *req = MockBreakRequest::create(
      mock_image_ctx.md_ctx, *ictx->asio_engine, mock_image_ctx.header_oid,
      locker, true, true, 0, true, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockManagedLockBreakRequest, GetWatchersError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_list_watchers(mock_image_ctx, -EINVAL, "dead client", 123);

  C_SaferCond ctx;
  Locker locker{entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123};
  MockBreakRequest *req = MockBreakRequest::create(
      mock_image_ctx.md_ctx, *ictx->asio_engine, mock_image_ctx.header_oid,
      locker, true, true, 0, false, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockManagedLockBreakRequest, GetWatchersAlive) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_list_watchers(mock_image_ctx, 0, "1.2.3.4", 123);

  C_SaferCond ctx;
  Locker locker{entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123};
  MockBreakRequest *req = MockBreakRequest::create(
      mock_image_ctx.md_ctx, *ictx->asio_engine, mock_image_ctx.header_oid,
      locker, true, true, 0, false, &ctx);
  req->send();
  ASSERT_EQ(-EAGAIN, ctx.wait());
}

TEST_F(TestMockManagedLockBreakRequest, GetLockerUpdated) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_list_watchers(mock_image_ctx, 0, "dead client", 123);

  MockGetLockerRequest mock_get_locker_request;
  expect_get_locker(mock_image_ctx, mock_get_locker_request,
                    {entity_name_t::CLIENT(2), "auto 123", "1.2.3.4:0/0", 123},
                    0);

  C_SaferCond ctx;
  Locker locker{entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123};
  MockBreakRequest *req = MockBreakRequest::create(
      mock_image_ctx.md_ctx, *ictx->asio_engine, mock_image_ctx.header_oid,
      locker, true, false, 0, false, &ctx);
  req->send();
  ASSERT_EQ(-EAGAIN, ctx.wait());
}

TEST_F(TestMockManagedLockBreakRequest, GetLockerBusy) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_list_watchers(mock_image_ctx, 0, "dead client", 123);

  MockGetLockerRequest mock_get_locker_request;
  expect_get_locker(mock_image_ctx, mock_get_locker_request,
                    {entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123},
                    -EBUSY);

  C_SaferCond ctx;
  Locker locker{entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123};
  MockBreakRequest *req = MockBreakRequest::create(
      mock_image_ctx.md_ctx, *ictx->asio_engine, mock_image_ctx.header_oid,
      locker, true, false, 0, false, &ctx);
  req->send();
  ASSERT_EQ(-EAGAIN, ctx.wait());
}

TEST_F(TestMockManagedLockBreakRequest, GetLockerMissing) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_list_watchers(mock_image_ctx, 0, "dead client", 123);

  MockGetLockerRequest mock_get_locker_request;
  expect_get_locker(mock_image_ctx, mock_get_locker_request,
                    {entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123},
                    -ENOENT);

  C_SaferCond ctx;
  Locker locker{entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123};
  MockBreakRequest *req = MockBreakRequest::create(
      mock_image_ctx.md_ctx, *ictx->asio_engine, mock_image_ctx.header_oid,
      locker, true, false, 0, false, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockManagedLockBreakRequest, GetLockerError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_list_watchers(mock_image_ctx, 0, "dead client", 123);

  MockGetLockerRequest mock_get_locker_request;
  expect_get_locker(mock_image_ctx, mock_get_locker_request, {}, -EINVAL);

  C_SaferCond ctx;
  Locker locker{entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123};
  MockBreakRequest *req = MockBreakRequest::create(
      mock_image_ctx.md_ctx, *ictx->asio_engine, mock_image_ctx.header_oid,
      locker, true, false, 0, false, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockManagedLockBreakRequest, BlacklistDisabled) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_list_watchers(mock_image_ctx, 0, "dead client", 123);

  MockGetLockerRequest mock_get_locker_request;
  expect_get_locker(mock_image_ctx, mock_get_locker_request,
                    {entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123},
                    0);

  expect_break_lock(mock_image_ctx, 0);

  C_SaferCond ctx;
  Locker locker{entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123};
  MockBreakRequest *req = MockBreakRequest::create(
      mock_image_ctx.md_ctx, *ictx->asio_engine, mock_image_ctx.header_oid,
      locker, true, false, 0, false, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockManagedLockBreakRequest, BlacklistSelf) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_list_watchers(mock_image_ctx, 0, "dead client", 456);

  MockGetLockerRequest mock_get_locker_request;
  expect_get_locker(mock_image_ctx, mock_get_locker_request,
                    {entity_name_t::CLIENT(456), "auto 123", "1.2.3.4:0/0",
                     123}, 0);

  expect_get_instance_id(mock_image_ctx, 456);

  C_SaferCond ctx;
  Locker locker{entity_name_t::CLIENT(456), "auto 123", "1.2.3.4:0/0", 123};
  MockBreakRequest *req = MockBreakRequest::create(
      mock_image_ctx.md_ctx, *ictx->asio_engine, mock_image_ctx.header_oid,
      locker, true, true, 0, false, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockManagedLockBreakRequest, BlacklistError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_list_watchers(mock_image_ctx, 0, "dead client", 123);

  MockGetLockerRequest mock_get_locker_request;
  expect_get_locker(mock_image_ctx, mock_get_locker_request,
                    {entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123},
                    0);

  expect_blacklist_add(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  Locker locker{entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123};
  MockBreakRequest *req = MockBreakRequest::create(
      mock_image_ctx.md_ctx, *ictx->asio_engine, mock_image_ctx.header_oid,
      locker, true, true, 0, false, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockManagedLockBreakRequest, BreakLockMissing) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_list_watchers(mock_image_ctx, 0, "dead client", 123);

  MockGetLockerRequest mock_get_locker_request;
  expect_get_locker(mock_image_ctx, mock_get_locker_request,
                    {entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123},
                    0);

  expect_blacklist_add(mock_image_ctx, 0);
  expect_break_lock(mock_image_ctx, -ENOENT);

  C_SaferCond ctx;
  Locker locker{entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123};
  MockBreakRequest *req = MockBreakRequest::create(
      mock_image_ctx.md_ctx, *ictx->asio_engine, mock_image_ctx.header_oid,
      locker, true, true, 0, false, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockManagedLockBreakRequest, BreakLockError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_list_watchers(mock_image_ctx, 0, "dead client", 123);

  MockGetLockerRequest mock_get_locker_request;
  expect_get_locker(mock_image_ctx, mock_get_locker_request,
                    {entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123},
                    0);

  expect_blacklist_add(mock_image_ctx, 0);
  expect_break_lock(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  Locker locker{entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123};
  MockBreakRequest *req = MockBreakRequest::create(
      mock_image_ctx.md_ctx, *ictx->asio_engine, mock_image_ctx.header_oid,
      locker, true, true, 0, false, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace managed_lock
} // namespace librbd

