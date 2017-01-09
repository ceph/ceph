// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "cls/lock/cls_lock_ops.h"
#include "librbd/managed_lock/AcquireRequest.h"
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
}

// template definitions
#include "librbd/managed_lock/AcquireRequest.cc"
template class librbd::managed_lock::AcquireRequest<librbd::MockImageCtx>;

#include "librbd/ManagedLock.cc"
template class librbd::ManagedLock<librbd::MockImageCtx>;

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
  typedef ManagedLock<MockImageCtx> MockManagedLock;

  void expect_lock(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("lock"), StrEq("lock"), _, _, _))
                  .WillOnce(Return(r));
  }

  void expect_unlock(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("lock"), StrEq("unlock"), _, _, _))
                  .WillOnce(Return(r));
  }

  void expect_get_lock_info(MockImageCtx &mock_image_ctx, int r,
                            const entity_name_t &locker_entity,
                            const std::string &locker_address,
                            const std::string &locker_cookie,
                            const std::string &lock_tag,
                            ClsLockType lock_type) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("lock"),
                               StrEq("get_info"), _, _, _));
    if (r < 0 && r != -ENOENT) {
      expect.WillOnce(Return(r));
    } else {
      entity_name_t entity(locker_entity);
      entity_addr_t entity_addr;
      entity_addr.parse(locker_address.c_str(), NULL);

      cls_lock_get_info_reply reply;
      if (r != -ENOENT) {
        reply.lockers = decltype(reply.lockers){
          {rados::cls::lock::locker_id_t(entity, locker_cookie),
           rados::cls::lock::locker_info_t(utime_t(), entity_addr, "")}};
        reply.tag = lock_tag;
        reply.lock_type = lock_type;
      }

      bufferlist bl;
      ::encode(reply, bl, CEPH_FEATURES_SUPPORTED_DEFAULT);

      std::string str(bl.c_str(), bl.length());
      expect.WillOnce(DoAll(WithArg<5>(CopyInBufferlist(str)), Return(0)));
    }
  }

  void expect_list_watchers(MockImageCtx &mock_image_ctx, int r,
                            const std::string &address, uint64_t watch_handle) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               list_watchers(mock_image_ctx.header_oid, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      obj_watch_t watcher;
      strcpy(watcher.addr, (address + ":0/0").c_str());
      watcher.cookie = watch_handle;

      std::list<obj_watch_t> watchers;
      watchers.push_back(watcher);

      expect.WillOnce(DoAll(SetArgPointee<1>(watchers), Return(0)));
    }
  }

  void expect_blacklist_add(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_rados_client(), blacklist_add(_, _))
                  .WillOnce(Return(r));
  }

  void expect_break_lock(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("lock"),
                     StrEq("break_lock"), _, _, _)).WillOnce(Return(r));
  }

};

TEST_F(TestMockManagedLockAcquireRequest, Success) {

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_lock(mock_image_ctx, 0);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockManagedLockAcquireRequest, LockBusy) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_lock_info(mock_image_ctx, 0, entity_name_t::CLIENT(1), "1.2.3.4",
                       "auto 123", MockManagedLock::WATCHER_LOCK_TAG,
                       LOCK_EXCLUSIVE);
  expect_list_watchers(mock_image_ctx, 0, "dead client", 123);
  expect_blacklist_add(mock_image_ctx, 0);
  expect_break_lock(mock_image_ctx, 0);
  expect_lock(mock_image_ctx, -ENOENT);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, &ctx);
  req->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockManagedLockAcquireRequest, GetLockInfoError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_lock_info(mock_image_ctx, -EINVAL, entity_name_t::CLIENT(1), "",
                       "", "", LOCK_EXCLUSIVE);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockManagedLockAcquireRequest, GetLockInfoEmpty) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_lock_info(mock_image_ctx, -ENOENT, entity_name_t::CLIENT(1), "",
                       "", "", LOCK_EXCLUSIVE);
  expect_lock(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockManagedLockAcquireRequest, GetLockInfoExternalTag) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_lock_info(mock_image_ctx, 0, entity_name_t::CLIENT(1), "1.2.3.4",
                       "auto 123", "external tag", LOCK_EXCLUSIVE);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, &ctx);
  req->send();
  ASSERT_EQ(-EBUSY, ctx.wait());
}

TEST_F(TestMockManagedLockAcquireRequest, GetLockInfoShared) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_lock_info(mock_image_ctx, 0, entity_name_t::CLIENT(1), "1.2.3.4",
                       "auto 123", MockManagedLock::WATCHER_LOCK_TAG,
                       LOCK_SHARED);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, &ctx);
  req->send();
  ASSERT_EQ(-EBUSY, ctx.wait());
}

TEST_F(TestMockManagedLockAcquireRequest, GetLockInfoExternalCookie) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_lock_info(mock_image_ctx, 0, entity_name_t::CLIENT(1), "1.2.3.4",
                       "external cookie", MockManagedLock::WATCHER_LOCK_TAG,
                       LOCK_EXCLUSIVE);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, &ctx);
  req->send();
  ASSERT_EQ(-EBUSY, ctx.wait());
}

TEST_F(TestMockManagedLockAcquireRequest, GetWatchersError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_lock_info(mock_image_ctx, 0, entity_name_t::CLIENT(1), "1.2.3.4",
                       "auto 123", MockManagedLock::WATCHER_LOCK_TAG,
                       LOCK_EXCLUSIVE);
  expect_list_watchers(mock_image_ctx, -EINVAL, "dead client", 123);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockManagedLockAcquireRequest, GetWatchersAlive) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_lock_info(mock_image_ctx, 0, entity_name_t::CLIENT(1), "1.2.3.4",
                       "auto 123", MockManagedLock::WATCHER_LOCK_TAG,
                       LOCK_EXCLUSIVE);
  expect_list_watchers(mock_image_ctx, 0, "1.2.3.4", 123);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, &ctx);
  req->send();
  ASSERT_EQ(-EAGAIN, ctx.wait());
}

TEST_F(TestMockManagedLockAcquireRequest, BlacklistDisabled) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  CephContext *cct = reinterpret_cast<CephContext *>(mock_image_ctx.md_ctx.cct());
  cct->_conf->set_val("rbd_blacklist_on_break_lock", "false");

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_lock_info(mock_image_ctx, 0, entity_name_t::CLIENT(1), "1.2.3.4",
                       "auto 123", MockManagedLock::WATCHER_LOCK_TAG,
                       LOCK_EXCLUSIVE);
  expect_list_watchers(mock_image_ctx, 0, "dead client", 123);
  expect_break_lock(mock_image_ctx, 0);
  expect_lock(mock_image_ctx, -ENOENT);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, &ctx);
  req->send();
  ASSERT_EQ(-ENOENT, ctx.wait());

  cct->_conf->set_val("rbd_blacklist_on_break_lock", "true");
}

TEST_F(TestMockManagedLockAcquireRequest, BlacklistError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_lock_info(mock_image_ctx, 0, entity_name_t::CLIENT(1), "1.2.3.4",
                       "auto 123", MockManagedLock::WATCHER_LOCK_TAG,
                       LOCK_EXCLUSIVE);
  expect_list_watchers(mock_image_ctx, 0, "dead client", 123);
  expect_blacklist_add(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockManagedLockAcquireRequest, BreakLockMissing) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_lock_info(mock_image_ctx, 0, entity_name_t::CLIENT(1), "1.2.3.4",
                       "auto 123", MockManagedLock::WATCHER_LOCK_TAG,
                       LOCK_EXCLUSIVE);
  expect_list_watchers(mock_image_ctx, 0, "dead client", 123);
  expect_blacklist_add(mock_image_ctx, 0);
  expect_break_lock(mock_image_ctx, -ENOENT);
  expect_lock(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockManagedLockAcquireRequest, BreakLockError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_lock_info(mock_image_ctx, 0, entity_name_t::CLIENT(1), "1.2.3.4",
                       "auto 123", MockManagedLock::WATCHER_LOCK_TAG,
                       LOCK_EXCLUSIVE);
  expect_list_watchers(mock_image_ctx, 0, "dead client", 123);
  expect_blacklist_add(mock_image_ctx, 0);
  expect_break_lock(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace managed_lock
} // namespace librbd
