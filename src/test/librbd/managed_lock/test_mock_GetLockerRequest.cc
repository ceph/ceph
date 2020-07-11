// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "cls/lock/cls_lock_ops.h"
#include "librbd/managed_lock/GetLockerRequest.h"
#include "librbd/managed_lock/Types.h"
#include "librbd/managed_lock/Utils.h"
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
} // namespace librbd

// template definitions
#include "librbd/managed_lock/GetLockerRequest.cc"

namespace librbd {
namespace managed_lock {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockManagedLockGetLockerRequest : public TestMockFixture {
public:
  typedef GetLockerRequest<MockTestImageCtx> MockGetLockerRequest;

  void expect_get_lock_info(MockTestImageCtx &mock_image_ctx, int r,
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
        reply.lockers.emplace(
          rados::cls::lock::locker_id_t(entity, locker_cookie),
          rados::cls::lock::locker_info_t(utime_t(), entity_addr, ""));
        reply.tag = lock_tag;
        reply.lock_type = lock_type;
      }

      bufferlist bl;
      encode(reply, bl, CEPH_FEATURES_SUPPORTED_DEFAULT);

      std::string str(bl.c_str(), bl.length());
      expect.WillOnce(DoAll(WithArg<5>(CopyInBufferlist(str)), Return(0)));
    }
  }
};

TEST_F(TestMockManagedLockGetLockerRequest, SuccessExclusive) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_get_lock_info(mock_image_ctx, 0, entity_name_t::CLIENT(1), "1.2.3.4",
                       "auto 123", util::get_watcher_lock_tag(),
                       ClsLockType::EXCLUSIVE);

  C_SaferCond ctx;
  Locker locker;
  MockGetLockerRequest *req = MockGetLockerRequest::create(
    mock_image_ctx.md_ctx, mock_image_ctx.header_oid, true, &locker, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(entity_name_t::CLIENT(1), locker.entity);
  ASSERT_EQ("1.2.3.4:0/0", locker.address);
  ASSERT_EQ("auto 123", locker.cookie);
  ASSERT_EQ(123U, locker.handle);
}

TEST_F(TestMockManagedLockGetLockerRequest, SuccessShared) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_get_lock_info(mock_image_ctx, 0, entity_name_t::CLIENT(1), "1.2.3.4",
                       "auto 123", util::get_watcher_lock_tag(),
                       ClsLockType::SHARED);

  C_SaferCond ctx;
  Locker locker;
  MockGetLockerRequest *req = MockGetLockerRequest::create(
    mock_image_ctx.md_ctx, mock_image_ctx.header_oid, false, &locker, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(entity_name_t::CLIENT(1), locker.entity);
  ASSERT_EQ("1.2.3.4:0/0", locker.address);
  ASSERT_EQ("auto 123", locker.cookie);
  ASSERT_EQ(123U, locker.handle);
}

TEST_F(TestMockManagedLockGetLockerRequest, GetLockInfoError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_get_lock_info(mock_image_ctx, -EINVAL, entity_name_t::CLIENT(1), "",
                       "", "", ClsLockType::EXCLUSIVE);

  C_SaferCond ctx;
  Locker locker;
  MockGetLockerRequest *req = MockGetLockerRequest::create(
    mock_image_ctx.md_ctx, mock_image_ctx.header_oid, true, &locker, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockManagedLockGetLockerRequest, GetLockInfoEmpty) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_get_lock_info(mock_image_ctx, -ENOENT, entity_name_t::CLIENT(1), "",
                       "", "", ClsLockType::EXCLUSIVE);

  C_SaferCond ctx;
  Locker locker;
  MockGetLockerRequest *req = MockGetLockerRequest::create(
    mock_image_ctx.md_ctx, mock_image_ctx.header_oid, true, &locker, &ctx);
  req->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockManagedLockGetLockerRequest, GetLockInfoExternalTag) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_get_lock_info(mock_image_ctx, 0, entity_name_t::CLIENT(1), "1.2.3.4",
                       "auto 123", "external tag", ClsLockType::EXCLUSIVE);

  C_SaferCond ctx;
  Locker locker;
  MockGetLockerRequest *req = MockGetLockerRequest::create(
    mock_image_ctx.md_ctx, mock_image_ctx.header_oid, true, &locker, &ctx);
  req->send();
  ASSERT_EQ(-EBUSY, ctx.wait());
}

TEST_F(TestMockManagedLockGetLockerRequest, GetLockInfoIncompatibleShared) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_get_lock_info(mock_image_ctx, 0, entity_name_t::CLIENT(1), "1.2.3.4",
                       "auto 123", util::get_watcher_lock_tag(),
                       ClsLockType::SHARED);

  C_SaferCond ctx;
  Locker locker;
  MockGetLockerRequest *req = MockGetLockerRequest::create(
    mock_image_ctx.md_ctx, mock_image_ctx.header_oid, true, &locker, &ctx);
  req->send();
  ASSERT_EQ(-EBUSY, ctx.wait());
}

TEST_F(TestMockManagedLockGetLockerRequest, GetLockInfoIncompatibleExclusive) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_get_lock_info(mock_image_ctx, 0, entity_name_t::CLIENT(1), "1.2.3.4",
                       "auto 123", util::get_watcher_lock_tag(),
                       ClsLockType::EXCLUSIVE);

  C_SaferCond ctx;
  Locker locker;
  MockGetLockerRequest *req = MockGetLockerRequest::create(
    mock_image_ctx.md_ctx, mock_image_ctx.header_oid, false, &locker, &ctx);
  req->send();
  ASSERT_EQ(-EBUSY, ctx.wait());
}

TEST_F(TestMockManagedLockGetLockerRequest, GetLockInfoExternalCookie) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_get_lock_info(mock_image_ctx, 0, entity_name_t::CLIENT(1), "1.2.3.4",
                       "external cookie", util::get_watcher_lock_tag(),
                       ClsLockType::EXCLUSIVE);

  C_SaferCond ctx;
  Locker locker;
  MockGetLockerRequest *req = MockGetLockerRequest::create(
    mock_image_ctx.md_ctx, mock_image_ctx.header_oid, true, &locker, &ctx);
  req->send();
  ASSERT_EQ(-EBUSY, ctx.wait());
}

} // namespace managed_lock
} // namespace librbd
