// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "cls/lock/cls_lock_ops.h"
#include "librbd/ObjectMap.h"
#include "librbd/object_map/LockRequest.h"

// template definitions
#include "librbd/object_map/LockRequest.cc"

namespace librbd {
namespace object_map {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockObjectMapLockRequest : public TestMockFixture {
public:
  typedef LockRequest<MockImageCtx> MockLockRequest;

  void expect_lock(MockImageCtx &mock_image_ctx, int r) {
    std::string oid(ObjectMap::object_map_name(mock_image_ctx.id, CEPH_NOSNAP));
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(oid, _, StrEq("lock"), StrEq("lock"), _, _, _))
                  .WillOnce(Return(r));
  }

  void expect_get_lock_info(MockImageCtx &mock_image_ctx, int r) {
    std::string oid(ObjectMap::object_map_name(mock_image_ctx.id, CEPH_NOSNAP));
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(oid, _, StrEq("lock"), StrEq("get_info"), _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      entity_name_t entity1(entity_name_t::CLIENT(1));
      entity_name_t entity2(entity_name_t::CLIENT(2));

      cls_lock_get_info_reply reply;
      reply.lockers = decltype(reply.lockers){
        {rados::cls::lock::locker_id_t(entity1, "cookie1"),
         rados::cls::lock::locker_info_t()},
        {rados::cls::lock::locker_id_t(entity2, "cookie2"),
         rados::cls::lock::locker_info_t()}};

      bufferlist bl;
      ::encode(reply, bl);

      std::string str(bl.c_str(), bl.length());
      expect.WillOnce(DoAll(WithArg<5>(CopyInBufferlist(str)), Return(r)));
    }
  }

  void expect_break_lock(MockImageCtx &mock_image_ctx, int r) {
    std::string oid(ObjectMap::object_map_name(mock_image_ctx.id, CEPH_NOSNAP));
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(oid, _, StrEq("lock"), StrEq("break_lock"), _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.Times(2).WillRepeatedly(Return(0));
    }
  }
};

TEST_F(TestMockObjectMapLockRequest, Success) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  C_SaferCond ctx;
  MockLockRequest *req = new MockLockRequest(mock_image_ctx, &ctx);

  InSequence seq;
  expect_lock(mock_image_ctx, 0);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockObjectMapLockRequest, LockBusy) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  C_SaferCond ctx;
  MockLockRequest *req = new MockLockRequest(mock_image_ctx, &ctx);

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_lock_info(mock_image_ctx, 0);
  expect_break_lock(mock_image_ctx, 0);
  expect_lock(mock_image_ctx, 0);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockObjectMapLockRequest, LockError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  C_SaferCond ctx;
  MockLockRequest *req = new MockLockRequest(mock_image_ctx, &ctx);

  InSequence seq;
  expect_lock(mock_image_ctx, -ENOENT);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockObjectMapLockRequest, GetLockInfoMissing) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  C_SaferCond ctx;
  MockLockRequest *req = new MockLockRequest(mock_image_ctx, &ctx);

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_lock_info(mock_image_ctx, -ENOENT);
  expect_lock(mock_image_ctx, 0);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockObjectMapLockRequest, GetLockInfoError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  C_SaferCond ctx;
  MockLockRequest *req = new MockLockRequest(mock_image_ctx, &ctx);

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_lock_info(mock_image_ctx, -EINVAL);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockObjectMapLockRequest, BreakLockMissing) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  C_SaferCond ctx;
  MockLockRequest *req = new MockLockRequest(mock_image_ctx, &ctx);

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_lock_info(mock_image_ctx, 0);
  expect_break_lock(mock_image_ctx, -ENOENT);
  expect_lock(mock_image_ctx, 0);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockObjectMapLockRequest, BreakLockError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  C_SaferCond ctx;
  MockLockRequest *req = new MockLockRequest(mock_image_ctx, &ctx);

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_lock_info(mock_image_ctx, 0);
  expect_break_lock(mock_image_ctx, -EINVAL);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockObjectMapLockRequest, LockErrorAfterBrokeLock) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  C_SaferCond ctx;
  MockLockRequest *req = new MockLockRequest(mock_image_ctx, &ctx);

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_lock_info(mock_image_ctx, 0);
  expect_break_lock(mock_image_ctx, 0);
  expect_lock(mock_image_ctx, -EBUSY);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

} // namespace object_map
} // namespace librbd
