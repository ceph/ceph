// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockExclusiveLock.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockImageState.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "include/rbd/librbd.hpp"
#include "librbd/Utils.h"
#include "librbd/trash/MoveRequest.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  static MockTestImageCtx *s_instance;
  static MockTestImageCtx *create(const std::string &image_name,
                                  const std::string &image_id,
                                  const char *snap, librados::IoCtx& p,
                                  bool read_only) {
    ceph_assert(s_instance != nullptr);
    s_instance->construct(image_name, image_id);
    return s_instance;
  }

  MOCK_METHOD2(construct, void(const std::string&, const std::string&));

  MockTestImageCtx(librbd::ImageCtx &image_ctx)
      : librbd::MockImageCtx(image_ctx) {
    s_instance = this;
  }
};

MockTestImageCtx *MockTestImageCtx::s_instance = nullptr;

} // anonymous namespace
} // namespace librbd

#include "librbd/trash/MoveRequest.cc"

namespace librbd {
namespace trash {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

struct TestMockTrashMoveRequest : public TestMockFixture {
  typedef MoveRequest<librbd::MockTestImageCtx> MockMoveRequest;

  void expect_trash_add(MockTestImageCtx &mock_image_ctx,
                        const std::string& image_id,
                        cls::rbd::TrashImageSource trash_image_source,
                        const std::string& name,
                        const utime_t& end_time,
                        int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(StrEq("rbd_trash"), _, StrEq("rbd"), StrEq("trash_add"),
                     _, _, _, _))
      .WillOnce(WithArg<4>(Invoke([=](bufferlist& in_bl) {
                             std::string id;
                             cls::rbd::TrashImageSpec trash_image_spec;

                             auto bl_it = in_bl.cbegin();
                             decode(id, bl_it);
                             decode(trash_image_spec, bl_it);

                             EXPECT_EQ(id, image_id);
                             EXPECT_EQ(trash_image_spec.source,
                                       trash_image_source);
                             EXPECT_EQ(trash_image_spec.name, name);
                             EXPECT_EQ(trash_image_spec.deferment_end_time,
                                       end_time);
                             return r;
                           })));
  }

  void expect_aio_remove(MockTestImageCtx &mock_image_ctx,
                         const std::string& oid, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx), remove(oid, _))
      .WillOnce(Return(r));
  }

  void expect_dir_remove(MockTestImageCtx& mock_image_ctx,
                         const std::string& name, const std::string& id,
                         int r) {
    bufferlist in_bl;
    encode(name, in_bl);
    encode(id, in_bl);

    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(StrEq("rbd_directory"), _, StrEq("rbd"), StrEq("dir_remove_image"),
                     ContentsEqual(in_bl), _, _, _))
      .WillOnce(Return(r));
  }
};

TEST_F(TestMockTrashMoveRequest, Success) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  utime_t delete_time{ceph_clock_now()};
  expect_trash_add(mock_image_ctx, "image id",
                   cls::rbd::TRASH_IMAGE_SOURCE_USER, "image name", delete_time,
                   0);
  expect_aio_remove(mock_image_ctx, util::id_obj_name("image name"), 0);
  expect_dir_remove(mock_image_ctx, "image name", "image id", 0);

  C_SaferCond ctx;
  cls::rbd::TrashImageSpec trash_image_spec{
    cls::rbd::TRASH_IMAGE_SOURCE_USER, "image name", delete_time,
    delete_time};
  auto req = MockMoveRequest::create(mock_image_ctx.md_ctx, "image id",
                                     trash_image_spec, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockTrashMoveRequest, TrashAddError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  utime_t delete_time{ceph_clock_now()};
  expect_trash_add(mock_image_ctx, "image id",
                   cls::rbd::TRASH_IMAGE_SOURCE_USER, "image name", delete_time,
                   -EPERM);

  C_SaferCond ctx;
  cls::rbd::TrashImageSpec trash_image_spec{
    cls::rbd::TRASH_IMAGE_SOURCE_USER, "image name", delete_time,
    delete_time};
  auto req = MockMoveRequest::create(mock_image_ctx.md_ctx, "image id",
                                     trash_image_spec, &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockTrashMoveRequest, RemoveIdError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  utime_t delete_time{ceph_clock_now()};
  expect_trash_add(mock_image_ctx, "image id",
                   cls::rbd::TRASH_IMAGE_SOURCE_USER, "image name", delete_time,
                   0);
  expect_aio_remove(mock_image_ctx, util::id_obj_name("image name"), -EPERM);

  C_SaferCond ctx;
  cls::rbd::TrashImageSpec trash_image_spec{
    cls::rbd::TRASH_IMAGE_SOURCE_USER, "image name", delete_time,
    delete_time};
  auto req = MockMoveRequest::create(mock_image_ctx.md_ctx, "image id",
                                     trash_image_spec, &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockTrashMoveRequest, DirectoryRemoveError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  utime_t delete_time{ceph_clock_now()};
  expect_trash_add(mock_image_ctx, "image id",
                   cls::rbd::TRASH_IMAGE_SOURCE_USER, "image name", delete_time,
                   0);
  expect_aio_remove(mock_image_ctx, util::id_obj_name("image name"), 0);
  expect_dir_remove(mock_image_ctx, "image name", "image id", -EPERM);

  C_SaferCond ctx;
  cls::rbd::TrashImageSpec trash_image_spec{
    cls::rbd::TRASH_IMAGE_SOURCE_USER, "image name", delete_time,
    delete_time};
  auto req = MockMoveRequest::create(mock_image_ctx.md_ctx, "image id",
                                     trash_image_spec, &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

} // namespace trash
} // namespace librbd
