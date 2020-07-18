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
#include "librbd/image/TypeTraits.h"
#include "librbd/image/RemoveRequest.h"
#include "librbd/internal.h"
#include "librbd/trash/RemoveRequest.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace image {

// template <>
// struct TypeTraits<MockTestImageCtx> {
//   typedef librbd::MockContextWQ ContextWQ;
// };

template <>
class RemoveRequest<MockTestImageCtx> {
private:
  typedef ::librbd::image::TypeTraits<MockTestImageCtx> TypeTraits;
  typedef typename TypeTraits::ContextWQ ContextWQ;
public:
  static RemoveRequest *s_instance;
  static RemoveRequest *create(librados::IoCtx &ioctx,
                               const std::string &image_name,
                               const std::string &image_id,
                               bool force, bool from_trash_remove,
                               ProgressContext &prog_ctx,
                               ContextWQ *op_work_queue,
                               Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  static RemoveRequest *create(librados::IoCtx &ioctx, MockTestImageCtx *image_ctx,
                               bool force, bool from_trash_remove,
                               ProgressContext &prog_ctx,
                               ContextWQ *op_work_queue,
                               Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  Context *on_finish = nullptr;

  RemoveRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

RemoveRequest<MockTestImageCtx> *RemoveRequest<MockTestImageCtx>::s_instance;

} // namespace image
} // namespace librbd

#include "librbd/trash/RemoveRequest.cc"

namespace librbd {
namespace trash {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;

struct TestMockTrashRemoveRequest : public TestMockFixture {
  typedef RemoveRequest<librbd::MockTestImageCtx> MockRemoveRequest;
  typedef image::RemoveRequest<librbd::MockTestImageCtx> MockImageRemoveRequest;

  NoOpProgressContext m_prog_ctx;

  void expect_set_state(MockTestImageCtx& mock_image_ctx,
                        cls::rbd::TrashImageState trash_set_state,
                        cls::rbd::TrashImageState trash_expect_state,
                        int r) {
    bufferlist in_bl;
    encode(mock_image_ctx.id, in_bl);
    encode(trash_set_state, in_bl);
    encode(trash_expect_state, in_bl);

    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(StrEq("rbd_trash"), _, StrEq("rbd"), StrEq("trash_state_set"),
                     ContentsEqual(in_bl), _, _, _))
      .WillOnce(Return(r));
  }

  void expect_set_deleting_state(MockTestImageCtx& mock_image_ctx, int r) {
    expect_set_state(mock_image_ctx, cls::rbd::TRASH_IMAGE_STATE_REMOVING,
                     cls::rbd::TRASH_IMAGE_STATE_NORMAL, r);
  }

  void expect_restore_normal_state(MockTestImageCtx& mock_image_ctx, int r) {
    expect_set_state(mock_image_ctx, cls::rbd::TRASH_IMAGE_STATE_NORMAL,
                     cls::rbd::TRASH_IMAGE_STATE_REMOVING, r);
  }

  void expect_close_image(MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.state, close(_))
      .WillOnce(Invoke([r](Context *on_finish) {
                  on_finish->complete(r);
                }));
    EXPECT_CALL(mock_image_ctx, destroy());
  }

  void expect_remove_image(MockImageRemoveRequest& mock_image_remove_request,
                           int r) {
    EXPECT_CALL(mock_image_remove_request, send())
      .WillOnce(Invoke([&mock_image_remove_request, r]() {
          mock_image_remove_request.on_finish->complete(r);
        }));
  }

  void expect_remove_trash_entry(MockTestImageCtx& mock_image_ctx,
                                 int r) {
    bufferlist in_bl;
    encode(mock_image_ctx.id, in_bl);

    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(StrEq("rbd_trash"), _, StrEq("rbd"), StrEq("trash_remove"),
                     ContentsEqual(in_bl), _, _, _))
      .WillOnce(Return(r));
  }
};

TEST_F(TestMockTrashRemoveRequest, Success) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockImageRemoveRequest mock_image_remove_request;

  InSequence seq;
  expect_set_deleting_state(mock_image_ctx, 0);
  expect_remove_image(mock_image_remove_request, 0);
  expect_remove_trash_entry(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto req = MockRemoveRequest::create(mock_image_ctx.md_ctx, &mock_image_ctx,
                                       nullptr, false, m_prog_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}
TEST_F(TestMockTrashRemoveRequest, SetDeletingStateError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockImageRemoveRequest mock_image_remove_request;

  InSequence seq;
  expect_set_deleting_state(mock_image_ctx, -EINVAL);
  expect_close_image(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto req = MockRemoveRequest::create(mock_image_ctx.md_ctx, &mock_image_ctx,
                                       nullptr, false, m_prog_ctx, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockTrashRemoveRequest, RemoveImageError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockImageRemoveRequest mock_image_remove_request;

  InSequence seq;
  expect_set_deleting_state(mock_image_ctx, 0);
  expect_remove_image(mock_image_remove_request, -EINVAL);
  expect_restore_normal_state(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto req = MockRemoveRequest::create(mock_image_ctx.md_ctx, &mock_image_ctx,
                                       nullptr, false, m_prog_ctx, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockTrashRemoveRequest, RemoveTrashEntryError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockImageRemoveRequest mock_image_remove_request;

  InSequence seq;
  expect_set_deleting_state(mock_image_ctx, 0);
  expect_remove_image(mock_image_remove_request, 0);
  expect_remove_trash_entry(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  auto req = MockRemoveRequest::create(mock_image_ctx.md_ctx, &mock_image_ctx,
                                       nullptr, false, m_prog_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

} // namespace trash
} // namespace librbd
