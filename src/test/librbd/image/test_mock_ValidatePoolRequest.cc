// -*- mode:c++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace
} // namespace librbd

// template definitions
#include "librbd/image/ValidatePoolRequest.cc"

namespace librbd {
namespace image {

using ::testing::_;
using ::testing::DoDefault;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockImageValidatePoolRequest : public TestMockFixture {
public:
  typedef ValidatePoolRequest<MockTestImageCtx> MockValidatePoolRequest;

  void SetUp() override {
    TestMockFixture::SetUp();
    m_ioctx.remove(RBD_INFO);
    ASSERT_EQ(0, open_image(m_image_name, &image_ctx));
  }

  void expect_clone(librados::MockTestMemIoCtxImpl &mock_io_ctx) {
    EXPECT_CALL(mock_io_ctx, clone())
      .WillOnce(Invoke([&mock_io_ctx]() {
          mock_io_ctx.get();
          return &mock_io_ctx;
        }));
  }

  void expect_read_rbd_info(librados::MockTestMemIoCtxImpl &mock_io_ctx,
                            const std::string& data, int r) {
    auto& expect = EXPECT_CALL(mock_io_ctx, read(StrEq(RBD_INFO), 0, 0, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(WithArg<3>(Invoke([data](bufferlist* bl) {
          bl->append(data);
          return 0;
        })));
    }
  }

  void expect_write_rbd_info(librados::MockTestMemIoCtxImpl &mock_io_ctx,
                             const std::string& data, int r) {
    bufferlist bl;
    bl.append(data);

    EXPECT_CALL(mock_io_ctx, write(StrEq(RBD_INFO), ContentsEqual(bl),
                                   data.length(), 0, _))
      .WillOnce(Return(r));
  }

  void expect_allocate_snap_id(librados::MockTestMemIoCtxImpl &mock_io_ctx,
                               int r) {
    auto &expect = EXPECT_CALL(mock_io_ctx, selfmanaged_snap_create(_));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_release_snap_id(librados::MockTestMemIoCtxImpl &mock_io_ctx,
                              int r) {
    auto &expect = EXPECT_CALL(mock_io_ctx, selfmanaged_snap_remove(_));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  librbd::ImageCtx *image_ctx;
};

TEST_F(TestMockImageValidatePoolRequest, Success) {
  librados::MockTestMemIoCtxImpl &mock_io_ctx(get_mock_io_ctx(m_ioctx));

  InSequence seq;
  expect_clone(mock_io_ctx);
  expect_read_rbd_info(mock_io_ctx, "", -ENOENT);
  expect_allocate_snap_id(mock_io_ctx, 0);
  expect_write_rbd_info(mock_io_ctx, "validate", 0);
  expect_release_snap_id(mock_io_ctx, 0);
  expect_write_rbd_info(mock_io_ctx, "overwrite validated", 0);

  C_SaferCond ctx;
  auto req = new MockValidatePoolRequest(m_ioctx, image_ctx->op_work_queue,
                                         &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageValidatePoolRequest, AlreadyValidated) {
  librados::MockTestMemIoCtxImpl &mock_io_ctx(get_mock_io_ctx(m_ioctx));

  InSequence seq;
  expect_clone(mock_io_ctx);
  expect_read_rbd_info(mock_io_ctx, "overwrite validated", 0);

  C_SaferCond ctx;
  auto req = new MockValidatePoolRequest(m_ioctx, image_ctx->op_work_queue,
                                         &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageValidatePoolRequest, SnapshotsValidated) {
  librados::MockTestMemIoCtxImpl &mock_io_ctx(get_mock_io_ctx(m_ioctx));

  InSequence seq;
  expect_clone(mock_io_ctx);
  expect_read_rbd_info(mock_io_ctx, "validate", 0);
  expect_write_rbd_info(mock_io_ctx, "overwrite validated", 0);

  C_SaferCond ctx;
  auto req = new MockValidatePoolRequest(m_ioctx, image_ctx->op_work_queue,
                                         &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageValidatePoolRequest, ReadError) {
  librados::MockTestMemIoCtxImpl &mock_io_ctx(get_mock_io_ctx(m_ioctx));

  InSequence seq;
  expect_clone(mock_io_ctx);
  expect_read_rbd_info(mock_io_ctx, "", -EPERM);

  C_SaferCond ctx;
  auto req = new MockValidatePoolRequest(m_ioctx, image_ctx->op_work_queue,
                                         &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockImageValidatePoolRequest, CreateSnapshotError) {
  librados::MockTestMemIoCtxImpl &mock_io_ctx(get_mock_io_ctx(m_ioctx));

  InSequence seq;
  expect_clone(mock_io_ctx);
  expect_read_rbd_info(mock_io_ctx, "", 0);
  expect_allocate_snap_id(mock_io_ctx, -EPERM);

  C_SaferCond ctx;
  auto req = new MockValidatePoolRequest(m_ioctx, image_ctx->op_work_queue,
                                         &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockImageValidatePoolRequest, WriteError) {
  librados::MockTestMemIoCtxImpl &mock_io_ctx(get_mock_io_ctx(m_ioctx));

  InSequence seq;
  expect_clone(mock_io_ctx);
  expect_read_rbd_info(mock_io_ctx, "", -ENOENT);
  expect_allocate_snap_id(mock_io_ctx, 0);
  expect_write_rbd_info(mock_io_ctx, "validate", -EPERM);
  expect_release_snap_id(mock_io_ctx, -EINVAL);

  C_SaferCond ctx;
  auto req = new MockValidatePoolRequest(m_ioctx, image_ctx->op_work_queue,
                                         &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockImageValidatePoolRequest, RemoveSnapshotError) {
  librados::MockTestMemIoCtxImpl &mock_io_ctx(get_mock_io_ctx(m_ioctx));

  InSequence seq;
  expect_clone(mock_io_ctx);
  expect_read_rbd_info(mock_io_ctx, "", -ENOENT);
  expect_allocate_snap_id(mock_io_ctx, 0);
  expect_write_rbd_info(mock_io_ctx, "validate", 0);
  expect_release_snap_id(mock_io_ctx, -EPERM);
  expect_write_rbd_info(mock_io_ctx, "overwrite validated", 0);

  C_SaferCond ctx;
  auto req = new MockValidatePoolRequest(m_ioctx, image_ctx->op_work_queue,
                                         &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageValidatePoolRequest, OverwriteError) {
  librados::MockTestMemIoCtxImpl &mock_io_ctx(get_mock_io_ctx(m_ioctx));

  InSequence seq;
  expect_clone(mock_io_ctx);
  expect_read_rbd_info(mock_io_ctx, "", -ENOENT);
  expect_allocate_snap_id(mock_io_ctx, 0);
  expect_write_rbd_info(mock_io_ctx, "validate", 0);
  expect_release_snap_id(mock_io_ctx, 0);
  expect_write_rbd_info(mock_io_ctx, "overwrite validated", -EOPNOTSUPP);

  C_SaferCond ctx;
  auto req = new MockValidatePoolRequest(m_ioctx, image_ctx->op_work_queue,
                                         &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace image
} // namespace librbd
