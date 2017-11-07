// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockObjectMap.h"
#include "test/librbd/mock/cache/MockImageCache.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "librbd/io/CopyupRequest.h"
#include "librbd/io/ImageRequest.h"
#include "librbd/io/ObjectRequest.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace util {

inline ImageCtx *get_image_ctx(MockImageCtx *image_ctx) {
  return image_ctx->image_ctx;
}

} // namespace util

namespace io {

template <>
struct CopyupRequest<librbd::MockImageCtx> {
  static CopyupRequest* create(librbd::MockImageCtx *ictx,
                               const std::string &oid, uint64_t objectno,
                               Extents &&image_extents,
                               const ZTracer::Trace &parent_trace) {
    return nullptr;
  }

  MOCK_METHOD0(send, void());
};

template <>
struct ImageRequest<librbd::MockImageCtx> {
  static void aio_read(librbd::MockImageCtx *ictx, AioCompletion *c,
                       Extents &&image_extents, ReadResult &&read_result,
                       int op_flags, const ZTracer::Trace &parent_trace) {
  }
};

} // namespace io
} // namespace librbd

#include "librbd/io/ObjectRequest.cc"

namespace librbd {
namespace io {

using ::testing::_;
using ::testing::DoDefault;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArg;

struct TestMockIoObjectRequest : public TestMockFixture {
  typedef ObjectRequest<librbd::MockImageCtx> MockObjectRequest;
  typedef ObjectReadRequest<librbd::MockImageCtx> MockObjectReadRequest;

  void expect_object_may_exist(MockTestImageCtx &mock_image_ctx,
                               uint64_t object_no, bool exists) {
    if (mock_image_ctx.object_map != nullptr) {
      EXPECT_CALL(*mock_image_ctx.object_map, object_may_exist(object_no))
        .WillOnce(Return(exists));
    }
  }

  void expect_get_parent_overlap(MockTestImageCtx &mock_image_ctx,
                                 librados::snap_t snap_id, uint64_t overlap,
                                 int r) {
    EXPECT_CALL(mock_image_ctx, get_parent_overlap(snap_id, _))
      .WillOnce(WithArg<1>(Invoke([overlap, r](uint64_t *o) {
                             *o = overlap;
                             return r;
                           })));
  }

  void expect_prune_parent_extents(MockTestImageCtx &mock_image_ctx,
                                   const MockObjectRequest::Extents& extents,
                                   uint64_t overlap, uint64_t object_overlap) {
    EXPECT_CALL(mock_image_ctx, prune_parent_extents(_, overlap))
      .WillOnce(WithArg<0>(Invoke([extents, object_overlap](MockObjectRequest::Extents& e) {
                             e = extents;
                             return object_overlap;
                           })));
  }

  void expect_get_read_flags(MockTestImageCtx &mock_image_ctx,
                             librados::snap_t snap_id, int flags) {
    EXPECT_CALL(mock_image_ctx, get_read_flags(snap_id))
      .WillOnce(Return(flags));
  }

  void expect_read(MockTestImageCtx &mock_image_ctx,
                   const std::string& oid, uint64_t off, uint64_t len,
                   int r) {
    auto& expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.data_ctx),
                               read(oid, len, off, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_sparse_read(MockTestImageCtx &mock_image_ctx,
                          const std::string& oid, uint64_t off, uint64_t len,
                          int r) {
    auto& expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.data_ctx),
                               sparse_read(oid, off, len, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }
};

TEST_F(TestMockIoObjectRequest, Read) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ictx->sparse_read_threshold_bytes = 8096;

  MockTestImageCtx mock_image_ctx(*ictx);
  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  InSequence seq;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 0, 0);
  expect_prune_parent_extents(mock_image_ctx, {}, 0, 0);
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_get_read_flags(mock_image_ctx, CEPH_NOSNAP, 0);
  expect_read(mock_image_ctx, "object0", 0, 4096, 0);

  C_SaferCond ctx;
  auto req = MockObjectReadRequest::create(
    &mock_image_ctx, "object0", 0, 0, 4096, CEPH_NOSNAP, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, SparseReadThreshold) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ictx->sparse_read_threshold_bytes = ictx->get_object_size();

  MockTestImageCtx mock_image_ctx(*ictx);
  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  InSequence seq;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 0, 0);
  expect_prune_parent_extents(mock_image_ctx, {}, 0, 0);
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_get_read_flags(mock_image_ctx, CEPH_NOSNAP, 0);
  expect_sparse_read(mock_image_ctx, "object0", 0,
                     ictx->sparse_read_threshold_bytes, 0);

  C_SaferCond ctx;
  auto req = MockObjectReadRequest::create(
    &mock_image_ctx, "object0", 0, 0, ictx->sparse_read_threshold_bytes,
    CEPH_NOSNAP, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

} // namespace io
} // namespace librbd
