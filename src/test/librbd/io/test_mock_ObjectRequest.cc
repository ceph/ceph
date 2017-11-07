// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockObjectMap.h"
#include "test/librbd/mock/cache/MockImageCache.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "include/rbd/librbd.hpp"
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
  static CopyupRequest* s_instance;
  static CopyupRequest* create(librbd::MockImageCtx *ictx,
                               const std::string &oid, uint64_t objectno,
                               Extents &&image_extents,
                               const ZTracer::Trace &parent_trace) {
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  CopyupRequest() {
    s_instance = this;
  }
};

template <>
struct ImageRequest<librbd::MockImageCtx> {
  static ImageRequest *s_instance;
  static void aio_read(librbd::MockImageCtx *ictx, AioCompletion *c,
                       Extents &&image_extents, ReadResult &&read_result,
                       int op_flags, const ZTracer::Trace &parent_trace) {
    s_instance->aio_read(c, image_extents);
  }

  MOCK_METHOD2(aio_read, void(AioCompletion *, const Extents&));

  ImageRequest() {
    s_instance = this;
  }
};

CopyupRequest<librbd::MockImageCtx>* CopyupRequest<librbd::MockImageCtx>::s_instance = nullptr;
ImageRequest<librbd::MockImageCtx>* ImageRequest<librbd::MockImageCtx>::s_instance = nullptr;

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
using ::testing::WithArgs;

struct TestMockIoObjectRequest : public TestMockFixture {
  typedef ObjectRequest<librbd::MockImageCtx> MockObjectRequest;
  typedef ObjectReadRequest<librbd::MockImageCtx> MockObjectReadRequest;
  typedef CopyupRequest<librbd::MockImageCtx> MockCopyupRequest;
  typedef ImageRequest<librbd::MockImageCtx> MockImageRequest;

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
                                   const Extents& extents,
                                   uint64_t overlap, uint64_t object_overlap) {
    EXPECT_CALL(mock_image_ctx, prune_parent_extents(_, overlap))
      .WillOnce(WithArg<0>(Invoke([extents, object_overlap](Extents& e) {
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
                   const std::string& data, int r) {
    bufferlist bl;
    bl.append(data);

    auto& expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.data_ctx),
                               read(oid, len, off, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(WithArg<3>(Invoke([bl](bufferlist *out_bl) {
                                   out_bl->append(bl);
                                   return bl.length();
                                 })));
    }
  }

  void expect_sparse_read(MockTestImageCtx &mock_image_ctx,
                          const std::string& oid, uint64_t off, uint64_t len,
                          const std::string& data, int r) {
    bufferlist bl;
    bl.append(data);

    auto& expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.data_ctx),
                               sparse_read(oid, off, len, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(WithArg<4>(Invoke([bl](bufferlist *out_bl) {
                                   out_bl->append(bl);
                                   return bl.length();
                                 })));
    }
  }

  void expect_cache_read(MockTestImageCtx &mock_image_ctx,
                         const std::string& oid, uint64_t object_no,
                         uint64_t off, uint64_t len, const std::string& data,
                         int r) {
    bufferlist bl;
    bl.append(data);

    EXPECT_CALL(mock_image_ctx, aio_read_from_cache({oid}, object_no, _, len,
                                                    off, _, _, _))
      .WillOnce(WithArgs<2, 5>(Invoke([bl, r](bufferlist *out_bl,
                                           Context *on_finish) {
                                 out_bl->append(bl);
                                 on_finish->complete(r);
                               })));
  }

  void expect_aio_read(MockImageRequest& mock_image_request,
                       Extents&& extents, int r) {
    EXPECT_CALL(mock_image_request, aio_read(_, extents))
      .WillOnce(WithArg<0>(Invoke([r](AioCompletion* aio_comp) {
                             aio_comp->set_request_count(1);
                             aio_comp->add_request();
                             aio_comp->complete_request(r);
                           })));
  }

  void expect_copyup(MockCopyupRequest& mock_copyup_request, int r) {
    EXPECT_CALL(mock_copyup_request, send())
      .WillOnce(Invoke([&mock_copyup_request, r]() {
                }));
  }
};

TEST_F(TestMockIoObjectRequest, Read) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ictx->sparse_read_threshold_bytes = 8096;

  MockTestImageCtx mock_image_ctx(*ictx);
  mock_image_ctx.object_cacher = nullptr;

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  InSequence seq;
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_get_read_flags(mock_image_ctx, CEPH_NOSNAP, 0);
  expect_read(mock_image_ctx, "object0", 0, 4096, std::string(4096, '1'), 0);

  C_SaferCond ctx;
  auto req = MockObjectReadRequest::create(
    &mock_image_ctx, "object0", 0, 0, 4096, CEPH_NOSNAP, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, SparseReadThreshold) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ictx->sparse_read_threshold_bytes = ictx->get_object_size();

  MockTestImageCtx mock_image_ctx(*ictx);
  mock_image_ctx.object_cacher = nullptr;

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  InSequence seq;
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_get_read_flags(mock_image_ctx, CEPH_NOSNAP, 0);
  expect_sparse_read(mock_image_ctx, "object0", 0,
                     ictx->sparse_read_threshold_bytes,
                     std::string(ictx->sparse_read_threshold_bytes, '1'), 0);

  C_SaferCond ctx;
  auto req = MockObjectReadRequest::create(
    &mock_image_ctx, "object0", 0, 0, ictx->sparse_read_threshold_bytes,
    CEPH_NOSNAP, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, ReadError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ictx->sparse_read_threshold_bytes = 8096;

  MockTestImageCtx mock_image_ctx(*ictx);
  mock_image_ctx.object_cacher = nullptr;

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  InSequence seq;
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_get_read_flags(mock_image_ctx, CEPH_NOSNAP, 0);
  expect_read(mock_image_ctx, "object0", 0, 4096, "", -EPERM);

  C_SaferCond ctx;
  auto req = MockObjectReadRequest::create(
    &mock_image_ctx, "object0", 0, 0, 4096, CEPH_NOSNAP, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, CacheRead) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ictx->sparse_read_threshold_bytes = 8096;

  MockTestImageCtx mock_image_ctx(*ictx);
  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_cache_read(mock_image_ctx, "object0", 0, 0, 4096,
                    std::string(4096, '1'), 0);

  C_SaferCond ctx;
  auto req = MockObjectReadRequest::create(
    &mock_image_ctx, "object0", 0, 0, 4096, CEPH_NOSNAP, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, CacheReadError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ictx->sparse_read_threshold_bytes = 8096;

  MockTestImageCtx mock_image_ctx(*ictx);
  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_cache_read(mock_image_ctx, "object0", 0, 0, 4096,
                    "", -EPERM);

  C_SaferCond ctx;
  auto req = MockObjectReadRequest::create(
    &mock_image_ctx, "object0", 0, 0, 4096, CEPH_NOSNAP, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, ParentRead) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::Image image;
  librbd::RBD rbd;
  ASSERT_EQ(0, rbd.open(m_ioctx, image, m_image_name.c_str(), NULL));
  ASSERT_EQ(0, image.snap_create("one"));
  ASSERT_EQ(0, image.snap_protect("one"));
  image.close();

  std::string clone_name = get_temp_image_name();
  int order = 0;
  ASSERT_EQ(0, rbd.clone(m_ioctx, m_image_name.c_str(), "one", m_ioctx,
                         clone_name.c_str(), RBD_FEATURE_LAYERING, &order));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(clone_name, &ictx));
  ictx->sparse_read_threshold_bytes = 8096;
  ictx->clone_copy_on_read = false;

  MockTestImageCtx mock_image_ctx(*ictx);
  mock_image_ctx.parent = &mock_image_ctx;
  mock_image_ctx.object_cacher = nullptr;

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  InSequence seq;
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_get_read_flags(mock_image_ctx, CEPH_NOSNAP, 0);
  expect_read(mock_image_ctx, "object0", 0, 4096, "", -ENOENT);

  MockImageRequest mock_image_request;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 4096, 0);
  expect_prune_parent_extents(mock_image_ctx, {{0, 4096}}, 4096, 4096);
  expect_aio_read(mock_image_request, {{0, 4096}}, 0);

  C_SaferCond ctx;
  auto req = MockObjectReadRequest::create(
    &mock_image_ctx, "object0", 0, 0, 4096, CEPH_NOSNAP, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, ParentReadError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::Image image;
  librbd::RBD rbd;
  ASSERT_EQ(0, rbd.open(m_ioctx, image, m_image_name.c_str(), NULL));
  ASSERT_EQ(0, image.snap_create("one"));
  ASSERT_EQ(0, image.snap_protect("one"));
  image.close();

  std::string clone_name = get_temp_image_name();
  int order = 0;
  ASSERT_EQ(0, rbd.clone(m_ioctx, m_image_name.c_str(), "one", m_ioctx,
                         clone_name.c_str(), RBD_FEATURE_LAYERING, &order));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(clone_name, &ictx));
  ictx->sparse_read_threshold_bytes = 8096;
  ictx->clone_copy_on_read = false;

  MockTestImageCtx mock_image_ctx(*ictx);
  mock_image_ctx.parent = &mock_image_ctx;
  mock_image_ctx.object_cacher = nullptr;

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  InSequence seq;
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_get_read_flags(mock_image_ctx, CEPH_NOSNAP, 0);
  expect_read(mock_image_ctx, "object0", 0, 4096, "", -ENOENT);

  MockImageRequest mock_image_request;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 4096, 0);
  expect_prune_parent_extents(mock_image_ctx, {{0, 4096}}, 4096, 4096);
  expect_aio_read(mock_image_request, {{0, 4096}}, -EPERM);

  C_SaferCond ctx;
  auto req = MockObjectReadRequest::create(
    &mock_image_ctx, "object0", 0, 0, 4096, CEPH_NOSNAP, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, CopyOnRead) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::Image image;
  librbd::RBD rbd;
  ASSERT_EQ(0, rbd.open(m_ioctx, image, m_image_name.c_str(), NULL));
  ASSERT_EQ(0, image.snap_create("one"));
  ASSERT_EQ(0, image.snap_protect("one"));
  image.close();

  std::string clone_name = get_temp_image_name();
  int order = 0;
  ASSERT_EQ(0, rbd.clone(m_ioctx, m_image_name.c_str(), "one", m_ioctx,
                         clone_name.c_str(), RBD_FEATURE_LAYERING, &order));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(clone_name, &ictx));
  ictx->sparse_read_threshold_bytes = 8096;
  ictx->clone_copy_on_read = true;

  MockTestImageCtx mock_image_ctx(*ictx);
  mock_image_ctx.parent = &mock_image_ctx;
  mock_image_ctx.object_cacher = nullptr;

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  InSequence seq;
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_get_read_flags(mock_image_ctx, CEPH_NOSNAP, 0);
  expect_read(mock_image_ctx, "object0", 0, 4096, "", -ENOENT);

  MockImageRequest mock_image_request;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 4096, 0);
  expect_prune_parent_extents(mock_image_ctx, {{0, 4096}}, 4096, 4096);
  expect_aio_read(mock_image_request, {{0, 4096}}, 0);

  MockCopyupRequest mock_copyup_request;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 4096, 0);
  expect_prune_parent_extents(mock_image_ctx, {{0, 4096}}, 4096, 4096);
  expect_copyup(mock_copyup_request, 0);

  C_SaferCond ctx;
  auto req = MockObjectReadRequest::create(
    &mock_image_ctx, "object0", 0, 0, 4096, CEPH_NOSNAP, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

} // namespace io
} // namespace librbd
