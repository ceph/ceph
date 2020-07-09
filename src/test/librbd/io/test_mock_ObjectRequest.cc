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
  MOCK_METHOD0(send, void());
  MOCK_METHOD1(append_request, void(AbstractObjectWriteRequest<librbd::MockTestImageCtx>*));
};

template <>
struct CopyupRequest<librbd::MockTestImageCtx> : public CopyupRequest<librbd::MockImageCtx> {
  static CopyupRequest* s_instance;
  static CopyupRequest* create(librbd::MockTestImageCtx *ictx,
                               uint64_t objectno, Extents &&image_extents,
                               const ZTracer::Trace &parent_trace) {
    return s_instance;
  }

  CopyupRequest() {
    s_instance = this;
  }
};

template <>
struct ImageRequest<librbd::MockTestImageCtx> {
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

CopyupRequest<librbd::MockTestImageCtx>* CopyupRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
ImageRequest<librbd::MockTestImageCtx>* ImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

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
  typedef ObjectRequest<librbd::MockTestImageCtx> MockObjectRequest;
  typedef ObjectReadRequest<librbd::MockTestImageCtx> MockObjectReadRequest;
  typedef ObjectWriteRequest<librbd::MockTestImageCtx> MockObjectWriteRequest;
  typedef ObjectDiscardRequest<librbd::MockTestImageCtx> MockObjectDiscardRequest;
  typedef ObjectWriteSameRequest<librbd::MockTestImageCtx> MockObjectWriteSameRequest;
  typedef ObjectCompareAndWriteRequest<librbd::MockTestImageCtx> MockObjectCompareAndWriteRequest;
  typedef AbstractObjectWriteRequest<librbd::MockTestImageCtx> MockAbstractObjectWriteRequest;
  typedef CopyupRequest<librbd::MockTestImageCtx> MockCopyupRequest;
  typedef ImageRequest<librbd::MockTestImageCtx> MockImageRequest;

  void expect_object_may_exist(MockTestImageCtx &mock_image_ctx,
                               uint64_t object_no, bool exists) {
    if (mock_image_ctx.object_map != nullptr) {
      EXPECT_CALL(*mock_image_ctx.object_map, object_may_exist(object_no))
        .WillOnce(Return(exists));
    }
  }

  void expect_get_object_size(MockTestImageCtx &mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, get_object_size()).WillRepeatedly(Return(
      mock_image_ctx.layout.object_size));
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

  void expect_is_lock_owner(MockExclusiveLock& mock_exclusive_lock) {
    EXPECT_CALL(mock_exclusive_lock, is_lock_owner()).WillRepeatedly(
      Return(true));
  }

  void expect_read(MockTestImageCtx &mock_image_ctx,
                   const std::string& oid, uint64_t off, uint64_t len,
                   const std::string& data, int r) {
    bufferlist bl;
    bl.append(data);

    auto& expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.data_ctx),
                               read(oid, len, off, _, _));
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
                               sparse_read(oid, off, len, _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(WithArg<4>(Invoke([bl](bufferlist *out_bl) {
                                   out_bl->append(bl);
                                   return bl.length();
                                 })));
    }
  }

  void expect_aio_read(MockTestImageCtx &mock_image_ctx,
                       MockImageRequest& mock_image_request,
                       Extents&& extents, int r) {
    EXPECT_CALL(mock_image_request, aio_read(_, extents))
      .WillOnce(WithArg<0>(Invoke([&mock_image_ctx, r](AioCompletion* aio_comp) {
                             aio_comp->set_request_count(1);
                             mock_image_ctx.image_ctx->op_work_queue->queue(new C_AioRequest(aio_comp), r);
                           })));
  }

  void expect_copyup(MockCopyupRequest& mock_copyup_request, int r) {
    EXPECT_CALL(mock_copyup_request, send())
      .WillOnce(Invoke([]() {}));
  }

  void expect_copyup(MockCopyupRequest& mock_copyup_request,
                     MockAbstractObjectWriteRequest** write_request, int r) {
    EXPECT_CALL(mock_copyup_request, append_request(_))
      .WillOnce(Invoke([write_request](MockAbstractObjectWriteRequest *req) {
                  *write_request = req;
                }));
    EXPECT_CALL(mock_copyup_request, send())
      .WillOnce(Invoke([write_request, r]() {
                  (*write_request)->handle_copyup(r);
                }));
  }

  void expect_object_map_update(MockTestImageCtx &mock_image_ctx,
                                uint64_t start_object, uint64_t end_object,
                                uint8_t state,
                                const boost::optional<uint8_t> &current_state,
                                bool updated, int ret_val) {
    if (mock_image_ctx.object_map != nullptr) {
      EXPECT_CALL(*mock_image_ctx.object_map,
                  aio_update(CEPH_NOSNAP, start_object, end_object, state,
                             current_state, _, false, _))
        .WillOnce(WithArg<7>(Invoke([&mock_image_ctx, updated, ret_val](Context *ctx) {
                               if (updated) {
                                 mock_image_ctx.op_work_queue->queue(ctx, ret_val);
                               }
                               return updated;
                             })));
    }
  }

  void expect_assert_exists(MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.data_ctx),
                assert_exists(_, _))
      .WillOnce(Return(r));
  }

  void expect_write(MockTestImageCtx &mock_image_ctx,
                    uint64_t offset, uint64_t length, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.data_ctx),
                               write(_, _, length, offset, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_write_full(MockTestImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.data_ctx),
                               write_full(_, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_writesame(MockTestImageCtx &mock_image_ctx,
                        uint64_t offset, uint64_t length, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.data_ctx),
                               writesame(_, _, length, offset, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_remove(MockTestImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.data_ctx),
                               remove(_, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_create(MockTestImageCtx &mock_image_ctx, bool exclusive) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.data_ctx), create(_, exclusive, _))
      .Times(1);
  }

  void expect_truncate(MockTestImageCtx &mock_image_ctx, int offset, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.data_ctx),
                               truncate(_, offset, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_zero(MockTestImageCtx &mock_image_ctx, int offset, int length,
                   int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.data_ctx),
                               zero(_, offset, length, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_cmpext(MockTestImageCtx &mock_image_ctx, int offset, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.data_ctx),
                               cmpext(_, offset, _, _));
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
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_get_read_flags(mock_image_ctx, CEPH_NOSNAP, 0);
  expect_read(mock_image_ctx, ictx->get_object_name(0), 0, 4096,
              std::string(4096, '1'), 0);

  bufferlist bl;
  ExtentMap extent_map;
  C_SaferCond ctx;
  auto req = MockObjectReadRequest::create(
    &mock_image_ctx, 0, 0, 4096, CEPH_NOSNAP, 0, {}, &bl, &extent_map, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
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
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_get_read_flags(mock_image_ctx, CEPH_NOSNAP, 0);
  expect_sparse_read(mock_image_ctx, ictx->get_object_name(0), 0,
                     ictx->sparse_read_threshold_bytes,
                     std::string(ictx->sparse_read_threshold_bytes, '1'), 0);

  bufferlist bl;
  ExtentMap extent_map;
  C_SaferCond ctx;
  auto req = MockObjectReadRequest::create(
    &mock_image_ctx, 0, 0, ictx->sparse_read_threshold_bytes, CEPH_NOSNAP, 0,
    {}, &bl, &extent_map, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, ReadError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ictx->sparse_read_threshold_bytes = 8096;

  MockTestImageCtx mock_image_ctx(*ictx);

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  InSequence seq;
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_get_read_flags(mock_image_ctx, CEPH_NOSNAP, 0);
  expect_read(mock_image_ctx, ictx->get_object_name(0), 0, 4096, "", -EPERM);

  bufferlist bl;
  ExtentMap extent_map;
  C_SaferCond ctx;
  auto req = MockObjectReadRequest::create(
    &mock_image_ctx, 0, 0, 4096, CEPH_NOSNAP, 0, {}, &bl, &extent_map, &ctx);
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

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  InSequence seq;
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_get_read_flags(mock_image_ctx, CEPH_NOSNAP, 0);
  expect_read(mock_image_ctx, ictx->get_object_name(0), 0, 4096, "", -ENOENT);

  MockImageRequest mock_image_request;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 4096, 0);
  expect_prune_parent_extents(mock_image_ctx, {{0, 4096}}, 4096, 4096);
  expect_aio_read(mock_image_ctx, mock_image_request, {{0, 4096}}, 0);

  bufferlist bl;
  ExtentMap extent_map;
  C_SaferCond ctx;
  auto req = MockObjectReadRequest::create(
    &mock_image_ctx, 0, 0, 4096, CEPH_NOSNAP, 0, {}, &bl, &extent_map, &ctx);
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

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  InSequence seq;
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_get_read_flags(mock_image_ctx, CEPH_NOSNAP, 0);
  expect_read(mock_image_ctx, ictx->get_object_name(0), 0, 4096, "", -ENOENT);

  MockImageRequest mock_image_request;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 4096, 0);
  expect_prune_parent_extents(mock_image_ctx, {{0, 4096}}, 4096, 4096);
  expect_aio_read(mock_image_ctx, mock_image_request, {{0, 4096}}, -EPERM);

  bufferlist bl;
  ExtentMap extent_map;
  C_SaferCond ctx;
  auto req = MockObjectReadRequest::create(
    &mock_image_ctx, 0, 0, 4096, CEPH_NOSNAP, 0, {}, &bl, &extent_map, &ctx);
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

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  InSequence seq;
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_get_read_flags(mock_image_ctx, CEPH_NOSNAP, 0);
  expect_read(mock_image_ctx, ictx->get_object_name(0), 0, 4096, "", -ENOENT);

  MockImageRequest mock_image_request;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 4096, 0);
  expect_prune_parent_extents(mock_image_ctx, {{0, 4096}}, 4096, 4096);
  expect_aio_read(mock_image_ctx, mock_image_request, {{0, 4096}}, 0);

  MockCopyupRequest mock_copyup_request;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 4096, 0);
  expect_prune_parent_extents(mock_image_ctx, {{0, 4096}}, 4096, 4096);
  expect_copyup(mock_copyup_request, 0);

  bufferlist bl;
  ExtentMap extent_map;
  C_SaferCond ctx;
  auto req = MockObjectReadRequest::create(
    &mock_image_ctx, 0, 0, 4096, CEPH_NOSNAP, 0, {}, &bl, &extent_map, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, Write) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_get_object_size(mock_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
    expect_is_lock_owner(mock_exclusive_lock);
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  bufferlist bl;
  bl.append(std::string(4096, '1'));

  InSequence seq;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 0, 0);
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_EXISTS, {}, false, 0);
  expect_write(mock_image_ctx, 0, 4096, 0);

  C_SaferCond ctx;
  auto req = MockObjectWriteRequest::create_write(
    &mock_image_ctx, 0, 0, std::move(bl), mock_image_ctx.snapc, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, WriteFull) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_get_object_size(mock_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
    expect_is_lock_owner(mock_exclusive_lock);
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  bufferlist bl;
  bl.append(std::string(ictx->get_object_size(), '1'));

  InSequence seq;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 0, 0);
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_EXISTS, {}, false, 0);
  expect_write_full(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto req = MockObjectWriteRequest::create_write(
    &mock_image_ctx, 0, 0, std::move(bl), mock_image_ctx.snapc, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, WriteObjectMap) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);
  expect_get_object_size(mock_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_is_lock_owner(mock_exclusive_lock);

  MockObjectMap mock_object_map;
  mock_image_ctx.object_map = &mock_object_map;

  bufferlist bl;
  bl.append(std::string(4096, '1'));

  InSequence seq;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 0, 0);
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_EXISTS, {}, true, 0);
  expect_write(mock_image_ctx, 0, 4096, 0);

  C_SaferCond ctx;
  auto req = MockObjectWriteRequest::create_write(
    &mock_image_ctx, 0, 0, std::move(bl), mock_image_ctx.snapc, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, WriteError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_get_object_size(mock_image_ctx);

  bufferlist bl;
  bl.append(std::string(4096, '1'));

  InSequence seq;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 0, 0);
  expect_write(mock_image_ctx, 0, 4096, -EPERM);

  C_SaferCond ctx;
  auto req = MockObjectWriteRequest::create_write(
    &mock_image_ctx, 0, 0, std::move(bl), mock_image_ctx.snapc, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, Copyup) {
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

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_get_object_size(mock_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
    expect_is_lock_owner(mock_exclusive_lock);
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  bufferlist bl;
  bl.append(std::string(4096, '1'));

  InSequence seq;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 4096, 0);
  expect_prune_parent_extents(mock_image_ctx, {{0, 4096}}, 4096, 4096);
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_EXISTS, {}, false, 0);
  expect_assert_exists(mock_image_ctx, -ENOENT);

  MockAbstractObjectWriteRequest *write_request = nullptr;
  MockCopyupRequest mock_copyup_request;
  expect_copyup(mock_copyup_request, &write_request, 0);

  C_SaferCond ctx;
  auto req = MockObjectWriteRequest::create_write(
    &mock_image_ctx, 0, 0, std::move(bl), mock_image_ctx.snapc, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, CopyupRestart) {
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

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_get_object_size(mock_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
    expect_is_lock_owner(mock_exclusive_lock);
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  bufferlist bl;
  bl.append(std::string(4096, '1'));

  InSequence seq;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 4096, 0);
  expect_prune_parent_extents(mock_image_ctx, {{0, 4096}}, 4096, 4096);
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_EXISTS, {}, false, 0);
  expect_assert_exists(mock_image_ctx, -ENOENT);

  MockAbstractObjectWriteRequest *write_request = nullptr;
  MockCopyupRequest mock_copyup_request;
  expect_copyup(mock_copyup_request, &write_request, -ERESTART);
  expect_assert_exists(mock_image_ctx, 0);
  expect_write(mock_image_ctx, 0, 4096, 0);

  C_SaferCond ctx;
  auto req = MockObjectWriteRequest::create_write(
    &mock_image_ctx, 0, 0, std::move(bl), mock_image_ctx.snapc, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, CopyupOptimization) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING | RBD_FEATURE_OBJECT_MAP);

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

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_get_object_size(mock_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_is_lock_owner(mock_exclusive_lock);

  MockObjectMap mock_object_map;
  mock_image_ctx.object_map = &mock_object_map;

  bufferlist bl;
  bl.append(std::string(4096, '1'));

  InSequence seq;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 4096, 0);
  expect_prune_parent_extents(mock_image_ctx, {{0, 4096}}, 4096, 4096);
  expect_object_may_exist(mock_image_ctx, 0, false);

  MockAbstractObjectWriteRequest *write_request = nullptr;
  MockCopyupRequest mock_copyup_request;
  expect_copyup(mock_copyup_request, &write_request, 0);

  C_SaferCond ctx;
  auto req = MockObjectWriteRequest::create_write(
    &mock_image_ctx, 0, 0, std::move(bl), mock_image_ctx.snapc, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, CopyupError) {
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

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_get_object_size(mock_image_ctx);

  bufferlist bl;
  bl.append(std::string(4096, '1'));

  InSequence seq;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 4096, 0);
  expect_prune_parent_extents(mock_image_ctx, {{0, 4096}}, 4096, 4096);
  expect_assert_exists(mock_image_ctx, -ENOENT);

  MockAbstractObjectWriteRequest *write_request = nullptr;
  MockCopyupRequest mock_copyup_request;
  expect_copyup(mock_copyup_request, &write_request, -EPERM);

  C_SaferCond ctx;
  auto req = MockObjectWriteRequest::create_write(
    &mock_image_ctx, 0, 0, std::move(bl), mock_image_ctx.snapc, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, DiscardRemove) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_get_object_size(mock_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
    expect_is_lock_owner(mock_exclusive_lock);
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  InSequence seq;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 0, 0);
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_PENDING, {}, false, 0);
  expect_remove(mock_image_ctx, 0);
  expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_NONEXISTENT,
                           OBJECT_PENDING, false, 0);

  C_SaferCond ctx;
  auto req = MockObjectDiscardRequest::create_discard(
    &mock_image_ctx, 0, 0, mock_image_ctx.get_object_size(),
    mock_image_ctx.snapc, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, DiscardRemoveTruncate) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::Image image;
  librbd::RBD rbd;
  ASSERT_EQ(0, rbd.open(m_ioctx, image, m_image_name.c_str(), NULL));
  ASSERT_EQ(0, image.snap_create("one"));
  ASSERT_EQ(0, image.snap_protect("one"));
  uint64_t features;
  ASSERT_EQ(0, image.features(&features));
  image.close();

  std::string clone_name = get_temp_image_name();
  int order = 0;
  ASSERT_EQ(0, rbd.clone(m_ioctx, m_image_name.c_str(), "one", m_ioctx,
                         clone_name.c_str(), features, &order));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(clone_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_get_object_size(mock_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
    expect_is_lock_owner(mock_exclusive_lock);
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  InSequence seq;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 4096, 0);
  expect_prune_parent_extents(mock_image_ctx, {{0, 4096}}, 4096, 4096);
  expect_object_may_exist(mock_image_ctx, 0, false);
  expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_EXISTS, {}, false, 0);
  expect_create(mock_image_ctx, false);
  expect_truncate(mock_image_ctx, 0, 0);

  C_SaferCond ctx;
  auto req = MockObjectDiscardRequest::create_discard(
    &mock_image_ctx, 0, 0, mock_image_ctx.get_object_size(),
    mock_image_ctx.snapc, OBJECT_DISCARD_FLAG_DISABLE_CLONE_REMOVE, {}, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, DiscardTruncateAssertExists) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::Image image;
  librbd::RBD rbd;
  ASSERT_EQ(0, rbd.open(m_ioctx, image, m_image_name.c_str(), NULL));
  ASSERT_EQ(0, image.snap_create("one"));
  ASSERT_EQ(0, image.snap_protect("one"));
  uint64_t features;
  ASSERT_EQ(0, image.features(&features));
  image.close();

  std::string clone_name = get_temp_image_name();
  int order = 0;
  ASSERT_EQ(0, rbd.clone(m_ioctx, m_image_name.c_str(), "one", m_ioctx,
                         clone_name.c_str(), features, &order));
  ASSERT_EQ(0, rbd.open(m_ioctx, image, clone_name.c_str(), NULL));
  ASSERT_EQ(0, image.snap_create("one"));
  image.close();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(clone_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_get_object_size(mock_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
    expect_is_lock_owner(mock_exclusive_lock);
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  InSequence seq;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 4096, 0);
  expect_prune_parent_extents(mock_image_ctx, {{0, 4096}}, 4096, 4096);
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_EXISTS, {}, false, 0);
  expect_assert_exists(mock_image_ctx, 0);
  expect_truncate(mock_image_ctx, 0, 0);

  EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.data_ctx), create(_, _, _))
    .Times(0);

  C_SaferCond ctx;
  auto req = MockObjectDiscardRequest::create_discard(
    &mock_image_ctx, 0, 0, mock_image_ctx.get_object_size(),
    mock_image_ctx.snapc, OBJECT_DISCARD_FLAG_DISABLE_CLONE_REMOVE, {}, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, DiscardTruncate) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_get_object_size(mock_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
    expect_is_lock_owner(mock_exclusive_lock);
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  InSequence seq;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 0, 0);
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_EXISTS, {}, false, 0);
  expect_truncate(mock_image_ctx, 1, 0);

  EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.data_ctx), create(_, _, _))
    .Times(0);

  C_SaferCond ctx;
  auto req = MockObjectDiscardRequest::create_discard(
    &mock_image_ctx, 0, 1, mock_image_ctx.get_object_size() - 1,
    mock_image_ctx.snapc, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, DiscardZero) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_get_object_size(mock_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
    expect_is_lock_owner(mock_exclusive_lock);
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  InSequence seq;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 0, 0);
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_EXISTS, {}, false, 0);
  expect_zero(mock_image_ctx, 1, 1, 0);

  C_SaferCond ctx;
  auto req = MockObjectDiscardRequest::create_discard(
    &mock_image_ctx, 0, 1, 1, mock_image_ctx.snapc, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, DiscardDisableObjectMapUpdate) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_get_object_size(mock_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_is_lock_owner(mock_exclusive_lock);

  MockObjectMap mock_object_map;
  mock_image_ctx.object_map = &mock_object_map;

  InSequence seq;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 0, 0);
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_remove(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto req = MockObjectDiscardRequest::create_discard(
    &mock_image_ctx, 0, 0, mock_image_ctx.get_object_size(),
    mock_image_ctx.snapc,
    OBJECT_DISCARD_FLAG_DISABLE_CLONE_REMOVE |
      OBJECT_DISCARD_FLAG_DISABLE_OBJECT_MAP_UPDATE, {}, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, DiscardNoOp) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);
  expect_get_object_size(mock_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_is_lock_owner(mock_exclusive_lock);

  MockObjectMap mock_object_map;
  mock_image_ctx.object_map = &mock_object_map;

  InSequence seq;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 0, 0);
  expect_object_may_exist(mock_image_ctx, 0, false);

  C_SaferCond ctx;
  auto req = MockObjectDiscardRequest::create_discard(
    &mock_image_ctx, 0, 0, mock_image_ctx.get_object_size(),
    mock_image_ctx.snapc,
    OBJECT_DISCARD_FLAG_DISABLE_CLONE_REMOVE |
      OBJECT_DISCARD_FLAG_DISABLE_OBJECT_MAP_UPDATE, {},
    &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, WriteSame) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_get_object_size(mock_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
    expect_is_lock_owner(mock_exclusive_lock);
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  bufferlist bl;
  bl.append(std::string(4096, '1'));

  InSequence seq;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 0, 0);
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_EXISTS, {}, false, 0);
  expect_writesame(mock_image_ctx, 0, 4096, 0);

  C_SaferCond ctx;
  auto req = MockObjectWriteSameRequest::create_write_same(
    &mock_image_ctx, 0, 0, 4096, std::move(bl), mock_image_ctx.snapc, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, CompareAndWrite) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_get_object_size(mock_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
    expect_is_lock_owner(mock_exclusive_lock);
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  bufferlist cmp_bl;
  cmp_bl.append_zero(4096);

  bufferlist bl;
  bl.append(std::string(4096, '1'));

  InSequence seq;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 0, 0);
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_EXISTS, {}, false, 0);
  expect_cmpext(mock_image_ctx, 0, 0);
  expect_write(mock_image_ctx, 0, 4096, 0);

  C_SaferCond ctx;
  uint64_t mismatch_offset;
  auto req = MockObjectWriteSameRequest::create_compare_and_write(
    &mock_image_ctx, 0, 0, std::move(cmp_bl), std::move(bl),
    mock_image_ctx.snapc, &mismatch_offset, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, CompareAndWriteFull) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_get_object_size(mock_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
    expect_is_lock_owner(mock_exclusive_lock);
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  bufferlist cmp_bl;
  cmp_bl.append_zero(ictx->get_object_size());

  bufferlist bl;
  bl.append(std::string(ictx->get_object_size(), '1'));

  InSequence seq;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 0, 0);
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_EXISTS, {}, false, 0);
  expect_cmpext(mock_image_ctx, 0, 0);
  expect_write_full(mock_image_ctx, 0);

  C_SaferCond ctx;
  uint64_t mismatch_offset;
  auto req = MockObjectWriteSameRequest::create_compare_and_write(
    &mock_image_ctx, 0, 0, std::move(cmp_bl), std::move(bl),
    mock_image_ctx.snapc, &mismatch_offset, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, CompareAndWriteCopyup) {
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

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_get_object_size(mock_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
    expect_is_lock_owner(mock_exclusive_lock);
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  bufferlist cmp_bl;
  cmp_bl.append_zero(4096);

  bufferlist bl;
  bl.append(std::string(4096, '1'));

  InSequence seq;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 4096, 0);
  expect_prune_parent_extents(mock_image_ctx, {{0, 4096}}, 4096, 4096);
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_EXISTS, {}, false, 0);
  expect_assert_exists(mock_image_ctx, -ENOENT);

  MockAbstractObjectWriteRequest *write_request = nullptr;
  MockCopyupRequest mock_copyup_request;
  expect_copyup(mock_copyup_request, &write_request, 0);

  expect_assert_exists(mock_image_ctx, 0);
  expect_cmpext(mock_image_ctx, 0, 0);
  expect_write(mock_image_ctx, 0, 4096, 0);

  C_SaferCond ctx;
  uint64_t mismatch_offset;
  auto req = MockObjectWriteSameRequest::create_compare_and_write(
    &mock_image_ctx, 0, 0, std::move(cmp_bl), std::move(bl),
    mock_image_ctx.snapc, &mismatch_offset, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, CompareAndWriteMismatch) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_get_object_size(mock_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
    expect_is_lock_owner(mock_exclusive_lock);
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  bufferlist cmp_bl;
  cmp_bl.append_zero(4096);

  bufferlist bl;
  bl.append(std::string(4096, '1'));

  InSequence seq;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 0, 0);
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_EXISTS, {}, false, 0);
  expect_cmpext(mock_image_ctx, 0, -MAX_ERRNO - 1);

  C_SaferCond ctx;
  uint64_t mismatch_offset;
  auto req = MockObjectWriteSameRequest::create_compare_and_write(
    &mock_image_ctx, 0, 0, std::move(cmp_bl), std::move(bl),
    mock_image_ctx.snapc, &mismatch_offset, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(-EILSEQ, ctx.wait());
  ASSERT_EQ(1ULL, mismatch_offset);
}

TEST_F(TestMockIoObjectRequest, ObjectMapError) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);
  expect_get_object_size(mock_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_is_lock_owner(mock_exclusive_lock);

  MockObjectMap mock_object_map;
  mock_image_ctx.object_map = &mock_object_map;

  bufferlist bl;
  bl.append(std::string(4096, '1'));

  InSequence seq;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 0, 0);
  expect_object_may_exist(mock_image_ctx, 0, true);
  expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_EXISTS, {}, true,
                           -EBLACKLISTED);

  C_SaferCond ctx;
  auto req = MockObjectWriteRequest::create_write(
    &mock_image_ctx, 0, 0, std::move(bl), mock_image_ctx.snapc, 0, {}, &ctx);
  req->send();
  ASSERT_EQ(-EBLACKLISTED, ctx.wait());
}

} // namespace io
} // namespace librbd

