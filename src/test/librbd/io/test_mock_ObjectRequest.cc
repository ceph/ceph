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
#include "librbd/io/Utils.h"

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
  MOCK_METHOD2(append_request, void(AbstractObjectWriteRequest<librbd::MockTestImageCtx>*,
                                    const Extents&));
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

CopyupRequest<librbd::MockTestImageCtx>* CopyupRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

template <>
struct ImageListSnapsRequest<librbd::MockTestImageCtx> {
  static ImageListSnapsRequest* s_instance;

  AioCompletion* aio_comp;
  Extents image_extents;
  SnapshotDelta* snapshot_delta;

  ImageListSnapsRequest() {
    s_instance = this;
  }
  ImageListSnapsRequest(
      librbd::MockImageCtx& image_ctx, AioCompletion* aio_comp,
      Extents&& image_extents, SnapIds&& snap_ids, int list_snaps_flags,
      SnapshotDelta* snapshot_delta, const ZTracer::Trace& parent_trace) {
    ceph_assert(s_instance != nullptr);
    s_instance->aio_comp = aio_comp;
    s_instance->image_extents = image_extents;
    s_instance->snapshot_delta = snapshot_delta;
  }


  MOCK_METHOD0(execute_send, void());
  void send() {
    ceph_assert(s_instance != nullptr);
    s_instance->execute_send();
  }
};

ImageListSnapsRequest<librbd::MockTestImageCtx>* ImageListSnapsRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

namespace util {

template <> void file_to_extents(
        MockTestImageCtx* image_ctx, uint64_t offset, uint64_t length,
        uint64_t buffer_offset,
        striper::LightweightObjectExtents* object_extents) {
  Striper::file_to_extents(image_ctx->cct, &image_ctx->layout, offset, length,
                           0, buffer_offset, object_extents);
}

template <> void extent_to_file(
        MockTestImageCtx* image_ctx, uint64_t object_no, uint64_t offset,
        uint64_t length,
        std::vector<std::pair<uint64_t, uint64_t> >& extents) {
  Striper::extent_to_file(image_ctx->cct, &image_ctx->layout, object_no,
                          offset, length, extents);
}

namespace {

struct Mock {
  static Mock* s_instance;

  Mock() {
    s_instance = this;
  }

  MOCK_METHOD6(read_parent,
               void(librbd::MockTestImageCtx *, uint64_t, ReadExtents*,
                    librados::snap_t, const ZTracer::Trace &, Context*));
};

Mock *Mock::s_instance = nullptr;

} // anonymous namespace

template<> void read_parent(
    librbd::MockTestImageCtx *image_ctx, uint64_t object_no,
    ReadExtents* extents, librados::snap_t snap_id,
    const ZTracer::Trace &trace, Context* on_finish) {
  Mock::s_instance->read_parent(image_ctx, object_no, extents, snap_id, trace,
                                on_finish);
}

} // namespace util

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
  typedef ObjectListSnapsRequest<librbd::MockTestImageCtx> MockObjectListSnapsRequest;
  typedef AbstractObjectWriteRequest<librbd::MockTestImageCtx> MockAbstractObjectWriteRequest;
  typedef CopyupRequest<librbd::MockTestImageCtx> MockCopyupRequest;
  typedef ImageListSnapsRequest<librbd::MockTestImageCtx> MockImageListSnapsRequest;
  typedef util::Mock MockUtils;

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

    auto& mock_io_ctx = librados::get_mock_io_ctx(
      mock_image_ctx.rados_api, *mock_image_ctx.get_data_io_context());
    auto& expect = EXPECT_CALL(mock_io_ctx, read(oid, len, off, _, _, _));
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

    auto& mock_io_ctx = librados::get_mock_io_ctx(
      mock_image_ctx.rados_api, *mock_image_ctx.get_data_io_context());
    auto& expect = EXPECT_CALL(mock_io_ctx,
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

  void expect_read_parent(MockUtils &mock_utils, uint64_t object_no,
                          ReadExtents* extents, librados::snap_t snap_id,
                          int r) {
    EXPECT_CALL(mock_utils,
                read_parent(_, object_no, extents, snap_id, _, _))
      .WillOnce(WithArg<5>(CompleteContext(r, static_cast<asio::ContextWQ*>(nullptr))));
  }

  void expect_copyup(MockCopyupRequest& mock_copyup_request, int r) {
    EXPECT_CALL(mock_copyup_request, send())
      .WillOnce(Invoke([]() {}));
  }

  void expect_copyup(MockCopyupRequest& mock_copyup_request,
                     MockAbstractObjectWriteRequest** write_request, int r) {
    EXPECT_CALL(mock_copyup_request, append_request(_, _))
      .WillOnce(WithArg<0>(
        Invoke([write_request](MockAbstractObjectWriteRequest *req) {
            *write_request = req;
          })));
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
    auto& mock_io_ctx = librados::get_mock_io_ctx(
      mock_image_ctx.rados_api, *mock_image_ctx.get_data_io_context());
    EXPECT_CALL(mock_io_ctx, assert_exists(_, _))
      .WillOnce(Return(r));
  }

  void expect_write(MockTestImageCtx &mock_image_ctx,
                    uint64_t offset, uint64_t length, int r) {
    auto& mock_io_ctx = librados::get_mock_io_ctx(
      mock_image_ctx.rados_api, *mock_image_ctx.get_data_io_context());
    auto &expect = EXPECT_CALL(mock_io_ctx,
                               write(_, _, length, offset, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_write_full(MockTestImageCtx &mock_image_ctx, int r) {
    auto& mock_io_ctx = librados::get_mock_io_ctx(
      mock_image_ctx.rados_api, *mock_image_ctx.get_data_io_context());
    auto &expect = EXPECT_CALL(mock_io_ctx, write_full(_, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_writesame(MockTestImageCtx &mock_image_ctx,
                        uint64_t offset, uint64_t length, int r) {
    auto& mock_io_ctx = librados::get_mock_io_ctx(
      mock_image_ctx.rados_api, *mock_image_ctx.get_data_io_context());
    auto &expect = EXPECT_CALL(mock_io_ctx, writesame(_, _, length, offset, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_remove(MockTestImageCtx &mock_image_ctx, int r) {
    auto& mock_io_ctx = librados::get_mock_io_ctx(
      mock_image_ctx.rados_api, *mock_image_ctx.get_data_io_context());
    auto &expect = EXPECT_CALL(mock_io_ctx, remove(_, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_create(MockTestImageCtx &mock_image_ctx, bool exclusive) {
    auto& mock_io_ctx = librados::get_mock_io_ctx(
      mock_image_ctx.rados_api, *mock_image_ctx.get_data_io_context());
    EXPECT_CALL(mock_io_ctx, create(_, exclusive, _))
      .Times(1);
  }

  void expect_truncate(MockTestImageCtx &mock_image_ctx, int offset, int r) {
    auto& mock_io_ctx = librados::get_mock_io_ctx(
      mock_image_ctx.rados_api, *mock_image_ctx.get_data_io_context());
    auto &expect = EXPECT_CALL(mock_io_ctx, truncate(_, offset, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_zero(MockTestImageCtx &mock_image_ctx, int offset, int length,
                   int r) {
    auto& mock_io_ctx = librados::get_mock_io_ctx(
      mock_image_ctx.rados_api, *mock_image_ctx.get_data_io_context());
    auto &expect = EXPECT_CALL(mock_io_ctx, zero(_, offset, length, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_cmpext(MockTestImageCtx &mock_image_ctx, int offset, int r) {
    auto& mock_io_ctx = librados::get_mock_io_ctx(
      mock_image_ctx.rados_api, *mock_image_ctx.get_data_io_context());
    auto &expect = EXPECT_CALL(mock_io_ctx, cmpext(_, offset, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_list_snaps(MockTestImageCtx &mock_image_ctx,
                        const librados::snap_set_t& snap_set, int r) {
    auto io_context = *mock_image_ctx.get_data_io_context();
    io_context.read_snap(CEPH_SNAPDIR);
    auto& mock_io_ctx = librados::get_mock_io_ctx(mock_image_ctx.rados_api,
                                                  io_context);
    EXPECT_CALL(mock_io_ctx, list_snaps(_, _))
      .WillOnce(WithArg<1>(Invoke(
        [snap_set, r](librados::snap_set_t* out_snap_set) {
          *out_snap_set = snap_set;
          return r;
        })));
  }

  void expect_image_list_snaps(MockImageListSnapsRequest& req,
                               const Extents& image_extents,
                               const SnapshotDelta& image_snapshot_delta,
                               int r) {
    EXPECT_CALL(req, execute_send())
      .WillOnce(Invoke(
        [&req, image_extents, image_snapshot_delta, r]() {
          ASSERT_EQ(image_extents, req.image_extents);
          *req.snapshot_delta = image_snapshot_delta;

          auto aio_comp = req.aio_comp;
          aio_comp->set_request_count(1);
          aio_comp->add_request();
          aio_comp->complete_request(r);
        }));
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
  expect_read(mock_image_ctx, ictx->get_object_name(0), 8192, 4096,
              std::string(4096, '2'), 0);

  C_SaferCond ctx;
  uint64_t version;
  ReadExtents extents = {{0, 4096}, {8192, 4096}};
  auto req = MockObjectReadRequest::create(
          &mock_image_ctx, 0, &extents,
          mock_image_ctx.get_data_io_context(), 0, 0, {},
          &version, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  bufferlist expected_bl1;
  expected_bl1.append(std::string(4096, '1'));
  bufferlist expected_bl2;
  expected_bl2.append(std::string(4096, '2'));

  ASSERT_EQ(extents[0].extent_map.size(), 0);
  ASSERT_EQ(extents[1].extent_map.size(), 0);
  ASSERT_TRUE(extents[0].bl.contents_equal(expected_bl1));
  ASSERT_TRUE(extents[1].bl.contents_equal(expected_bl2));
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

  C_SaferCond ctx;

  ReadExtents extents = {{0, ictx->sparse_read_threshold_bytes}};
  auto req = MockObjectReadRequest::create(
    &mock_image_ctx, 0, &extents,
    mock_image_ctx.get_data_io_context(), 0, 0, {}, nullptr, &ctx);
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

  C_SaferCond ctx;
  ReadExtents extents = {{0, 4096}};
  auto req = MockObjectReadRequest::create(
    &mock_image_ctx, 0, &extents,
    mock_image_ctx.get_data_io_context(), 0, 0, {},
    nullptr, &ctx);
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

  MockUtils mock_utils;
  ReadExtents extents = {{0, 4096}};
  expect_read_parent(mock_utils, 0, &extents, CEPH_NOSNAP, 0);

  C_SaferCond ctx;
  auto req = MockObjectReadRequest::create(
    &mock_image_ctx, 0, &extents,
    mock_image_ctx.get_data_io_context(), 0, 0, {},
    nullptr, &ctx);
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

  MockUtils mock_utils;
  ReadExtents extents = {{0, 4096}};
  expect_read_parent(mock_utils, 0, &extents, CEPH_NOSNAP, -EPERM);

  C_SaferCond ctx;
  auto req = MockObjectReadRequest::create(
    &mock_image_ctx, 0, &extents,
    mock_image_ctx.get_data_io_context(), 0, 0, {},
    nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, SkipParentRead) {
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

  ReadExtents extents = {{0, 4096}};
  C_SaferCond ctx;
  auto req = MockObjectReadRequest::create(
    &mock_image_ctx, 0, &extents, mock_image_ctx.get_data_io_context(), 0,
    READ_FLAG_DISABLE_READ_FROM_PARENT, {}, nullptr, &ctx);
  req->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
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

  MockUtils mock_utils;
  ReadExtents extents = {{0, 4096}};
  expect_read_parent(mock_utils, 0, &extents, CEPH_NOSNAP, 0);

  MockCopyupRequest mock_copyup_request;
  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 4096, 0);
  expect_prune_parent_extents(mock_image_ctx, {{0, 4096}}, 4096, 4096);
  expect_copyup(mock_copyup_request, 0);

  C_SaferCond ctx;
  auto req = MockObjectReadRequest::create(
    &mock_image_ctx, 0, &extents,
    mock_image_ctx.get_data_io_context(), 0, 0, {},
    nullptr, &ctx);
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
    &mock_image_ctx, 0, 0, std::move(bl), mock_image_ctx.get_data_io_context(),
    0, 0, std::nullopt, {}, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, WriteWithCreateExclusiveFlag) {
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

  // exclusive create should succeed
  {
    bufferlist bl;
    bl.append(std::string(4096, '1'));

    InSequence seq;
    expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 0, 0);
    expect_object_may_exist(mock_image_ctx, 0, true);
    expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_EXISTS, {}, false,
                             0);
    expect_write(mock_image_ctx, 0, 4096, 0);

    C_SaferCond ctx;
    auto req = MockObjectWriteRequest::create_write(
            &mock_image_ctx, 0, 0, std::move(bl),
            mock_image_ctx.get_data_io_context(), 0,
            OBJECT_WRITE_FLAG_CREATE_EXCLUSIVE, std::nullopt, {}, &ctx);
    req->send();
    ASSERT_EQ(0, ctx.wait());
  }

  // exclusive create should fail since object already exists
  {
    bufferlist bl;
    bl.append(std::string(4096, '1'));

    InSequence seq;
    expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 0, 0);
    expect_object_may_exist(mock_image_ctx, 0, true);
    expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_EXISTS, {}, false,
                             0);

    C_SaferCond ctx;
    auto req = MockObjectWriteRequest::create_write(
            &mock_image_ctx, 0, 0, std::move(bl),
            mock_image_ctx.get_data_io_context(), 0,
            OBJECT_WRITE_FLAG_CREATE_EXCLUSIVE, std::nullopt, {}, &ctx);
    req->send();
    ASSERT_EQ(-EEXIST, ctx.wait());
  }
}

TEST_F(TestMockIoObjectRequest, WriteWithAssertVersion) {
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

  // write an object
  {
    bufferlist bl;
    bl.append(std::string(4096, '1'));

    InSequence seq;
    expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 0, 0);
    expect_object_may_exist(mock_image_ctx, 0, true);
    expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_EXISTS, {}, false,
                             0);
    expect_write(mock_image_ctx, 0, 4096, 0);

    C_SaferCond ctx;
    auto req = MockObjectWriteRequest::create_write(
            &mock_image_ctx, 0, 0, std::move(bl),
            mock_image_ctx.get_data_io_context(), 0, 0,
            std::nullopt, {}, &ctx);
    req->send();
    ASSERT_EQ(0, ctx.wait());
  }

  // assert version should succeed (version = 1)
  {
    bufferlist bl;
    bl.append(std::string(4096, '1'));

    InSequence seq;
    expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 0, 0);
    expect_object_may_exist(mock_image_ctx, 0, true);
    expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_EXISTS, {}, false,
                             0);
    expect_write(mock_image_ctx, 0, 4096, 0);

    C_SaferCond ctx;
    auto req = MockObjectWriteRequest::create_write(
            &mock_image_ctx, 0, 0, std::move(bl),
            mock_image_ctx.get_data_io_context(), 0, 0,
            std::make_optional(1), {}, &ctx);
    req->send();
    ASSERT_EQ(0, ctx.wait());
  }

  // assert with wrong (lower) version (version = 2)
  {
    bufferlist bl;
    bl.append(std::string(4096, '1'));

    InSequence seq;
    expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 0, 0);
    expect_object_may_exist(mock_image_ctx, 0, true);
    expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_EXISTS, {}, false,
                             0);

    C_SaferCond ctx;
    auto req = MockObjectWriteRequest::create_write(
            &mock_image_ctx, 0, 0, std::move(bl),
            mock_image_ctx.get_data_io_context(), 0, 0,
            std::make_optional(1), {}, &ctx);
    req->send();
    ASSERT_EQ(-ERANGE, ctx.wait());
  }

  // assert with wrong (higher) version (version = 2)
  {
    bufferlist bl;
    bl.append(std::string(4096, '1'));

    InSequence seq;
    expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 0, 0);
    expect_object_may_exist(mock_image_ctx, 0, true);
    expect_object_map_update(mock_image_ctx, 0, 1, OBJECT_EXISTS, {}, false,
                             0);

    C_SaferCond ctx;
    auto req = MockObjectWriteRequest::create_write(
            &mock_image_ctx, 0, 0, std::move(bl),
            mock_image_ctx.get_data_io_context(), 0, 0, std::make_optional(3),
            {}, &ctx);
    req->send();
    ASSERT_EQ(-EOVERFLOW, ctx.wait());
  }
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
    &mock_image_ctx, 0, 0, std::move(bl), mock_image_ctx.get_data_io_context(),
    0, 0, std::nullopt, {}, &ctx);
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
    &mock_image_ctx, 0, 0, std::move(bl), mock_image_ctx.get_data_io_context(),
    0, 0, std::nullopt, {}, &ctx);
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
    &mock_image_ctx, 0, 0, std::move(bl), mock_image_ctx.get_data_io_context(),
    0, 0, std::nullopt, {}, &ctx);
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
    &mock_image_ctx, 0, 0, std::move(bl), mock_image_ctx.get_data_io_context(),
    0, 0, std::nullopt, {}, &ctx);
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
    &mock_image_ctx, 0, 0, std::move(bl), mock_image_ctx.get_data_io_context(),
    0, 0, std::nullopt, {}, &ctx);
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
    &mock_image_ctx, 0, 0, std::move(bl), mock_image_ctx.get_data_io_context(),
    0, 0, std::nullopt, {}, &ctx);
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
    &mock_image_ctx, 0, 0, std::move(bl), mock_image_ctx.get_data_io_context(),
    0, 0, std::nullopt, {}, &ctx);
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
    mock_image_ctx.get_data_io_context(), 0, {}, &ctx);
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
    mock_image_ctx.get_data_io_context(),
    OBJECT_DISCARD_FLAG_DISABLE_CLONE_REMOVE, {}, &ctx);
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
    mock_image_ctx.get_data_io_context(),
    OBJECT_DISCARD_FLAG_DISABLE_CLONE_REMOVE, {}, &ctx);
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
    mock_image_ctx.get_data_io_context(), 0, {}, &ctx);
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
    &mock_image_ctx, 0, 1, 1, mock_image_ctx.get_data_io_context(), 0, {},
    &ctx);
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
    mock_image_ctx.get_data_io_context(),
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
    mock_image_ctx.get_data_io_context(),
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
    &mock_image_ctx, 0, 0, 4096, std::move(bl),
    mock_image_ctx.get_data_io_context(), 0, {}, &ctx);
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
    mock_image_ctx.get_data_io_context(), &mismatch_offset, 0, {}, &ctx);
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
    mock_image_ctx.get_data_io_context(), &mismatch_offset, 0, {}, &ctx);
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
    mock_image_ctx.get_data_io_context(), &mismatch_offset, 0, {}, &ctx);
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
    mock_image_ctx.get_data_io_context(), &mismatch_offset, 0, {}, &ctx);
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
                           -EBLOCKLISTED);

  C_SaferCond ctx;
  auto req = MockObjectWriteRequest::create_write(
    &mock_image_ctx, 0, 0, std::move(bl), mock_image_ctx.get_data_io_context(),
    0, 0, std::nullopt, {}, &ctx);
  req->send();
  ASSERT_EQ(-EBLOCKLISTED, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, ListSnaps) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  mock_image_ctx.snaps = {3, 4, 5, 6, 7};

  librados::snap_set_t snap_set;
  snap_set.seq = 6;
  librados::clone_info_t clone_info;

  clone_info.cloneid = 3;
  clone_info.snaps = {3};
  clone_info.overlap = std::vector<std::pair<uint64_t,uint64_t>>{
    {0, 4194304}};
  clone_info.size = 4194304;
  snap_set.clones.push_back(clone_info);

  clone_info.cloneid = 4;
  clone_info.snaps = {4};
  clone_info.overlap = std::vector<std::pair<uint64_t,uint64_t>>{
    {278528, 4096}, {442368, 4096}, {1859584, 4096}, {2224128, 4096},
    {2756608, 4096}, {3227648, 4096}, {3739648, 4096}, {3903488, 4096}};
  clone_info.size = 4194304;
  snap_set.clones.push_back(clone_info);

  clone_info.cloneid = 6;
  clone_info.snaps = {5, 6};
  clone_info.overlap = std::vector<std::pair<uint64_t,uint64_t>>{
    {425984, 4096}, {440320, 1024}, {1925120, 4096}, {2125824, 4096},
    {2215936, 5120}, {3067904, 4096}};
  clone_info.size = 3072000;
  snap_set.clones.push_back(clone_info);

  clone_info.cloneid = CEPH_NOSNAP;
  clone_info.snaps = {};
  clone_info.overlap = {};
  clone_info.size = 4194304;
  snap_set.clones.push_back(clone_info);

  expect_list_snaps(mock_image_ctx, snap_set, 0);

  SnapshotDelta snapshot_delta;
  C_SaferCond ctx;
  auto req = MockObjectListSnapsRequest::create(
    &mock_image_ctx, 0,
    {{440320, 1024}, {2122728, 1024}, {2220032, 2048}, {3072000, 4096}},
    {3, 4, 5, 6, 7, CEPH_NOSNAP}, 0, {}, &snapshot_delta, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  SnapshotDelta expected_snapshot_delta;
  expected_snapshot_delta[{5,6}].insert(
    440320, 1024, {SPARSE_EXTENT_STATE_DATA, 1024});
  expected_snapshot_delta[{5,6}].insert(
    2122728, 1024, {SPARSE_EXTENT_STATE_DATA, 1024});
  expected_snapshot_delta[{5,6}].insert(
    2220032, 2048, {SPARSE_EXTENT_STATE_DATA, 2048});
  expected_snapshot_delta[{7,CEPH_NOSNAP}].insert(
    2122728, 1024, {SPARSE_EXTENT_STATE_DATA, 1024});
  expected_snapshot_delta[{7,CEPH_NOSNAP}].insert(
    2221056, 1024, {SPARSE_EXTENT_STATE_DATA, 1024});
  expected_snapshot_delta[{7,CEPH_NOSNAP}].insert(
    3072000, 4096, {SPARSE_EXTENT_STATE_DATA, 4096});
  expected_snapshot_delta[{5,5}].insert(
    3072000, 4096, {SPARSE_EXTENT_STATE_ZEROED, 4096});
  ASSERT_EQ(expected_snapshot_delta, snapshot_delta);
}

TEST_F(TestMockIoObjectRequest, ListSnapsENOENT) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  expect_list_snaps(mock_image_ctx, {}, -ENOENT);

  SnapshotDelta snapshot_delta;
  C_SaferCond ctx;
  auto req = MockObjectListSnapsRequest::create(
    &mock_image_ctx, 0,
    {{440320, 1024}},
    {0, CEPH_NOSNAP}, 0, {}, &snapshot_delta, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  SnapshotDelta expected_snapshot_delta;
  expected_snapshot_delta[{0,0}].insert(
    440320, 1024, {SPARSE_EXTENT_STATE_DNE, 1024});
  ASSERT_EQ(expected_snapshot_delta, snapshot_delta);
}

TEST_F(TestMockIoObjectRequest, ListSnapsDNE) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  mock_image_ctx.snaps = {2, 3, 4};

  librados::snap_set_t snap_set;
  snap_set.seq = 6;
  librados::clone_info_t clone_info;

  clone_info.cloneid = 4;
  clone_info.snaps = {3, 4};
  clone_info.overlap = std::vector<std::pair<uint64_t,uint64_t>>{
    {0, 4194304}};
  clone_info.size = 4194304;
  snap_set.clones.push_back(clone_info);

  expect_list_snaps(mock_image_ctx, snap_set, 0);

  SnapshotDelta snapshot_delta;
  C_SaferCond ctx;
  auto req = MockObjectListSnapsRequest::create(
    &mock_image_ctx, 0,
    {{440320, 1024}},
    {2, 3, 4}, 0, {}, &snapshot_delta, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  SnapshotDelta expected_snapshot_delta;
  expected_snapshot_delta[{2,2}].insert(
    440320, 1024, {SPARSE_EXTENT_STATE_DNE, 1024});
  expected_snapshot_delta[{3,4}].insert(
    440320, 1024, {SPARSE_EXTENT_STATE_DATA, 1024});
  ASSERT_EQ(expected_snapshot_delta, snapshot_delta);
}

TEST_F(TestMockIoObjectRequest, ListSnapsEmpty) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  expect_list_snaps(mock_image_ctx, {}, 0);

  SnapshotDelta snapshot_delta;
  C_SaferCond ctx;
  auto req = MockObjectListSnapsRequest::create(
    &mock_image_ctx, 0,
    {{440320, 1024}},
    {0, CEPH_NOSNAP}, 0, {}, &snapshot_delta, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  SnapshotDelta expected_snapshot_delta;
  expected_snapshot_delta[{0,0}].insert(
    440320, 1024, {SPARSE_EXTENT_STATE_ZEROED, 1024});
  ASSERT_EQ(expected_snapshot_delta, snapshot_delta);
}

TEST_F(TestMockIoObjectRequest, ListSnapsError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  expect_list_snaps(mock_image_ctx, {}, -EPERM);

  SnapshotDelta snapshot_delta;
  C_SaferCond ctx;
  auto req = MockObjectListSnapsRequest::create(
    &mock_image_ctx, 0,
    {{440320, 1024}, {2122728, 1024}, {2220032, 2048}, {3072000, 4096}},
    {3, CEPH_NOSNAP}, 0, {}, &snapshot_delta, &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockIoObjectRequest, ListSnapsParent) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  mock_image_ctx.parent = &mock_image_ctx;

  InSequence seq;

  expect_list_snaps(mock_image_ctx, {}, -ENOENT);

  expect_get_parent_overlap(mock_image_ctx, CEPH_NOSNAP, 4096, 0);
  expect_prune_parent_extents(mock_image_ctx, {{0, 4096}}, 4096, 4096);

  MockImageListSnapsRequest mock_image_list_snaps_request;
  SnapshotDelta image_snapshot_delta;
  image_snapshot_delta[{1,6}].insert(
    0, 1024, {SPARSE_EXTENT_STATE_DATA, 1024});
  expect_image_list_snaps(mock_image_list_snaps_request,
                          {{0, 4096}}, image_snapshot_delta, 0);

  SnapshotDelta snapshot_delta;
  C_SaferCond ctx;
  auto req = MockObjectListSnapsRequest::create(
    &mock_image_ctx, 0,
    {{440320, 1024}, {2122728, 1024}, {2220032, 2048}, {3072000, 4096}},
    {0, CEPH_NOSNAP}, 0, {}, &snapshot_delta, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  SnapshotDelta expected_snapshot_delta;
  expected_snapshot_delta[{0,0}].insert(
    0, 1024, {SPARSE_EXTENT_STATE_DATA, 1024});
  ASSERT_EQ(expected_snapshot_delta, snapshot_delta);
}

TEST_F(TestMockIoObjectRequest, ListSnapsWholeObject) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  mock_image_ctx.snaps = {3};

  InSequence seq;

  librados::snap_set_t snap_set;
  snap_set.seq = 3;
  librados::clone_info_t clone_info;

  clone_info.cloneid = 3;
  clone_info.snaps = {3};
  clone_info.overlap = std::vector<std::pair<uint64_t,uint64_t>>{{0, 1}};
  clone_info.size = mock_image_ctx.layout.object_size;
  snap_set.clones.push_back(clone_info);

  clone_info.cloneid = CEPH_NOSNAP;
  clone_info.snaps = {};
  clone_info.overlap = {};
  clone_info.size = mock_image_ctx.layout.object_size;
  snap_set.clones.push_back(clone_info);

  expect_list_snaps(mock_image_ctx, snap_set, 0);

  {
    SnapshotDelta snapshot_delta;
    C_SaferCond ctx;
    auto req = MockObjectListSnapsRequest::create(
      &mock_image_ctx, 0, {{0, mock_image_ctx.layout.object_size - 1}},
      {3, CEPH_NOSNAP}, 0, {}, &snapshot_delta, &ctx);
    req->send();
    ASSERT_EQ(0, ctx.wait());

    SnapshotDelta expected_snapshot_delta;
    expected_snapshot_delta[{CEPH_NOSNAP,CEPH_NOSNAP}].insert(
      1, mock_image_ctx.layout.object_size - 2,
      {SPARSE_EXTENT_STATE_DATA, mock_image_ctx.layout.object_size - 2});
    EXPECT_EQ(expected_snapshot_delta, snapshot_delta);
  }

  expect_list_snaps(mock_image_ctx, snap_set, 0);

  {
    SnapshotDelta snapshot_delta;
    C_SaferCond ctx;
    auto req = MockObjectListSnapsRequest::create(
      &mock_image_ctx, 0, {{0, mock_image_ctx.layout.object_size - 1}},
      {3, CEPH_NOSNAP}, LIST_SNAPS_FLAG_WHOLE_OBJECT, {}, &snapshot_delta,
      &ctx);
    req->send();
    ASSERT_EQ(0, ctx.wait());

    SnapshotDelta expected_snapshot_delta;
    expected_snapshot_delta[{CEPH_NOSNAP,CEPH_NOSNAP}].insert(
      0, mock_image_ctx.layout.object_size - 1,
      {SPARSE_EXTENT_STATE_DATA, mock_image_ctx.layout.object_size - 1});
    EXPECT_EQ(expected_snapshot_delta, snapshot_delta);
  }
}

TEST_F(TestMockIoObjectRequest, ListSnapsWholeObjectEndSize) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  mock_image_ctx.snaps = {3};

  InSequence seq;

  librados::snap_set_t snap_set;
  snap_set.seq = 3;
  librados::clone_info_t clone_info;

  clone_info.cloneid = CEPH_NOSNAP;
  clone_info.snaps = {};
  clone_info.overlap = {};
  // smaller than object extent (i.e. the op) to test end_size handling
  clone_info.size = mock_image_ctx.layout.object_size - 2;
  snap_set.clones.push_back(clone_info);

  expect_list_snaps(mock_image_ctx, snap_set, 0);

  {
    SnapshotDelta snapshot_delta;
    C_SaferCond ctx;
    auto req = MockObjectListSnapsRequest::create(
      &mock_image_ctx, 0, {{0, mock_image_ctx.layout.object_size - 1}},
      {4, CEPH_NOSNAP}, 0, {}, &snapshot_delta, &ctx);
    req->send();
    ASSERT_EQ(0, ctx.wait());

    EXPECT_TRUE(snapshot_delta.empty());
  }

  expect_list_snaps(mock_image_ctx, snap_set, 0);

  {
    SnapshotDelta snapshot_delta;
    C_SaferCond ctx;
    auto req = MockObjectListSnapsRequest::create(
      &mock_image_ctx, 0, {{0, mock_image_ctx.layout.object_size - 1}},
      {4, CEPH_NOSNAP}, LIST_SNAPS_FLAG_WHOLE_OBJECT, {}, &snapshot_delta,
      &ctx);
    req->send();
    ASSERT_EQ(0, ctx.wait());

    EXPECT_TRUE(snapshot_delta.empty());
  }
}

TEST_F(TestMockIoObjectRequest, ListSnapsNoSnapsInSnapSet) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  mock_image_ctx.snaps = {3};

  InSequence seq;

  librados::snap_set_t snap_set;
  snap_set.seq = 3;
  librados::clone_info_t clone_info;

  clone_info.cloneid = 3;
  clone_info.snaps = {};
  clone_info.overlap = {};
  clone_info.size = 0;
  snap_set.clones.push_back(clone_info);

  expect_list_snaps(mock_image_ctx, snap_set, 0);

  SnapshotDelta snapshot_delta;
  C_SaferCond ctx;
  auto req = MockObjectListSnapsRequest::create(
    &mock_image_ctx, 0, {{0, mock_image_ctx.layout.object_size - 1}},
    {0, CEPH_NOSNAP}, 0, {}, &snapshot_delta, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  SnapshotDelta expected_snapshot_delta;
  expected_snapshot_delta[{CEPH_NOSNAP,CEPH_NOSNAP}].insert(
    0, mock_image_ctx.layout.object_size - 1,
    {SPARSE_EXTENT_STATE_DATA, mock_image_ctx.layout.object_size - 1});
  EXPECT_EQ(expected_snapshot_delta, snapshot_delta);
}

TEST(SparseExtents, Split) {
  SparseExtents extents;
  extents.insert(50, 100, {SPARSE_EXTENT_STATE_DATA, 100});
  extents.erase(80, 30);
  extents.insert(45, 10, {SPARSE_EXTENT_STATE_ZEROED, 10});
  extents.insert(140, 20, {SPARSE_EXTENT_STATE_DNE, 20});
  extents.insert(125, 5, {SPARSE_EXTENT_STATE_ZEROED, 5});

  SparseExtents expected_extents = {
    {45, {10, {SPARSE_EXTENT_STATE_ZEROED, 10}}},
    {55, {25, {SPARSE_EXTENT_STATE_DATA, 25}}},
    {110, {15, {SPARSE_EXTENT_STATE_DATA, 15}}},
    {125, {5, {SPARSE_EXTENT_STATE_ZEROED, 5}}},
    {130, {10, {SPARSE_EXTENT_STATE_DATA, 10}}},
    {140, {20, {SPARSE_EXTENT_STATE_DNE, 20}}}
  };
  EXPECT_EQ(expected_extents, extents);
}

TEST(SparseExtents, Merge) {
  SparseExtents extents;
  extents.insert(50, 100, {SPARSE_EXTENT_STATE_DATA, 100});
  extents.insert(30, 15, {SPARSE_EXTENT_STATE_ZEROED, 15});
  extents.insert(45, 10, {SPARSE_EXTENT_STATE_DATA, 10});
  extents.insert(200, 40, {SPARSE_EXTENT_STATE_DNE, 40});
  extents.insert(160, 25, {SPARSE_EXTENT_STATE_DNE, 25});
  extents.insert(140, 20, {SPARSE_EXTENT_STATE_DATA, 20});
  extents.insert(25, 5, {SPARSE_EXTENT_STATE_ZEROED, 5});
  extents.insert(185, 15, {SPARSE_EXTENT_STATE_DNE, 15});

  SparseExtents expected_extents = {
    {25, {20, {SPARSE_EXTENT_STATE_ZEROED, 20}}},
    {45, {115, {SPARSE_EXTENT_STATE_DATA, 115}}},
    {160, {80, {SPARSE_EXTENT_STATE_DNE, 80}}}
  };
  EXPECT_EQ(expected_extents, extents);
}

TEST(SparseBufferlist, Split) {
  bufferlist bl;
  bl.append(std::string(5, '1'));
  bl.append(std::string(25, '2'));
  bl.append(std::string(30, '3'));
  bl.append(std::string(15, '4'));
  bl.append(std::string(5, '5'));
  bl.append(std::string(10, '6'));
  bl.append(std::string(10, '7'));
  bufferlist expected_bl1;
  expected_bl1.append(std::string(25, '2'));
  bufferlist expected_bl2;
  expected_bl2.append(std::string(15, '4'));
  bufferlist expected_bl3;
  expected_bl3.append(std::string(10, '6'));

  SparseBufferlist extents;
  extents.insert(50, 100, {SPARSE_EXTENT_STATE_DATA, 100, std::move(bl)});
  extents.erase(80, 30);
  extents.insert(45, 10, {SPARSE_EXTENT_STATE_ZEROED, 10});
  extents.insert(140, 20, {SPARSE_EXTENT_STATE_DNE, 20});
  extents.insert(125, 5, {SPARSE_EXTENT_STATE_ZEROED, 5});

  SparseBufferlist expected_extents = {
    {45, {10, {SPARSE_EXTENT_STATE_ZEROED, 10}}},
    {55, {25, {SPARSE_EXTENT_STATE_DATA, 25, std::move(expected_bl1)}}},
    {110, {15, {SPARSE_EXTENT_STATE_DATA, 15, std::move(expected_bl2)}}},
    {125, {5, {SPARSE_EXTENT_STATE_ZEROED, 5}}},
    {130, {10, {SPARSE_EXTENT_STATE_DATA, 10, std::move(expected_bl3)}}},
    {140, {20, {SPARSE_EXTENT_STATE_DNE, 20}}}
  };
  EXPECT_EQ(expected_extents, extents);
}

TEST(SparseBufferlist, SplitData) {
  bufferlist bl1;
  bl1.append(std::string(100, '1'));
  bufferlist bl2;
  bl2.append(std::string(15, '2'));
  bufferlist bl3;
  bl3.append(std::string(40, '3'));
  bufferlist bl4;
  bl4.append(std::string(10, '4'));
  bufferlist expected_bl1 = bl2;
  bufferlist expected_bl2;
  expected_bl2.append(std::string(35, '1'));
  bufferlist expected_bl3 = bl4;
  bufferlist expected_bl4;
  expected_bl4.append(std::string(30, '1'));
  bufferlist expected_bl5;
  expected_bl5.append(std::string(5, '3'));
  bufferlist expected_bl6;
  expected_bl6.append(std::string(15, '3'));

  SparseBufferlist extents;
  extents.insert(50, 100, {SPARSE_EXTENT_STATE_DATA, 100, std::move(bl1)});
  extents.insert(40, 15, {SPARSE_EXTENT_STATE_DATA, 15, std::move(bl2)});
  extents.insert(130, 40, {SPARSE_EXTENT_STATE_DATA, 40, std::move(bl3)});
  extents.erase(135, 20);
  extents.insert(90, 10, {SPARSE_EXTENT_STATE_DATA, 10, std::move(bl4)});

  SparseBufferlist expected_extents = {
    {40, {15, {SPARSE_EXTENT_STATE_DATA, 15, std::move(expected_bl1)}}},
    {55, {35, {SPARSE_EXTENT_STATE_DATA, 35, std::move(expected_bl2)}}},
    {90, {10, {SPARSE_EXTENT_STATE_DATA, 10, std::move(expected_bl3)}}},
    {100, {30, {SPARSE_EXTENT_STATE_DATA, 30, std::move(expected_bl4)}}},
    {130, {5, {SPARSE_EXTENT_STATE_DATA, 5, std::move(expected_bl5)}}},
    {155, {15, {SPARSE_EXTENT_STATE_DATA, 15, std::move(expected_bl6)}}}
  };
  EXPECT_EQ(expected_extents, extents);
}

TEST(SparseBufferlist, Merge) {
  bufferlist bl1;
  bl1.append(std::string(100, '1'));
  bufferlist bl2;
  bl2.append(std::string(10, '2'));
  bufferlist bl3;
  bl3.append(std::string(20, '3'));
  bufferlist expected_bl;
  expected_bl.append(std::string(10, '2'));
  expected_bl.append(std::string(85, '1'));
  expected_bl.append(std::string(20, '3'));

  SparseBufferlist extents;
  extents.insert(50, 100, {SPARSE_EXTENT_STATE_DATA, 100, std::move(bl1)});
  extents.insert(30, 15, {SPARSE_EXTENT_STATE_ZEROED, 15});
  extents.insert(45, 10, {SPARSE_EXTENT_STATE_DATA, 10, std::move(bl2)});
  extents.insert(200, 40, {SPARSE_EXTENT_STATE_DNE, 40});
  extents.insert(160, 25, {SPARSE_EXTENT_STATE_DNE, 25});
  extents.insert(140, 20, {SPARSE_EXTENT_STATE_DATA, 20, std::move(bl3)});
  extents.insert(25, 5, {SPARSE_EXTENT_STATE_ZEROED, 5});
  extents.insert(185, 15, {SPARSE_EXTENT_STATE_DNE, 15});

  SparseBufferlist expected_extents = {
    {25, {20, {SPARSE_EXTENT_STATE_ZEROED, 20}}},
    {45, {115, {SPARSE_EXTENT_STATE_DATA, 115, std::move(expected_bl)}}},
    {160, {80, {SPARSE_EXTENT_STATE_DNE, 80}}}
  };
  EXPECT_EQ(expected_extents, extents);
}

} // namespace io
} // namespace librbd

