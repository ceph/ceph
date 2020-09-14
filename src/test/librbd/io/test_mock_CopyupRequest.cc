// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockExclusiveLock.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockJournal.h"
#include "test/librbd/mock/MockObjectMap.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "include/rbd/librbd.hpp"
#include "librbd/api/Io.h"
#include "librbd/deep_copy/ObjectCopyRequest.h"
#include "librbd/io/CopyupRequest.h"
#include "librbd/io/ImageRequest.h"
#include "librbd/io/ObjectRequest.h"
#include "librbd/io/ReadResult.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx,
                   MockTestImageCtx* mock_parent_image_ctx = nullptr)
    : MockImageCtx(image_ctx) {
    parent = mock_parent_image_ctx;
  }
  ~MockTestImageCtx() override {
    // copyups need to complete prior to attempting to delete this object
    wait_for_async_ops();
  }

  std::map<uint64_t, librbd::io::CopyupRequest<librbd::MockTestImageCtx>*> copyup_list;
};

} // anonymous namespace

namespace util {

inline ImageCtx *get_image_ctx(MockImageCtx *image_ctx) {
  return image_ctx->image_ctx;
}

} // namespace util

namespace deep_copy {

template <>
struct ObjectCopyRequest<librbd::MockTestImageCtx> {
  static ObjectCopyRequest* s_instance;
  static ObjectCopyRequest* create(librbd::MockImageCtx* parent_image_ctx,
                                   librbd::MockTestImageCtx* image_ctx,
                                   librados::snap_t src_snap_id_start,
                                   librados::snap_t dst_snap_id_start,
                                   const SnapMap &snap_map,
                                   uint64_t object_number, bool flatten,
                                   Handler*, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->object_number = object_number;
    s_instance->flatten = flatten;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  uint64_t object_number;
  bool flatten;
  Context *on_finish;

  ObjectCopyRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

ObjectCopyRequest<librbd::MockTestImageCtx>* ObjectCopyRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace deep_copy

namespace io {

template <>
struct ObjectRequest<librbd::MockTestImageCtx> {
  static void add_write_hint(librbd::MockTestImageCtx&,
                             neorados::WriteOp*) {
  }
};

template <>
struct AbstractObjectWriteRequest<librbd::MockTestImageCtx> {
  C_SaferCond ctx;
  void handle_copyup(int r) {
    ctx.complete(r);
  }

  MOCK_CONST_METHOD0(get_pre_write_object_map_state, uint8_t());
  MOCK_CONST_METHOD0(is_empty_write_op, bool());

  MOCK_METHOD1(add_copyup_ops, void(neorados::WriteOp*));
};

template <>
struct ImageRequest<librbd::MockTestImageCtx> {
  static ImageRequest *s_instance;
  static void aio_read(librbd::MockImageCtx *ictx, AioCompletion *c,
                       Extents &&image_extents, ReadResult &&read_result,
                       int op_flags, const ZTracer::Trace &parent_trace) {
    s_instance->aio_read(c, image_extents, &read_result);
  }

  MOCK_METHOD3(aio_read, void(AioCompletion *, const Extents&, ReadResult*));

  ImageRequest() {
    s_instance = this;
  }
};

ImageRequest<librbd::MockTestImageCtx>* ImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace io
} // namespace librbd

static bool operator==(const SnapContext& rhs, const SnapContext& lhs) {
  return (rhs.seq == lhs.seq && rhs.snaps == lhs.snaps);
}

#include "librbd/AsyncObjectThrottle.cc"
#include "librbd/io/CopyupRequest.cc"

namespace librbd {
namespace io {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;
using ::testing::WithArgs;
using ::testing::WithoutArgs;

struct TestMockIoCopyupRequest : public TestMockFixture {
  typedef CopyupRequest<librbd::MockTestImageCtx> MockCopyupRequest;
  typedef ImageRequest<librbd::MockTestImageCtx> MockImageRequest;
  typedef ObjectRequest<librbd::MockTestImageCtx> MockObjectRequest;
  typedef AbstractObjectWriteRequest<librbd::MockTestImageCtx> MockAbstractObjectWriteRequest;
  typedef deep_copy::ObjectCopyRequest<librbd::MockTestImageCtx> MockObjectCopyRequest;

  void SetUp() override {
    TestMockFixture::SetUp();
    if (!is_feature_enabled(RBD_FEATURE_LAYERING)) {
      return;
    }

    m_parent_image_name = m_image_name;
    m_image_name = get_temp_image_name();

    librbd::Image image;
    librbd::RBD rbd;
    ASSERT_EQ(0, rbd.open(m_ioctx, image, m_parent_image_name.c_str(),
                          nullptr));
    ASSERT_EQ(0, image.snap_create("one"));
    ASSERT_EQ(0, image.snap_protect("one"));

    uint64_t features;
    ASSERT_EQ(0, image.features(&features));
    image.close();

    int order = 0;
    ASSERT_EQ(0, rbd.clone(m_ioctx, m_parent_image_name.c_str(), "one", m_ioctx,
                           m_image_name.c_str(), features, &order));
  }

  void expect_get_parent_overlap(MockTestImageCtx& mock_image_ctx,
                                 librados::snap_t snap_id, uint64_t overlap,
                                 int r) {
    EXPECT_CALL(mock_image_ctx, get_parent_overlap(snap_id, _))
      .WillOnce(WithArg<1>(Invoke([overlap, r](uint64_t *o) {
                             *o = overlap;
                             return r;
                           })));
  }

  void expect_prune_parent_extents(MockTestImageCtx& mock_image_ctx,
                                   uint64_t overlap, uint64_t object_overlap) {
    EXPECT_CALL(mock_image_ctx, prune_parent_extents(_, overlap))
      .WillOnce(WithoutArgs(Invoke([object_overlap]() {
                              return object_overlap;
                            })));
  }

  void expect_read_parent(MockTestImageCtx& mock_image_ctx,
                          MockImageRequest& mock_image_request,
                          const Extents& image_extents,
                          const std::string& data, int r) {
    EXPECT_CALL(mock_image_request, aio_read(_, image_extents, _))
      .WillOnce(WithArgs<0, 2>(Invoke(
        [&mock_image_ctx, image_extents, data, r](
            AioCompletion* aio_comp, ReadResult* read_result) {
          aio_comp->read_result = std::move(*read_result);
          aio_comp->set_request_count(1);
          auto ctx = new ReadResult::C_ImageReadRequest(aio_comp,
                                                        image_extents);
          ctx->bl.append(data);
          mock_image_ctx.image_ctx->op_work_queue->queue(ctx, r);
        })));
  }

  void expect_copyup(MockTestImageCtx& mock_image_ctx, uint64_t snap_id,
                     const std::string& oid, const std::string& data, int r) {
    bufferlist in_bl;
    in_bl.append(data);

    SnapContext snapc;
    if (snap_id == CEPH_NOSNAP) {
      snapc = mock_image_ctx.snapc;
    }

    auto& mock_io_ctx = librados::get_mock_io_ctx(
      mock_image_ctx.rados_api, *mock_image_ctx.get_data_io_context());
    EXPECT_CALL(mock_io_ctx,
                exec(oid, _, StrEq("rbd"), StrEq("copyup"),
                     ContentsEqual(in_bl), _, _, snapc))
      .WillOnce(Return(r));
  }

  void expect_sparse_copyup(MockTestImageCtx &mock_image_ctx, uint64_t snap_id,
                            const std::string &oid,
                            const std::map<uint64_t, uint64_t> &extent_map,
                            const std::string &data, int r) {
    bufferlist data_bl;
    data_bl.append(data);

    bufferlist in_bl;
    encode(extent_map, in_bl);
    encode(data_bl, in_bl);

    SnapContext snapc;
    if (snap_id == CEPH_NOSNAP) {
      snapc = mock_image_ctx.snapc;
    }

    auto& mock_io_ctx = librados::get_mock_io_ctx(
      mock_image_ctx.rados_api, *mock_image_ctx.get_data_io_context());
    EXPECT_CALL(mock_io_ctx,
                exec(oid, _, StrEq("rbd"), StrEq("sparse_copyup"),
                     ContentsEqual(in_bl), _, _, snapc))
      .WillOnce(Return(r));
  }

  void expect_write(MockTestImageCtx& mock_image_ctx, uint64_t snap_id,
                    const std::string& oid, int r) {
    SnapContext snapc;
    if (snap_id == CEPH_NOSNAP) {
      snapc = mock_image_ctx.snapc;
    }

    auto& mock_io_ctx = librados::get_mock_io_ctx(
      mock_image_ctx.rados_api, *mock_image_ctx.get_data_io_context());
    EXPECT_CALL(mock_io_ctx, write(oid, _, 0, 0, snapc))
      .WillOnce(Return(r));
  }

  void expect_test_features(MockTestImageCtx& mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, test_features(_, _))
      .WillRepeatedly(WithArg<0>(Invoke([&mock_image_ctx](uint64_t features) {
              return (mock_image_ctx.features & features) != 0;
            })));
  }

  void expect_is_lock_owner(MockTestImageCtx& mock_image_ctx) {
    if (mock_image_ctx.exclusive_lock != nullptr) {
      EXPECT_CALL(*mock_image_ctx.exclusive_lock,
                  is_lock_owner()).WillRepeatedly(Return(true));
    }
  }

  void expect_is_empty_write_op(MockAbstractObjectWriteRequest& mock_write_request,
                                bool is_empty) {
    EXPECT_CALL(mock_write_request, is_empty_write_op())
      .WillOnce(Return(is_empty));
  }

  void expect_add_copyup_ops(MockAbstractObjectWriteRequest& mock_write_request) {
    EXPECT_CALL(mock_write_request, add_copyup_ops(_))
      .WillOnce(Invoke([](neorados::WriteOp* op) {
                  op->write(0, bufferlist{});
                }));
  }

  void expect_get_pre_write_object_map_state(MockTestImageCtx& mock_image_ctx,
                                             MockAbstractObjectWriteRequest& mock_write_request,
                                             uint8_t state) {
    if (mock_image_ctx.object_map != nullptr) {
      EXPECT_CALL(mock_write_request, get_pre_write_object_map_state())
        .WillOnce(Return(state));
    }
  }

  void expect_object_map_at(MockTestImageCtx& mock_image_ctx,
                            uint64_t object_no, uint8_t state) {
    if (mock_image_ctx.object_map != nullptr) {
      EXPECT_CALL(*mock_image_ctx.object_map, at(object_no))
        .WillOnce(Return(state));
    }
  }

  void expect_object_map_update(MockTestImageCtx& mock_image_ctx,
                                uint64_t snap_id, uint64_t object_no,
                                uint8_t state, bool updated, int ret_val) {
    if (mock_image_ctx.object_map != nullptr) {
      if (!mock_image_ctx.image_ctx->test_features(RBD_FEATURE_FAST_DIFF) &&
          state == OBJECT_EXISTS_CLEAN) {
        state = OBJECT_EXISTS;
      }

      EXPECT_CALL(*mock_image_ctx.object_map,
                  aio_update(snap_id, object_no, object_no + 1, state,
                             boost::optional<uint8_t>(), _,
                             (snap_id != CEPH_NOSNAP), _))
        .WillOnce(WithArg<7>(Invoke([&mock_image_ctx, updated, ret_val](Context *ctx) {
                               if (updated) {
                                 mock_image_ctx.op_work_queue->queue(ctx, ret_val);
                               }
                               return updated;
                             })));
    }
  }

  void expect_object_copy(MockTestImageCtx& mock_image_ctx,
                          MockObjectCopyRequest& mock_object_copy_request,
                          bool flatten, int r) {
    EXPECT_CALL(mock_object_copy_request, send())
      .WillOnce(Invoke(
        [&mock_image_ctx, &mock_object_copy_request, flatten, r]() {
          ASSERT_EQ(flatten, mock_object_copy_request.flatten);
          mock_image_ctx.op_work_queue->queue(
            mock_object_copy_request.on_finish, r);
        }));
  }

  void flush_async_operations(librbd::ImageCtx* ictx) {
    api::Io<>::flush(*ictx);
  }

  std::string m_parent_image_name;
};

TEST_F(TestMockIoCopyupRequest, Standard) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_parent_image_ctx(*ictx->parent);
  MockTestImageCtx mock_image_ctx(*ictx, &mock_parent_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  expect_op_work_queue(mock_image_ctx);
  expect_is_lock_owner(mock_image_ctx);

  InSequence seq;

  MockImageRequest mock_image_request;
  std::string data(4096, '1');
  expect_read_parent(mock_parent_image_ctx, mock_image_request, {{0, 4096}},
                     data, 0);

  MockAbstractObjectWriteRequest mock_write_request;
  expect_get_pre_write_object_map_state(mock_image_ctx, mock_write_request,
                                        OBJECT_EXISTS);
  expect_object_map_at(mock_image_ctx, 0, OBJECT_NONEXISTENT);
  expect_object_map_update(mock_image_ctx, CEPH_NOSNAP, 0, OBJECT_EXISTS, true,
                           0);

  expect_add_copyup_ops(mock_write_request);
  expect_sparse_copyup(mock_image_ctx, CEPH_NOSNAP, ictx->get_object_name(0),
                       {{0, 4096}}, data, 0);
  expect_write(mock_image_ctx, CEPH_NOSNAP, ictx->get_object_name(0), 0);

  auto req = new MockCopyupRequest(&mock_image_ctx, 0,
                                   {{0, 4096}}, {});
  mock_image_ctx.copyup_list[0] = req;
  req->append_request(&mock_write_request);
  req->send();

  ASSERT_EQ(0, mock_write_request.ctx.wait());
}

TEST_F(TestMockIoCopyupRequest, StandardWithSnaps) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ictx->image_lock.lock();
  ictx->add_snap(cls::rbd::UserSnapshotNamespace(), "2", 2, ictx->size,
                 ictx->parent_md, RBD_PROTECTION_STATUS_UNPROTECTED,
                 0, {});
  ictx->add_snap(cls::rbd::UserSnapshotNamespace(), "1", 1, ictx->size,
                 ictx->parent_md, RBD_PROTECTION_STATUS_UNPROTECTED,
                 0, {});
  ictx->snapc = {2, {2, 1}};
  ictx->image_lock.unlock();

  MockTestImageCtx mock_parent_image_ctx(*ictx->parent);
  MockTestImageCtx mock_image_ctx(*ictx, &mock_parent_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  expect_test_features(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);
  expect_is_lock_owner(mock_image_ctx);

  InSequence seq;

  MockImageRequest mock_image_request;
  std::string data(4096, '1');
  expect_read_parent(mock_parent_image_ctx, mock_image_request, {{0, 4096}},
                     data, 0);

  MockAbstractObjectWriteRequest mock_write_request;
  expect_get_pre_write_object_map_state(mock_image_ctx, mock_write_request,
                                        OBJECT_EXISTS);
  expect_object_map_at(mock_image_ctx, 0, OBJECT_NONEXISTENT);
  expect_object_map_update(mock_image_ctx, 1, 0, OBJECT_EXISTS, true, 0);
  expect_object_map_update(mock_image_ctx, 2, 0, OBJECT_EXISTS_CLEAN, true, 0);
  expect_object_map_update(mock_image_ctx, CEPH_NOSNAP, 0, OBJECT_EXISTS, true,
                           0);

  expect_add_copyup_ops(mock_write_request);
  expect_sparse_copyup(mock_image_ctx, 0, ictx->get_object_name(0), {{0, 4096}},
                       data, 0);
  expect_write(mock_image_ctx, CEPH_NOSNAP, ictx->get_object_name(0), 0);

  auto req = new MockCopyupRequest(&mock_image_ctx, 0,
                                   {{0, 4096}}, {});
  mock_image_ctx.copyup_list[0] = req;
  req->append_request(&mock_write_request);
  req->send();

  ASSERT_EQ(0, mock_write_request.ctx.wait());
}

TEST_F(TestMockIoCopyupRequest, CopyOnRead) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_parent_image_ctx(*ictx->parent);
  MockTestImageCtx mock_image_ctx(*ictx, &mock_parent_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  expect_op_work_queue(mock_image_ctx);
  expect_is_lock_owner(mock_image_ctx);

  InSequence seq;

  MockImageRequest mock_image_request;
  std::string data(4096, '1');
  expect_read_parent(mock_parent_image_ctx, mock_image_request, {{0, 4096}},
                     data, 0);

  expect_object_map_at(mock_image_ctx, 0, OBJECT_NONEXISTENT);
  expect_object_map_update(mock_image_ctx, CEPH_NOSNAP, 0, OBJECT_EXISTS, true,
                           0);

  expect_sparse_copyup(mock_image_ctx, CEPH_NOSNAP, ictx->get_object_name(0),
                       {{0, 4096}}, data, 0);

  auto req = new MockCopyupRequest(&mock_image_ctx, 0,
                                   {{0, 4096}}, {});
  mock_image_ctx.copyup_list[0] = req;
  req->send();
  flush_async_operations(ictx);
}

TEST_F(TestMockIoCopyupRequest, CopyOnReadWithSnaps) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ictx->image_lock.lock();
  ictx->add_snap(cls::rbd::UserSnapshotNamespace(), "1", 1, ictx->size,
                 ictx->parent_md, RBD_PROTECTION_STATUS_UNPROTECTED,
                 0, {});
  ictx->snapc = {1, {1}};
  ictx->image_lock.unlock();

  MockTestImageCtx mock_parent_image_ctx(*ictx->parent);
  MockTestImageCtx mock_image_ctx(*ictx, &mock_parent_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  expect_test_features(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);
  expect_is_lock_owner(mock_image_ctx);

  InSequence seq;

  MockImageRequest mock_image_request;
  std::string data(4096, '1');
  expect_read_parent(mock_parent_image_ctx, mock_image_request, {{0, 4096}},
                     data, 0);

  expect_object_map_at(mock_image_ctx, 0, OBJECT_NONEXISTENT);
  expect_object_map_update(mock_image_ctx, 1, 0, OBJECT_EXISTS, true, 0);
  expect_object_map_update(mock_image_ctx, CEPH_NOSNAP, 0, OBJECT_EXISTS_CLEAN,
                           true, 0);

  expect_sparse_copyup(mock_image_ctx, 0, ictx->get_object_name(0), {{0, 4096}},
                       data, 0);

  auto req = new MockCopyupRequest(&mock_image_ctx, 0,
                                   {{0, 4096}}, {});
  mock_image_ctx.copyup_list[0] = req;
  req->send();
  flush_async_operations(ictx);
}

TEST_F(TestMockIoCopyupRequest, DeepCopy) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_parent_image_ctx(*ictx->parent);
  MockTestImageCtx mock_image_ctx(*ictx, &mock_parent_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  expect_op_work_queue(mock_image_ctx);
  expect_is_lock_owner(mock_image_ctx);

  InSequence seq;

  MockAbstractObjectWriteRequest mock_write_request;
  MockObjectCopyRequest mock_object_copy_request;
  mock_image_ctx.migration_info = {1, "", "", "image id", {}, ictx->size, true};
  expect_is_empty_write_op(mock_write_request, false);
  expect_object_copy(mock_image_ctx, mock_object_copy_request, true, 0);

  expect_is_empty_write_op(mock_write_request, false);
  expect_get_pre_write_object_map_state(mock_image_ctx, mock_write_request,
                                        OBJECT_EXISTS);
  expect_object_map_at(mock_image_ctx, 0, OBJECT_NONEXISTENT);
  expect_object_map_update(mock_image_ctx, CEPH_NOSNAP, 0, OBJECT_EXISTS, true,
                           0);

  expect_add_copyup_ops(mock_write_request);
  expect_sparse_copyup(mock_image_ctx, CEPH_NOSNAP, ictx->get_object_name(0),
                       {}, "", 0);
  expect_write(mock_image_ctx, CEPH_NOSNAP, ictx->get_object_name(0), 0);

  auto req = new MockCopyupRequest(&mock_image_ctx, 0,
                                   {{0, 4096}}, {});
  mock_image_ctx.copyup_list[0] = req;
  req->append_request(&mock_write_request);
  req->send();

  ASSERT_EQ(0, mock_write_request.ctx.wait());
}

TEST_F(TestMockIoCopyupRequest, DeepCopyOnRead) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_parent_image_ctx(*ictx->parent);
  MockTestImageCtx mock_image_ctx(*ictx, &mock_parent_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  expect_op_work_queue(mock_image_ctx);
  expect_is_lock_owner(mock_image_ctx);

  InSequence seq;

  MockObjectCopyRequest mock_object_copy_request;
  mock_image_ctx.migration_info = {1, "", "", "image id", {}, ictx->size,
                                   false};
  expect_object_copy(mock_image_ctx, mock_object_copy_request, true, 0);

  expect_object_map_at(mock_image_ctx, 0, OBJECT_NONEXISTENT);
  expect_object_map_update(mock_image_ctx, CEPH_NOSNAP, 0, OBJECT_EXISTS, true,
                           0);

  expect_sparse_copyup(mock_image_ctx, CEPH_NOSNAP, ictx->get_object_name(0),
                       {}, "", 0);

  auto req = new MockCopyupRequest(&mock_image_ctx, 0,
                                   {{0, 4096}}, {});
  mock_image_ctx.copyup_list[0] = req;
  req->send();
  flush_async_operations(ictx);
}

TEST_F(TestMockIoCopyupRequest, DeepCopyWithPostSnaps) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ictx->image_lock.lock();
  ictx->add_snap(cls::rbd::UserSnapshotNamespace(), "3", 3, ictx->size,
                 ictx->parent_md, RBD_PROTECTION_STATUS_UNPROTECTED,
                 0, {});
  ictx->add_snap(cls::rbd::UserSnapshotNamespace(), "2", 2, ictx->size,
                 ictx->parent_md, RBD_PROTECTION_STATUS_UNPROTECTED,
                 0, {});
  ictx->add_snap(cls::rbd::UserSnapshotNamespace(), "1", 1, ictx->size,
                 ictx->parent_md, RBD_PROTECTION_STATUS_UNPROTECTED,
                 0, {});
  ictx->snapc = {3, {3, 2, 1}};
  ictx->image_lock.unlock();

  MockTestImageCtx mock_parent_image_ctx(*ictx->parent);
  MockTestImageCtx mock_image_ctx(*ictx, &mock_parent_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  expect_test_features(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);
  expect_is_lock_owner(mock_image_ctx);

  InSequence seq;

  MockAbstractObjectWriteRequest mock_write_request;
  MockObjectCopyRequest mock_object_copy_request;
  mock_image_ctx.migration_info = {1, "", "", "image id",
                                   {{CEPH_NOSNAP, {2, 1}}},
                                   ictx->size, true};
  expect_is_empty_write_op(mock_write_request, false);
  expect_object_copy(mock_image_ctx, mock_object_copy_request, true, 0);

  expect_is_empty_write_op(mock_write_request, false);
  expect_get_parent_overlap(mock_image_ctx, 1, 0, 0);
  expect_get_parent_overlap(mock_image_ctx, 2, 1, 0);
  expect_prune_parent_extents(mock_image_ctx, 1, 1);
  expect_get_parent_overlap(mock_image_ctx, 3, 1, 0);
  expect_prune_parent_extents(mock_image_ctx, 1, 1);
  expect_get_pre_write_object_map_state(mock_image_ctx, mock_write_request,
                                        OBJECT_EXISTS);
  expect_object_map_at(mock_image_ctx, 0, OBJECT_NONEXISTENT);
  expect_object_map_update(mock_image_ctx, 2, 0, OBJECT_EXISTS, true, 0);
  expect_object_map_update(mock_image_ctx, 3, 0, OBJECT_EXISTS_CLEAN, true, 0);
  expect_object_map_update(mock_image_ctx, CEPH_NOSNAP, 0, OBJECT_EXISTS, true,
                           0);

  expect_add_copyup_ops(mock_write_request);
  expect_sparse_copyup(mock_image_ctx, CEPH_NOSNAP, ictx->get_object_name(0),
                       {}, "", 0);
  expect_write(mock_image_ctx, CEPH_NOSNAP, ictx->get_object_name(0), 0);

  auto req = new MockCopyupRequest(&mock_image_ctx, 0,
                                   {{0, 4096}}, {});
  mock_image_ctx.copyup_list[0] = req;
  req->append_request(&mock_write_request);
  req->send();

  ASSERT_EQ(0, mock_write_request.ctx.wait());
}

TEST_F(TestMockIoCopyupRequest, DeepCopyWithPreAndPostSnaps) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ictx->image_lock.lock();
  ictx->add_snap(cls::rbd::UserSnapshotNamespace(), "4", 4, ictx->size,
                 ictx->parent_md, RBD_PROTECTION_STATUS_UNPROTECTED,
                 0, {});
  ictx->add_snap(cls::rbd::UserSnapshotNamespace(), "3", 3, ictx->size,
                 ictx->parent_md, RBD_PROTECTION_STATUS_UNPROTECTED,
                 0, {});
  ictx->add_snap(cls::rbd::UserSnapshotNamespace(), "2", 2, ictx->size,
                 ictx->parent_md, RBD_PROTECTION_STATUS_UNPROTECTED,
                 0, {});
  ictx->add_snap(cls::rbd::UserSnapshotNamespace(), "1", 1, ictx->size,
                 ictx->parent_md, RBD_PROTECTION_STATUS_UNPROTECTED,
                 0, {});
  ictx->snapc = {4, {4, 3, 2, 1}};
  ictx->image_lock.unlock();

  MockTestImageCtx mock_parent_image_ctx(*ictx->parent);
  MockTestImageCtx mock_image_ctx(*ictx, &mock_parent_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  expect_test_features(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);
  expect_is_lock_owner(mock_image_ctx);

  InSequence seq;

  MockAbstractObjectWriteRequest mock_write_request;
  MockObjectCopyRequest mock_object_copy_request;
  mock_image_ctx.migration_info = {1, "", "", "image id",
                                   {{CEPH_NOSNAP, {2, 1}}, {10, {1}}},
                                   ictx->size, true};
  expect_is_empty_write_op(mock_write_request, false);
  expect_object_copy(mock_image_ctx, mock_object_copy_request, true, 0);

  expect_is_empty_write_op(mock_write_request, false);
  expect_get_parent_overlap(mock_image_ctx, 2, 0, 0);
  expect_get_parent_overlap(mock_image_ctx, 3, 1, 0);
  expect_prune_parent_extents(mock_image_ctx, 1, 1);
  expect_get_parent_overlap(mock_image_ctx, 4, 1, 0);
  expect_prune_parent_extents(mock_image_ctx, 1, 1);
  expect_get_pre_write_object_map_state(mock_image_ctx, mock_write_request,
                                        OBJECT_EXISTS);
  expect_object_map_at(mock_image_ctx, 0, OBJECT_NONEXISTENT);
  expect_object_map_update(mock_image_ctx, 3, 0, OBJECT_EXISTS_CLEAN, true, 0);
  expect_object_map_update(mock_image_ctx, 4, 0, OBJECT_EXISTS_CLEAN, true, 0);
  expect_object_map_update(mock_image_ctx, CEPH_NOSNAP, 0, OBJECT_EXISTS, true,
                           0);

  expect_add_copyup_ops(mock_write_request);
  expect_sparse_copyup(mock_image_ctx, CEPH_NOSNAP, ictx->get_object_name(0),
                       {}, "", 0);
  expect_write(mock_image_ctx, CEPH_NOSNAP, ictx->get_object_name(0), 0);

  auto req = new MockCopyupRequest(&mock_image_ctx, 0,
                                   {{0, 4096}}, {});
  mock_image_ctx.copyup_list[0] = req;
  req->append_request(&mock_write_request);
  req->send();

  ASSERT_EQ(0, mock_write_request.ctx.wait());
}

TEST_F(TestMockIoCopyupRequest, ZeroedCopyup) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  expect_op_work_queue(mock_image_ctx);
  expect_is_lock_owner(mock_image_ctx);

  InSequence seq;

  MockAbstractObjectWriteRequest mock_write_request;
  expect_is_empty_write_op(mock_write_request, false);
  expect_get_pre_write_object_map_state(mock_image_ctx, mock_write_request,
                                        OBJECT_EXISTS);
  expect_object_map_at(mock_image_ctx, 0, OBJECT_NONEXISTENT);
  expect_object_map_update(mock_image_ctx, CEPH_NOSNAP, 0, OBJECT_EXISTS, true,
                           0);

  expect_add_copyup_ops(mock_write_request);
  expect_sparse_copyup(mock_image_ctx, CEPH_NOSNAP, ictx->get_object_name(0),
                       {}, "", 0);
  expect_write(mock_image_ctx, CEPH_NOSNAP, ictx->get_object_name(0), 0);

  auto req = new MockCopyupRequest(&mock_image_ctx, 0,
                                   {{0, 4096}}, {});
  mock_image_ctx.copyup_list[0] = req;
  req->append_request(&mock_write_request);
  req->send();

  ASSERT_EQ(0, mock_write_request.ctx.wait());
}

TEST_F(TestMockIoCopyupRequest, ZeroedCopyOnRead) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_parent_image_ctx(*ictx->parent);
  MockTestImageCtx mock_image_ctx(*ictx, &mock_parent_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  expect_op_work_queue(mock_image_ctx);
  expect_is_lock_owner(mock_image_ctx);

  InSequence seq;

  MockImageRequest mock_image_request;
  std::string data(4096, '\0');
  expect_read_parent(mock_parent_image_ctx, mock_image_request, {{0, 4096}},
                     data, 0);

  expect_object_map_at(mock_image_ctx, 0, OBJECT_NONEXISTENT);
  expect_object_map_update(mock_image_ctx, CEPH_NOSNAP, 0, OBJECT_EXISTS, true,
                           0);

  expect_sparse_copyup(mock_image_ctx, CEPH_NOSNAP, ictx->get_object_name(0),
                       {}, "", 0);

  auto req = new MockCopyupRequest(&mock_image_ctx, 0,
                                   {{0, 4096}}, {});
  mock_image_ctx.copyup_list[0] = req;
  req->send();
  flush_async_operations(ictx);
}

TEST_F(TestMockIoCopyupRequest, NoOpCopyup) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_parent_image_ctx(*ictx->parent);
  MockTestImageCtx mock_image_ctx(*ictx, &mock_parent_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  expect_op_work_queue(mock_image_ctx);
  expect_is_lock_owner(mock_image_ctx);

  InSequence seq;

  MockImageRequest mock_image_request;
  expect_read_parent(mock_parent_image_ctx, mock_image_request, {{0, 4096}},
                     "", -ENOENT);

  MockAbstractObjectWriteRequest mock_write_request;
  expect_is_empty_write_op(mock_write_request, true);

  auto req = new MockCopyupRequest(&mock_image_ctx, 0,
                                   {{0, 4096}}, {});
  mock_image_ctx.copyup_list[0] = req;
  req->append_request(&mock_write_request);
  req->send();

  ASSERT_EQ(0, mock_write_request.ctx.wait());
}

TEST_F(TestMockIoCopyupRequest, RestartWrite) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_parent_image_ctx(*ictx->parent);
  MockTestImageCtx mock_image_ctx(*ictx, &mock_parent_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  expect_op_work_queue(mock_image_ctx);
  expect_is_lock_owner(mock_image_ctx);

  InSequence seq;

  MockImageRequest mock_image_request;
  std::string data(4096, '1');
  expect_read_parent(mock_parent_image_ctx, mock_image_request, {{0, 4096}},
                     data, 0);

  MockAbstractObjectWriteRequest mock_write_request1;
  expect_get_pre_write_object_map_state(mock_image_ctx, mock_write_request1,
                                        OBJECT_EXISTS);
  expect_object_map_at(mock_image_ctx, 0, OBJECT_NONEXISTENT);
  expect_object_map_update(mock_image_ctx, CEPH_NOSNAP, 0, OBJECT_EXISTS, true,
                           0);

  auto req = new MockCopyupRequest(&mock_image_ctx, 0,
                                   {{0, 4096}}, {});
  expect_add_copyup_ops(mock_write_request1);
  expect_sparse_copyup(mock_image_ctx, CEPH_NOSNAP, ictx->get_object_name(0),
                       {{0, 4096}}, data, 0);

  MockAbstractObjectWriteRequest mock_write_request2;
  auto& mock_io_ctx = librados::get_mock_io_ctx(
    mock_image_ctx.rados_api, *mock_image_ctx.get_data_io_context());
  EXPECT_CALL(mock_io_ctx, write(ictx->get_object_name(0), _, 0, 0, _))
    .WillOnce(WithoutArgs(Invoke([req, &mock_write_request2]() {
                            req->append_request(&mock_write_request2);
                            return 0;
                          })));

  mock_image_ctx.copyup_list[0] = req;
  req->append_request(&mock_write_request1);
  req->send();

  ASSERT_EQ(0, mock_write_request1.ctx.wait());
  ASSERT_EQ(-ERESTART, mock_write_request2.ctx.wait());
}

TEST_F(TestMockIoCopyupRequest, ReadFromParentError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_parent_image_ctx(*ictx->parent);
  MockTestImageCtx mock_image_ctx(*ictx, &mock_parent_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  expect_op_work_queue(mock_image_ctx);
  expect_is_lock_owner(mock_image_ctx);

  InSequence seq;

  MockImageRequest mock_image_request;
  expect_read_parent(mock_parent_image_ctx, mock_image_request, {{0, 4096}},
                     "", -EPERM);

  MockAbstractObjectWriteRequest mock_write_request;
  expect_is_empty_write_op(mock_write_request, false);

  auto req = new MockCopyupRequest(&mock_image_ctx, 0,
                                   {{0, 4096}}, {});
  mock_image_ctx.copyup_list[0] = req;
  req->append_request(&mock_write_request);
  req->send();

  ASSERT_EQ(-EPERM, mock_write_request.ctx.wait());
}

TEST_F(TestMockIoCopyupRequest, DeepCopyError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_parent_image_ctx(*ictx->parent);
  MockTestImageCtx mock_image_ctx(*ictx, &mock_parent_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  expect_op_work_queue(mock_image_ctx);
  expect_is_lock_owner(mock_image_ctx);

  InSequence seq;

  MockAbstractObjectWriteRequest mock_write_request;
  MockObjectCopyRequest mock_object_copy_request;
  mock_image_ctx.migration_info = {1, "", "", "image id", {}, ictx->size, true};
  expect_is_empty_write_op(mock_write_request, false);
  expect_object_copy(mock_image_ctx, mock_object_copy_request, true, -EPERM);

  expect_is_empty_write_op(mock_write_request, false);

  auto req = new MockCopyupRequest(&mock_image_ctx, 0,
                                   {{0, 4096}}, {});
  mock_image_ctx.copyup_list[0] = req;
  req->append_request(&mock_write_request);
  req->send();

  ASSERT_EQ(-EPERM, mock_write_request.ctx.wait());
}

TEST_F(TestMockIoCopyupRequest, UpdateObjectMapError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING | RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_parent_image_ctx(*ictx->parent);
  MockTestImageCtx mock_image_ctx(*ictx, &mock_parent_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  expect_op_work_queue(mock_image_ctx);
  expect_is_lock_owner(mock_image_ctx);

  InSequence seq;

  MockImageRequest mock_image_request;
  std::string data(4096, '1');
  expect_read_parent(mock_parent_image_ctx, mock_image_request, {{0, 4096}},
                     data, 0);

  MockAbstractObjectWriteRequest mock_write_request;
  expect_get_pre_write_object_map_state(mock_image_ctx, mock_write_request,
                                        OBJECT_EXISTS);
  expect_object_map_at(mock_image_ctx, 0, OBJECT_NONEXISTENT);
  expect_object_map_update(mock_image_ctx, CEPH_NOSNAP, 0, OBJECT_EXISTS, true,
                           -EINVAL);

  auto req = new MockCopyupRequest(&mock_image_ctx, 0,
                                   {{0, 4096}}, {});
  mock_image_ctx.copyup_list[0] = req;
  req->append_request(&mock_write_request);
  req->send();

  ASSERT_EQ(-EINVAL, mock_write_request.ctx.wait());
}

TEST_F(TestMockIoCopyupRequest, CopyupError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ictx->image_lock.lock();
  ictx->add_snap(cls::rbd::UserSnapshotNamespace(), "1", 1, ictx->size,
                 ictx->parent_md, RBD_PROTECTION_STATUS_UNPROTECTED,
                 0, {});
  ictx->snapc = {1, {1}};
  ictx->image_lock.unlock();

  MockTestImageCtx mock_parent_image_ctx(*ictx->parent);
  MockTestImageCtx mock_image_ctx(*ictx, &mock_parent_image_ctx);

  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  expect_test_features(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);
  expect_is_lock_owner(mock_image_ctx);

  InSequence seq;

  MockImageRequest mock_image_request;
  std::string data(4096, '1');
  expect_read_parent(mock_parent_image_ctx, mock_image_request, {{0, 4096}},
                     data, 0);

  MockAbstractObjectWriteRequest mock_write_request;
  expect_get_pre_write_object_map_state(mock_image_ctx, mock_write_request,
                                        OBJECT_EXISTS);
  expect_object_map_at(mock_image_ctx, 0, OBJECT_NONEXISTENT);
  expect_object_map_update(mock_image_ctx, 1, 0, OBJECT_EXISTS, true, 0);
  expect_object_map_update(mock_image_ctx, CEPH_NOSNAP, 0, OBJECT_EXISTS, true,
                           0);

  expect_add_copyup_ops(mock_write_request);
  expect_sparse_copyup(mock_image_ctx, 0, ictx->get_object_name(0), {{0, 4096}},
                       data, -EPERM);
  expect_write(mock_image_ctx, CEPH_NOSNAP, ictx->get_object_name(0), 0);

  auto req = new MockCopyupRequest(&mock_image_ctx, 0,
                                   {{0, 4096}}, {});
  mock_image_ctx.copyup_list[0] = req;
  req->append_request(&mock_write_request);
  req->send();

  ASSERT_EQ(-EPERM, mock_write_request.ctx.wait());
  flush_async_operations(ictx);
}

TEST_F(TestMockIoCopyupRequest, SparseCopyupNotSupported) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_parent_image_ctx(*ictx->parent);
  MockTestImageCtx mock_image_ctx(*ictx, &mock_parent_image_ctx);
  mock_image_ctx.enable_sparse_copyup = false;

  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  expect_op_work_queue(mock_image_ctx);
  expect_is_lock_owner(mock_image_ctx);

  InSequence seq;

  MockImageRequest mock_image_request;
  std::string data(4096, '1');
  expect_read_parent(mock_parent_image_ctx, mock_image_request, {{0, 4096}},
                     data, 0);

  MockAbstractObjectWriteRequest mock_write_request;
  expect_get_pre_write_object_map_state(mock_image_ctx, mock_write_request,
                                        OBJECT_EXISTS);
  expect_object_map_at(mock_image_ctx, 0, OBJECT_NONEXISTENT);
  expect_object_map_update(mock_image_ctx, CEPH_NOSNAP, 0, OBJECT_EXISTS, true,
                           0);

  expect_add_copyup_ops(mock_write_request);
  expect_copyup(mock_image_ctx, CEPH_NOSNAP, ictx->get_object_name(0), data, 0);
  expect_write(mock_image_ctx, CEPH_NOSNAP, ictx->get_object_name(0), 0);

  auto req = new MockCopyupRequest(&mock_image_ctx, 0, {{0, 4096}}, {});
  mock_image_ctx.copyup_list[0] = req;
  req->append_request(&mock_write_request);
  req->send();

  ASSERT_EQ(0, mock_write_request.ctx.wait());
}

} // namespace io
} // namespace librbd
