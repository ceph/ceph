// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "include/rbd/librbd.hpp"
#include "librbd/AsioEngine.h"
#include "librbd/DeepCopyRequest.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/internal.h"
#include "librbd/api/Image.h"
#include "librbd/deep_copy/Handler.h"
#include "librbd/deep_copy/ImageCopyRequest.h"
#include "librbd/deep_copy/MetadataCopyRequest.h"
#include "librbd/deep_copy/SnapshotCopyRequest.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockObjectMap.h"
#include "test/librbd/test_support.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <boost/scope_exit.hpp>

namespace librbd {

namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  explicit MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace deep_copy {

template <>
class ImageCopyRequest<librbd::MockTestImageCtx> {
public:
  static ImageCopyRequest* s_instance;
  Context *on_finish;

  static ImageCopyRequest* create(
      librbd::MockTestImageCtx *src_image_ctx,
      librbd::MockTestImageCtx *dst_image_ctx,
      librados::snap_t src_snap_id_start,
      librados::snap_t src_snap_id_end,
      librados::snap_t dst_snap_id_start,
      bool flatten, const ObjectNumber &object_number,
      const SnapSeqs &snap_seqs, Handler *handler,
      Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  ImageCopyRequest() {
    s_instance = this;
  }

  void put() {
  }

  void get() {
  }

  MOCK_METHOD0(cancel, void());
  MOCK_METHOD0(send, void());
};

template <>
class MetadataCopyRequest<librbd::MockTestImageCtx> {
public:
  static MetadataCopyRequest* s_instance;
  Context *on_finish;

  static MetadataCopyRequest* create(librbd::MockTestImageCtx *src_image_ctx,
                                     librbd::MockTestImageCtx *dst_image_ctx,
                                     Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MetadataCopyRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

template <>
class SnapshotCopyRequest<librbd::MockTestImageCtx> {
public:
  static SnapshotCopyRequest* s_instance;
  Context *on_finish;

  static SnapshotCopyRequest* create(librbd::MockTestImageCtx *src_image_ctx,
                                     librbd::MockTestImageCtx *dst_image_ctx,
                                     librados::snap_t src_snap_id_start,
                                     librados::snap_t src_snap_id_end,
                                     librados::snap_t dst_snap_id_start,
                                     bool flatten,
                                     librbd::asio::ContextWQ *work_queue,
                                     SnapSeqs *snap_seqs, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  SnapshotCopyRequest() {
    s_instance = this;
  }

  void put() {
  }

  void get() {
  }

  MOCK_METHOD0(cancel, void());
  MOCK_METHOD0(send, void());
};

ImageCopyRequest<librbd::MockTestImageCtx>* ImageCopyRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
MetadataCopyRequest<librbd::MockTestImageCtx>* MetadataCopyRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
SnapshotCopyRequest<librbd::MockTestImageCtx>* SnapshotCopyRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace deep_copy
} // namespace librbd

// template definitions
#include "librbd/DeepCopyRequest.cc"

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::ReturnNew;
using ::testing::SetArgPointee;
using ::testing::WithArg;

class TestMockDeepCopyRequest : public TestMockFixture {
public:
  typedef librbd::DeepCopyRequest<librbd::MockTestImageCtx> MockDeepCopyRequest;
  typedef librbd::deep_copy::ImageCopyRequest<librbd::MockTestImageCtx> MockImageCopyRequest;
  typedef librbd::deep_copy::MetadataCopyRequest<librbd::MockTestImageCtx> MockMetadataCopyRequest;
  typedef librbd::deep_copy::SnapshotCopyRequest<librbd::MockTestImageCtx> MockSnapshotCopyRequest;

  librbd::ImageCtx *m_src_image_ctx;
  librbd::ImageCtx *m_dst_image_ctx;

  std::shared_ptr<librbd::AsioEngine> m_asio_engine;
  librbd::asio::ContextWQ *m_work_queue;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, open_image(m_image_name, &m_src_image_ctx));

    ASSERT_EQ(0, open_image(m_image_name, &m_dst_image_ctx));

    m_asio_engine = std::make_shared<librbd::AsioEngine>(
      m_src_image_ctx->md_ctx);
    m_work_queue = m_asio_engine->get_work_queue();
  }

  void TearDown() override {
    TestMockFixture::TearDown();
  }

  void expect_test_features(librbd::MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, test_features(_, _))
      .WillRepeatedly(WithArg<0>(Invoke([&mock_image_ctx](uint64_t features) {
              return (mock_image_ctx.features & features) != 0;
            })));
  }

  void expect_start_op(librbd::MockExclusiveLock &mock_exclusive_lock) {
    EXPECT_CALL(mock_exclusive_lock, start_op(_)).WillOnce(Return(new LambdaContext([](int){})));
  }

  void expect_rollback_object_map(librbd::MockObjectMap &mock_object_map, int r) {
    EXPECT_CALL(mock_object_map, rollback(_, _))
      .WillOnce(WithArg<1>(Invoke([this, r](Context *ctx) {
              m_work_queue->queue(ctx, r);
            })));
  }

  void expect_create_object_map(librbd::MockTestImageCtx &mock_image_ctx,
                                librbd::MockObjectMap *mock_object_map) {
    EXPECT_CALL(mock_image_ctx, create_object_map(CEPH_NOSNAP))
      .WillOnce(Return(mock_object_map));
  }

  void expect_open_object_map(librbd::MockTestImageCtx &mock_image_ctx,
                              librbd::MockObjectMap &mock_object_map, int r) {
    EXPECT_CALL(mock_object_map, open(_))
      .WillOnce(Invoke([this, r](Context *ctx) {
          m_work_queue->queue(ctx, r);
        }));
  }

  void expect_copy_snapshots(
      MockSnapshotCopyRequest &mock_snapshot_copy_request, int r) {
    EXPECT_CALL(mock_snapshot_copy_request, send())
      .WillOnce(Invoke([this, &mock_snapshot_copy_request, r]() {
            m_work_queue->queue(mock_snapshot_copy_request.on_finish, r);
          }));
  }

  void expect_copy_image(MockImageCopyRequest &mock_image_copy_request, int r) {
    EXPECT_CALL(mock_image_copy_request, send())
      .WillOnce(Invoke([this, &mock_image_copy_request, r]() {
            m_work_queue->queue(mock_image_copy_request.on_finish, r);
          }));
  }

  void expect_copy_object_map(librbd::MockExclusiveLock &mock_exclusive_lock,
                              librbd::MockObjectMap *mock_object_map, int r) {
    expect_start_op(mock_exclusive_lock);
    expect_rollback_object_map(*mock_object_map, r);
  }

  void expect_refresh_object_map(librbd::MockTestImageCtx &mock_image_ctx,
                                 librbd::MockObjectMap *mock_object_map,
                                 int r) {
    expect_start_op(*mock_image_ctx.exclusive_lock);
    expect_create_object_map(mock_image_ctx, mock_object_map);
    expect_open_object_map(mock_image_ctx, *mock_object_map, r);
  }

  void expect_copy_metadata(MockMetadataCopyRequest &mock_metadata_copy_request,
                            int r) {
    EXPECT_CALL(mock_metadata_copy_request, send())
      .WillOnce(Invoke([this, &mock_metadata_copy_request, r]() {
            m_work_queue->queue(mock_metadata_copy_request.on_finish, r);
          }));
  }
};

TEST_F(TestMockDeepCopyRequest, SimpleCopy) {
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockImageCopyRequest mock_image_copy_request;
  MockMetadataCopyRequest mock_metadata_copy_request;
  MockSnapshotCopyRequest mock_snapshot_copy_request;

  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_dst_image_ctx.exclusive_lock = &mock_exclusive_lock;

  librbd::MockObjectMap mock_object_map;

  mock_dst_image_ctx.object_map = nullptr;
  if (is_feature_enabled(RBD_FEATURE_OBJECT_MAP)) {
    mock_dst_image_ctx.object_map = &mock_object_map;
  }

  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_copy_snapshots(mock_snapshot_copy_request, 0);
  expect_copy_image(mock_image_copy_request, 0);
  if (mock_dst_image_ctx.object_map != nullptr) {
    expect_refresh_object_map(mock_dst_image_ctx, &mock_object_map, 0);
  }
  expect_copy_metadata(mock_metadata_copy_request, 0);

  C_SaferCond ctx;
  librbd::SnapSeqs snap_seqs;
  librbd::deep_copy::NoOpHandler no_op;
  auto request = librbd::DeepCopyRequest<librbd::MockTestImageCtx>::create(
      &mock_src_image_ctx, &mock_dst_image_ctx, 0, CEPH_NOSNAP, 0, false,
      boost::none, m_work_queue, &snap_seqs, &no_op, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockDeepCopyRequest, ErrorOnCopySnapshots) {
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockSnapshotCopyRequest mock_snapshot_copy_request;

  InSequence seq;
  expect_copy_snapshots(mock_snapshot_copy_request, -EINVAL);

  C_SaferCond ctx;
  librbd::SnapSeqs snap_seqs;
  librbd::deep_copy::NoOpHandler no_op;
  auto request = librbd::DeepCopyRequest<librbd::MockTestImageCtx>::create(
      &mock_src_image_ctx, &mock_dst_image_ctx, 0, CEPH_NOSNAP, 0, false,
      boost::none, m_work_queue, &snap_seqs, &no_op, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockDeepCopyRequest, ErrorOnRefreshObjectMap) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockImageCopyRequest mock_image_copy_request;
  MockSnapshotCopyRequest mock_snapshot_copy_request;

  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_dst_image_ctx.exclusive_lock = &mock_exclusive_lock;

  librbd::MockObjectMap *mock_object_map = new librbd::MockObjectMap();
  mock_dst_image_ctx.object_map = mock_object_map;

  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_copy_snapshots(mock_snapshot_copy_request, 0);
  expect_copy_image(mock_image_copy_request, 0);
  expect_start_op(*mock_dst_image_ctx.exclusive_lock);
  expect_create_object_map(mock_dst_image_ctx, mock_object_map);
  expect_open_object_map(mock_dst_image_ctx, *mock_object_map, -EINVAL);

  C_SaferCond ctx;
  librbd::SnapSeqs snap_seqs;
  librbd::deep_copy::NoOpHandler no_op;
  auto request = librbd::DeepCopyRequest<librbd::MockTestImageCtx>::create(
      &mock_src_image_ctx, &mock_dst_image_ctx, 0, CEPH_NOSNAP, 0, false,
      boost::none, m_work_queue, &snap_seqs, &no_op, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockDeepCopyRequest, ErrorOnCopyImage) {
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockImageCopyRequest mock_image_copy_request;
  MockSnapshotCopyRequest mock_snapshot_copy_request;

  InSequence seq;
  expect_copy_snapshots(mock_snapshot_copy_request, 0);
  expect_copy_image(mock_image_copy_request, -EINVAL);

  C_SaferCond ctx;
  librbd::SnapSeqs snap_seqs;
  librbd::deep_copy::NoOpHandler no_op;
  auto request = librbd::DeepCopyRequest<librbd::MockTestImageCtx>::create(
      &mock_src_image_ctx, &mock_dst_image_ctx, 0, CEPH_NOSNAP, 0, false,
      boost::none, m_work_queue, &snap_seqs, &no_op, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockDeepCopyRequest, ErrorOnCopyMetadata) {
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockImageCopyRequest mock_image_copy_request;
  MockMetadataCopyRequest mock_metadata_copy_request;
  MockSnapshotCopyRequest mock_snapshot_copy_request;

  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_dst_image_ctx.exclusive_lock = &mock_exclusive_lock;

  librbd::MockObjectMap mock_object_map;

  mock_dst_image_ctx.object_map = nullptr;
  if (is_feature_enabled(RBD_FEATURE_OBJECT_MAP)) {
    mock_dst_image_ctx.object_map = &mock_object_map;
  }

  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_copy_snapshots(mock_snapshot_copy_request, 0);
  expect_copy_image(mock_image_copy_request, 0);
  if (mock_dst_image_ctx.object_map != nullptr) {
    expect_refresh_object_map(mock_dst_image_ctx, &mock_object_map, 0);
  }
  expect_copy_metadata(mock_metadata_copy_request, -EINVAL);

  C_SaferCond ctx;
  librbd::SnapSeqs snap_seqs;
  librbd::deep_copy::NoOpHandler no_op;
  auto request = librbd::DeepCopyRequest<librbd::MockTestImageCtx>::create(
      &mock_src_image_ctx, &mock_dst_image_ctx, 0, CEPH_NOSNAP, 0, false,
      boost::none, m_work_queue, &snap_seqs, &no_op, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockDeepCopyRequest, Snap) {
  EXPECT_EQ(0, snap_create(*m_src_image_ctx, "copy"));
  EXPECT_EQ(0, librbd::api::Image<>::snap_set(m_src_image_ctx,
                                              cls::rbd::UserSnapshotNamespace(),
                                              "copy"));

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockImageCopyRequest mock_image_copy_request;
  MockMetadataCopyRequest mock_metadata_copy_request;
  MockSnapshotCopyRequest mock_snapshot_copy_request;

  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_dst_image_ctx.exclusive_lock = &mock_exclusive_lock;

  librbd::MockObjectMap mock_object_map;
  if (is_feature_enabled(RBD_FEATURE_OBJECT_MAP)) {
    mock_dst_image_ctx.object_map = &mock_object_map;
  }

  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_copy_snapshots(mock_snapshot_copy_request, 0);
  expect_copy_image(mock_image_copy_request, 0);
  if (mock_dst_image_ctx.object_map != nullptr) {
    expect_copy_object_map(mock_exclusive_lock, &mock_object_map, 0);
    expect_refresh_object_map(mock_dst_image_ctx, &mock_object_map, 0);
  }
  expect_copy_metadata(mock_metadata_copy_request, 0);

  C_SaferCond ctx;
  librbd::SnapSeqs snap_seqs = {{m_src_image_ctx->snap_id, 123}};
  librbd::deep_copy::NoOpHandler no_op;
  auto request = librbd::DeepCopyRequest<librbd::MockTestImageCtx>::create(
      &mock_src_image_ctx, &mock_dst_image_ctx, 0, m_src_image_ctx->snap_id,
      0, false, boost::none, m_work_queue, &snap_seqs, &no_op, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockDeepCopyRequest, ErrorOnRollbackObjectMap) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  EXPECT_EQ(0, snap_create(*m_src_image_ctx, "copy"));
  EXPECT_EQ(0, librbd::api::Image<>::snap_set(m_src_image_ctx,
                                              cls::rbd::UserSnapshotNamespace(),
                                              "copy"));

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);
  MockImageCopyRequest mock_image_copy_request;
  MockMetadataCopyRequest mock_metadata_copy_request;
  MockSnapshotCopyRequest mock_snapshot_copy_request;

  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_dst_image_ctx.exclusive_lock = &mock_exclusive_lock;

  librbd::MockObjectMap mock_object_map;
  mock_dst_image_ctx.object_map = &mock_object_map;

  expect_test_features(mock_dst_image_ctx);

  InSequence seq;
  expect_copy_snapshots(mock_snapshot_copy_request, 0);
  expect_copy_image(mock_image_copy_request, 0);
  expect_copy_object_map(mock_exclusive_lock, &mock_object_map, -EINVAL);

  C_SaferCond ctx;
  librbd::SnapSeqs snap_seqs = {{m_src_image_ctx->snap_id, 123}};
  librbd::deep_copy::NoOpHandler no_op;
  auto request = librbd::DeepCopyRequest<librbd::MockTestImageCtx>::create(
      &mock_src_image_ctx, &mock_dst_image_ctx, 0, m_src_image_ctx->snap_id,
      0, false, boost::none, m_work_queue, &snap_seqs, &no_op, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}
