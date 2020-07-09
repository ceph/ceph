// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "librbd/internal.h"
#include "librbd/Operations.h"
#include "librbd/image/GetMetadataRequest.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_replayer/snapshot/ApplyImageStateRequest.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  explicit MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace image {

template <>
struct GetMetadataRequest<MockTestImageCtx> {
  std::map<std::string, bufferlist>* pairs = nullptr;
  Context* on_finish = nullptr;

  static GetMetadataRequest* s_instance;
  static GetMetadataRequest* create(librados::IoCtx& io_ctx,
                                    const std::string& oid,
                                    bool filter_internal,
                                    const std::string& filter_key_prefix,
                                    const std::string& last_key,
                                    size_t max_results,
                                    std::map<std::string, bufferlist>* pairs,
                                    Context* on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->pairs = pairs;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  GetMetadataRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

GetMetadataRequest<MockTestImageCtx>* GetMetadataRequest<MockTestImageCtx>::s_instance = nullptr;

} // namespace image
} // namespace librbd

#include "tools/rbd_mirror/image_replayer/snapshot/ApplyImageStateRequest.cc"

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace snapshot {

class TestMockImageReplayerSnapshotApplyImageStateRequest : public TestMockFixture {
public:
  typedef ApplyImageStateRequest<librbd::MockTestImageCtx> MockApplyImageStateRequest;
  typedef librbd::image::GetMetadataRequest<librbd::MockTestImageCtx> MockGetMetadataRequest;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &m_local_image_ctx));

    ASSERT_EQ(0, create_image(rbd, m_remote_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_remote_io_ctx, m_image_name, &m_remote_image_ctx));

    m_mock_local_image_ctx = new librbd::MockTestImageCtx(*m_local_image_ctx);
    m_mock_remote_image_ctx = new librbd::MockTestImageCtx(*m_remote_image_ctx);
  }

  void TearDown() override {
    delete m_mock_remote_image_ctx;
    delete m_mock_local_image_ctx;
    TestMockFixture::TearDown();
  }

  void expect_rename_image(const std::string& name, int r) {
    EXPECT_CALL(*m_mock_local_image_ctx->operations, execute_rename(name, _))
      .WillOnce(WithArg<1>(Invoke([this, r](Context* ctx) {
          m_threads->work_queue->queue(ctx, r);
        })));
  }

  void expect_update_features(uint64_t features, bool enable, int r) {
    EXPECT_CALL(*m_mock_local_image_ctx->operations,
                execute_update_features(features, enable, _, 0U))
      .WillOnce(WithArg<2>(Invoke([this, r](Context* ctx) {
          m_threads->work_queue->queue(ctx, r);
        })));
  }

  void expect_get_metadata(MockGetMetadataRequest& mock_get_metadata_request,
                           const std::map<std::string, bufferlist>& pairs,
                           int r) {
    EXPECT_CALL(mock_get_metadata_request, send())
      .WillOnce(Invoke([this, &mock_get_metadata_request, pairs, r]() {
          *mock_get_metadata_request.pairs = pairs;
          m_threads->work_queue->queue(mock_get_metadata_request.on_finish, r);
        }));
  }

  void expect_update_metadata(const std::vector<std::string>& remove,
                              const std::map<std::string, bufferlist>& pairs,
                              int r) {
    for (auto& key : remove) {
      bufferlist bl;
      ceph::encode(key, bl);
      EXPECT_CALL(get_mock_io_ctx(m_mock_local_image_ctx->md_ctx),
                  exec(m_mock_local_image_ctx->header_oid, _, StrEq("rbd"),
                  StrEq("metadata_remove"), ContentsEqual(bl), _, _, _))
        .WillOnce(Return(r));
      if (r < 0) {
        return;
      }
    }

    if (!pairs.empty()) {
      bufferlist bl;
      ceph::encode(pairs, bl);
      EXPECT_CALL(get_mock_io_ctx(m_mock_local_image_ctx->md_ctx),
                  exec(m_mock_local_image_ctx->header_oid, _, StrEq("rbd"),
                  StrEq("metadata_set"), ContentsEqual(bl), _, _, _))
        .WillOnce(Return(r));
    }
  }

  void expect_unprotect_snapshot(const std::string& name, int r) {
    EXPECT_CALL(*m_mock_local_image_ctx->operations,
                execute_snap_unprotect({cls::rbd::UserSnapshotNamespace{}},
                                       name, _))
      .WillOnce(WithArg<2>(Invoke([this, r](Context* ctx) {
          m_threads->work_queue->queue(ctx, r);
        })));
  }

  void expect_remove_snapshot(const std::string& name, int r) {
    EXPECT_CALL(*m_mock_local_image_ctx->operations,
                execute_snap_remove({cls::rbd::UserSnapshotNamespace{}},
                                     name, _))
      .WillOnce(WithArg<2>(Invoke([this, r](Context* ctx) {
          m_threads->work_queue->queue(ctx, r);
        })));
  }

  void expect_protect_snapshot(const std::string& name, int r) {
    EXPECT_CALL(*m_mock_local_image_ctx->operations,
                execute_snap_protect({cls::rbd::UserSnapshotNamespace{}},
                                     name, _))
      .WillOnce(WithArg<2>(Invoke([this, r](Context* ctx) {
          m_threads->work_queue->queue(ctx, r);
        })));
  }

  void expect_rename_snapshot(uint64_t snap_id, const std::string& name,
                              int r) {
    EXPECT_CALL(*m_mock_local_image_ctx->operations,
                execute_snap_rename(snap_id, name, _))
      .WillOnce(WithArg<2>(Invoke([this, r](Context* ctx) {
          m_threads->work_queue->queue(ctx, r);
        })));
  }


  void expect_set_snap_limit(uint64_t limit, int r) {
    EXPECT_CALL(*m_mock_local_image_ctx->operations,
                execute_snap_set_limit(limit, _))
      .WillOnce(WithArg<1>(Invoke([this, r](Context* ctx) {
          m_threads->work_queue->queue(ctx, r);
        })));
  }

  librbd::ImageCtx *m_local_image_ctx;
  librbd::ImageCtx *m_remote_image_ctx;

  librbd::MockTestImageCtx *m_mock_local_image_ctx = nullptr;
  librbd::MockTestImageCtx *m_mock_remote_image_ctx = nullptr;

};

TEST_F(TestMockImageReplayerSnapshotApplyImageStateRequest, NoChanges) {
  InSequence seq;

  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_get_metadata_request, {}, 0);

  expect_set_snap_limit(0, 0);

  librbd::mirror::snapshot::ImageState image_state;
  image_state.name = m_image_name;
  image_state.features = m_remote_image_ctx->features;

  C_SaferCond ctx;
  auto req = MockApplyImageStateRequest::create(
    "local mirror uuid", "remote mirror uuid", m_mock_local_image_ctx,
    m_mock_remote_image_ctx, image_state, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageReplayerSnapshotApplyImageStateRequest, RenameImage) {
  InSequence seq;

  expect_rename_image("new name", 0);

  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_get_metadata_request, {}, 0);

  expect_set_snap_limit(0, 0);

  librbd::mirror::snapshot::ImageState image_state;
  image_state.name = "new name";
  image_state.features = m_remote_image_ctx->features;

  C_SaferCond ctx;
  auto req = MockApplyImageStateRequest::create(
    "local mirror uuid", "remote mirror uuid", m_mock_local_image_ctx,
    m_mock_remote_image_ctx, image_state, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageReplayerSnapshotApplyImageStateRequest, RenameImageError) {
  InSequence seq;

  expect_rename_image("new name", -EINVAL);

  librbd::mirror::snapshot::ImageState image_state;
  image_state.name = "new name";
  image_state.features = m_remote_image_ctx->features;

  C_SaferCond ctx;
  auto req = MockApplyImageStateRequest::create(
    "local mirror uuid", "remote mirror uuid", m_mock_local_image_ctx,
    m_mock_remote_image_ctx, image_state, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerSnapshotApplyImageStateRequest, UpdateFeatures) {
  InSequence seq;

  expect_update_features(RBD_FEATURE_DEEP_FLATTEN, false, 0);
  expect_update_features(RBD_FEATURE_OBJECT_MAP, true, 0);

  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_get_metadata_request, {}, 0);

  expect_set_snap_limit(0, 0);

  librbd::mirror::snapshot::ImageState image_state;
  image_state.name = m_image_name;
  image_state.features = RBD_FEATURE_EXCLUSIVE_LOCK |
                         RBD_FEATURE_OBJECT_MAP;
  m_mock_local_image_ctx->features = RBD_FEATURE_EXCLUSIVE_LOCK |
                                     RBD_FEATURE_DEEP_FLATTEN;

  C_SaferCond ctx;
  auto req = MockApplyImageStateRequest::create(
    "local mirror uuid", "remote mirror uuid", m_mock_local_image_ctx,
    m_mock_remote_image_ctx, image_state, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageReplayerSnapshotApplyImageStateRequest, UpdateFeaturesError) {
  InSequence seq;

  expect_update_features(RBD_FEATURE_DEEP_FLATTEN, false, -EINVAL);

  librbd::mirror::snapshot::ImageState image_state;
  image_state.name = m_image_name;
  image_state.features = RBD_FEATURE_EXCLUSIVE_LOCK |
                         RBD_FEATURE_OBJECT_MAP;
  m_mock_local_image_ctx->features = RBD_FEATURE_EXCLUSIVE_LOCK |
                                     RBD_FEATURE_DEEP_FLATTEN;

  C_SaferCond ctx;
  auto req = MockApplyImageStateRequest::create(
    "local mirror uuid", "remote mirror uuid", m_mock_local_image_ctx,
    m_mock_remote_image_ctx, image_state, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerSnapshotApplyImageStateRequest, UpdateImageMeta) {
  InSequence seq;

  bufferlist data_bl;
  ceph::encode("data", data_bl);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_get_metadata_request,
                      {{"key1", {}}, {"key2", {}}}, 0);
  expect_update_metadata({"key2"}, {{"key1", data_bl}}, 0);

  expect_set_snap_limit(0, 0);

  librbd::mirror::snapshot::ImageState image_state;
  image_state.name = m_image_name;
  image_state.features = m_remote_image_ctx->features;
  image_state.metadata = {{"key1", data_bl}};

  C_SaferCond ctx;
  auto req = MockApplyImageStateRequest::create(
    "local mirror uuid", "remote mirror uuid", m_mock_local_image_ctx,
    m_mock_remote_image_ctx, image_state, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageReplayerSnapshotApplyImageStateRequest, GetImageMetaError) {
  InSequence seq;

  bufferlist data_bl;
  ceph::encode("data", data_bl);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_get_metadata_request,
                      {{"key1", {}}, {"key2", {}}}, -EINVAL);

  librbd::mirror::snapshot::ImageState image_state;
  image_state.name = m_image_name;
  image_state.features = m_remote_image_ctx->features;
  image_state.metadata = {{"key1", data_bl}};

  C_SaferCond ctx;
  auto req = MockApplyImageStateRequest::create(
    "local mirror uuid", "remote mirror uuid", m_mock_local_image_ctx,
    m_mock_remote_image_ctx, image_state, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerSnapshotApplyImageStateRequest, UpdateImageMetaError) {
  InSequence seq;

  bufferlist data_bl;
  ceph::encode("data", data_bl);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_get_metadata_request,
                      {{"key1", {}}, {"key2", {}}}, 0);
  expect_update_metadata({"key2"}, {{"key1", data_bl}}, -EINVAL);

  librbd::mirror::snapshot::ImageState image_state;
  image_state.name = m_image_name;
  image_state.features = m_remote_image_ctx->features;
  image_state.metadata = {{"key1", data_bl}};

  C_SaferCond ctx;
  auto req = MockApplyImageStateRequest::create(
    "local mirror uuid", "remote mirror uuid", m_mock_local_image_ctx,
    m_mock_remote_image_ctx, image_state, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerSnapshotApplyImageStateRequest, UnprotectSnapshot) {
  InSequence seq;

  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_get_metadata_request, {}, 0);

  expect_unprotect_snapshot("snap1", 0);

  expect_set_snap_limit(0, 0);

  librbd::mirror::snapshot::ImageState image_state;
  image_state.name = m_image_name;
  image_state.features = m_remote_image_ctx->features;
  image_state.snapshots = {
    {1U, {cls::rbd::UserSnapshotNamespace{}, "snap1",
          RBD_PROTECTION_STATUS_UNPROTECTED}}};
  m_mock_local_image_ctx->snap_info = {
    {11U, librbd::SnapInfo{"snap1", cls::rbd::UserSnapshotNamespace{},
                           0U, {}, RBD_PROTECTION_STATUS_PROTECTED, 0, {}}},
    {12U, librbd::SnapInfo{"snap2", cls::rbd::MirrorSnapshotNamespace{
       cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY, {}, "remote mirror uuid",
       1, true, 0, {{1, 11}, {2, CEPH_NOSNAP}}},
     0, {}, 0, 0, {}}}};

  C_SaferCond ctx;
  auto req = MockApplyImageStateRequest::create(
    "local mirror uuid", "remote mirror uuid", m_mock_local_image_ctx,
    m_mock_remote_image_ctx, image_state, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageReplayerSnapshotApplyImageStateRequest, UnprotectSnapshotError) {
  InSequence seq;

  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_get_metadata_request, {}, 0);

  expect_unprotect_snapshot("snap1", -EINVAL);

  librbd::mirror::snapshot::ImageState image_state;
  image_state.name = m_image_name;
  image_state.features = m_remote_image_ctx->features;
  image_state.snapshots = {
    {1U, {cls::rbd::UserSnapshotNamespace{}, "snap1",
          RBD_PROTECTION_STATUS_UNPROTECTED}}};
  m_mock_local_image_ctx->snap_info = {
    {11U, librbd::SnapInfo{"snap1", cls::rbd::UserSnapshotNamespace{},
                           0U, {}, RBD_PROTECTION_STATUS_PROTECTED, 0, {}}},
    {12U, librbd::SnapInfo{"snap2", cls::rbd::MirrorSnapshotNamespace{
       cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY, {}, "remote mirror uuid",
       1, true, 0, {{1, 11}, {2, CEPH_NOSNAP}}},
     0, {}, 0, 0, {}}}};

  C_SaferCond ctx;
  auto req = MockApplyImageStateRequest::create(
    "local mirror uuid", "remote mirror uuid", m_mock_local_image_ctx,
    m_mock_remote_image_ctx, image_state, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerSnapshotApplyImageStateRequest, RemoveSnapshot) {
  InSequence seq;

  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_get_metadata_request, {}, 0);

  expect_remove_snapshot("snap1", 0);

  expect_set_snap_limit(0, 0);

  librbd::mirror::snapshot::ImageState image_state;
  image_state.name = m_image_name;
  image_state.features = m_remote_image_ctx->features;
  m_mock_local_image_ctx->snap_info = {
    {11U, librbd::SnapInfo{"snap1", cls::rbd::UserSnapshotNamespace{},
                           0U, {}, RBD_PROTECTION_STATUS_UNPROTECTED, 0, {}}},
    {12U, librbd::SnapInfo{"snap2", cls::rbd::MirrorSnapshotNamespace{
       cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY, {}, "remote mirror uuid",
       1, true, 0, {{1, 11}, {2, CEPH_NOSNAP}}},
     0, {}, 0, 0, {}}}};

  C_SaferCond ctx;
  auto req = MockApplyImageStateRequest::create(
    "local mirror uuid", "remote mirror uuid", m_mock_local_image_ctx,
    m_mock_remote_image_ctx, image_state, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageReplayerSnapshotApplyImageStateRequest, RemoveSnapshotError) {
  InSequence seq;

  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_get_metadata_request, {}, 0);

  expect_remove_snapshot("snap1", -EINVAL);

  librbd::mirror::snapshot::ImageState image_state;
  image_state.name = m_image_name;
  image_state.features = m_remote_image_ctx->features;
  m_mock_local_image_ctx->snap_info = {
    {11U, librbd::SnapInfo{"snap1", cls::rbd::UserSnapshotNamespace{},
                           0U, {}, RBD_PROTECTION_STATUS_UNPROTECTED, 0, {}}},
    {12U, librbd::SnapInfo{"snap2", cls::rbd::MirrorSnapshotNamespace{
       cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY, {}, "remote mirror uuid",
       1, true, 0, {{1, 11}, {2, CEPH_NOSNAP}}},
     0, {}, 0, 0, {}}}};

  C_SaferCond ctx;
  auto req = MockApplyImageStateRequest::create(
    "local mirror uuid", "remote mirror uuid", m_mock_local_image_ctx,
    m_mock_remote_image_ctx, image_state, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerSnapshotApplyImageStateRequest, ProtectSnapshot) {
  InSequence seq;

  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_get_metadata_request, {}, 0);

  expect_protect_snapshot("snap1", 0);

  expect_set_snap_limit(0, 0);

  librbd::mirror::snapshot::ImageState image_state;
  image_state.name = m_image_name;
  image_state.features = m_remote_image_ctx->features;
  image_state.snapshots = {
    {1U, {cls::rbd::UserSnapshotNamespace{}, "snap1",
          RBD_PROTECTION_STATUS_PROTECTED}}};
  m_mock_local_image_ctx->snap_info = {
    {11U, librbd::SnapInfo{"snap1", cls::rbd::UserSnapshotNamespace{},
                           0U, {}, RBD_PROTECTION_STATUS_UNPROTECTED, 0, {}}},
    {12U, librbd::SnapInfo{"snap2", cls::rbd::MirrorSnapshotNamespace{
       cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY, {}, "remote mirror uuid",
       1, true, 0, {{1, 11}, {2, CEPH_NOSNAP}}},
     0, {}, 0, 0, {}}}};

  C_SaferCond ctx;
  auto req = MockApplyImageStateRequest::create(
    "local mirror uuid", "remote mirror uuid", m_mock_local_image_ctx,
    m_mock_remote_image_ctx, image_state, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageReplayerSnapshotApplyImageStateRequest, ProtectSnapshotError) {
  InSequence seq;

  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_get_metadata_request, {}, 0);

  expect_protect_snapshot("snap1", -EINVAL);

  librbd::mirror::snapshot::ImageState image_state;
  image_state.name = m_image_name;
  image_state.features = m_remote_image_ctx->features;
  image_state.snapshots = {
    {1U, {cls::rbd::UserSnapshotNamespace{}, "snap1",
          RBD_PROTECTION_STATUS_PROTECTED}}};
  m_mock_local_image_ctx->snap_info = {
    {11U, librbd::SnapInfo{"snap1", cls::rbd::UserSnapshotNamespace{},
                           0U, {}, RBD_PROTECTION_STATUS_UNPROTECTED, 0, {}}},
    {12U, librbd::SnapInfo{"snap2", cls::rbd::MirrorSnapshotNamespace{
       cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY, {}, "remote mirror uuid",
       1, true, 0, {{1, 11}, {2, CEPH_NOSNAP}}},
     0, {}, 0, 0, {}}}};

  C_SaferCond ctx;
  auto req = MockApplyImageStateRequest::create(
    "local mirror uuid", "remote mirror uuid", m_mock_local_image_ctx,
    m_mock_remote_image_ctx, image_state, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerSnapshotApplyImageStateRequest, RenameSnapshot) {
  InSequence seq;

  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_get_metadata_request, {}, 0);

  expect_rename_snapshot(11, "snap1-renamed", 0);

  expect_set_snap_limit(0, 0);

  librbd::mirror::snapshot::ImageState image_state;
  image_state.name = m_image_name;
  image_state.features = m_remote_image_ctx->features;
  image_state.snapshots = {
    {1U, {cls::rbd::UserSnapshotNamespace{}, "snap1-renamed",
          RBD_PROTECTION_STATUS_PROTECTED}}};
  m_mock_local_image_ctx->snap_info = {
    {11U, librbd::SnapInfo{"snap1", cls::rbd::UserSnapshotNamespace{},
                           0U, {}, RBD_PROTECTION_STATUS_PROTECTED, 0, {}}},
    {12U, librbd::SnapInfo{"snap2", cls::rbd::MirrorSnapshotNamespace{
       cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY, {}, "remote mirror uuid",
       1, true, 0, {{1, 11}, {2, CEPH_NOSNAP}}},
     0, {}, 0, 0, {}}}};

  C_SaferCond ctx;
  auto req = MockApplyImageStateRequest::create(
    "local mirror uuid", "remote mirror uuid", m_mock_local_image_ctx,
    m_mock_remote_image_ctx, image_state, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageReplayerSnapshotApplyImageStateRequest, RenameSnapshotError) {
  InSequence seq;

  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_get_metadata_request, {}, 0);

  expect_rename_snapshot(11, "snap1-renamed", -EINVAL);

  librbd::mirror::snapshot::ImageState image_state;
  image_state.name = m_image_name;
  image_state.features = m_remote_image_ctx->features;
  image_state.snapshots = {
    {1U, {cls::rbd::UserSnapshotNamespace{}, "snap1-renamed",
          RBD_PROTECTION_STATUS_PROTECTED}}};
  m_mock_local_image_ctx->snap_info = {
    {11U, librbd::SnapInfo{"snap1", cls::rbd::UserSnapshotNamespace{},
                           0U, {}, RBD_PROTECTION_STATUS_PROTECTED, 0, {}}},
    {12U, librbd::SnapInfo{"snap2", cls::rbd::MirrorSnapshotNamespace{
       cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY, {}, "remote mirror uuid",
       1, true, 0, {{1, 11}, {2, CEPH_NOSNAP}}},
     0, {}, 0, 0, {}}}};

  C_SaferCond ctx;
  auto req = MockApplyImageStateRequest::create(
    "local mirror uuid", "remote mirror uuid", m_mock_local_image_ctx,
    m_mock_remote_image_ctx, image_state, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerSnapshotApplyImageStateRequest, SetSnapshotLimitError) {
  InSequence seq;

  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_get_metadata_request, {}, 0);

  expect_set_snap_limit(0, -EINVAL);

  librbd::mirror::snapshot::ImageState image_state;
  image_state.name = m_image_name;
  image_state.features = m_remote_image_ctx->features;

  C_SaferCond ctx;
  auto req = MockApplyImageStateRequest::create(
    "local mirror uuid", "remote mirror uuid", m_mock_local_image_ctx,
    m_mock_remote_image_ctx, image_state, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace journal
} // namespace image_replayer
} // namespace mirror
} // namespace rbd
