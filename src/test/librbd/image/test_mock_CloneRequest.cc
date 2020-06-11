// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockContextWQ.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/deep_copy/MetadataCopyRequest.h"
#include "librbd/image/TypeTraits.h"
#include "librbd/image/AttachChildRequest.h"
#include "librbd/image/AttachParentRequest.h"
#include "librbd/image/CreateRequest.h"
#include "librbd/image/RemoveRequest.h"
#include "librbd/mirror/EnableRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  static MockTestImageCtx* s_instance;
  static MockTestImageCtx* create(const std::string &image_name,
                                  const std::string &image_id,
                                  const char *snap, librados::IoCtx& p,
                                  bool read_only) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }
  static MockTestImageCtx* create(const std::string &image_name,
                                  const std::string &image_id,
                                  librados::snap_t snap_id, IoCtx& p,
                                  bool read_only) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
    s_instance = this;
  }
};

MockTestImageCtx* MockTestImageCtx::s_instance = nullptr;

} // anonymous namespace

namespace deep_copy {

template <>
struct MetadataCopyRequest<MockTestImageCtx> {
  Context* on_finish = nullptr;

  static MetadataCopyRequest* s_instance;
  static MetadataCopyRequest* create(MockTestImageCtx* src_image_ctx,
                                     MockTestImageCtx* dst_image_ctx,
                                     Context* on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MetadataCopyRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

MetadataCopyRequest<MockTestImageCtx>* MetadataCopyRequest<MockTestImageCtx>::s_instance = nullptr;

} // namespace deep_copy

namespace image {

template <>
struct AttachChildRequest<MockTestImageCtx> {
  uint32_t clone_format;
  Context* on_finish = nullptr;
  static AttachChildRequest* s_instance;
  static AttachChildRequest* create(MockTestImageCtx *,
                                    MockTestImageCtx *,
                                    const librados::snap_t &,
                                    MockTestImageCtx *,
                                    const librados::snap_t &,
                                    uint32_t clone_format,
                                    Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->clone_format = clone_format;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  AttachChildRequest() {
    s_instance = this;
  }
};

AttachChildRequest<MockTestImageCtx>* AttachChildRequest<MockTestImageCtx>::s_instance = nullptr;

template <>
struct AttachParentRequest<MockTestImageCtx> {
  Context* on_finish = nullptr;
  static AttachParentRequest* s_instance;
  static AttachParentRequest* create(MockTestImageCtx&,
                                     const cls::rbd::ParentImageSpec& pspec,
                                     uint64_t parent_overlap, bool reattach,
                                     Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  AttachParentRequest() {
    s_instance = this;
  }
};

AttachParentRequest<MockTestImageCtx>* AttachParentRequest<MockTestImageCtx>::s_instance = nullptr;

template <>
struct CreateRequest<MockTestImageCtx> {
  Context* on_finish = nullptr;
  static CreateRequest* s_instance;
  static CreateRequest* create(const ConfigProxy& config, IoCtx &ioctx,
                               const std::string &image_name,
                               const std::string &image_id, uint64_t size,
                               const ImageOptions &image_options,
                               bool skip_mirror_enable,
                               cls::rbd::MirrorImageMode mode,
                               const std::string &non_primary_global_image_id,
                               const std::string &primary_mirror_uuid,
                               asio::ContextWQ *op_work_queue,
                               Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  CreateRequest() {
    s_instance = this;
  }
};

CreateRequest<MockTestImageCtx>* CreateRequest<MockTestImageCtx>::s_instance = nullptr;

template <>
struct RemoveRequest<MockTestImageCtx> {
  Context* on_finish = nullptr;
  static RemoveRequest* s_instance;
  static RemoveRequest* create(librados::IoCtx &ioctx,
                               const std::string &image_name,
                               const std::string &image_id,
                               bool force, bool from_trash_remove,
                               ProgressContext &prog_ctx,
                               asio::ContextWQ *op_work_queue,
                               Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  RemoveRequest() {
    s_instance = this;
  }
};

RemoveRequest<MockTestImageCtx>* RemoveRequest<MockTestImageCtx>::s_instance = nullptr;

} // namespace image

namespace mirror {

template <>
struct EnableRequest<MockTestImageCtx> {
  Context* on_finish = nullptr;
  static EnableRequest* s_instance;
  static EnableRequest* create(MockTestImageCtx* image_ctx,
                               cls::rbd::MirrorImageMode mode,
                               const std::string &non_primary_global_image_id,
                               bool image_clean, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    EXPECT_TRUE(image_clean);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  EnableRequest() {
    s_instance = this;
  }
};

EnableRequest<MockTestImageCtx>* EnableRequest<MockTestImageCtx>::s_instance = nullptr;

} // namespace mirror
} // namespace librbd

// template definitions
#include "librbd/image/CloneRequest.cc"

namespace librbd {
namespace image {

using ::testing::_;
using ::testing::Invoke;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockImageCloneRequest : public TestMockFixture {
public:
  typedef CloneRequest<MockTestImageCtx> MockCloneRequest;
  typedef AttachChildRequest<MockTestImageCtx> MockAttachChildRequest;
  typedef AttachParentRequest<MockTestImageCtx> MockAttachParentRequest;
  typedef CreateRequest<MockTestImageCtx> MockCreateRequest;
  typedef RemoveRequest<MockTestImageCtx> MockRemoveRequest;
  typedef deep_copy::MetadataCopyRequest<MockTestImageCtx> MockMetadataCopyRequest;
  typedef mirror::EnableRequest<MockTestImageCtx> MockMirrorEnableRequest;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, _rados.conf_set("rbd_default_clone_format", "2"));

    ASSERT_EQ(0, open_image(m_image_name, &image_ctx));
    ASSERT_EQ(0, image_ctx->operations->snap_create(
                   cls::rbd::UserSnapshotNamespace{}, "snap"));
    if (is_feature_enabled(RBD_FEATURE_LAYERING)) {
      ASSERT_EQ(0, image_ctx->operations->snap_protect(
                     cls::rbd::UserSnapshotNamespace{}, "snap"));

      uint64_t snap_id = image_ctx->snap_ids[
        {cls::rbd::UserSnapshotNamespace{}, "snap"}];
      ASSERT_NE(CEPH_NOSNAP, snap_id);

      C_SaferCond ctx;
      image_ctx->state->snap_set(snap_id, &ctx);
      ASSERT_EQ(0, ctx.wait());
    }
  }

  void expect_get_min_compat_client(int8_t min_compat_client, int r) {
    auto mock_rados_client = get_mock_io_ctx(m_ioctx).get_mock_rados_client();
    EXPECT_CALL(*mock_rados_client, get_min_compatible_client(_, _))
      .WillOnce(Invoke([min_compat_client, r](int8_t* min, int8_t* required_min) {
                  *min = min_compat_client;
                  *required_min = min_compat_client;
                  return r;
                }));
  }

  void expect_get_image_size(MockTestImageCtx &mock_image_ctx, uint64_t snap_id,
                             uint64_t size) {
    EXPECT_CALL(mock_image_ctx, get_image_size(snap_id))
      .WillOnce(Return(size));
  }

  void expect_is_snap_protected(MockImageCtx &mock_image_ctx, bool is_protected,
                                int r) {
    EXPECT_CALL(mock_image_ctx, is_snap_protected(_, _))
      .WillOnce(WithArg<1>(Invoke([is_protected, r](bool* is_prot) {
                             *is_prot = is_protected;
                             return r;
                           })));
  }

  void expect_create(MockCreateRequest& mock_create_request, int r) {
    EXPECT_CALL(mock_create_request, send())
      .WillOnce(Invoke([this, &mock_create_request, r]() {
                  image_ctx->op_work_queue->queue(mock_create_request.on_finish, r);
                }));
  }

  void expect_open(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.state, open(true, _))
      .WillOnce(WithArg<1>(Invoke([this, r](Context* ctx) {
                             image_ctx->op_work_queue->queue(ctx, r);
                           })));
    if (r < 0) {
      EXPECT_CALL(mock_image_ctx, destroy());
    }
  }

  void expect_attach_parent(MockAttachParentRequest& mock_request, int r) {
    EXPECT_CALL(mock_request, send())
      .WillOnce(Invoke([this, &mock_request, r]() {
                  image_ctx->op_work_queue->queue(mock_request.on_finish, r);
                }));
  }

  void expect_attach_child(MockAttachChildRequest& mock_request,
                           uint32_t clone_format, int r) {
    EXPECT_CALL(mock_request, send())
      .WillOnce(Invoke([this, &mock_request, clone_format, r]() {
                  EXPECT_EQ(mock_request.clone_format, clone_format);
                  image_ctx->op_work_queue->queue(mock_request.on_finish, r);
                }));
  }

  void expect_metadata_copy(MockMetadataCopyRequest& mock_request, int r) {
    EXPECT_CALL(mock_request, send())
      .WillOnce(Invoke([this, &mock_request, r]() {
        image_ctx->op_work_queue->queue(mock_request.on_finish, r);
       }));
  }

  void expect_test_features(MockTestImageCtx &mock_image_ctx,
                            uint64_t features, bool enabled) {
    EXPECT_CALL(mock_image_ctx, test_features(features))
      .WillOnce(Return(enabled));
  }

  void expect_mirror_mode_get(MockTestImageCtx &mock_image_ctx,
                              cls::rbd::MirrorMode mirror_mode, int r) {
    bufferlist out_bl;
    encode(static_cast<uint32_t>(mirror_mode), out_bl);

    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(RBD_MIRRORING, _, StrEq("rbd"), StrEq("mirror_mode_get"),
                     _, _, _))
      .WillOnce(WithArg<5>(Invoke([out_bl, r](bufferlist* out) {
                             *out = out_bl;
                             return r;
                           })));
  }

  void expect_mirror_enable(MockMirrorEnableRequest& mock_mirror_enable_request,
                            int r) {
    EXPECT_CALL(mock_mirror_enable_request, send())
      .WillOnce(Invoke([this, &mock_mirror_enable_request, r]() {
                  image_ctx->op_work_queue->queue(mock_mirror_enable_request.on_finish, r);
                }));
  }

  void expect_close(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.state, close(_))
      .WillOnce(Invoke([this, r](Context* ctx) {
                  image_ctx->op_work_queue->queue(ctx, r);
                }));
    EXPECT_CALL(mock_image_ctx, destroy());
  }

  void expect_remove(MockRemoveRequest& mock_remove_request, int r) {
    EXPECT_CALL(mock_remove_request, send())
      .WillOnce(Invoke([this, &mock_remove_request, r]() {
                  image_ctx->op_work_queue->queue(mock_remove_request.on_finish, r);
                }));
  }

  librbd::ImageCtx *image_ctx;
};

TEST_F(TestMockImageCloneRequest, SuccessV1) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  ASSERT_EQ(0, _rados.conf_set("rbd_default_clone_format", "1"));

  MockTestImageCtx mock_image_ctx(*image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_open(mock_image_ctx, 0);

  expect_get_image_size(mock_image_ctx, mock_image_ctx.snaps.front(), 123);
  expect_is_snap_protected(mock_image_ctx, true, 0);

  MockCreateRequest mock_create_request;
  expect_create(mock_create_request, 0);

  expect_open(mock_image_ctx, 0);

  MockAttachParentRequest mock_attach_parent_request;
  expect_attach_parent(mock_attach_parent_request, 0);

  MockAttachChildRequest mock_attach_child_request;
  expect_attach_child(mock_attach_child_request, 1, 0);

  MockMetadataCopyRequest mock_request;
  expect_metadata_copy(mock_request, 0);

  MockMirrorEnableRequest mock_mirror_enable_request;
  if (is_feature_enabled(RBD_FEATURE_JOURNALING)) {
    expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING, true);
    expect_mirror_mode_get(mock_image_ctx, cls::rbd::MIRROR_MODE_POOL, 0);

    expect_mirror_enable(mock_mirror_enable_request, 0);
  } else {
    expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING, false);
  }

  expect_close(mock_image_ctx, 0);
  expect_close(mock_image_ctx, 0);

  C_SaferCond ctx;
  ImageOptions clone_opts;
  auto req = new MockCloneRequest(m_cct->_conf, m_ioctx, "parent id", "", {}, 123,
                                  m_ioctx, "clone name", "clone id", clone_opts,
                                  cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "", "",
                                  image_ctx->op_work_queue, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageCloneRequest, SuccessV2) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  ASSERT_EQ(0, _rados.conf_set("rbd_default_clone_format", "2"));

  MockTestImageCtx mock_image_ctx(*image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_open(mock_image_ctx, 0);

  expect_get_image_size(mock_image_ctx, mock_image_ctx.snaps.front(), 123);
  expect_is_snap_protected(mock_image_ctx, true, 0);

  MockCreateRequest mock_create_request;
  expect_create(mock_create_request, 0);

  expect_open(mock_image_ctx, 0);

  MockAttachParentRequest mock_attach_parent_request;
  expect_attach_parent(mock_attach_parent_request, 0);

  MockAttachChildRequest mock_attach_child_request;
  expect_attach_child(mock_attach_child_request, 2, 0);

  MockMetadataCopyRequest mock_request;
  expect_metadata_copy(mock_request, 0);

  MockMirrorEnableRequest mock_mirror_enable_request;
  if (is_feature_enabled(RBD_FEATURE_JOURNALING)) {
    expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING, true);
    expect_mirror_mode_get(mock_image_ctx, cls::rbd::MIRROR_MODE_POOL, 0);

    expect_mirror_enable(mock_mirror_enable_request, 0);
  } else {
    expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING, false);
  }

  expect_close(mock_image_ctx, 0);
  expect_close(mock_image_ctx, 0);

  C_SaferCond ctx;
  ImageOptions clone_opts;
  auto req = new MockCloneRequest(m_cct->_conf, m_ioctx, "parent id", "", {}, 123,
                                  m_ioctx, "clone name", "clone id", clone_opts,
                                  cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "", "",
                                  image_ctx->op_work_queue, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageCloneRequest, SuccessAuto) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  ASSERT_EQ(0, _rados.conf_set("rbd_default_clone_format", "auto"));

  MockTestImageCtx mock_image_ctx(*image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_open(mock_image_ctx, 0);

  expect_get_image_size(mock_image_ctx, mock_image_ctx.snaps.front(), 123);
  expect_is_snap_protected(mock_image_ctx, true, 0);

  MockCreateRequest mock_create_request;
  expect_create(mock_create_request, 0);

  expect_open(mock_image_ctx, 0);

  MockAttachParentRequest mock_attach_parent_request;
  expect_attach_parent(mock_attach_parent_request, 0);

  MockAttachChildRequest mock_attach_child_request;
  expect_attach_child(mock_attach_child_request, 2, 0);

  MockMetadataCopyRequest mock_request;
  expect_metadata_copy(mock_request, 0);

  MockMirrorEnableRequest mock_mirror_enable_request;
  if (is_feature_enabled(RBD_FEATURE_JOURNALING)) {
    expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING, true);
    expect_mirror_mode_get(mock_image_ctx, cls::rbd::MIRROR_MODE_POOL, 0);

    expect_mirror_enable(mock_mirror_enable_request, 0);
  } else {
    expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING, false);
  }

  expect_close(mock_image_ctx, 0);
  expect_close(mock_image_ctx, 0);

  C_SaferCond ctx;
  ImageOptions clone_opts;
  auto req = new MockCloneRequest(m_cct->_conf, m_ioctx, "parent id", "", {}, 123,
                                  m_ioctx, "clone name", "clone id", clone_opts,
                                  cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "", "",
                                  image_ctx->op_work_queue, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageCloneRequest, OpenParentError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_open(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  ImageOptions clone_opts;
  auto req = new MockCloneRequest(m_cct->_conf, m_ioctx, "parent id", "", {}, 123,
                                  m_ioctx, "clone name", "clone id", clone_opts,
                                  cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "", "",
                                  image_ctx->op_work_queue, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageCloneRequest, CreateError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_open(mock_image_ctx, 0);

  expect_get_image_size(mock_image_ctx, mock_image_ctx.snaps.front(), 123);
  expect_is_snap_protected(mock_image_ctx, true, 0);

  MockCreateRequest mock_create_request;
  expect_create(mock_create_request, -EINVAL);

  expect_close(mock_image_ctx, 0);

  C_SaferCond ctx;
  ImageOptions clone_opts;
  auto req = new MockCloneRequest(m_cct->_conf, m_ioctx, "parent id", "", {}, 123,
                                  m_ioctx, "clone name", "clone id", clone_opts,
                                  cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "", "",
                                  image_ctx->op_work_queue, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageCloneRequest, OpenError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_open(mock_image_ctx, 0);

  expect_get_image_size(mock_image_ctx, mock_image_ctx.snaps.front(), 123);
  expect_is_snap_protected(mock_image_ctx, true, 0);

  MockCreateRequest mock_create_request;
  expect_create(mock_create_request, 0);

  expect_open(mock_image_ctx, -EINVAL);

  MockRemoveRequest mock_remove_request;
  expect_remove(mock_remove_request, 0);

  expect_close(mock_image_ctx, 0);

  C_SaferCond ctx;
  ImageOptions clone_opts;
  auto req = new MockCloneRequest(m_cct->_conf, m_ioctx, "parent id", "", {}, 123,
                                  m_ioctx, "clone name", "clone id", clone_opts,
                                  cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "", "",
                                  image_ctx->op_work_queue, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageCloneRequest, AttachParentError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_open(mock_image_ctx, 0);

  expect_get_image_size(mock_image_ctx, mock_image_ctx.snaps.front(), 123);
  expect_is_snap_protected(mock_image_ctx, true, 0);

  MockCreateRequest mock_create_request;
  expect_create(mock_create_request, 0);

  expect_open(mock_image_ctx, 0);

  MockAttachParentRequest mock_attach_parent_request;
  expect_attach_parent(mock_attach_parent_request, -EINVAL);

  expect_close(mock_image_ctx, 0);

  MockRemoveRequest mock_remove_request;
  expect_remove(mock_remove_request, 0);

  expect_close(mock_image_ctx, 0);

  C_SaferCond ctx;
  ImageOptions clone_opts;
  auto req = new MockCloneRequest(m_cct->_conf, m_ioctx, "parent id", "", {}, 123,
                                  m_ioctx, "clone name", "clone id", clone_opts,
                                  cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "", "",
                                  image_ctx->op_work_queue, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageCloneRequest, AttachChildError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_open(mock_image_ctx, 0);

  expect_get_image_size(mock_image_ctx, mock_image_ctx.snaps.front(), 123);
  expect_is_snap_protected(mock_image_ctx, true, 0);

  MockCreateRequest mock_create_request;
  expect_create(mock_create_request, 0);

  expect_open(mock_image_ctx, 0);

  MockAttachParentRequest mock_attach_parent_request;
  expect_attach_parent(mock_attach_parent_request, 0);

  MockAttachChildRequest mock_attach_child_request;
  expect_attach_child(mock_attach_child_request, 2, -EINVAL);

  expect_close(mock_image_ctx, 0);

  MockRemoveRequest mock_remove_request;
  expect_remove(mock_remove_request, 0);

  expect_close(mock_image_ctx, 0);

  C_SaferCond ctx;
  ImageOptions clone_opts;
  auto req = new MockCloneRequest(m_cct->_conf, m_ioctx, "parent id", "", {}, 123,
                                  m_ioctx, "clone name", "clone id", clone_opts,
                                  cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "", "",
                                  image_ctx->op_work_queue, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageCloneRequest, MetadataCopyError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_open(mock_image_ctx, 0);

  expect_get_image_size(mock_image_ctx, mock_image_ctx.snaps.front(), 123);
  expect_is_snap_protected(mock_image_ctx, true, 0);

  MockCreateRequest mock_create_request;
  expect_create(mock_create_request, 0);

  expect_open(mock_image_ctx, 0);

  MockAttachParentRequest mock_attach_parent_request;
  expect_attach_parent(mock_attach_parent_request, 0);

  MockAttachChildRequest mock_attach_child_request;
  expect_attach_child(mock_attach_child_request, 2, 0);

  MockMetadataCopyRequest mock_request;
  expect_metadata_copy(mock_request, -EINVAL);

  expect_close(mock_image_ctx, 0);

  MockRemoveRequest mock_remove_request;
  expect_remove(mock_remove_request, 0);

  expect_close(mock_image_ctx, 0);

  C_SaferCond ctx;
  ImageOptions clone_opts;
  auto req = new MockCloneRequest(m_cct->_conf, m_ioctx, "parent id", "", {}, 123,
                                  m_ioctx, "clone name", "clone id", clone_opts,
                                  cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "", "",
                                  image_ctx->op_work_queue, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageCloneRequest, GetMirrorModeError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING | RBD_FEATURE_JOURNALING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_open(mock_image_ctx, 0);

  expect_get_image_size(mock_image_ctx, mock_image_ctx.snaps.front(), 123);
  expect_is_snap_protected(mock_image_ctx, true, 0);

  MockCreateRequest mock_create_request;
  expect_create(mock_create_request, 0);

  expect_open(mock_image_ctx, 0);

  MockAttachParentRequest mock_attach_parent_request;
  expect_attach_parent(mock_attach_parent_request, 0);

  MockAttachChildRequest mock_attach_child_request;
  expect_attach_child(mock_attach_child_request, 2, 0);

  MockMetadataCopyRequest mock_request;
  expect_metadata_copy(mock_request, 0);

  expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING, true);
  expect_mirror_mode_get(mock_image_ctx, cls::rbd::MIRROR_MODE_POOL, -EINVAL);

  expect_close(mock_image_ctx, 0);

  MockRemoveRequest mock_remove_request;
  expect_remove(mock_remove_request, 0);

  expect_close(mock_image_ctx, 0);

  C_SaferCond ctx;
  ImageOptions clone_opts;
  auto req = new MockCloneRequest(m_cct->_conf, m_ioctx, "parent id", "", {}, 123,
                                  m_ioctx, "clone name", "clone id", clone_opts,
                                  cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "", "",
                                  image_ctx->op_work_queue, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageCloneRequest, MirrorEnableError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING | RBD_FEATURE_JOURNALING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_open(mock_image_ctx, 0);

  expect_get_image_size(mock_image_ctx, mock_image_ctx.snaps.front(), 123);
  expect_is_snap_protected(mock_image_ctx, true, 0);

  MockCreateRequest mock_create_request;
  expect_create(mock_create_request, 0);

  expect_open(mock_image_ctx, 0);

  MockAttachParentRequest mock_attach_parent_request;
  expect_attach_parent(mock_attach_parent_request, 0);

  MockAttachChildRequest mock_attach_child_request;
  expect_attach_child(mock_attach_child_request, 2, 0);

  MockMetadataCopyRequest mock_request;
  expect_metadata_copy(mock_request, 0);

  expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING, true);
  expect_mirror_mode_get(mock_image_ctx, cls::rbd::MIRROR_MODE_POOL, 0);

  MockMirrorEnableRequest mock_mirror_enable_request;
  expect_mirror_enable(mock_mirror_enable_request, -EINVAL);

  expect_close(mock_image_ctx, 0);

  MockRemoveRequest mock_remove_request;
  expect_remove(mock_remove_request, 0);

  expect_close(mock_image_ctx, 0);

  C_SaferCond ctx;
  ImageOptions clone_opts;
  auto req = new MockCloneRequest(m_cct->_conf, m_ioctx, "parent id", "", {}, 123,
                                  m_ioctx, "clone name", "clone id", clone_opts,
                                  cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "", "",
                                  image_ctx->op_work_queue, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageCloneRequest, CloseError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_open(mock_image_ctx, 0);

  expect_get_image_size(mock_image_ctx, mock_image_ctx.snaps.front(), 123);
  expect_is_snap_protected(mock_image_ctx, true, 0);

  MockCreateRequest mock_create_request;
  expect_create(mock_create_request, 0);

  expect_open(mock_image_ctx, 0);

  MockAttachParentRequest mock_attach_parent_request;
  expect_attach_parent(mock_attach_parent_request, 0);

  MockAttachChildRequest mock_attach_child_request;
  expect_attach_child(mock_attach_child_request, 2, 0);

  MockMetadataCopyRequest mock_request;
  expect_metadata_copy(mock_request, 0);

  expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING, false);

  expect_close(mock_image_ctx, -EINVAL);

  MockRemoveRequest mock_remove_request;
  expect_remove(mock_remove_request, 0);

  expect_close(mock_image_ctx, 0);

  C_SaferCond ctx;
  ImageOptions clone_opts;
  auto req = new MockCloneRequest(m_cct->_conf, m_ioctx, "parent id", "", {}, 123,
                                  m_ioctx, "clone name", "clone id", clone_opts,
                                  cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "", "",
                                  image_ctx->op_work_queue, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageCloneRequest, RemoveError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_open(mock_image_ctx, 0);

  expect_get_image_size(mock_image_ctx, mock_image_ctx.snaps.front(), 123);
  expect_is_snap_protected(mock_image_ctx, true, 0);

  MockCreateRequest mock_create_request;
  expect_create(mock_create_request, 0);

  expect_open(mock_image_ctx, -EINVAL);

  MockRemoveRequest mock_remove_request;
  expect_remove(mock_remove_request, -EPERM);

  expect_close(mock_image_ctx, 0);

  C_SaferCond ctx;
  ImageOptions clone_opts;
  auto req = new MockCloneRequest(m_cct->_conf, m_ioctx, "parent id", "", {}, 123,
                                  m_ioctx, "clone name", "clone id", clone_opts,
                                  cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "", "",
                                  image_ctx->op_work_queue, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageCloneRequest, CloseParentError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_open(mock_image_ctx, 0);

  expect_get_image_size(mock_image_ctx, mock_image_ctx.snaps.front(), 123);
  expect_is_snap_protected(mock_image_ctx, true, 0);

  MockCreateRequest mock_create_request;
  expect_create(mock_create_request, 0);

  expect_open(mock_image_ctx, -EINVAL);

  MockRemoveRequest mock_remove_request;
  expect_remove(mock_remove_request, 0);

  expect_close(mock_image_ctx, -EPERM);

  C_SaferCond ctx;
  ImageOptions clone_opts;
  auto req = new MockCloneRequest(m_cct->_conf, m_ioctx, "parent id", "", {}, 123,
                                  m_ioctx, "clone name", "clone id", clone_opts,
                                  cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "", "",
                                  image_ctx->op_work_queue, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageCloneRequest, SnapshotMirrorEnableNonPrimary) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_open(mock_image_ctx, 0);

  expect_get_image_size(mock_image_ctx, mock_image_ctx.snaps.front(), 123);
  expect_is_snap_protected(mock_image_ctx, true, 0);

  MockCreateRequest mock_create_request;
  expect_create(mock_create_request, 0);

  expect_open(mock_image_ctx, 0);

  MockAttachParentRequest mock_attach_parent_request;
  expect_attach_parent(mock_attach_parent_request, 0);

  MockAttachChildRequest mock_attach_child_request;
  expect_attach_child(mock_attach_child_request, 2, 0);

  MockMetadataCopyRequest mock_request;
  expect_metadata_copy(mock_request, 0);

  MockMirrorEnableRequest mock_mirror_enable_request;
  expect_mirror_enable(mock_mirror_enable_request, 0);

  expect_close(mock_image_ctx, 0);
  expect_close(mock_image_ctx, 0);

  C_SaferCond ctx;
  ImageOptions clone_opts;
  auto req = new MockCloneRequest(m_cct->_conf, m_ioctx, "parent id", "", {}, 123,
                                  m_ioctx, "clone name", "clone id", clone_opts,
                                  cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                                  "global image id", "primary mirror uuid",
                                  image_ctx->op_work_queue, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

} // namespace image
} // namespace librbd
