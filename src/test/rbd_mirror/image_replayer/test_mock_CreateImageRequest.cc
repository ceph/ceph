// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "include/rbd/librbd.hpp"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "tools/rbd_mirror/PoolMetaCache.h"
#include "tools/rbd_mirror/image_replayer/CreateImageRequest.h"
#include "tools/rbd_mirror/image_replayer/CloseImageRequest.h"
#include "tools/rbd_mirror/image_replayer/OpenImageRequest.h"
#include "tools/rbd_mirror/image_replayer/OpenLocalImageRequest.h"
#include "librbd/image/CreateRequest.h"
#include "librbd/image/CloneRequest.h"
#include "tools/rbd_mirror/Threads.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  explicit MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace image {

template<>
struct CreateRequest<librbd::MockTestImageCtx> {
  static CreateRequest *s_instance;
  Context *on_finish = nullptr;

  static CreateRequest *create(const ConfigProxy& config, IoCtx &ioctx,
                               const std::string &imgname,
                               const std::string &imageid, uint64_t size,
                               const librbd::ImageOptions &image_options,
                               bool skip_mirror_enable,
                               cls::rbd::MirrorImageMode mode,
                               const std::string &non_primary_global_image_id,
                               const std::string &primary_mirror_uuid,
                               MockContextWQ *op_work_queue,
                               Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    EXPECT_FALSE(non_primary_global_image_id.empty());
    EXPECT_FALSE(primary_mirror_uuid.empty());
    EXPECT_FALSE(skip_mirror_enable);
    s_instance->on_finish = on_finish;
    s_instance->construct(ioctx);
    return s_instance;
  }

  CreateRequest() {
    s_instance = this;
  }

  ~CreateRequest() {
    s_instance = nullptr;
  }

  MOCK_METHOD0(send, void());
  MOCK_METHOD1(construct, void(librados::IoCtx &ioctx));
};

CreateRequest<librbd::MockTestImageCtx>*
  CreateRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

template<>
struct CloneRequest<librbd::MockTestImageCtx> {
  static CloneRequest *s_instance;
  Context *on_finish = nullptr;

  static CloneRequest *create(ConfigProxy& config, IoCtx &p_ioctx,
                              const std::string &p_id,
                              const std::string &p_snap_name,
                              const cls::rbd::SnapshotNamespace& snap_ns,
                              uint64_t p_snap_id,
			      IoCtx &c_ioctx, const std::string &c_name,
			      const std::string &c_id, ImageOptions c_options,
                              cls::rbd::MirrorImageMode mode,
			      const std::string &non_primary_global_image_id,
			      const std::string &primary_mirror_uuid,
			      MockContextWQ *op_work_queue,
                              Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    s_instance->construct();
    return s_instance;
  }

  CloneRequest() {
    s_instance = this;
  }

  ~CloneRequest() {
    s_instance = nullptr;
  }

  MOCK_METHOD0(send, void());
  MOCK_METHOD0(construct, void());
};

CloneRequest<librbd::MockTestImageCtx>*
  CloneRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace image
} // namespace librbd

namespace rbd {
namespace mirror {

template <>
struct Threads<librbd::MockTestImageCtx> {
  ceph::mutex &timer_lock;
  SafeTimer *timer;
  librbd::asio::ContextWQ *work_queue;

  Threads(Threads<librbd::ImageCtx> *threads)
    : timer_lock(threads->timer_lock), timer(threads->timer),
      work_queue(threads->work_queue) {
  }
};

namespace image_replayer {

template<>
struct CloseImageRequest<librbd::MockTestImageCtx> {
  static CloseImageRequest* s_instance;
  Context *on_finish = nullptr;

  static CloseImageRequest* create(librbd::MockTestImageCtx **image_ctx,
                                   Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->construct(*image_ctx);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  CloseImageRequest() {
    ceph_assert(s_instance == nullptr);
    s_instance = this;
  }
  ~CloseImageRequest() {
    s_instance = nullptr;
  }

  MOCK_METHOD1(construct, void(librbd::MockTestImageCtx *image_ctx));
  MOCK_METHOD0(send, void());
};

template<>
struct OpenImageRequest<librbd::MockTestImageCtx> {
  static OpenImageRequest* s_instance;
  librbd::MockTestImageCtx **image_ctx = nullptr;
  Context *on_finish = nullptr;

  static OpenImageRequest* create(librados::IoCtx &io_ctx,
                                  librbd::MockTestImageCtx **image_ctx,
                                  const std::string &image_id,
                                  bool read_only, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->image_ctx = image_ctx;
    s_instance->on_finish = on_finish;
    s_instance->construct(io_ctx, image_id);
    return s_instance;
  }

  OpenImageRequest() {
    ceph_assert(s_instance == nullptr);
    s_instance = this;
  }
  ~OpenImageRequest() {
    s_instance = nullptr;
  }

  MOCK_METHOD2(construct, void(librados::IoCtx &io_ctx,
                               const std::string &image_id));
  MOCK_METHOD0(send, void());
};

CloseImageRequest<librbd::MockTestImageCtx>*
  CloseImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
OpenImageRequest<librbd::MockTestImageCtx>*
  OpenImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

// template definitions
#include "tools/rbd_mirror/image_replayer/CreateImageRequest.cc"
template class rbd::mirror::image_replayer::CreateImageRequest<librbd::MockTestImageCtx>;

namespace rbd {
namespace mirror {
namespace image_replayer {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

MATCHER_P(IsSameIoCtx, io_ctx, "") {
  return &get_mock_io_ctx(arg) == &get_mock_io_ctx(*io_ctx);
}

class TestMockImageReplayerCreateImageRequest : public TestMockFixture {
public:
  typedef Threads<librbd::MockTestImageCtx> MockThreads;
  typedef librbd::image::CreateRequest<librbd::MockTestImageCtx> MockCreateRequest;
  typedef librbd::image::CloneRequest<librbd::MockTestImageCtx> MockCloneRequest;
  typedef CreateImageRequest<librbd::MockTestImageCtx> MockCreateImageRequest;
  typedef OpenImageRequest<librbd::MockTestImageCtx> MockOpenImageRequest;
  typedef CloseImageRequest<librbd::MockTestImageCtx> MockCloseImageRequest;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_remote_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_remote_io_ctx, m_image_name, &m_remote_image_ctx));
  }

  void snap_create(librbd::ImageCtx *image_ctx, const std::string &snap_name) {
    librbd::NoOpProgressContext prog_ctx;
    ASSERT_EQ(0, image_ctx->operations->snap_create(cls::rbd::UserSnapshotNamespace(),
					            snap_name, 0, prog_ctx));
    ASSERT_EQ(0, image_ctx->operations->snap_protect(cls::rbd::UserSnapshotNamespace(),
						     snap_name));
    ASSERT_EQ(0, image_ctx->state->refresh());
  }

  int clone_image(librbd::ImageCtx *parent_image_ctx,
                  const std::string &snap_name, const std::string &clone_name) {
    snap_create(parent_image_ctx, snap_name);

    int order = 0;
    return librbd::clone(m_remote_io_ctx, parent_image_ctx->name.c_str(),
                         snap_name.c_str(), m_remote_io_ctx,
                         clone_name.c_str(), parent_image_ctx->features,
                         &order, 0, 0);
  }

  void expect_create_image(MockCreateRequest &mock_create_request,
                           librados::IoCtx &ioctx, int r) {
    EXPECT_CALL(mock_create_request, construct(IsSameIoCtx(&ioctx)));
    EXPECT_CALL(mock_create_request, send())
      .WillOnce(Invoke([this, &mock_create_request, r]() {
            m_threads->work_queue->queue(mock_create_request.on_finish, r);
          }));
  }

  void expect_ioctx_create(librados::IoCtx &io_ctx) {
    librados::MockTestMemIoCtxImpl &io_ctx_impl = get_mock_io_ctx(io_ctx);
    EXPECT_CALL(*get_mock_io_ctx(io_ctx).get_mock_rados_client(), create_ioctx(_, _))
      .WillOnce(DoAll(GetReference(&io_ctx_impl),
                      Return(&get_mock_io_ctx(io_ctx))));
  }

  void expect_get_parent_global_image_id(librados::IoCtx &io_ctx,
                                         const std::string &global_id, int r) {
    cls::rbd::MirrorImage mirror_image;
    mirror_image.global_image_id = global_id;

    bufferlist bl;
    encode(mirror_image, bl);

    EXPECT_CALL(get_mock_io_ctx(io_ctx),
                exec(RBD_MIRRORING, _, StrEq("rbd"), StrEq("mirror_image_get"), _, _, _))
      .WillOnce(DoAll(WithArg<5>(Invoke([bl](bufferlist *out_bl) {
                                          *out_bl = bl;
                                        })),
                      Return(r)));
  }

  void expect_mirror_image_get_image_id(librados::IoCtx &io_ctx,
                                        const std::string &image_id, int r) {
    bufferlist bl;
    encode(image_id, bl);

    EXPECT_CALL(get_mock_io_ctx(io_ctx),
                exec(RBD_MIRRORING, _, StrEq("rbd"), StrEq("mirror_image_get_image_id"), _, _, _))
      .WillOnce(DoAll(WithArg<5>(Invoke([bl](bufferlist *out_bl) {
                                          *out_bl = bl;
                                        })),
                      Return(r)));
  }

  void expect_open_image(MockOpenImageRequest &mock_open_image_request,
                         librados::IoCtx &io_ctx, const std::string &image_id,
                         librbd::MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(mock_open_image_request, construct(IsSameIoCtx(&io_ctx), image_id));
    EXPECT_CALL(mock_open_image_request, send())
      .WillOnce(Invoke([this, &mock_open_image_request, &mock_image_ctx, r]() {
          *mock_open_image_request.image_ctx = &mock_image_ctx;
          m_threads->work_queue->queue(mock_open_image_request.on_finish, r);
        }));
  }

  void expect_test_op_features(librbd::MockTestImageCtx& mock_image_ctx,
                               bool enabled) {
    EXPECT_CALL(mock_image_ctx,
                test_op_features(RBD_OPERATION_FEATURE_CLONE_CHILD))
      .WillOnce(Return(enabled));
  }

  void expect_clone_image(MockCloneRequest &mock_clone_request,
                          int r) {
    EXPECT_CALL(mock_clone_request, construct());
    EXPECT_CALL(mock_clone_request, send())
      .WillOnce(Invoke([this, &mock_clone_request, r]() {
            m_threads->work_queue->queue(mock_clone_request.on_finish, r);
          }));
  }

  void expect_close_image(MockCloseImageRequest &mock_close_image_request,
                          librbd::MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(mock_close_image_request, construct(&mock_image_ctx));
    EXPECT_CALL(mock_close_image_request, send())
      .WillOnce(Invoke([this, &mock_close_image_request, r]() {
          m_threads->work_queue->queue(mock_close_image_request.on_finish, r);
        }));
  }

  MockCreateImageRequest *create_request(MockThreads* mock_threads,
                                         const std::string &global_image_id,
                                         const std::string &remote_mirror_uuid,
                                         const std::string &local_image_name,
					 const std::string &local_image_id,
                                         librbd::MockTestImageCtx &mock_remote_image_ctx,
                                         Context *on_finish) {
    return new MockCreateImageRequest(mock_threads, m_local_io_ctx,
                                      global_image_id, remote_mirror_uuid,
                                      local_image_name, local_image_id,
                                      &mock_remote_image_ctx,
                                      &m_pool_meta_cache,
                                      cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                                      on_finish);
  }

  PoolMetaCache m_pool_meta_cache{g_ceph_context};
  librbd::ImageCtx *m_remote_image_ctx;
};

TEST_F(TestMockImageReplayerCreateImageRequest, Create) {
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockCreateRequest mock_create_request;

  InSequence seq;
  expect_create_image(mock_create_request, m_local_io_ctx, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockCreateImageRequest *request = create_request(&mock_threads, "global uuid",
                                                   "remote uuid", "image name",
                                                   "101241a7c4c9",
                                                   mock_remote_image_ctx, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageReplayerCreateImageRequest, CreateError) {
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockCreateRequest mock_create_request;

  InSequence seq;
  expect_create_image(mock_create_request, m_local_io_ctx, -EINVAL);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockCreateImageRequest *request = create_request(&mock_threads, "global uuid",
                                                   "remote uuid", "image name",
                                                   "101241a7c4c9",
                                                   mock_remote_image_ctx, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerCreateImageRequest, CloneGetGlobalImageIdError) {
  std::string clone_image_name = get_temp_image_name();
  ASSERT_EQ(0, clone_image(m_remote_image_ctx, "snap", clone_image_name));

  librbd::ImageCtx *remote_clone_image_ctx;
  ASSERT_EQ(0, open_image(m_remote_io_ctx, clone_image_name,
               &remote_clone_image_ctx));

  librbd::MockTestImageCtx mock_remote_clone_image_ctx(*remote_clone_image_ctx);

  InSequence seq;
  expect_ioctx_create(m_remote_io_ctx);
  expect_ioctx_create(m_local_io_ctx);
  expect_get_parent_global_image_id(m_remote_io_ctx, "global uuid", -ENOENT);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockCreateImageRequest *request = create_request(&mock_threads, "global uuid",
                                                   "remote uuid", "image name",
                                                   "101241a7c4c9",
                                                   mock_remote_clone_image_ctx,
                                                   &ctx);
  request->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockImageReplayerCreateImageRequest, CloneGetLocalParentImageIdError) {
  std::string clone_image_name = get_temp_image_name();
  ASSERT_EQ(0, clone_image(m_remote_image_ctx, "snap", clone_image_name));

  librbd::ImageCtx *remote_clone_image_ctx;
  ASSERT_EQ(0, open_image(m_remote_io_ctx, clone_image_name,
               &remote_clone_image_ctx));

  librbd::MockTestImageCtx mock_remote_clone_image_ctx(*remote_clone_image_ctx);

  InSequence seq;
  expect_ioctx_create(m_remote_io_ctx);
  expect_ioctx_create(m_local_io_ctx);
  expect_get_parent_global_image_id(m_remote_io_ctx, "global uuid", 0);
  expect_mirror_image_get_image_id(m_local_io_ctx, "local parent id", -ENOENT);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockCreateImageRequest *request = create_request(&mock_threads, "global uuid",
                                                   "remote uuid", "image name",
                                                   "101241a7c4c9",
                                                   mock_remote_clone_image_ctx,
                                                   &ctx);
  request->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockImageReplayerCreateImageRequest, CloneOpenRemoteParentError) {
  std::string clone_image_name = get_temp_image_name();
  ASSERT_EQ(0, clone_image(m_remote_image_ctx, "snap", clone_image_name));

  librbd::ImageCtx *remote_clone_image_ctx;
  ASSERT_EQ(0, open_image(m_remote_io_ctx, clone_image_name,
               &remote_clone_image_ctx));

  librbd::MockTestImageCtx mock_remote_parent_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_remote_clone_image_ctx(*remote_clone_image_ctx);
  MockOpenImageRequest mock_open_image_request;

  InSequence seq;
  expect_ioctx_create(m_remote_io_ctx);
  expect_ioctx_create(m_local_io_ctx);
  expect_get_parent_global_image_id(m_remote_io_ctx, "global uuid", 0);
  expect_mirror_image_get_image_id(m_local_io_ctx, "local parent id", 0);

  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    m_remote_image_ctx->id, mock_remote_parent_image_ctx,
                    -ENOENT);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockCreateImageRequest *request = create_request(&mock_threads, "global uuid",
                                                   "remote uuid", "image name",
                                                   "101241a7c4c9",
                                                   mock_remote_clone_image_ctx,
                                                   &ctx);
  request->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockImageReplayerCreateImageRequest, CloneParentImageSyncing) {
  librbd::RBD rbd;
  librbd::ImageCtx *local_image_ctx;
  ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
  ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &local_image_ctx));
  snap_create(local_image_ctx, "snap");
  snap_create(m_remote_image_ctx, ".rbd-mirror.local parent uuid.1234");

  std::string clone_image_name = get_temp_image_name();
  ASSERT_EQ(0, clone_image(m_remote_image_ctx, "snap", clone_image_name));

  librbd::ImageCtx *remote_clone_image_ctx;
  ASSERT_EQ(0, open_image(m_remote_io_ctx, clone_image_name,
               &remote_clone_image_ctx));

  m_pool_meta_cache.set_local_pool_meta(
    m_local_io_ctx.get_id(), {"local parent uuid"});

  librbd::MockTestImageCtx mock_remote_parent_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_remote_clone_image_ctx(*remote_clone_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  MockCloseImageRequest mock_close_image_request;

  InSequence seq;
  expect_ioctx_create(m_remote_io_ctx);
  expect_ioctx_create(m_local_io_ctx);
  expect_get_parent_global_image_id(m_remote_io_ctx, "global uuid", 0);
  expect_mirror_image_get_image_id(m_local_io_ctx, "local parent id", 0);

  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    m_remote_image_ctx->id, mock_remote_parent_image_ctx, 0);
  expect_close_image(mock_close_image_request, mock_remote_parent_image_ctx, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockCreateImageRequest *request = create_request(&mock_threads, "global uuid",
                                                   "remote uuid", "image name",
                                                   "101241a7c4c9",
                                                   mock_remote_clone_image_ctx,
                                                   &ctx);
  request->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockImageReplayerCreateImageRequest, CloneError) {
  librbd::RBD rbd;
  librbd::ImageCtx *local_image_ctx;
  ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
  ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &local_image_ctx));
  snap_create(local_image_ctx, "snap");

  std::string clone_image_name = get_temp_image_name();
  ASSERT_EQ(0, clone_image(m_remote_image_ctx, "snap", clone_image_name));

  librbd::ImageCtx *remote_clone_image_ctx;
  ASSERT_EQ(0, open_image(m_remote_io_ctx, clone_image_name,
               &remote_clone_image_ctx));

  m_pool_meta_cache.set_local_pool_meta(
    m_local_io_ctx.get_id(), {"local parent uuid"});

  librbd::MockTestImageCtx mock_remote_parent_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_remote_clone_image_ctx(*remote_clone_image_ctx);
  MockCloneRequest mock_clone_request;
  MockOpenImageRequest mock_open_image_request;
  MockCloseImageRequest mock_close_image_request;

  InSequence seq;
  expect_ioctx_create(m_remote_io_ctx);
  expect_ioctx_create(m_local_io_ctx);
  expect_get_parent_global_image_id(m_remote_io_ctx, "global uuid", 0);
  expect_mirror_image_get_image_id(m_local_io_ctx, "local parent id", 0);

  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    m_remote_image_ctx->id, mock_remote_parent_image_ctx, 0);
  expect_test_op_features(mock_remote_clone_image_ctx, false);
  expect_clone_image(mock_clone_request, -EINVAL);
  expect_close_image(mock_close_image_request, mock_remote_parent_image_ctx, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockCreateImageRequest *request = create_request(&mock_threads, "global uuid",
                                                   "remote uuid", "image name",
                                                   "101241a7c4c9",
                                                   mock_remote_clone_image_ctx,
                                                   &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerCreateImageRequest, CloneRemoteParentCloseError) {
  librbd::RBD rbd;
  librbd::ImageCtx *local_image_ctx;
  ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
  ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &local_image_ctx));
  snap_create(local_image_ctx, "snap");

  std::string clone_image_name = get_temp_image_name();
  ASSERT_EQ(0, clone_image(m_remote_image_ctx, "snap", clone_image_name));

  librbd::ImageCtx *remote_clone_image_ctx;
  ASSERT_EQ(0, open_image(m_remote_io_ctx, clone_image_name,
               &remote_clone_image_ctx));

  m_pool_meta_cache.set_local_pool_meta(
    m_local_io_ctx.get_id(), {"local parent uuid"});

  librbd::MockTestImageCtx mock_remote_parent_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_remote_clone_image_ctx(*remote_clone_image_ctx);
  MockCloneRequest mock_clone_request;
  MockOpenImageRequest mock_open_image_request;
  MockCloseImageRequest mock_close_image_request;

  InSequence seq;
  expect_ioctx_create(m_remote_io_ctx);
  expect_ioctx_create(m_local_io_ctx);
  expect_get_parent_global_image_id(m_remote_io_ctx, "global uuid", 0);
  expect_mirror_image_get_image_id(m_local_io_ctx, "local parent id", 0);

  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    m_remote_image_ctx->id, mock_remote_parent_image_ctx, 0);
  expect_test_op_features(mock_remote_clone_image_ctx, false);
  expect_clone_image(mock_clone_request, 0);
  expect_close_image(mock_close_image_request, mock_remote_parent_image_ctx,
                     -EINVAL);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockCreateImageRequest *request = create_request(&mock_threads, "global uuid",
                                                   "remote uuid", "image name",
                                                   "101241a7c4c9",
                                                   mock_remote_clone_image_ctx,
                                                   &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd
