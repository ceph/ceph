// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "include/rbd/librbd.hpp"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "tools/rbd_mirror/image_replayer/CreateImageRequest.h"
#include "tools/rbd_mirror/image_replayer/CloseImageRequest.h"
#include "tools/rbd_mirror/image_replayer/OpenImageRequest.h"
#include "tools/rbd_mirror/image_replayer/OpenLocalImageRequest.h"
#include "tools/rbd_mirror/image_replayer/Utils.h"
#include "tools/rbd_mirror/Threads.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

} // namespace librbd

namespace rbd {
namespace mirror {
namespace image_replayer {

struct CreateCloneImage {
  static CreateCloneImage *s_instance;
  static CreateCloneImage *get_instance() {
    assert(s_instance != nullptr);
    return s_instance;
  }

  CreateCloneImage() {
    assert(s_instance == nullptr);
    s_instance = this;
  }
  ~CreateCloneImage() {
    s_instance = nullptr;
  }

  MOCK_METHOD3(create, int(const std::string &image_name,
                           const std::string &non_primary_global_image_id,
                           const std::string &primary_mirror_uuid));
  MOCK_METHOD3(clone, int(const std::string &image_name,
                          const std::string &non_primary_global_image_id,
                          const std::string &primary_mirror_uuid));
};

CreateCloneImage *CreateCloneImage::s_instance = nullptr;

namespace utils {

template <>
int create_image<librbd::MockTestImageCtx>(librados::IoCtx& io_ctx,
                                           librbd::MockTestImageCtx *_image_ctx,
                                           const char *imgname, uint64_t bid,
                                           uint64_t size, int order,
                                           uint64_t features,
                                           uint64_t stripe_unit,
                                           uint64_t stripe_count,
                                           uint8_t journal_order,
                                           uint8_t journal_splay_width,
                                           const std::string &journal_pool,
                                           const std::string &non_primary_global_image_id,
                                           const std::string &primary_mirror_uuid) {
  return CreateCloneImage::get_instance()->create(imgname,
                                                  non_primary_global_image_id,
                                                  primary_mirror_uuid);
}

template <>
int clone_image<librbd::MockTestImageCtx>(librbd::MockTestImageCtx *p_imctx,
                                          librados::IoCtx& c_ioctx,
                                          const char *c_name,
                                          librbd::ImageOptions& c_opts,
                                          const std::string &non_primary_global_image_id,
                                          const std::string &remote_mirror_uuid) {
  return CreateCloneImage::get_instance()->clone(c_name,
                                                 non_primary_global_image_id,
                                                 remote_mirror_uuid);
}

} // namespace utils

template<>
struct CloseImageRequest<librbd::MockTestImageCtx> {
  static CloseImageRequest* s_instance;
  Context *on_finish = nullptr;

  static CloseImageRequest* create(librbd::MockTestImageCtx **image_ctx,
                                   ContextWQ *work_queue, bool destroy_only,
                                   Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->construct(*image_ctx);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  CloseImageRequest() {
    assert(s_instance == nullptr);
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
                                  bool read_only, ContextWQ *work_queue,
                                  Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->image_ctx = image_ctx;
    s_instance->on_finish = on_finish;
    s_instance->construct(io_ctx, image_id);
    return s_instance;
  }

  OpenImageRequest() {
    assert(s_instance == nullptr);
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
  typedef CreateImageRequest<librbd::MockTestImageCtx> MockCreateImageRequest;
  typedef OpenImageRequest<librbd::MockTestImageCtx> MockOpenImageRequest;
  typedef CloseImageRequest<librbd::MockTestImageCtx> MockCloseImageRequest;

  virtual void SetUp() {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_remote_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_remote_io_ctx, m_image_name, &m_remote_image_ctx));
  }

  int clone_image(librbd::ImageCtx *parent_image_ctx,
                  const std::string &snap_name, const std::string &clone_name) {
    {
      librbd::ImageCtx *ictx = new librbd::ImageCtx(parent_image_ctx->name,
						    "", "", m_remote_io_ctx,
                                                    false);
      ictx->state->open(false);
      EXPECT_EQ(0, ictx->operations->snap_create(snap_name.c_str()));
      EXPECT_EQ(0, ictx->operations->snap_protect(snap_name.c_str()));
      ictx->state->close();
    }

    EXPECT_EQ(0, parent_image_ctx->state->refresh());

    int order = 0;
    return librbd::clone(m_remote_io_ctx, parent_image_ctx->name.c_str(),
                         snap_name.c_str(), m_remote_io_ctx,
                         clone_name.c_str(), parent_image_ctx->features,
                         &order, 0, 0);
  }

  void expect_create_image(CreateCloneImage &create_clone_image,
                           const std::string &local_image_name,
                           const std::string &global_image_id,
                           const std::string &remote_mirror_uuid, int r) {
    EXPECT_CALL(create_clone_image, create(local_image_name, global_image_id,
                                           remote_mirror_uuid))
      .WillOnce(Return(r));
  }

  void expect_ioctx_create(librados::IoCtx &io_ctx) {
    EXPECT_CALL(*get_mock_io_ctx(io_ctx).get_mock_rados_client(), create_ioctx(_, _))
      .WillOnce(Return(&get_mock_io_ctx(io_ctx)));
  }

  void expect_get_parent_global_image_id(librados::IoCtx &io_ctx,
                                         const std::string &global_id, int r) {
    cls::rbd::MirrorImage mirror_image;
    mirror_image.global_image_id = global_id;

    bufferlist bl;
    ::encode(mirror_image, bl);

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
    ::encode(image_id, bl);

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

  void expect_snap_set(librbd::MockTestImageCtx &mock_image_ctx,
                       const std::string &snap_name, int r) {
    EXPECT_CALL(*mock_image_ctx.state, snap_set(StrEq(snap_name), _))
      .WillOnce(WithArg<1>(Invoke([this, r](Context *on_finish) {
          m_threads->work_queue->queue(on_finish, r);
        })));
  }

  void expect_clone_image(CreateCloneImage &create_clone_image,
                          const std::string &local_image_name,
                          const std::string &global_image_id,
                          const std::string &remote_mirror_uuid, int r) {
    EXPECT_CALL(create_clone_image, clone(local_image_name, global_image_id,
                                          remote_mirror_uuid))
      .WillOnce(Return(r));
  }

  void expect_close_image(MockCloseImageRequest &mock_close_image_request,
                          librbd::MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(mock_close_image_request, construct(&mock_image_ctx));
    EXPECT_CALL(mock_close_image_request, send())
      .WillOnce(Invoke([this, &mock_close_image_request, r]() {
          m_threads->work_queue->queue(mock_close_image_request.on_finish, r);
        }));
  }

  MockCreateImageRequest *create_request(const std::string &global_image_id,
                                         const std::string &remote_mirror_uuid,
                                         const std::string &local_image_name,
                                         librbd::MockTestImageCtx &mock_remote_image_ctx,
                                         Context *on_finish) {
    return new MockCreateImageRequest(m_local_io_ctx, m_threads->work_queue,
                                      global_image_id, remote_mirror_uuid,
                                      local_image_name, &mock_remote_image_ctx,
                                      on_finish);
  }

  librbd::ImageCtx *m_remote_image_ctx;
};

TEST_F(TestMockImageReplayerCreateImageRequest, Create) {
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);

  CreateCloneImage create_clone_image;

  InSequence seq;
  expect_create_image(create_clone_image, "image name", "global uuid",
                      "remote uuid", 0);

  C_SaferCond ctx;
  MockCreateImageRequest *request = create_request("global uuid", "remote uuid",
                                                   "image name",
                                                   mock_remote_image_ctx, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageReplayerCreateImageRequest, CreateError) {
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);

  CreateCloneImage create_clone_image;

  InSequence seq;
  expect_create_image(create_clone_image, "image name", "global uuid",
                      "remote uuid", -EINVAL);

  C_SaferCond ctx;
  MockCreateImageRequest *request = create_request("global uuid", "remote uuid",
                                                   "image name",
                                                   mock_remote_image_ctx, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerCreateImageRequest, Clone) {
  librbd::RBD rbd;
  librbd::ImageCtx *local_image_ctx;
  ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
  ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &local_image_ctx));

  std::string clone_image_name = get_temp_image_name();
  ASSERT_EQ(0, clone_image(m_remote_image_ctx, "snap", clone_image_name));

  librbd::ImageCtx *remote_clone_image_ctx;
  ASSERT_EQ(0, open_image(m_remote_io_ctx, clone_image_name,
               &remote_clone_image_ctx));

  librbd::MockTestImageCtx mock_remote_parent_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_parent_image_ctx(*local_image_ctx);
  librbd::MockTestImageCtx mock_remote_clone_image_ctx(*remote_clone_image_ctx);
  CreateCloneImage create_clone_image;
  MockOpenImageRequest mock_open_image_request;
  MockCloseImageRequest mock_close_image_request;

  InSequence seq;
  expect_ioctx_create(m_remote_io_ctx);
  expect_ioctx_create(m_local_io_ctx);
  expect_get_parent_global_image_id(m_remote_io_ctx, "global uuid", 0);
  expect_mirror_image_get_image_id(m_local_io_ctx, "local parent id", 0);

  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    m_remote_image_ctx->id, mock_remote_parent_image_ctx, 0);
  expect_open_image(mock_open_image_request, m_local_io_ctx,
                    "local parent id", mock_local_parent_image_ctx, 0);
  expect_snap_set(mock_local_parent_image_ctx, "snap", 0);
  expect_clone_image(create_clone_image, "image name", "global uuid",
                      "remote uuid", 0);
  expect_close_image(mock_close_image_request, mock_local_parent_image_ctx, 0);
  expect_close_image(mock_close_image_request, mock_remote_parent_image_ctx, 0);

  C_SaferCond ctx;
  MockCreateImageRequest *request = create_request("global uuid", "remote uuid",
                                                   "image name",
                                                   mock_remote_clone_image_ctx,
                                                   &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageReplayerCreateImageRequest, CloneGetGlobalImageIdError) {
  std::string clone_image_name = get_temp_image_name();
  ASSERT_EQ(0, clone_image(m_remote_image_ctx, "snap", clone_image_name));

  librbd::ImageCtx *remote_clone_image_ctx;
  ASSERT_EQ(0, open_image(m_remote_io_ctx, clone_image_name,
               &remote_clone_image_ctx));

  librbd::MockTestImageCtx mock_remote_clone_image_ctx(*remote_clone_image_ctx);
  CreateCloneImage create_clone_image;

  InSequence seq;
  expect_ioctx_create(m_remote_io_ctx);
  expect_ioctx_create(m_local_io_ctx);
  expect_get_parent_global_image_id(m_remote_io_ctx, "global uuid", -ENOENT);

  C_SaferCond ctx;
  MockCreateImageRequest *request = create_request("global uuid", "remote uuid",
                                                   "image name",
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
  CreateCloneImage create_clone_image;

  InSequence seq;
  expect_ioctx_create(m_remote_io_ctx);
  expect_ioctx_create(m_local_io_ctx);
  expect_get_parent_global_image_id(m_remote_io_ctx, "global uuid", 0);
  expect_mirror_image_get_image_id(m_local_io_ctx, "local parent id", -ENOENT);

  C_SaferCond ctx;
  MockCreateImageRequest *request = create_request("global uuid", "remote uuid",
                                                   "image name",
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
  CreateCloneImage create_clone_image;
  MockOpenImageRequest mock_open_image_request;

  InSequence seq;
  expect_ioctx_create(m_remote_io_ctx);
  expect_ioctx_create(m_local_io_ctx);
  expect_get_parent_global_image_id(m_remote_io_ctx, "global uuid", 0);
  expect_mirror_image_get_image_id(m_local_io_ctx, "local parent id", 0);

  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    m_remote_image_ctx->id, mock_remote_parent_image_ctx, -ENOENT);

  C_SaferCond ctx;
  MockCreateImageRequest *request = create_request("global uuid", "remote uuid",
                                                   "image name",
                                                   mock_remote_clone_image_ctx,
                                                   &ctx);
  request->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockImageReplayerCreateImageRequest, CloneOpenLocalParentError) {
  librbd::RBD rbd;
  librbd::ImageCtx *local_image_ctx;
  ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
  ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &local_image_ctx));

  std::string clone_image_name = get_temp_image_name();
  ASSERT_EQ(0, clone_image(m_remote_image_ctx, "snap", clone_image_name));

  librbd::ImageCtx *remote_clone_image_ctx;
  ASSERT_EQ(0, open_image(m_remote_io_ctx, clone_image_name,
               &remote_clone_image_ctx));

  librbd::MockTestImageCtx mock_remote_parent_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_parent_image_ctx(*local_image_ctx);
  librbd::MockTestImageCtx mock_remote_clone_image_ctx(*remote_clone_image_ctx);
  CreateCloneImage create_clone_image;
  MockOpenImageRequest mock_open_image_request;
  MockCloseImageRequest mock_close_image_request;

  InSequence seq;
  expect_ioctx_create(m_remote_io_ctx);
  expect_ioctx_create(m_local_io_ctx);
  expect_get_parent_global_image_id(m_remote_io_ctx, "global uuid", 0);
  expect_mirror_image_get_image_id(m_local_io_ctx, "local parent id", 0);

  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    m_remote_image_ctx->id, mock_remote_parent_image_ctx, 0);
  expect_open_image(mock_open_image_request, m_local_io_ctx,
                    "local parent id", mock_local_parent_image_ctx, -ENOENT);
  expect_close_image(mock_close_image_request, mock_remote_parent_image_ctx, 0);

  C_SaferCond ctx;
  MockCreateImageRequest *request = create_request("global uuid", "remote uuid",
                                                   "image name",
                                                   mock_remote_clone_image_ctx,
                                                   &ctx);
  request->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockImageReplayerCreateImageRequest, CloneSnapSetError) {
  librbd::RBD rbd;
  librbd::ImageCtx *local_image_ctx;
  ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
  ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &local_image_ctx));

  std::string clone_image_name = get_temp_image_name();
  ASSERT_EQ(0, clone_image(m_remote_image_ctx, "snap", clone_image_name));

  librbd::ImageCtx *remote_clone_image_ctx;
  ASSERT_EQ(0, open_image(m_remote_io_ctx, clone_image_name,
               &remote_clone_image_ctx));

  librbd::MockTestImageCtx mock_remote_parent_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_parent_image_ctx(*local_image_ctx);
  librbd::MockTestImageCtx mock_remote_clone_image_ctx(*remote_clone_image_ctx);
  CreateCloneImage create_clone_image;
  MockOpenImageRequest mock_open_image_request;
  MockCloseImageRequest mock_close_image_request;

  InSequence seq;
  expect_ioctx_create(m_remote_io_ctx);
  expect_ioctx_create(m_local_io_ctx);
  expect_get_parent_global_image_id(m_remote_io_ctx, "global uuid", 0);
  expect_mirror_image_get_image_id(m_local_io_ctx, "local parent id", 0);

  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    m_remote_image_ctx->id, mock_remote_parent_image_ctx, 0);
  expect_open_image(mock_open_image_request, m_local_io_ctx,
                    "local parent id", mock_local_parent_image_ctx, 0);
  expect_snap_set(mock_local_parent_image_ctx, "snap", -ENOENT);
  expect_close_image(mock_close_image_request, mock_local_parent_image_ctx, 0);
  expect_close_image(mock_close_image_request, mock_remote_parent_image_ctx, 0);

  C_SaferCond ctx;
  MockCreateImageRequest *request = create_request("global uuid", "remote uuid",
                                                   "image name",
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

  std::string clone_image_name = get_temp_image_name();
  ASSERT_EQ(0, clone_image(m_remote_image_ctx, "snap", clone_image_name));

  librbd::ImageCtx *remote_clone_image_ctx;
  ASSERT_EQ(0, open_image(m_remote_io_ctx, clone_image_name,
               &remote_clone_image_ctx));

  librbd::MockTestImageCtx mock_remote_parent_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_parent_image_ctx(*local_image_ctx);
  librbd::MockTestImageCtx mock_remote_clone_image_ctx(*remote_clone_image_ctx);
  CreateCloneImage create_clone_image;
  MockOpenImageRequest mock_open_image_request;
  MockCloseImageRequest mock_close_image_request;

  InSequence seq;
  expect_ioctx_create(m_remote_io_ctx);
  expect_ioctx_create(m_local_io_ctx);
  expect_get_parent_global_image_id(m_remote_io_ctx, "global uuid", 0);
  expect_mirror_image_get_image_id(m_local_io_ctx, "local parent id", 0);

  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    m_remote_image_ctx->id, mock_remote_parent_image_ctx, 0);
  expect_open_image(mock_open_image_request, m_local_io_ctx,
                    "local parent id", mock_local_parent_image_ctx, 0);
  expect_snap_set(mock_local_parent_image_ctx, "snap", 0);
  expect_clone_image(create_clone_image, "image name", "global uuid",
                      "remote uuid", -EINVAL);
  expect_close_image(mock_close_image_request, mock_local_parent_image_ctx, 0);
  expect_close_image(mock_close_image_request, mock_remote_parent_image_ctx, 0);

  C_SaferCond ctx;
  MockCreateImageRequest *request = create_request("global uuid", "remote uuid",
                                                   "image name",
                                                   mock_remote_clone_image_ctx,
                                                   &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerCreateImageRequest, CloneLocalParentCloseError) {
  librbd::RBD rbd;
  librbd::ImageCtx *local_image_ctx;
  ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
  ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &local_image_ctx));

  std::string clone_image_name = get_temp_image_name();
  ASSERT_EQ(0, clone_image(m_remote_image_ctx, "snap", clone_image_name));

  librbd::ImageCtx *remote_clone_image_ctx;
  ASSERT_EQ(0, open_image(m_remote_io_ctx, clone_image_name,
               &remote_clone_image_ctx));

  librbd::MockTestImageCtx mock_remote_parent_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_parent_image_ctx(*local_image_ctx);
  librbd::MockTestImageCtx mock_remote_clone_image_ctx(*remote_clone_image_ctx);
  CreateCloneImage create_clone_image;
  MockOpenImageRequest mock_open_image_request;
  MockCloseImageRequest mock_close_image_request;

  InSequence seq;
  expect_ioctx_create(m_remote_io_ctx);
  expect_ioctx_create(m_local_io_ctx);
  expect_get_parent_global_image_id(m_remote_io_ctx, "global uuid", 0);
  expect_mirror_image_get_image_id(m_local_io_ctx, "local parent id", 0);

  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    m_remote_image_ctx->id, mock_remote_parent_image_ctx, 0);
  expect_open_image(mock_open_image_request, m_local_io_ctx,
                    "local parent id", mock_local_parent_image_ctx, 0);
  expect_snap_set(mock_local_parent_image_ctx, "snap", 0);
  expect_clone_image(create_clone_image, "image name", "global uuid",
                      "remote uuid", 0);
  expect_close_image(mock_close_image_request, mock_local_parent_image_ctx, -EINVAL);
  expect_close_image(mock_close_image_request, mock_remote_parent_image_ctx, 0);

  C_SaferCond ctx;
  MockCreateImageRequest *request = create_request("global uuid", "remote uuid",
                                                   "image name",
                                                   mock_remote_clone_image_ctx,
                                                   &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageReplayerCreateImageRequest, CloneRemoteParentCloseError) {
  librbd::RBD rbd;
  librbd::ImageCtx *local_image_ctx;
  ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
  ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &local_image_ctx));

  std::string clone_image_name = get_temp_image_name();
  ASSERT_EQ(0, clone_image(m_remote_image_ctx, "snap", clone_image_name));

  librbd::ImageCtx *remote_clone_image_ctx;
  ASSERT_EQ(0, open_image(m_remote_io_ctx, clone_image_name,
               &remote_clone_image_ctx));

  librbd::MockTestImageCtx mock_remote_parent_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_parent_image_ctx(*local_image_ctx);
  librbd::MockTestImageCtx mock_remote_clone_image_ctx(*remote_clone_image_ctx);
  CreateCloneImage create_clone_image;
  MockOpenImageRequest mock_open_image_request;
  MockCloseImageRequest mock_close_image_request;

  InSequence seq;
  expect_ioctx_create(m_remote_io_ctx);
  expect_ioctx_create(m_local_io_ctx);
  expect_get_parent_global_image_id(m_remote_io_ctx, "global uuid", 0);
  expect_mirror_image_get_image_id(m_local_io_ctx, "local parent id", 0);

  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    m_remote_image_ctx->id, mock_remote_parent_image_ctx, 0);
  expect_open_image(mock_open_image_request, m_local_io_ctx,
                    "local parent id", mock_local_parent_image_ctx, 0);
  expect_snap_set(mock_local_parent_image_ctx, "snap", 0);
  expect_clone_image(create_clone_image, "image name", "global uuid",
                      "remote uuid", 0);
  expect_close_image(mock_close_image_request, mock_local_parent_image_ctx, 0);
  expect_close_image(mock_close_image_request, mock_remote_parent_image_ctx, -EINVAL);

  C_SaferCond ctx;
  MockCreateImageRequest *request = create_request("global uuid", "remote uuid",
                                                   "image name",
                                                   mock_remote_clone_image_ctx,
                                                   &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd
