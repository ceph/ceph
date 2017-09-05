// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/journal/TypeTraits.h"
#include "tools/rbd_mirror/image_replayer/GetMirrorImageIdRequest.h"
#include "tools/rbd_mirror/image_replayer/PrepareRemoteImageRequest.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"

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

template <>
struct GetMirrorImageIdRequest<librbd::MockTestImageCtx> {
  static GetMirrorImageIdRequest* s_instance;
  std::string* image_id = nullptr;
  Context* on_finish = nullptr;

  static GetMirrorImageIdRequest* create(librados::IoCtx& io_ctx,
                                         const std::string& global_image_id,
                                         std::string* image_id,
                                         Context* on_finish) {
    assert(s_instance != nullptr);
    s_instance->image_id = image_id;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  GetMirrorImageIdRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

GetMirrorImageIdRequest<librbd::MockTestImageCtx>* GetMirrorImageIdRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

// template definitions
#include "tools/rbd_mirror/image_replayer/PrepareRemoteImageRequest.cc"

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

class TestMockImageReplayerPrepareRemoteImageRequest : public TestMockFixture {
public:
  typedef PrepareRemoteImageRequest<librbd::MockTestImageCtx> MockPrepareRemoteImageRequest;
  typedef GetMirrorImageIdRequest<librbd::MockTestImageCtx> MockGetMirrorImageIdRequest;

  void expect_get_mirror_image_id(MockGetMirrorImageIdRequest& mock_get_mirror_image_id_request,
                                  const std::string& image_id, int r) {
    EXPECT_CALL(mock_get_mirror_image_id_request, send())
      .WillOnce(Invoke([&mock_get_mirror_image_id_request, image_id, r]() {
                  *mock_get_mirror_image_id_request.image_id = image_id;
                  mock_get_mirror_image_id_request.on_finish->complete(r);
                }));
  }

  void expect_mirror_uuid_get(librados::IoCtx &io_ctx,
                              const std::string &mirror_uuid, int r) {
    bufferlist bl;
    ::encode(mirror_uuid, bl);

    EXPECT_CALL(get_mock_io_ctx(io_ctx),
                exec(RBD_MIRRORING, _, StrEq("rbd"), StrEq("mirror_uuid_get"), _, _, _))
      .WillOnce(DoAll(WithArg<5>(Invoke([bl](bufferlist *out_bl) {
                                          *out_bl = bl;
                                        })),
                      Return(r)));
  }
};

TEST_F(TestMockImageReplayerPrepareRemoteImageRequest, Success) {
  InSequence seq;
  expect_mirror_uuid_get(m_remote_io_ctx, "remote mirror uuid", 0);
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request,
                             "remote image id", 0);

  std::string remote_mirror_uuid;
  std::string remote_image_id;
  C_SaferCond ctx;
  auto req = MockPrepareRemoteImageRequest::create(m_remote_io_ctx,
                                                   "global image id",
                                                   &remote_mirror_uuid,
                                                   &remote_image_id,
                                                   &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(std::string("remote mirror uuid"), remote_mirror_uuid);
  ASSERT_EQ(std::string("remote image id"), remote_image_id);
}

TEST_F(TestMockImageReplayerPrepareRemoteImageRequest, MirrorUuidError) {
  InSequence seq;
  expect_mirror_uuid_get(m_remote_io_ctx, "", -EINVAL);

  std::string remote_mirror_uuid;
  std::string remote_image_id;
  C_SaferCond ctx;
  auto req = MockPrepareRemoteImageRequest::create(m_remote_io_ctx,
                                                   "global image id",
                                                   &remote_mirror_uuid,
                                                   &remote_image_id,
                                                   &ctx);
  req->send();

  ASSERT_EQ(-EINVAL, ctx.wait());
  ASSERT_EQ(std::string(""), remote_mirror_uuid);
}

TEST_F(TestMockImageReplayerPrepareRemoteImageRequest, MirrorImageIdError) {
  InSequence seq;
  expect_mirror_uuid_get(m_remote_io_ctx, "remote mirror uuid", 0);
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request, "", -EINVAL);

  std::string remote_mirror_uuid;
  std::string remote_image_id;
  C_SaferCond ctx;
  auto req = MockPrepareRemoteImageRequest::create(m_remote_io_ctx,
                                                   "global image id",
                                                   &remote_mirror_uuid,
                                                   &remote_image_id,
                                                   &ctx);
  req->send();

  ASSERT_EQ(-EINVAL, ctx.wait());
  ASSERT_EQ(std::string("remote mirror uuid"), remote_mirror_uuid);
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd
