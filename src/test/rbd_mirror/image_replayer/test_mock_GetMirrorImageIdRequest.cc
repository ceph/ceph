// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/journal/TypeTraits.h"
#include "tools/rbd_mirror/image_replayer/GetMirrorImageIdRequest.h"
#include "test/journal/mock/MockJournaler.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockJournal.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace
} // namespace librbd

// template definitions
#include "tools/rbd_mirror/image_replayer/GetMirrorImageIdRequest.cc"

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
using ::testing::WithArgs;

class TestMockImageReplayerGetMirrorImageIdRequest : public TestMockFixture {
public:
  typedef GetMirrorImageIdRequest<librbd::MockTestImageCtx> MockGetMirrorImageIdRequest;

  void expect_mirror_image_get_image_id(librados::IoCtx &io_ctx,
                                        const std::string &image_id, int r) {
    bufferlist bl;
    encode(image_id, bl);

    EXPECT_CALL(get_mock_io_ctx(io_ctx),
                exec(RBD_MIRRORING, _, StrEq("rbd"),
                     StrEq("mirror_image_get_image_id"), _, _, _, _))
      .WillOnce(DoAll(WithArg<5>(Invoke([bl](bufferlist *out_bl) {
                                          *out_bl = bl;
                                        })),
                      Return(r)));
  }

};

TEST_F(TestMockImageReplayerGetMirrorImageIdRequest, Success) {
  InSequence seq;
  expect_mirror_image_get_image_id(m_local_io_ctx, "image id", 0);

  std::string image_id;
  C_SaferCond ctx;
  auto req = MockGetMirrorImageIdRequest::create(m_local_io_ctx,
                                                 "global image id",
                                                 &image_id, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(std::string("image id"), image_id);
}

TEST_F(TestMockImageReplayerGetMirrorImageIdRequest, MirrorImageIdDNE) {
  InSequence seq;
  expect_mirror_image_get_image_id(m_local_io_ctx, "", -ENOENT);

  std::string image_id;
  C_SaferCond ctx;
  auto req = MockGetMirrorImageIdRequest::create(m_local_io_ctx,
                                                 "global image id",
                                                 &image_id, &ctx);
  req->send();

  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockImageReplayerGetMirrorImageIdRequest, MirrorImageIdError) {
  InSequence seq;
  expect_mirror_image_get_image_id(m_local_io_ctx, "", -EINVAL);

  std::string image_id;
  C_SaferCond ctx;
  auto req = MockGetMirrorImageIdRequest::create(m_local_io_ctx,
                                                 "global image id",
                                                 &image_id, &ctx);
  req->send();

  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd
