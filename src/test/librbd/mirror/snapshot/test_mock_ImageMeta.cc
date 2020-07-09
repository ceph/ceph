// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "librbd/ImageState.h"
#include "librbd/mirror/snapshot/ImageMeta.h"
#include "librbd/mirror/snapshot/Utils.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx& image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace
} // namespace librbd

#include "librbd/mirror/snapshot/ImageMeta.cc"

namespace librbd {
namespace mirror {
namespace snapshot {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockMirrorSnapshotImageMeta : public TestMockFixture {
public:
  typedef ImageMeta<MockTestImageCtx> MockImageMeta;

  void expect_metadata_get(MockTestImageCtx& mock_image_ctx,
                           const std::string& mirror_uuid,
                           const std::string& value, int r) {
    bufferlist in_bl;
    ceph::encode(util::get_image_meta_key(mirror_uuid), in_bl);

    bufferlist out_bl;
    ceph::encode(value, out_bl);

    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                     StrEq("metadata_get"), ContentsEqual(in_bl), _, _, _))
      .WillOnce(DoAll(WithArg<5>(CopyInBufferlist(out_bl)),
                      Return(r)));
  }

  void expect_metadata_set(MockTestImageCtx& mock_image_ctx,
                           const std::string& mirror_uuid,
                           const std::string& value, int r) {
    bufferlist value_bl;
    value_bl.append(value);

    bufferlist in_bl;
    ceph::encode(
      std::map<std::string, bufferlist>{
        {util::get_image_meta_key(mirror_uuid), value_bl}},
      in_bl);

    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                     StrEq("metadata_set"), ContentsEqual(in_bl), _, _, _))
      .WillOnce(Return(r));
  }
};

TEST_F(TestMockMirrorSnapshotImageMeta, Load) {
  librbd::ImageCtx* image_ctx;
  ASSERT_EQ(0, open_image(m_image_name, &image_ctx));
  MockTestImageCtx mock_image_ctx(*image_ctx);

  InSequence seq;
  expect_metadata_get(mock_image_ctx, "mirror uuid",
                      "{\"resync_requested\": true}", 0);

  MockImageMeta mock_image_meta(&mock_image_ctx, "mirror uuid");
  C_SaferCond ctx;
  mock_image_meta.load(&ctx);
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotImageMeta, LoadError) {
  librbd::ImageCtx* image_ctx;
  ASSERT_EQ(0, open_image(m_image_name, &image_ctx));
  MockTestImageCtx mock_image_ctx(*image_ctx);

  InSequence seq;
  expect_metadata_get(mock_image_ctx, "mirror uuid",
                      "{\"resync_requested\": true}", -EINVAL);

  MockImageMeta mock_image_meta(&mock_image_ctx, "mirror uuid");
  C_SaferCond ctx;
  mock_image_meta.load(&ctx);
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotImageMeta, LoadCorrupt) {
  librbd::ImageCtx* image_ctx;
  ASSERT_EQ(0, open_image(m_image_name, &image_ctx));
  MockTestImageCtx mock_image_ctx(*image_ctx);

  InSequence seq;
  expect_metadata_get(mock_image_ctx, "mirror uuid",
                      "\"resync_requested\": true}", 0);

  MockImageMeta mock_image_meta(&mock_image_ctx, "mirror uuid");
  C_SaferCond ctx;
  mock_image_meta.load(&ctx);
  ASSERT_EQ(-EBADMSG, ctx.wait());
}

TEST_F(TestMockMirrorSnapshotImageMeta, Save) {
  librbd::ImageCtx* image_ctx;
  ASSERT_EQ(0, open_image(m_image_name, &image_ctx));
  MockTestImageCtx mock_image_ctx(*image_ctx);

  InSequence seq;
  expect_metadata_set(mock_image_ctx, "mirror uuid",
                      "{\"resync_requested\": true}", 0);

  MockImageMeta mock_image_meta(&mock_image_ctx, "mirror uuid");
  mock_image_meta.resync_requested = true;

  C_SaferCond ctx;
  mock_image_meta.save(&ctx);
  ASSERT_EQ(0, ctx.wait());

  // should have sent image-update notification
  ASSERT_TRUE(image_ctx->state->is_refresh_required());
}

TEST_F(TestMockMirrorSnapshotImageMeta, SaveError) {
  librbd::ImageCtx* image_ctx;
  ASSERT_EQ(0, open_image(m_image_name, &image_ctx));
  MockTestImageCtx mock_image_ctx(*image_ctx);

  InSequence seq;
  expect_metadata_set(mock_image_ctx, "mirror uuid",
                      "{\"resync_requested\": false}", -EINVAL);

  MockImageMeta mock_image_meta(&mock_image_ctx, "mirror uuid");

  C_SaferCond ctx;
  mock_image_meta.save(&ctx);
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd
