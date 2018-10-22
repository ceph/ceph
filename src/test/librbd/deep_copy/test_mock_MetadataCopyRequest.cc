// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "include/rbd/librbd.hpp"
#include "include/stringify.h"
#include "librbd/ImageCtx.h"
#include "librbd/deep_copy/MetadataCopyRequest.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/test_support.h"
#include <map>

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
#include "librbd/deep_copy/MetadataCopyRequest.cc"

namespace librbd {
namespace deep_copy {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockDeepCopyMetadataCopyRequest : public TestMockFixture {
public:
  typedef MetadataCopyRequest<librbd::MockTestImageCtx> MockMetadataCopyRequest;
  typedef std::map<std::string, bufferlist> Metadata;

  librbd::ImageCtx *m_src_image_ctx;
  librbd::ImageCtx *m_dst_image_ctx;
  ThreadPool *m_thread_pool;
  ContextWQ *m_work_queue;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &m_src_image_ctx));

    librbd::RBD rbd;
    std::string dst_image_name = get_temp_image_name();
    ASSERT_EQ(0, create_image_pp(rbd, m_ioctx, dst_image_name, m_image_size));
    ASSERT_EQ(0, open_image(dst_image_name, &m_dst_image_ctx));

    librbd::ImageCtx::get_thread_pool_instance(m_src_image_ctx->cct,
                                               &m_thread_pool, &m_work_queue);
  }

  void expect_metadata_list(librbd::MockTestImageCtx &mock_image_ctx,
                            const Metadata& metadata, int r) {
    bufferlist out_bl;
    encode(metadata, out_bl);

    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                     StrEq("metadata_list"), _, _, _))
                  .WillOnce(DoAll(WithArg<5>(CopyInBufferlist(out_bl)),
                                  Return(r)));
  }

  void expect_metadata_set(librbd::MockTestImageCtx &mock_image_ctx,
                           const Metadata& metadata, int r) {
    bufferlist in_bl;
    encode(metadata, in_bl);

    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                     StrEq("metadata_set"), ContentsEqual(in_bl), _, _))
                  .WillOnce(Return(r));
  }
};

TEST_F(TestMockDeepCopyMetadataCopyRequest, Success) {
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  size_t idx = 1;
  Metadata key_values_1;
  for (; idx <= 128; ++idx) {
    bufferlist bl;
    bl.append("value" + stringify(idx));
    key_values_1.emplace("key" + stringify(idx), bl);
  }

  Metadata key_values_2;
  for (; idx <= 255; ++idx) {
    bufferlist bl;
    bl.append("value" + stringify(idx));
    key_values_2.emplace("key" + stringify(idx), bl);
  }

  InSequence seq;
  expect_metadata_list(mock_src_image_ctx, key_values_1, 0);
  expect_metadata_set(mock_dst_image_ctx, key_values_1, 0);
  expect_metadata_list(mock_src_image_ctx, key_values_2, 0);
  expect_metadata_set(mock_dst_image_ctx, key_values_2, 0);

  C_SaferCond ctx;
  auto request = MockMetadataCopyRequest::create(&mock_src_image_ctx,
                                                 &mock_dst_image_ctx,
                                                 &ctx);
  request->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockDeepCopyMetadataCopyRequest, Empty) {
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  Metadata key_values;

  InSequence seq;
  expect_metadata_list(mock_src_image_ctx, key_values, 0);

  C_SaferCond ctx;
  auto request = MockMetadataCopyRequest::create(&mock_src_image_ctx,
                                                 &mock_dst_image_ctx,
                                                 &ctx);
  request->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockDeepCopyMetadataCopyRequest, MetadataListError) {
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  Metadata key_values;

  InSequence seq;
  expect_metadata_list(mock_src_image_ctx, key_values, -EINVAL);

  C_SaferCond ctx;
  auto request = MockMetadataCopyRequest::create(&mock_src_image_ctx,
                                                 &mock_dst_image_ctx,
                                                 &ctx);
  request->send();

  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockDeepCopyMetadataCopyRequest, MetadataSetError) {
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  Metadata key_values;
  bufferlist bl;
  bl.append("value");
  key_values.emplace("key", bl);

  InSequence seq;
  expect_metadata_list(mock_src_image_ctx, key_values, 0);
  expect_metadata_set(mock_dst_image_ctx, key_values, -EINVAL);

  C_SaferCond ctx;
  auto request = MockMetadataCopyRequest::create(&mock_src_image_ctx,
                                                 &mock_dst_image_ctx,
                                                 &ctx);
  request->send();

  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace deep_sync
} // namespace librbd
