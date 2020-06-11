// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "include/rbd/librbd.hpp"
#include "include/stringify.h"
#include "librbd/ImageCtx.h"
#include "librbd/deep_copy/MetadataCopyRequest.h"
#include "librbd/image/GetMetadataRequest.h"
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

namespace image {

template <>
struct GetMetadataRequest<MockTestImageCtx> {
  std::map<std::string, bufferlist>* pairs = nullptr;
  Context* on_finish = nullptr;

  static GetMetadataRequest* s_instance;
  static GetMetadataRequest* create(librados::IoCtx&,
                                    const std::string& oid,
                                    bool filter_internal,
                                    const std::string& filter_key_prefix,
                                    const std::string& last_key,
                                    uint32_t max_results,
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

} // namspace image
} // namespace librbd

// template definitions
#include "librbd/deep_copy/MetadataCopyRequest.cc"

namespace librbd {
namespace deep_copy {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockDeepCopyMetadataCopyRequest : public TestMockFixture {
public:
  typedef MetadataCopyRequest<librbd::MockTestImageCtx> MockMetadataCopyRequest;
  typedef image::GetMetadataRequest<MockTestImageCtx> MockGetMetadataRequest;
  typedef std::map<std::string, bufferlist> Metadata;

  librbd::ImageCtx *m_src_image_ctx;
  librbd::ImageCtx *m_dst_image_ctx;
  asio::ContextWQ *m_work_queue;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &m_src_image_ctx));

    librbd::RBD rbd;
    std::string dst_image_name = get_temp_image_name();
    ASSERT_EQ(0, create_image_pp(rbd, m_ioctx, dst_image_name, m_image_size));
    ASSERT_EQ(0, open_image(dst_image_name, &m_dst_image_ctx));

    librbd::ImageCtx::get_work_queue(m_src_image_ctx->cct, &m_work_queue);
  }

  void expect_get_metadata(MockGetMetadataRequest& mock_request,
                           const Metadata& metadata, int r) {
    EXPECT_CALL(mock_request, send())
      .WillOnce(Invoke([this, &mock_request, metadata, r]() {
        *mock_request.pairs = metadata;
        m_work_queue->queue(mock_request.on_finish, r);
      }));
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
  MockGetMetadataRequest mock_request;
  expect_get_metadata(mock_request, key_values_1, 0);
  expect_metadata_set(mock_dst_image_ctx, key_values_1, 0);
  expect_get_metadata(mock_request, key_values_2, 0);
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
  MockGetMetadataRequest mock_request;
  expect_get_metadata(mock_request, key_values, 0);

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
  MockGetMetadataRequest mock_request;
  expect_get_metadata(mock_request, key_values, -EINVAL);

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
  MockGetMetadataRequest mock_request;
  expect_get_metadata(mock_request, key_values, 0);
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
