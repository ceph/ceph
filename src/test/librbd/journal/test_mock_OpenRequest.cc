// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/journal/mock/MockJournaler.h"
#include "common/ceph_mutex.h"
#include "cls/journal/cls_journal_types.h"
#include "librbd/journal/OpenRequest.h"
#include "librbd/journal/Types.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public MockImageCtx {
  explicit MockTestImageCtx(librbd::ImageCtx& image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace journal {

template <>
struct TypeTraits<MockTestImageCtx> {
  typedef ::journal::MockJournaler Journaler;
};

} // namespace journal
} // namespace librbd

// template definitions
#include "librbd/journal/OpenRequest.cc"
template class librbd::journal::OpenRequest<librbd::MockTestImageCtx>;

namespace librbd {
namespace journal {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::WithArg;

class TestMockJournalOpenRequest : public TestMockFixture {
public:
  typedef OpenRequest<MockTestImageCtx> MockOpenRequest;

  TestMockJournalOpenRequest() = default;

  void expect_init_journaler(::journal::MockJournaler &mock_journaler, int r) {
    EXPECT_CALL(mock_journaler, init(_))
                  .WillOnce(CompleteContext(r, static_cast<asio::ContextWQ*>(NULL)));
  }

  void expect_get_journaler_cached_client(::journal::MockJournaler &mock_journaler,
                                          int r) {
    journal::ImageClientMeta image_client_meta;
    image_client_meta.tag_class = 345;

    journal::ClientData client_data;
    client_data.client_meta = image_client_meta;

    cls::journal::Client client;
    encode(client_data, client.data);

    EXPECT_CALL(mock_journaler, get_cached_client("", _))
                  .WillOnce(DoAll(SetArgPointee<1>(client),
                                  Return(r)));
  }

  void expect_get_journaler_tags(MockImageCtx &mock_image_ctx,
                                 ::journal::MockJournaler &mock_journaler,
                                 int r) {
    journal::TagData tag_data;
    tag_data.mirror_uuid = "remote mirror";

    bufferlist tag_data_bl;
    encode(tag_data, tag_data_bl);

    ::journal::Journaler::Tags tags = {{0, 345, {}}, {1, 345, tag_data_bl}};
    EXPECT_CALL(mock_journaler, get_tags(345, _, _))
                  .WillOnce(DoAll(SetArgPointee<1>(tags),
                                  WithArg<2>(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue))));
  }

  ceph::mutex m_lock = ceph::make_mutex("m_lock");
  ImageClientMeta m_client_meta;
  uint64_t m_tag_tid = 0;
  TagData m_tag_data;
};

TEST_F(TestMockJournalOpenRequest, Success) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  ::journal::MockJournaler mock_journaler;

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_init_journaler(mock_journaler, 0);
  expect_get_journaler_cached_client(mock_journaler, 0);
  expect_get_journaler_tags(mock_image_ctx, mock_journaler, 0);

  C_SaferCond ctx;
  auto req = MockOpenRequest::create(&mock_image_ctx, &mock_journaler,
                                     &m_lock, &m_client_meta, &m_tag_tid,
                                     &m_tag_data, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(345U, m_client_meta.tag_class);
  ASSERT_EQ(1U, m_tag_tid);
  ASSERT_EQ("remote mirror", m_tag_data.mirror_uuid);
}

TEST_F(TestMockJournalOpenRequest, InitError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  ::journal::MockJournaler mock_journaler;

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_init_journaler(mock_journaler, -ENOENT);

  C_SaferCond ctx;
  auto req = MockOpenRequest::create(&mock_image_ctx, &mock_journaler,
                                     &m_lock, &m_client_meta, &m_tag_tid,
                                     &m_tag_data, &ctx);
  req->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockJournalOpenRequest, GetCachedClientError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  ::journal::MockJournaler mock_journaler;

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_init_journaler(mock_journaler, 0);
  expect_get_journaler_cached_client(mock_journaler, -EINVAL);

  C_SaferCond ctx;
  auto req = MockOpenRequest::create(&mock_image_ctx, &mock_journaler,
                                     &m_lock, &m_client_meta, &m_tag_tid,
                                     &m_tag_data, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockJournalOpenRequest, GetTagsError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  ::journal::MockJournaler mock_journaler;

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_init_journaler(mock_journaler, 0);
  expect_get_journaler_cached_client(mock_journaler, 0);
  expect_get_journaler_tags(mock_image_ctx, mock_journaler, -EBADMSG);

  C_SaferCond ctx;
  auto req = MockOpenRequest::create(&mock_image_ctx, &mock_journaler,
                                     &m_lock, &m_client_meta, &m_tag_tid,
                                     &m_tag_data, &ctx);
  req->send();
  ASSERT_EQ(-EBADMSG, ctx.wait());
}

} // namespace journal
} // namespace librbd
