// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/journal/mock/MockJournaler.h"
#include "librbd/journal/OpenRequest.h"
#include "librbd/journal/PromoteRequest.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx& image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace journal {

template <>
struct TypeTraits<MockTestImageCtx> {
  typedef ::journal::MockJournalerProxy Journaler;
};

template <>
struct OpenRequest<MockTestImageCtx> {
  Context *on_finish = nullptr;
  static OpenRequest *s_instance;
  static OpenRequest *create(MockTestImageCtx *image_ctx,
                             ::journal::MockJournalerProxy *journaler,
                             Mutex *lock, ImageClientMeta *client_meta,
                             uint64_t *tag_tid, journal::TagData *tag_data,
                             Context *on_finish) {
    assert(s_instance != nullptr);
    client_meta->tag_class = 456;
    tag_data->mirror_uuid = Journal<>::ORPHAN_MIRROR_UUID;
    *tag_tid = 567;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  OpenRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

OpenRequest<MockTestImageCtx> *OpenRequest<MockTestImageCtx>::s_instance = nullptr;

} // namespace journal
} // namespace librbd

// template definitions
#include "librbd/journal/PromoteRequest.cc"
template class librbd::journal::PromoteRequest<librbd::MockTestImageCtx>;

namespace librbd {
namespace journal {

using ::testing::_;
using ::testing::InSequence;
using ::testing::WithArg;

class TestMockJournalPromoteRequest : public TestMockFixture {
public:
  typedef PromoteRequest<MockTestImageCtx> MockPromoteRequest;
  typedef OpenRequest<MockTestImageCtx> MockOpenRequest;

  void expect_construct_journaler(::journal::MockJournaler &mock_journaler) {
    EXPECT_CALL(mock_journaler, construct());
  }

  void expect_open_journaler(MockTestImageCtx &mock_image_ctx,
                             MockOpenRequest &mock_open_request, int r) {
    EXPECT_CALL(mock_open_request, send())
      .WillOnce(FinishRequest(&mock_open_request, r, &mock_image_ctx));
  }

  void expect_allocate_tag(::journal::MockJournaler &mock_journaler,
                           const journal::TagPredecessor &predecessor, int r) {
    TagData tag_data;
    tag_data.mirror_uuid = Journal<>::LOCAL_MIRROR_UUID;
    tag_data.predecessor = predecessor;

    bufferlist tag_data_bl;
    ::encode(tag_data, tag_data_bl);

    EXPECT_CALL(mock_journaler, allocate_tag(456, ContentsEqual(tag_data_bl),
                                             _, _))
      .WillOnce(WithArg<3>(CompleteContext(r, static_cast<ContextWQ*>(NULL))));
  }

  void expect_shut_down_journaler(::journal::MockJournaler &mock_journaler,
                                  int r) {
    EXPECT_CALL(mock_journaler, shut_down(_))
      .WillOnce(CompleteContext(r, static_cast<ContextWQ*>(NULL)));
  }

};

TEST_F(TestMockJournalPromoteRequest, SuccessOrderly) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  ::journal::MockJournaler mock_journaler;
  MockOpenRequest mock_open_request;

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_construct_journaler(mock_journaler);
  expect_open_journaler(mock_image_ctx, mock_open_request, 0);
  expect_allocate_tag(mock_journaler,
                      {Journal<>::ORPHAN_MIRROR_UUID, true, 567, 1}, 0);
  expect_shut_down_journaler(mock_journaler, 0);

  C_SaferCond ctx;
  auto req = MockPromoteRequest::create(&mock_image_ctx, false, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockJournalPromoteRequest, SuccessForced) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  ::journal::MockJournaler mock_journaler;
  MockOpenRequest mock_open_request;

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_construct_journaler(mock_journaler);
  expect_open_journaler(mock_image_ctx, mock_open_request, 0);
  expect_allocate_tag(mock_journaler,
                      {Journal<>::LOCAL_MIRROR_UUID, true, 567, 0}, 0);
  expect_shut_down_journaler(mock_journaler, 0);

  C_SaferCond ctx;
  auto req = MockPromoteRequest::create(&mock_image_ctx, true, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockJournalPromoteRequest, OpenError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  ::journal::MockJournaler mock_journaler;
  MockOpenRequest mock_open_request;

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_construct_journaler(mock_journaler);
  expect_open_journaler(mock_image_ctx, mock_open_request, -ENOENT);
  expect_shut_down_journaler(mock_journaler, -EINVAL);

  C_SaferCond ctx;
  auto req = MockPromoteRequest::create(&mock_image_ctx, false, &ctx);
  req->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockJournalPromoteRequest, AllocateTagError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  ::journal::MockJournaler mock_journaler;
  MockOpenRequest mock_open_request;

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_construct_journaler(mock_journaler);
  expect_open_journaler(mock_image_ctx, mock_open_request, 0);
  expect_allocate_tag(mock_journaler,
                      {Journal<>::LOCAL_MIRROR_UUID, true, 567, 0}, -EBADMSG);
  expect_shut_down_journaler(mock_journaler, -EINVAL);

  C_SaferCond ctx;
  auto req = MockPromoteRequest::create(&mock_image_ctx, true, &ctx);
  req->send();
  ASSERT_EQ(-EBADMSG, ctx.wait());
}

TEST_F(TestMockJournalPromoteRequest, ShutDownError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  ::journal::MockJournaler mock_journaler;
  MockOpenRequest mock_open_request;

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_construct_journaler(mock_journaler);
  expect_open_journaler(mock_image_ctx, mock_open_request, 0);
  expect_allocate_tag(mock_journaler,
                      {Journal<>::LOCAL_MIRROR_UUID, true, 567, 0}, 0);
  expect_shut_down_journaler(mock_journaler, -EINVAL);

  C_SaferCond ctx;
  auto req = MockPromoteRequest::create(&mock_image_ctx, true, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace journal
} // namespace librbd
