// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/journal/mock/MockJournaler.h"
#include "cls/journal/cls_journal_types.h"
#include "librbd/journal/CreateRequest.h"
#include "librbd/journal/RemoveRequest.h"
#include "librbd/journal/ResetRequest.h"

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
struct CreateRequest<MockTestImageCtx> {
  static CreateRequest* s_instance;
  Context* on_finish = nullptr;

  static CreateRequest* create(IoCtx &ioctx, const std::string &imageid,
                               uint8_t order, uint8_t splay_width,
                               const std::string &object_pool,
                               uint64_t tag_class, TagData &tag_data,
                               const std::string &client_id,
                               ContextWQ *op_work_queue, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  CreateRequest() {
    s_instance = this;
  }
};

template <>
struct RemoveRequest<MockTestImageCtx> {
  static RemoveRequest* s_instance;
  Context* on_finish = nullptr;

  static RemoveRequest* create(IoCtx &ioctx, const std::string &image_id,
                               const std::string &client_id,
                               ContextWQ *op_work_queue, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  RemoveRequest() {
    s_instance = this;
  }
};

CreateRequest<MockTestImageCtx>* CreateRequest<MockTestImageCtx>::s_instance = nullptr;
RemoveRequest<MockTestImageCtx>* RemoveRequest<MockTestImageCtx>::s_instance = nullptr;

} // namespace journal
} // namespace librbd

#include "librbd/journal/ResetRequest.cc"

namespace librbd {
namespace journal {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArg;

class TestMockJournalResetRequest : public TestMockFixture {
public:
  typedef ResetRequest<MockTestImageCtx> MockResetRequest;
  typedef CreateRequest<MockTestImageCtx> MockCreateRequest;
  typedef RemoveRequest<MockTestImageCtx> MockRemoveRequest;

  void expect_construct_journaler(::journal::MockJournaler &mock_journaler) {
    EXPECT_CALL(mock_journaler, construct());
  }

  void expect_init_journaler(::journal::MockJournaler &mock_journaler, int r) {
    EXPECT_CALL(mock_journaler, init(_))
      .WillOnce(CompleteContext(r, static_cast<ContextWQ*>(NULL)));
  }

  void expect_get_metadata(::journal::MockJournaler& mock_journaler) {
    EXPECT_CALL(mock_journaler, get_metadata(_, _, _))
      .WillOnce(Invoke([](uint8_t* order, uint8_t* splay_width,
                          int64_t* pool_id) {
                  *order = 24;
                  *splay_width = 4;
                  *pool_id = -1;
                }));
  }

  void expect_shut_down_journaler(::journal::MockJournaler &mock_journaler,
                                  int r) {
    EXPECT_CALL(mock_journaler, shut_down(_))
      .WillOnce(CompleteContext(r, static_cast<ContextWQ*>(NULL)));
  }

  void expect_remove(MockRemoveRequest& mock_remove_request, int r) {
    EXPECT_CALL(mock_remove_request, send())
      .WillOnce(Invoke([&mock_remove_request, r]() {
                  mock_remove_request.on_finish->complete(r);
                }));
  }

  void expect_create(MockCreateRequest& mock_create_request, int r) {
    EXPECT_CALL(mock_create_request, send())
      .WillOnce(Invoke([&mock_create_request, r]() {
                  mock_create_request.on_finish->complete(r);
                }));
  }
};

TEST_F(TestMockJournalResetRequest, Success) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  InSequence seq;
  ::journal::MockJournaler mock_journaler;
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_get_metadata(mock_journaler);
  expect_shut_down_journaler(mock_journaler, 0);

  MockRemoveRequest mock_remove_request;
  expect_remove(mock_remove_request, 0);

  MockCreateRequest mock_create_request;
  expect_create(mock_create_request, 0);

  ContextWQ* context_wq;
  Journal<>::get_work_queue(ictx->cct, &context_wq);

  C_SaferCond ctx;
  auto req = MockResetRequest::create(m_ioctx, "image id",
                                      Journal<>::IMAGE_CLIENT_ID,
                                      Journal<>::LOCAL_MIRROR_UUID,
                                      context_wq, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockJournalResetRequest, InitError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  InSequence seq;
  ::journal::MockJournaler mock_journaler;
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, -EINVAL);
  expect_shut_down_journaler(mock_journaler, 0);

  ContextWQ* context_wq;
  Journal<>::get_work_queue(ictx->cct, &context_wq);

  C_SaferCond ctx;
  auto req = MockResetRequest::create(m_ioctx, "image id",
                                      Journal<>::IMAGE_CLIENT_ID,
                                      Journal<>::LOCAL_MIRROR_UUID,
                                      context_wq, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockJournalResetRequest, ShutDownError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  InSequence seq;
  ::journal::MockJournaler mock_journaler;
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_get_metadata(mock_journaler);
  expect_shut_down_journaler(mock_journaler, -EINVAL);

  ContextWQ* context_wq;
  Journal<>::get_work_queue(ictx->cct, &context_wq);

  C_SaferCond ctx;
  auto req = MockResetRequest::create(m_ioctx, "image id",
                                      Journal<>::IMAGE_CLIENT_ID,
                                      Journal<>::LOCAL_MIRROR_UUID,
                                      context_wq, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockJournalResetRequest, RemoveError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  InSequence seq;
  ::journal::MockJournaler mock_journaler;
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_get_metadata(mock_journaler);
  expect_shut_down_journaler(mock_journaler, 0);

  MockRemoveRequest mock_remove_request;
  expect_remove(mock_remove_request, -EINVAL);

  ContextWQ* context_wq;
  Journal<>::get_work_queue(ictx->cct, &context_wq);

  C_SaferCond ctx;
  auto req = MockResetRequest::create(m_ioctx, "image id",
                                      Journal<>::IMAGE_CLIENT_ID,
                                      Journal<>::LOCAL_MIRROR_UUID,
                                      context_wq, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockJournalResetRequest, CreateError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  InSequence seq;
  ::journal::MockJournaler mock_journaler;
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_get_metadata(mock_journaler);
  expect_shut_down_journaler(mock_journaler, 0);

  MockRemoveRequest mock_remove_request;
  expect_remove(mock_remove_request, 0);

  MockCreateRequest mock_create_request;
  expect_create(mock_create_request, -EINVAL);

  ContextWQ* context_wq;
  Journal<>::get_work_queue(ictx->cct, &context_wq);

  C_SaferCond ctx;
  auto req = MockResetRequest::create(m_ioctx, "image id",
                                      Journal<>::IMAGE_CLIENT_ID,
                                      Journal<>::LOCAL_MIRROR_UUID,
                                      context_wq, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace journal
} // namespace librbd
