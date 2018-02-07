// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockContextWQ.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/journal/RemoveRequest.h"
#include "librbd/Operations.h"
#include "librbd/operation/TrimRequest.h"
#include "librbd/image/TypeTraits.h"
#include "librbd/image/DetachChildRequest.h"
#include "librbd/image/RemoveRequest.h"
#include "librbd/image/RefreshParentRequest.h"
#include "librbd/mirror/DisableRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <arpa/inet.h>
#include <list>
#include <boost/scope_exit.hpp>

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  static MockTestImageCtx* s_instance;
  static MockTestImageCtx* create(const std::string &image_name,
                                  const std::string &image_id,
                                  const char *snap, librados::IoCtx& p,
                                  bool read_only) {
    assert(s_instance != nullptr);
    return s_instance;
  }

  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
    s_instance = this;
  }
};

MockTestImageCtx* MockTestImageCtx::s_instance = nullptr;

} // anonymous namespace
namespace image {

template <>
struct TypeTraits<MockTestImageCtx> {
  typedef librbd::MockContextWQ ContextWQ;
};

template <>
class DetachChildRequest<MockTestImageCtx> {
public:
  static DetachChildRequest *s_instance;
  static DetachChildRequest *create(MockTestImageCtx &image_ctx,
                                    Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  Context *on_finish = nullptr;

  DetachChildRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

DetachChildRequest<MockTestImageCtx> *DetachChildRequest<MockTestImageCtx>::s_instance;

} // namespace image

namespace operation {

template <>
class TrimRequest<MockTestImageCtx> {
public:
  static TrimRequest *s_instance;
  static TrimRequest *create(MockTestImageCtx &image_ctx, Context *on_finish,
                             uint64_t original_size, uint64_t new_size,
                             ProgressContext &prog_ctx) {
    assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  Context *on_finish = nullptr;

  TrimRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

TrimRequest<MockTestImageCtx> *TrimRequest<MockTestImageCtx>::s_instance;

} // namespace operation

namespace journal {

template <>
class RemoveRequest<MockTestImageCtx> {
private:
  typedef ::librbd::image::TypeTraits<MockTestImageCtx> TypeTraits;
  typedef typename TypeTraits::ContextWQ ContextWQ;
public:
  static RemoveRequest *s_instance;
  static RemoveRequest *create(IoCtx &ioctx, const std::string &imageid,
                                      const std::string &client_id,
                                      ContextWQ *op_work_queue, Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  Context *on_finish = nullptr;

  RemoveRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

RemoveRequest<MockTestImageCtx> *RemoveRequest<MockTestImageCtx>::s_instance = nullptr;

} // namespace journal

namespace mirror {

template<>
class DisableRequest<MockTestImageCtx> {
public:
  static DisableRequest *s_instance;
  Context *on_finish = nullptr;

  static DisableRequest *create(MockTestImageCtx *image_ctx, bool force,
                                bool remove, Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  DisableRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

DisableRequest<MockTestImageCtx> *DisableRequest<MockTestImageCtx>::s_instance;

} // namespace mirror
} // namespace librbd

// template definitions
#include "librbd/image/RemoveRequest.cc"

namespace librbd {
namespace image {

using ::testing::_;
using ::testing::DoAll;
using ::testing::DoDefault;
using ::testing::Invoke;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::WithArg;
using ::testing::SetArgPointee;
using ::testing::StrEq;

class TestMockImageRemoveRequest : public TestMockFixture {
public:
  typedef ::librbd::image::TypeTraits<MockTestImageCtx> TypeTraits;
  typedef typename TypeTraits::ContextWQ ContextWQ;
  typedef RemoveRequest<MockTestImageCtx> MockRemoveRequest;
  typedef DetachChildRequest<MockTestImageCtx> MockDetachChildRequest;
  typedef librbd::operation::TrimRequest<MockTestImageCtx> MockTrimRequest;
  typedef librbd::journal::RemoveRequest<MockTestImageCtx> MockJournalRemoveRequest;
  typedef librbd::mirror::DisableRequest<MockTestImageCtx> MockMirrorDisableRequest;

  librbd::ImageCtx *m_test_imctx = NULL;
  MockTestImageCtx *m_mock_imctx = NULL;


  void TestImageRemoveSetUp() {
    ASSERT_EQ(0, open_image(m_image_name, &m_test_imctx));
    m_mock_imctx = new MockTestImageCtx(*m_test_imctx);
    librbd::MockTestImageCtx::s_instance = m_mock_imctx;
  }
  void TestImageRemoveTearDown() {
    librbd::MockTestImageCtx::s_instance = NULL;
    delete m_mock_imctx;
  }

  void expect_state_open(MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.state, open(_, _))
      .WillOnce(Invoke([r](bool open_parent, Context *on_ready) {
		  on_ready->complete(r);
                }));
    if (r < 0) {
      EXPECT_CALL(mock_image_ctx, destroy());
    }
  }

  void expect_state_close(MockTestImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.state, close(_))
      .WillOnce(Invoke([](Context *on_ready) {
                  on_ready->complete(0);
                }));
    EXPECT_CALL(mock_image_ctx, destroy());
  }

  void expect_wq_queue(ContextWQ &wq, int r) {
    EXPECT_CALL(wq, queue(_, r))
      .WillRepeatedly(Invoke([](Context *on_ready, int r) {
                  on_ready->complete(r);
                }));
  }

  void expect_get_group(MockTestImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                                    StrEq("image_group_get"), _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_trim(MockTestImageCtx &mock_image_ctx,
                   MockTrimRequest &mock_trim_request, int r) {
    EXPECT_CALL(mock_trim_request, send())
                  .WillOnce(FinishRequest(&mock_trim_request, r, &mock_image_ctx));
  }

  void expect_journal_remove(MockTestImageCtx &mock_image_ctx,
                   MockJournalRemoveRequest &mock_journal_remove_request, int r) {
    EXPECT_CALL(mock_journal_remove_request, send())
                  .WillOnce(FinishRequest(&mock_journal_remove_request, r, &mock_image_ctx));
  }

  void expect_mirror_disable(MockTestImageCtx &mock_image_ctx,
                   MockMirrorDisableRequest &mock_mirror_disable_request, int r) {
    EXPECT_CALL(mock_mirror_disable_request, send())
                  .WillOnce(FinishRequest(&mock_mirror_disable_request, r, &mock_image_ctx));
  }

  void expect_remove_mirror_image(librados::IoCtx &ioctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(ioctx),
                exec(StrEq("rbd_mirroring"), _, StrEq("rbd"), StrEq("mirror_image_remove"),
                     _, _, _))
      .WillOnce(Return(r));
  }

  void expect_mirror_image_get(MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(RBD_MIRRORING, _, StrEq("rbd"), StrEq("mirror_image_get"),
                     _, _, _))
      .WillOnce(Return(r));
  }

  void expect_dir_remove_image(librados::IoCtx &ioctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(ioctx),
                exec(RBD_DIRECTORY, _, StrEq("rbd"), StrEq("dir_remove_image"),
                     _, _, _))
      .WillOnce(Return(r));
  }

  void expect_detach_child(MockTestImageCtx &mock_image_ctx,
                           MockDetachChildRequest& mock_request, int r) {
    EXPECT_CALL(mock_request, send())
      .WillOnce(FinishRequest(&mock_request, r, &mock_image_ctx));
  }
};

TEST_F(TestMockImageRemoveRequest, SuccessV1) {
  REQUIRE_FORMAT_V1();
  TestImageRemoveSetUp();

  C_SaferCond ctx;
  librbd::NoOpProgressContext no_op;
  ContextWQ op_work_queue;
  MockTrimRequest mock_trim_request;
  MockJournalRemoveRequest mock_journal_remove_request;

  InSequence seq;
  expect_state_open(*m_mock_imctx, 0);
  expect_get_group(*m_mock_imctx, 0);
  expect_trim(*m_mock_imctx, mock_trim_request, 0);
  expect_op_work_queue(*m_mock_imctx);
  expect_state_close(*m_mock_imctx);
  expect_wq_queue(op_work_queue, 0);

  MockRemoveRequest *req = MockRemoveRequest::create(m_ioctx, m_image_name, "",
					      true, false, no_op, &op_work_queue, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());

  TestImageRemoveTearDown();
}

TEST_F(TestMockImageRemoveRequest, OpenFailV1) {
  REQUIRE_FORMAT_V1();
  TestImageRemoveSetUp();

  C_SaferCond ctx;
  librbd::NoOpProgressContext no_op;
  ContextWQ op_work_queue;
  MockTrimRequest mock_trim_request;

  InSequence seq;
  expect_state_open(*m_mock_imctx, -ENOENT);
  expect_wq_queue(op_work_queue, 0);

  MockRemoveRequest *req = MockRemoveRequest::create(m_ioctx, m_image_name, "",
					      true, false, no_op, &op_work_queue, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());

  TestImageRemoveTearDown();
}

TEST_F(TestMockImageRemoveRequest, SuccessV2CloneV1) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);
  TestImageRemoveSetUp();

  C_SaferCond ctx;
  librbd::NoOpProgressContext no_op;
  ContextWQ op_work_queue;
  MockTrimRequest mock_trim_request;
  MockJournalRemoveRequest mock_journal_remove_request;
  MockMirrorDisableRequest mock_mirror_disable_request;

  m_mock_imctx->parent_md.spec.pool_id = m_ioctx.get_id();
  m_mock_imctx->parent_md.spec.image_id = "parent id";
  m_mock_imctx->parent_md.spec.snap_id = 234;

  InSequence seq;
  expect_state_open(*m_mock_imctx, 0);
  expect_mirror_image_get(*m_mock_imctx, 0);
  expect_get_group(*m_mock_imctx, 0);
  expect_trim(*m_mock_imctx, mock_trim_request, 0);
  expect_op_work_queue(*m_mock_imctx);

  MockDetachChildRequest mock_detach_child_request;
  expect_detach_child(*m_mock_imctx, mock_detach_child_request, 0);

  expect_mirror_disable(*m_mock_imctx, mock_mirror_disable_request, 0);
  expect_state_close(*m_mock_imctx);
  expect_wq_queue(op_work_queue, 0);
  expect_journal_remove(*m_mock_imctx, mock_journal_remove_request, 0);
  expect_remove_mirror_image(m_ioctx, 0);
  expect_dir_remove_image(m_ioctx, 0);

  MockRemoveRequest *req = MockRemoveRequest::create(m_ioctx, m_image_name, "",
					      true, false, no_op, &op_work_queue, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());

  TestImageRemoveTearDown();
}

TEST_F(TestMockImageRemoveRequest, SuccessV2CloneV2) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);
  TestImageRemoveSetUp();

  C_SaferCond ctx;
  librbd::NoOpProgressContext no_op;
  ContextWQ op_work_queue;
  MockTrimRequest mock_trim_request;
  MockJournalRemoveRequest mock_journal_remove_request;
  MockMirrorDisableRequest mock_mirror_disable_request;

  m_mock_imctx->parent_md.spec.pool_id = m_ioctx.get_id();
  m_mock_imctx->parent_md.spec.image_id = "parent id";
  m_mock_imctx->parent_md.spec.snap_id = 234;

  InSequence seq;
  expect_state_open(*m_mock_imctx, 0);
  expect_mirror_image_get(*m_mock_imctx, 0);
  expect_get_group(*m_mock_imctx, 0);
  expect_trim(*m_mock_imctx, mock_trim_request, 0);
  expect_op_work_queue(*m_mock_imctx);

  MockDetachChildRequest mock_detach_child_request;
  expect_detach_child(*m_mock_imctx, mock_detach_child_request, 0);

  expect_mirror_disable(*m_mock_imctx, mock_mirror_disable_request, 0);
  expect_state_close(*m_mock_imctx);
  expect_wq_queue(op_work_queue, 0);
  expect_journal_remove(*m_mock_imctx, mock_journal_remove_request, 0);
  expect_remove_mirror_image(m_ioctx, 0);
  expect_dir_remove_image(m_ioctx, 0);

  MockRemoveRequest *req = MockRemoveRequest::create(m_ioctx, m_image_name, "",
					      true, false, no_op, &op_work_queue, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());

  TestImageRemoveTearDown();
}

TEST_F(TestMockImageRemoveRequest, NotExistsV2) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);
  TestImageRemoveSetUp();

  C_SaferCond ctx;
  librbd::NoOpProgressContext no_op;
  ContextWQ op_work_queue;
  MockTrimRequest mock_trim_request;
  MockJournalRemoveRequest mock_journal_remove_request;
  MockMirrorDisableRequest mock_mirror_disable_request;

  m_mock_imctx->parent_md.spec.pool_id = m_ioctx.get_id();
  m_mock_imctx->parent_md.spec.image_id = "parent id";
  m_mock_imctx->parent_md.spec.snap_id = 234;

  InSequence seq;
  expect_state_open(*m_mock_imctx, 0);
  expect_mirror_image_get(*m_mock_imctx, 0);
  expect_get_group(*m_mock_imctx, 0);
  expect_trim(*m_mock_imctx, mock_trim_request, 0);
  expect_op_work_queue(*m_mock_imctx);

  MockDetachChildRequest mock_detach_child_request;
  expect_detach_child(*m_mock_imctx, mock_detach_child_request, 0);

  expect_mirror_disable(*m_mock_imctx, mock_mirror_disable_request, 0);
  expect_state_close(*m_mock_imctx);
  expect_wq_queue(op_work_queue, 0);
  expect_journal_remove(*m_mock_imctx, mock_journal_remove_request, 0);
  expect_remove_mirror_image(m_ioctx, 0);
  expect_dir_remove_image(m_ioctx, -ENOENT);

  MockRemoveRequest *req = MockRemoveRequest::create(m_ioctx, m_image_name, "",
					      true, false, no_op, &op_work_queue, &ctx);
  req->send();
  ASSERT_EQ(-ENOENT, ctx.wait());

  TestImageRemoveTearDown();
}

} // namespace image
} // namespace librbd
