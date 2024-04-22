// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockJournalPolicy.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/internal.h"
#include "librbd/Journal.h"
#include "librbd/image/SetFlagsRequest.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/mirror/DisableRequest.h"
#include "librbd/journal/RemoveRequest.h"
#include "librbd/journal/StandardPolicy.h"
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"
#include "librbd/object_map/RemoveRequest.h"
#include "librbd/operation/DisableFeaturesRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace librbd {

namespace {

struct MockOperationImageCtx : public MockImageCtx {
  MockOperationImageCtx(librbd::ImageCtx& image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

template<>
struct Journal<MockOperationImageCtx> {
  static void get_work_queue(CephContext*, MockContextWQ**) {
  }
};

namespace image {

template<>
class SetFlagsRequest<MockOperationImageCtx> {
public:
  static SetFlagsRequest *s_instance;
  Context *on_finish = nullptr;

  static SetFlagsRequest *create(MockOperationImageCtx *image_ctx, uint64_t flags,
				 uint64_t mask, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  SetFlagsRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

SetFlagsRequest<MockOperationImageCtx> *SetFlagsRequest<MockOperationImageCtx>::s_instance;

} // namespace image

namespace journal {

template<>
class RemoveRequest<MockOperationImageCtx> {
public:
  static RemoveRequest *s_instance;
  Context *on_finish = nullptr;

  static RemoveRequest *create(IoCtx &ioctx, const std::string &imageid,
                               const std::string &client_id,
                               MockContextWQ *op_work_queue,
                               Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  RemoveRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

RemoveRequest<MockOperationImageCtx> *RemoveRequest<MockOperationImageCtx>::s_instance;

template<>
class StandardPolicy<MockOperationImageCtx> : public MockJournalPolicy {
public:
  StandardPolicy(MockOperationImageCtx* image_ctx) {
  }
};

template <>
struct TypeTraits<MockOperationImageCtx> {
  typedef librbd::MockContextWQ ContextWQ;
};

} // namespace journal

namespace mirror {

template<>
class DisableRequest<MockOperationImageCtx> {
public:
  static DisableRequest *s_instance;
  Context *on_finish = nullptr;

  static DisableRequest *create(MockOperationImageCtx *image_ctx, bool force,
                                bool remove, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  DisableRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

DisableRequest<MockOperationImageCtx> *DisableRequest<MockOperationImageCtx>::s_instance;

} // namespace mirror

namespace object_map {

template<>
class RemoveRequest<MockOperationImageCtx> {
public:
  static RemoveRequest *s_instance;
  Context *on_finish = nullptr;

  static RemoveRequest *create(MockOperationImageCtx *image_ctx, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  RemoveRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

RemoveRequest<MockOperationImageCtx> *RemoveRequest<MockOperationImageCtx>::s_instance;

} // namespace object_map

template <>
struct AsyncRequest<MockOperationImageCtx> : public AsyncRequest<MockImageCtx> {
  MockOperationImageCtx &m_image_ctx;

  AsyncRequest(MockOperationImageCtx &image_ctx, Context *on_finish)
    : AsyncRequest<MockImageCtx>(image_ctx, on_finish), m_image_ctx(image_ctx) {
  }
};

} // namespace librbd

// template definitions
#include "librbd/AsyncRequest.cc"
#include "librbd/AsyncObjectThrottle.cc"
#include "librbd/operation/Request.cc"
#include "librbd/operation/DisableFeaturesRequest.cc"

namespace librbd {
namespace operation {

using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArg;
using ::testing::_;

class TestMockOperationDisableFeaturesRequest : public TestMockFixture {
public:
  typedef librbd::image::SetFlagsRequest<MockOperationImageCtx> MockSetFlagsRequest;
  typedef librbd::journal::RemoveRequest<MockOperationImageCtx> MockRemoveJournalRequest;
  typedef librbd::mirror::DisableRequest<MockOperationImageCtx> MockDisableMirrorRequest;
  typedef librbd::object_map::RemoveRequest<MockOperationImageCtx> MockRemoveObjectMapRequest;
  typedef DisableFeaturesRequest<MockOperationImageCtx> MockDisableFeaturesRequest;

  class MirrorModeEnabler {
  public:
    MirrorModeEnabler(librados::IoCtx &ioctx, cls::rbd::MirrorMode mirror_mode)
      : m_ioctx(ioctx), m_mirror_mode(mirror_mode) {
      EXPECT_EQ(0, librbd::cls_client::mirror_uuid_set(&m_ioctx, "test-uuid"));
      EXPECT_EQ(0, librbd::cls_client::mirror_mode_set(&m_ioctx, m_mirror_mode));
    }

    ~MirrorModeEnabler() {
      EXPECT_EQ(0, librbd::cls_client::mirror_mode_set(
                &m_ioctx, cls::rbd::MIRROR_MODE_DISABLED));
    }
  private:
    librados::IoCtx &m_ioctx;
    cls::rbd::MirrorMode m_mirror_mode;
  };

  void expect_prepare_lock(MockOperationImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.state, prepare_lock(_))
      .WillOnce(Invoke([](Context *on_ready) {
	    on_ready->complete(0);
	  }));
    expect_op_work_queue(mock_image_ctx);
  }

  void expect_handle_prepare_lock_complete(MockOperationImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.state, handle_prepare_lock_complete());
  }

  void expect_block_writes(MockOperationImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.io_image_dispatcher, block_writes(_))
      .WillOnce(CompleteContext(0, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_unblock_writes(MockOperationImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.io_image_dispatcher, unblock_writes()).Times(1);
  }

  void expect_verify_lock_ownership(MockOperationImageCtx &mock_image_ctx) {
    if (mock_image_ctx.exclusive_lock != nullptr) {
      EXPECT_CALL(*mock_image_ctx.exclusive_lock, is_lock_owner())
	.WillRepeatedly(Return(true));
    }
  }

  void expect_block_requests(MockOperationImageCtx &mock_image_ctx) {
    if (mock_image_ctx.exclusive_lock != nullptr) {
      EXPECT_CALL(*mock_image_ctx.exclusive_lock, block_requests(0)).Times(1);
    }
  }

  void expect_unblock_requests(MockOperationImageCtx &mock_image_ctx) {
    if (mock_image_ctx.exclusive_lock != nullptr) {
      EXPECT_CALL(*mock_image_ctx.exclusive_lock, unblock_requests()).Times(1);
    }
  }

  void expect_set_flags_request_send(
    MockOperationImageCtx &mock_image_ctx,
    MockSetFlagsRequest &mock_set_flags_request, int r) {
    EXPECT_CALL(mock_set_flags_request, send())
      .WillOnce(FinishRequest(&mock_set_flags_request, r,
                              &mock_image_ctx));
  }

  void expect_disable_mirror_request_send(
    MockOperationImageCtx &mock_image_ctx,
    MockDisableMirrorRequest &mock_disable_mirror_request, int r) {
    EXPECT_CALL(mock_disable_mirror_request, send())
      .WillOnce(FinishRequest(&mock_disable_mirror_request, r,
                              &mock_image_ctx));
  }

  void expect_close_journal(MockOperationImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.journal, close(_))
      .WillOnce(Invoke([&mock_image_ctx, r](Context *on_finish) {
	    mock_image_ctx.journal = nullptr;
	    mock_image_ctx.image_ctx->op_work_queue->queue(on_finish, r);
	  }));
  }

  void expect_remove_journal_request_send(
    MockOperationImageCtx &mock_image_ctx,
    MockRemoveJournalRequest &mock_remove_journal_request, int r) {
    EXPECT_CALL(mock_remove_journal_request, send())
      .WillOnce(FinishRequest(&mock_remove_journal_request, r,
                              &mock_image_ctx));
  }

  void expect_remove_object_map_request_send(
    MockOperationImageCtx &mock_image_ctx,
    MockRemoveObjectMapRequest &mock_remove_object_map_request, int r) {
    EXPECT_CALL(mock_remove_object_map_request, send())
      .WillOnce(FinishRequest(&mock_remove_object_map_request, r,
                              &mock_image_ctx));
  }

  void expect_notify_update(MockOperationImageCtx &mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, notify_update(_))
      .WillOnce(CompleteContext(0, mock_image_ctx.image_ctx->op_work_queue));
  }

};

TEST_F(TestMockOperationDisableFeaturesRequest, All) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  uint64_t features;
  ASSERT_EQ(0, librbd::get_features(ictx, &features));

  uint64_t features_to_disable = RBD_FEATURES_MUTABLE & features;

  REQUIRE(features_to_disable);

  MockOperationImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
		      mock_object_map);

  expect_verify_lock_ownership(mock_image_ctx);

  MockSetFlagsRequest mock_set_flags_request;
  MockRemoveJournalRequest mock_remove_journal_request;
  MockDisableMirrorRequest mock_disable_mirror_request;
  MockRemoveObjectMapRequest mock_remove_object_map_request;

  ::testing::InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_block_writes(mock_image_ctx);
  if (mock_image_ctx.journal != nullptr) {
    expect_is_journal_replaying(*mock_image_ctx.journal);
  }
  expect_block_requests(mock_image_ctx);
  if (features_to_disable & RBD_FEATURE_JOURNALING) {
    expect_disable_mirror_request_send(mock_image_ctx,
                                       mock_disable_mirror_request, 0);
    expect_close_journal(mock_image_ctx, 0);
    expect_remove_journal_request_send(mock_image_ctx,
                                       mock_remove_journal_request, 0);
  }
  if (features_to_disable & RBD_FEATURE_OBJECT_MAP) {
    expect_remove_object_map_request_send(mock_image_ctx,
					  mock_remove_object_map_request, 0);
  }
  if (features_to_disable & (RBD_FEATURE_OBJECT_MAP | RBD_FEATURE_FAST_DIFF)) {
    expect_set_flags_request_send(mock_image_ctx,
				  mock_set_flags_request, 0);
  }
  expect_notify_update(mock_image_ctx);
  expect_unblock_requests(mock_image_ctx);
  expect_unblock_writes(mock_image_ctx);
  expect_handle_prepare_lock_complete(mock_image_ctx);

  C_SaferCond cond_ctx;
  MockDisableFeaturesRequest *req = new MockDisableFeaturesRequest(
    mock_image_ctx, &cond_ctx, 0, features_to_disable, false);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationDisableFeaturesRequest, ObjectMap) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockOperationImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
		      mock_object_map);

  expect_verify_lock_ownership(mock_image_ctx);

  MockSetFlagsRequest mock_set_flags_request;
  MockRemoveObjectMapRequest mock_remove_object_map_request;

  ::testing::InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_block_writes(mock_image_ctx);
  if (mock_image_ctx.journal != nullptr) {
    expect_is_journal_replaying(*mock_image_ctx.journal);
  }
  expect_block_requests(mock_image_ctx);
  expect_append_op_event(mock_image_ctx, true, 0);
  expect_remove_object_map_request_send(mock_image_ctx,
                                        mock_remove_object_map_request, 0);
  expect_set_flags_request_send(mock_image_ctx,
                                mock_set_flags_request, 0);
  expect_notify_update(mock_image_ctx);
  expect_unblock_requests(mock_image_ctx);
  expect_unblock_writes(mock_image_ctx);
  expect_handle_prepare_lock_complete(mock_image_ctx);
  expect_commit_op_event(mock_image_ctx, 0);

  C_SaferCond cond_ctx;
  MockDisableFeaturesRequest *req = new MockDisableFeaturesRequest(
    mock_image_ctx, &cond_ctx, 0,
    RBD_FEATURE_OBJECT_MAP | RBD_FEATURE_FAST_DIFF, false);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationDisableFeaturesRequest, ObjectMapError) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockOperationImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
		      mock_object_map);

  expect_verify_lock_ownership(mock_image_ctx);

  MockSetFlagsRequest mock_set_flags_request;
  MockRemoveObjectMapRequest mock_remove_object_map_request;

  ::testing::InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_block_writes(mock_image_ctx);
  if (mock_image_ctx.journal != nullptr) {
    expect_is_journal_replaying(*mock_image_ctx.journal);
  }
  expect_block_requests(mock_image_ctx);
  expect_append_op_event(mock_image_ctx, true, 0);
  expect_remove_object_map_request_send(mock_image_ctx,
                                        mock_remove_object_map_request, -EINVAL);
  expect_unblock_requests(mock_image_ctx);
  expect_unblock_writes(mock_image_ctx);
  expect_handle_prepare_lock_complete(mock_image_ctx);
  expect_commit_op_event(mock_image_ctx, -EINVAL);

  C_SaferCond cond_ctx;
  MockDisableFeaturesRequest *req = new MockDisableFeaturesRequest(
    mock_image_ctx, &cond_ctx, 0,
    RBD_FEATURE_OBJECT_MAP | RBD_FEATURE_FAST_DIFF, false);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

TEST_F(TestMockOperationDisableFeaturesRequest, Mirroring) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  MirrorModeEnabler mirror_mode_enabler(m_ioctx, cls::rbd::MIRROR_MODE_POOL);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockOperationImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
		      mock_object_map);

  expect_verify_lock_ownership(mock_image_ctx);

  MockRemoveJournalRequest mock_remove_journal_request;
  MockDisableMirrorRequest mock_disable_mirror_request;

  ::testing::InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_block_writes(mock_image_ctx);
  expect_is_journal_replaying(*mock_image_ctx.journal);
  expect_block_requests(mock_image_ctx);
  expect_disable_mirror_request_send(mock_image_ctx,
                                     mock_disable_mirror_request, 0);
  expect_close_journal(mock_image_ctx, 0);
  expect_remove_journal_request_send(mock_image_ctx,
                                     mock_remove_journal_request, 0);
  expect_notify_update(mock_image_ctx);
  expect_unblock_requests(mock_image_ctx);
  expect_unblock_writes(mock_image_ctx);
  expect_handle_prepare_lock_complete(mock_image_ctx);

  C_SaferCond cond_ctx;
  MockDisableFeaturesRequest *req = new MockDisableFeaturesRequest(
    mock_image_ctx, &cond_ctx, 0, RBD_FEATURE_JOURNALING, false);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationDisableFeaturesRequest, MirroringError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockOperationImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
		      mock_object_map);

  expect_verify_lock_ownership(mock_image_ctx);

  MockRemoveJournalRequest mock_remove_journal_request;
  MockDisableMirrorRequest mock_disable_mirror_request;

  ::testing::InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_block_writes(mock_image_ctx);
  expect_is_journal_replaying(*mock_image_ctx.journal);
  expect_block_requests(mock_image_ctx);
  expect_disable_mirror_request_send(mock_image_ctx,
                                     mock_disable_mirror_request, -EINVAL);
  expect_close_journal(mock_image_ctx, 0);
  expect_remove_journal_request_send(mock_image_ctx,
                                     mock_remove_journal_request, 0);
  expect_notify_update(mock_image_ctx);
  expect_unblock_requests(mock_image_ctx);
  expect_unblock_writes(mock_image_ctx);
  expect_handle_prepare_lock_complete(mock_image_ctx);

  C_SaferCond cond_ctx;
  MockDisableFeaturesRequest *req = new MockDisableFeaturesRequest(
    mock_image_ctx, &cond_ctx, 0, RBD_FEATURE_JOURNALING, false);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

} // namespace operation
} // namespace librbd
