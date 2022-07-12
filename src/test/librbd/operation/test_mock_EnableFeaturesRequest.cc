// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/Operations.h"
#include "librbd/internal.h"
#include "librbd/Journal.h"
#include "librbd/image/SetFlagsRequest.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/mirror/EnableRequest.h"
#include "librbd/journal/CreateRequest.h"
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"
#include "librbd/object_map/CreateRequest.h"
#include "librbd/operation/EnableFeaturesRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace librbd {

namespace {

struct MockOperationImageCtx : public MockImageCtx {
  MockOperationImageCtx(librbd::ImageCtx& image_ctx)
    : MockImageCtx(image_ctx) {
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
class CreateRequest<MockOperationImageCtx> {
public:
  static CreateRequest *s_instance;
  Context *on_finish = nullptr;

  static CreateRequest *create(IoCtx &ioctx, const std::string &imageid,
                               uint8_t order, uint8_t splay_width,
                               const std::string &object_pool,
                               uint64_t tag_class, TagData &tag_data,
                               const std::string &client_id,
                               MockContextWQ *op_work_queue,
                               Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  CreateRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

CreateRequest<MockOperationImageCtx> *CreateRequest<MockOperationImageCtx>::s_instance = nullptr;

template <>
struct TypeTraits<MockOperationImageCtx> {
  typedef librbd::MockContextWQ ContextWQ;
};

} // namespace journal

namespace mirror {

template<>
class EnableRequest<MockOperationImageCtx> {
public:
  static EnableRequest *s_instance;
  Context *on_finish = nullptr;

  static EnableRequest *create(MockOperationImageCtx *image_ctx,
                               cls::rbd::MirrorImageMode mirror_image_mode,
                               const std::string& non_primary_global_image_id,
                               bool image_clean, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  EnableRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

EnableRequest<MockOperationImageCtx> *EnableRequest<MockOperationImageCtx>::s_instance = nullptr;

} // namespace mirror

namespace object_map {

template<>
class CreateRequest<MockOperationImageCtx> {
public:
  static CreateRequest *s_instance;
  Context *on_finish = nullptr;

  static CreateRequest *create(MockOperationImageCtx *image_ctx, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  CreateRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

CreateRequest<MockOperationImageCtx> *CreateRequest<MockOperationImageCtx>::s_instance = nullptr;

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
#include "librbd/operation/EnableFeaturesRequest.cc"

namespace librbd {
namespace operation {

using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArg;
using ::testing::_;

class TestMockOperationEnableFeaturesRequest : public TestMockFixture {
public:
  typedef librbd::image::SetFlagsRequest<MockOperationImageCtx> MockSetFlagsRequest;
  typedef librbd::journal::CreateRequest<MockOperationImageCtx> MockCreateJournalRequest;
  typedef librbd::mirror::EnableRequest<MockOperationImageCtx> MockEnableMirrorRequest;
  typedef librbd::object_map::CreateRequest<MockOperationImageCtx> MockCreateObjectMapRequest;
  typedef EnableFeaturesRequest<MockOperationImageCtx> MockEnableFeaturesRequest;

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

  void ensure_features_disabled(librbd::ImageCtx *ictx,
                                uint64_t features_to_disable) {
    uint64_t features;

    ASSERT_EQ(0, librbd::get_features(ictx, &features));
    features_to_disable &= features;
    if (!features_to_disable) {
      return;
    }
    ASSERT_EQ(0, ictx->operations->update_features(features_to_disable, false));
    ASSERT_EQ(0, librbd::get_features(ictx, &features));
    ASSERT_EQ(0U, features & features_to_disable);
  }

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

  void expect_create_journal_request_send(
    MockOperationImageCtx &mock_image_ctx,
    MockCreateJournalRequest &mock_create_journal_request, int r) {
    EXPECT_CALL(mock_create_journal_request, send())
      .WillOnce(FinishRequest(&mock_create_journal_request, r,
                              &mock_image_ctx));
  }

  void expect_enable_mirror_request_send(
    MockOperationImageCtx &mock_image_ctx,
    MockEnableMirrorRequest &mock_enable_mirror_request, int r) {
    EXPECT_CALL(mock_enable_mirror_request, send())
      .WillOnce(FinishRequest(&mock_enable_mirror_request, r,
                              &mock_image_ctx));
  }

  void expect_create_object_map_request_send(
    MockOperationImageCtx &mock_image_ctx,
    MockCreateObjectMapRequest &mock_create_object_map_request, int r) {
    EXPECT_CALL(mock_create_object_map_request, send())
      .WillOnce(FinishRequest(&mock_create_object_map_request, r,
                              &mock_image_ctx));
  }

  void expect_notify_update(MockOperationImageCtx &mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, notify_update(_))
      .WillOnce(CompleteContext(0, mock_image_ctx.image_ctx->op_work_queue));
  }

};

TEST_F(TestMockOperationEnableFeaturesRequest, All) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  uint64_t features;
  ASSERT_EQ(0, librbd::get_features(ictx, &features));

  uint64_t features_to_enable = RBD_FEATURES_MUTABLE & features;

  REQUIRE(features_to_enable);

  ensure_features_disabled(ictx, features_to_enable);

  MockOperationImageCtx mock_image_ctx(*ictx);

  MockSetFlagsRequest mock_set_flags_request;
  MockCreateJournalRequest mock_create_journal_request;
  MockCreateObjectMapRequest mock_create_object_map_request;

  ::testing::InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_block_writes(mock_image_ctx);
  expect_block_requests(mock_image_ctx);
  if (features_to_enable & RBD_FEATURE_JOURNALING) {
    expect_create_journal_request_send(mock_image_ctx,
                                       mock_create_journal_request, 0);
  }
  if (features_to_enable & (RBD_FEATURE_OBJECT_MAP | RBD_FEATURE_FAST_DIFF)) {
    expect_set_flags_request_send(mock_image_ctx,
                                  mock_set_flags_request, 0);
  }
  if (features_to_enable & RBD_FEATURE_OBJECT_MAP) {
    expect_create_object_map_request_send(mock_image_ctx,
                                          mock_create_object_map_request, 0);
  }
  expect_notify_update(mock_image_ctx);
  expect_unblock_requests(mock_image_ctx);
  expect_unblock_writes(mock_image_ctx);
  expect_handle_prepare_lock_complete(mock_image_ctx);

  C_SaferCond cond_ctx;
  MockEnableFeaturesRequest *req = new MockEnableFeaturesRequest(
    mock_image_ctx, &cond_ctx, 0, features_to_enable);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationEnableFeaturesRequest, ObjectMap) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  uint64_t features;
  ASSERT_EQ(0, librbd::get_features(ictx, &features));

  ensure_features_disabled(
    ictx, RBD_FEATURE_OBJECT_MAP | RBD_FEATURE_FAST_DIFF);

  MockOperationImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  expect_verify_lock_ownership(mock_image_ctx);

  MockSetFlagsRequest mock_set_flags_request;
  MockCreateObjectMapRequest mock_create_object_map_request;

  ::testing::InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_block_writes(mock_image_ctx);
  if (mock_image_ctx.journal != nullptr) {
    expect_is_journal_replaying(*mock_image_ctx.journal);
  }
  expect_block_requests(mock_image_ctx);
  expect_append_op_event(mock_image_ctx, true, 0);
  expect_set_flags_request_send(mock_image_ctx,
                                mock_set_flags_request, 0);
  expect_create_object_map_request_send(mock_image_ctx,
                                        mock_create_object_map_request, 0);
  expect_notify_update(mock_image_ctx);
  expect_unblock_requests(mock_image_ctx);
  expect_unblock_writes(mock_image_ctx);
  expect_handle_prepare_lock_complete(mock_image_ctx);
  expect_commit_op_event(mock_image_ctx, 0);

  C_SaferCond cond_ctx;
  MockEnableFeaturesRequest *req = new MockEnableFeaturesRequest(
    mock_image_ctx, &cond_ctx, 0, RBD_FEATURE_OBJECT_MAP);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationEnableFeaturesRequest, ObjectMapError) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  uint64_t features;
  ASSERT_EQ(0, librbd::get_features(ictx, &features));

  ensure_features_disabled(
    ictx, RBD_FEATURE_OBJECT_MAP | RBD_FEATURE_FAST_DIFF);

  MockOperationImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  expect_verify_lock_ownership(mock_image_ctx);

  MockSetFlagsRequest mock_set_flags_request;
  MockCreateObjectMapRequest mock_create_object_map_request;

  ::testing::InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_block_writes(mock_image_ctx);
  if (mock_image_ctx.journal != nullptr) {
    expect_is_journal_replaying(*mock_image_ctx.journal);
  }
  expect_block_requests(mock_image_ctx);
  expect_append_op_event(mock_image_ctx, true, 0);
  expect_set_flags_request_send(mock_image_ctx,
                                mock_set_flags_request, 0);
  expect_create_object_map_request_send(
    mock_image_ctx, mock_create_object_map_request, -EINVAL);
  expect_unblock_requests(mock_image_ctx);
  expect_unblock_writes(mock_image_ctx);
  expect_handle_prepare_lock_complete(mock_image_ctx);
  expect_commit_op_event(mock_image_ctx, -EINVAL);

  C_SaferCond cond_ctx;
  MockEnableFeaturesRequest *req = new MockEnableFeaturesRequest(
    mock_image_ctx, &cond_ctx, 0, RBD_FEATURE_OBJECT_MAP);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

TEST_F(TestMockOperationEnableFeaturesRequest, SetFlagsError) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  uint64_t features;
  ASSERT_EQ(0, librbd::get_features(ictx, &features));

  ensure_features_disabled(
    ictx, RBD_FEATURE_OBJECT_MAP | RBD_FEATURE_FAST_DIFF);

  MockOperationImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  expect_verify_lock_ownership(mock_image_ctx);

  MockSetFlagsRequest mock_set_flags_request;
  MockCreateObjectMapRequest mock_create_object_map_request;

  ::testing::InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_block_writes(mock_image_ctx);
  if (mock_image_ctx.journal != nullptr) {
    expect_is_journal_replaying(*mock_image_ctx.journal);
  }
  expect_block_requests(mock_image_ctx);
  expect_append_op_event(mock_image_ctx, true, 0);
  expect_set_flags_request_send(mock_image_ctx,
                                mock_set_flags_request, -EINVAL);
  expect_unblock_requests(mock_image_ctx);
  expect_unblock_writes(mock_image_ctx);
  expect_handle_prepare_lock_complete(mock_image_ctx);
  expect_commit_op_event(mock_image_ctx, -EINVAL);

  C_SaferCond cond_ctx;
  MockEnableFeaturesRequest *req = new MockEnableFeaturesRequest(
    mock_image_ctx, &cond_ctx, 0, RBD_FEATURE_OBJECT_MAP);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

TEST_F(TestMockOperationEnableFeaturesRequest, Mirroring) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  MirrorModeEnabler mirror_mode_enabler(m_ioctx, cls::rbd::MIRROR_MODE_POOL);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  uint64_t features;
  ASSERT_EQ(0, librbd::get_features(ictx, &features));

  ensure_features_disabled(ictx, RBD_FEATURE_JOURNALING);

  MockOperationImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  expect_verify_lock_ownership(mock_image_ctx);

  MockCreateJournalRequest mock_create_journal_request;
  MockEnableMirrorRequest mock_enable_mirror_request;

  ::testing::InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_block_writes(mock_image_ctx);
  expect_block_requests(mock_image_ctx);
  expect_create_journal_request_send(mock_image_ctx,
                                     mock_create_journal_request, 0);
  expect_enable_mirror_request_send(mock_image_ctx,
                                    mock_enable_mirror_request, 0);
  expect_notify_update(mock_image_ctx);
  expect_unblock_requests(mock_image_ctx);
  expect_unblock_writes(mock_image_ctx);
  expect_handle_prepare_lock_complete(mock_image_ctx);

  C_SaferCond cond_ctx;
  MockEnableFeaturesRequest *req = new MockEnableFeaturesRequest(
    mock_image_ctx, &cond_ctx, 0, RBD_FEATURE_JOURNALING);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationEnableFeaturesRequest, JournalingError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  MirrorModeEnabler mirror_mode_enabler(m_ioctx, cls::rbd::MIRROR_MODE_POOL);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  uint64_t features;
  ASSERT_EQ(0, librbd::get_features(ictx, &features));

  ensure_features_disabled(ictx, RBD_FEATURE_JOURNALING);

  MockOperationImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  expect_verify_lock_ownership(mock_image_ctx);

  MockCreateJournalRequest mock_create_journal_request;
  MockEnableMirrorRequest mock_enable_mirror_request;

  ::testing::InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_block_writes(mock_image_ctx);
  expect_block_requests(mock_image_ctx);
  expect_create_journal_request_send(mock_image_ctx,
                                     mock_create_journal_request, -EINVAL);
  expect_unblock_requests(mock_image_ctx);
  expect_unblock_writes(mock_image_ctx);
  expect_handle_prepare_lock_complete(mock_image_ctx);

  C_SaferCond cond_ctx;
  MockEnableFeaturesRequest *req = new MockEnableFeaturesRequest(
    mock_image_ctx, &cond_ctx, 0, RBD_FEATURE_JOURNALING);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

TEST_F(TestMockOperationEnableFeaturesRequest, MirroringError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  MirrorModeEnabler mirror_mode_enabler(m_ioctx, cls::rbd::MIRROR_MODE_POOL);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  uint64_t features;
  ASSERT_EQ(0, librbd::get_features(ictx, &features));

  ensure_features_disabled(ictx, RBD_FEATURE_JOURNALING);

  MockOperationImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  MockJournal mock_journal;
  MockObjectMap mock_object_map;
  initialize_features(ictx, mock_image_ctx, mock_exclusive_lock, mock_journal,
                      mock_object_map);

  expect_verify_lock_ownership(mock_image_ctx);

  MockCreateJournalRequest mock_create_journal_request;
  MockEnableMirrorRequest mock_enable_mirror_request;

  ::testing::InSequence seq;
  expect_prepare_lock(mock_image_ctx);
  expect_block_writes(mock_image_ctx);
  expect_block_requests(mock_image_ctx);
  expect_create_journal_request_send(mock_image_ctx,
                                     mock_create_journal_request, 0);
  expect_enable_mirror_request_send(mock_image_ctx,
                                    mock_enable_mirror_request, -EINVAL);
  expect_notify_update(mock_image_ctx);
  expect_unblock_requests(mock_image_ctx);
  expect_unblock_writes(mock_image_ctx);
  expect_handle_prepare_lock_complete(mock_image_ctx);

  C_SaferCond cond_ctx;
  MockEnableFeaturesRequest *req = new MockEnableFeaturesRequest(
    mock_image_ctx, &cond_ctx, 0, RBD_FEATURE_JOURNALING);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

} // namespace operation
} // namespace librbd
