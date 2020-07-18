// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockImageState.h"
#include "test/librbd/mock/MockOperations.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "librbd/journal/PromoteRequest.h"
#include "librbd/mirror/DisableRequest.h"
#include "librbd/mirror/GetInfoRequest.h"
#include "librbd/mirror/ImageRemoveRequest.h"
#include "librbd/mirror/ImageStateUpdateRequest.h"
#include "librbd/mirror/snapshot/PromoteRequest.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public MockImageCtx {
  explicit MockTestImageCtx(librbd::ImageCtx& image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace journal {

template <>
struct PromoteRequest<librbd::MockTestImageCtx> {
  Context *on_finish = nullptr;
  static PromoteRequest *s_instance;
  static PromoteRequest *create(librbd::MockTestImageCtx *, bool force,
                                Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  PromoteRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

PromoteRequest<librbd::MockTestImageCtx> *PromoteRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace journal

namespace mirror {
template <>
struct GetInfoRequest<librbd::MockTestImageCtx> {
  cls::rbd::MirrorImage *mirror_image;
  PromotionState *promotion_state;
  Context *on_finish = nullptr;
  static GetInfoRequest *s_instance;
  static GetInfoRequest *create(librbd::MockTestImageCtx &,
                                cls::rbd::MirrorImage *mirror_image,
                                PromotionState *promotion_state,
                                std::string* primary_mirror_uuid,
                                Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->mirror_image = mirror_image;
    s_instance->promotion_state = promotion_state;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  GetInfoRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

template <>
struct ImageRemoveRequest<librbd::MockTestImageCtx> {
  static ImageRemoveRequest* s_instance;
  Context* on_finish = nullptr;

  static ImageRemoveRequest* create(
      librados::IoCtx& io_ctx,
      const std::string& global_image_id,
      const std::string& image_id,
      Context* on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  ImageRemoveRequest() {
    s_instance = this;
  }
};

template <>
struct ImageStateUpdateRequest<librbd::MockTestImageCtx> {
  static ImageStateUpdateRequest* s_instance;
  Context* on_finish = nullptr;

  static ImageStateUpdateRequest* create(
      librados::IoCtx& io_ctx,
      const std::string& image_id,
      cls::rbd::MirrorImageState mirror_image_state,
      const cls::rbd::MirrorImage& mirror_image,
      Context* on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  ImageStateUpdateRequest() {
    s_instance = this;
  }
};

GetInfoRequest<librbd::MockTestImageCtx> *GetInfoRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
ImageRemoveRequest<librbd::MockTestImageCtx> *ImageRemoveRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
ImageStateUpdateRequest<librbd::MockTestImageCtx> *ImageStateUpdateRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

namespace snapshot {

template <>
struct PromoteRequest<librbd::MockTestImageCtx> {
  Context *on_finish = nullptr;
  static PromoteRequest *s_instance;
  static PromoteRequest *create(librbd::MockTestImageCtx*,
                                const std::string& global_image_id,
                                Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  PromoteRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

PromoteRequest<librbd::MockTestImageCtx> *PromoteRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace snapshot
} // namespace mirror
} // namespace librbd

// template definitions
#include "librbd/mirror/DisableRequest.cc"
template class librbd::mirror::DisableRequest<librbd::MockTestImageCtx>;

ACTION_P(TestFeatures, image_ctx) {
  return ((image_ctx->features & arg0) != 0);
}

namespace librbd {
namespace mirror {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockMirrorDisableRequest : public TestMockFixture {
public:
  typedef DisableRequest<MockTestImageCtx> MockDisableRequest;
  typedef Journal<MockTestImageCtx> MockJournal;
  typedef journal::PromoteRequest<MockTestImageCtx> MockJournalPromoteRequest;
  typedef mirror::GetInfoRequest<MockTestImageCtx> MockGetInfoRequest;
  typedef mirror::ImageRemoveRequest<MockTestImageCtx> MockImageRemoveRequest;
  typedef mirror::ImageStateUpdateRequest<MockTestImageCtx> MockImageStateUpdateRequest;
  typedef mirror::snapshot::PromoteRequest<MockTestImageCtx> MockSnapshotPromoteRequest;

  void expect_get_mirror_info(MockTestImageCtx &mock_image_ctx,
                              MockGetInfoRequest &mock_get_info_request,
                              const cls::rbd::MirrorImage &mirror_image,
                              PromotionState promotion_state, int r) {

    EXPECT_CALL(mock_get_info_request, send())
      .WillOnce(
        Invoke([&mock_image_ctx, &mock_get_info_request, mirror_image,
                promotion_state, r]() {
                 if (r == 0) {
                   *mock_get_info_request.mirror_image = mirror_image;
                   *mock_get_info_request.promotion_state = promotion_state;
                 }
                 mock_image_ctx.op_work_queue->queue(
                   mock_get_info_request.on_finish, r);
               }));
  }

  void expect_mirror_image_state_update(
      MockTestImageCtx &mock_image_ctx,
      MockImageStateUpdateRequest& mock_request, int r) {
    EXPECT_CALL(mock_request, send())
      .WillOnce(
        Invoke([&mock_image_ctx, &mock_request, r]() {
          mock_image_ctx.op_work_queue->queue(mock_request.on_finish, r);
        }));
  }

  void expect_mirror_image_remove(
      MockTestImageCtx &mock_image_ctx,
      MockImageRemoveRequest& mock_request, int r) {
    EXPECT_CALL(mock_request, send())
      .WillOnce(
        Invoke([&mock_image_ctx, &mock_request, r]() {
          mock_image_ctx.op_work_queue->queue(mock_request.on_finish, r);
        }));
  }

  void expect_journal_client_list(MockTestImageCtx &mock_image_ctx,
                                  const std::set<cls::journal::Client> &clients,
                                  int r) {
    bufferlist bl;
    using ceph::encode;
    encode(clients, bl);

    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(::journal::Journaler::header_oid(mock_image_ctx.id),
                     _, StrEq("journal"), StrEq("client_list"), _, _, _, _))
      .WillOnce(DoAll(WithArg<5>(CopyInBufferlist(bl)),
                      Return(r)));
  }

  void expect_journal_client_unregister(MockTestImageCtx &mock_image_ctx,
                                        const std::string &client_id,
                                        int r) {
    bufferlist bl;
    using ceph::encode;
    encode(client_id, bl);

    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(::journal::Journaler::header_oid(mock_image_ctx.id),
                     _, StrEq("journal"), StrEq("client_unregister"),
                     ContentsEqual(bl), _, _, _))
      .WillOnce(Return(r));
  }

  void expect_journal_promote(MockTestImageCtx &mock_image_ctx,
                              MockJournalPromoteRequest &mock_promote_request,
                              int r) {
    EXPECT_CALL(mock_promote_request, send())
      .WillOnce(FinishRequest(&mock_promote_request, r, &mock_image_ctx));
  }

  void expect_snapshot_promote(MockTestImageCtx &mock_image_ctx,
                               MockSnapshotPromoteRequest &mock_promote_request,
                               int r) {
    EXPECT_CALL(mock_promote_request, send())
      .WillOnce(FinishRequest(&mock_promote_request, r, &mock_image_ctx));
  }

  void expect_is_refresh_required(MockTestImageCtx &mock_image_ctx,
                                  bool refresh_required) {
    EXPECT_CALL(*mock_image_ctx.state, is_refresh_required())
      .WillOnce(Return(refresh_required));
  }

  void expect_refresh_image(MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.state, refresh(_))
      .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_snap_remove(MockTestImageCtx &mock_image_ctx,
                          const std::string &snap_name, int r) {
    EXPECT_CALL(*mock_image_ctx.operations, snap_remove(_, StrEq(snap_name), _))
      .WillOnce(WithArg<2>(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue)));
  }

  template <typename T>
  bufferlist encode(const T &t) {
    using ceph::encode;
    bufferlist bl;
    encode(t, bl);
    return bl;
  }

};

TEST_F(TestMockMirrorDisableRequest, Success) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);
  expect_snap_remove(mock_image_ctx, "snap 1", 0);
  expect_snap_remove(mock_image_ctx, "snap 2", 0);

  InSequence seq;

  MockGetInfoRequest mock_get_info_request;
  expect_get_mirror_info(
    mock_image_ctx, mock_get_info_request,
    {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "global id",
     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, PROMOTION_STATE_PRIMARY, 0);
  MockImageStateUpdateRequest mock_image_state_update_request;
  expect_mirror_image_state_update(
    mock_image_ctx, mock_image_state_update_request, 0);
  expect_journal_client_list(
    mock_image_ctx, {
      {"", encode(journal::ClientData{journal::ImageClientMeta{}})},
      {"peer 1", encode(journal::ClientData{journal::MirrorPeerClientMeta{}})},
      {"peer 2", encode(journal::ClientData{journal::MirrorPeerClientMeta{
        "remote image id", {{cls::rbd::UserSnapshotNamespace(), "snap 1", boost::optional<uint64_t>(0)},
                            {cls::rbd::UserSnapshotNamespace(), "snap 2", boost::optional<uint64_t>(0)}}}
      })}
    }, 0);
  expect_journal_client_unregister(mock_image_ctx, "peer 1", 0);
  expect_journal_client_unregister(mock_image_ctx, "peer 2", 0);
  expect_journal_client_list(mock_image_ctx, {}, 0);
  MockImageRemoveRequest mock_image_remove_request;
  expect_mirror_image_remove(
    mock_image_ctx, mock_image_remove_request, 0);

  C_SaferCond ctx;
  auto req = new MockDisableRequest(&mock_image_ctx, false, true, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockMirrorDisableRequest, SuccessNoRemove) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  MockGetInfoRequest mock_get_info_request;
  expect_get_mirror_info(
    mock_image_ctx, mock_get_info_request,
    {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "global id",
     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, PROMOTION_STATE_PRIMARY, 0);
  MockImageStateUpdateRequest mock_image_state_update_request;
  expect_mirror_image_state_update(
    mock_image_ctx, mock_image_state_update_request, 0);
  expect_journal_client_list(mock_image_ctx, {}, 0);

  C_SaferCond ctx;
  auto req = new MockDisableRequest(&mock_image_ctx, false, false, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockMirrorDisableRequest, SuccessNonPrimary) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockJournalPromoteRequest mock_promote_request;

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  MockGetInfoRequest mock_get_info_request;
  expect_get_mirror_info(
    mock_image_ctx, mock_get_info_request,
    {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "global id",
     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, PROMOTION_STATE_NON_PRIMARY, 0);
  MockImageStateUpdateRequest mock_image_state_update_request;
  expect_mirror_image_state_update(
    mock_image_ctx, mock_image_state_update_request, 0);
  expect_journal_promote(mock_image_ctx, mock_promote_request, 0);
  expect_is_refresh_required(mock_image_ctx, false);
  expect_journal_client_list(mock_image_ctx, {}, 0);
  MockImageRemoveRequest mock_image_remove_request;
  expect_mirror_image_remove(
    mock_image_ctx, mock_image_remove_request, 0);

  C_SaferCond ctx;
  auto req = new MockDisableRequest(&mock_image_ctx, true, true, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockMirrorDisableRequest, NonPrimaryError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  MockGetInfoRequest mock_get_info_request;
  expect_get_mirror_info(
    mock_image_ctx, mock_get_info_request,
    {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "global id",
     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, PROMOTION_STATE_NON_PRIMARY, 0);

  C_SaferCond ctx;
  auto req = new MockDisableRequest(&mock_image_ctx, false, false, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorDisableRequest, GetMirrorInfoError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  MockGetInfoRequest mock_get_info_request;
  expect_get_mirror_info(
    mock_image_ctx, mock_get_info_request,
    {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "global id",
     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, PROMOTION_STATE_PRIMARY, -EINVAL);

  C_SaferCond ctx;
  auto req = new MockDisableRequest(&mock_image_ctx, false, true, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorDisableRequest, MirrorImageSetError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  MockGetInfoRequest mock_get_info_request;
  expect_get_mirror_info(
    mock_image_ctx, mock_get_info_request,
    {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "global id",
     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, PROMOTION_STATE_PRIMARY, 0);
  MockImageStateUpdateRequest mock_image_state_update_request;
  expect_mirror_image_state_update(
    mock_image_ctx, mock_image_state_update_request, -ENOENT);

  C_SaferCond ctx;
  auto req = new MockDisableRequest(&mock_image_ctx, false, true, &ctx);
  req->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockMirrorDisableRequest, JournalPromoteError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockJournalPromoteRequest mock_promote_request;

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  MockGetInfoRequest mock_get_info_request;
  expect_get_mirror_info(
    mock_image_ctx, mock_get_info_request,
    {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "global id",
     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, PROMOTION_STATE_NON_PRIMARY, 0);
  MockImageStateUpdateRequest mock_image_state_update_request;
  expect_mirror_image_state_update(
    mock_image_ctx, mock_image_state_update_request, 0);
  expect_journal_promote(mock_image_ctx, mock_promote_request, -EPERM);

  C_SaferCond ctx;
  auto req = new MockDisableRequest(&mock_image_ctx, true, true, &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockMirrorDisableRequest, JournalClientListError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  MockGetInfoRequest mock_get_info_request;
  expect_get_mirror_info(
    mock_image_ctx, mock_get_info_request,
    {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "global id",
     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, PROMOTION_STATE_PRIMARY, 0);
  MockImageStateUpdateRequest mock_image_state_update_request;
  expect_mirror_image_state_update(
    mock_image_ctx, mock_image_state_update_request, 0);
  expect_journal_client_list(mock_image_ctx, {}, -EBADMSG);

  C_SaferCond ctx;
  auto req = new MockDisableRequest(&mock_image_ctx, false, true, &ctx);
  req->send();
  ASSERT_EQ(-EBADMSG, ctx.wait());
}

TEST_F(TestMockMirrorDisableRequest, SnapRemoveError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);
  expect_snap_remove(mock_image_ctx, "snap 1", 0);
  expect_snap_remove(mock_image_ctx, "snap 2", -EPERM);

  InSequence seq;

  MockGetInfoRequest mock_get_info_request;
  expect_get_mirror_info(
    mock_image_ctx, mock_get_info_request,
    {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "global id",
     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, PROMOTION_STATE_PRIMARY, 0);
  MockImageStateUpdateRequest mock_image_state_update_request;
  expect_mirror_image_state_update(
    mock_image_ctx, mock_image_state_update_request, 0);
  expect_journal_client_list(
    mock_image_ctx, {
      {"", encode(journal::ClientData{journal::ImageClientMeta{}})},
      {"peer 1", encode(journal::ClientData{journal::MirrorPeerClientMeta{}})},
      {"peer 2", encode(journal::ClientData{journal::MirrorPeerClientMeta{
        "remote image id", {{cls::rbd::UserSnapshotNamespace(), "snap 1", boost::optional<uint64_t>(0)},
                            {cls::rbd::UserSnapshotNamespace(), "snap 2", boost::optional<uint64_t>(0)}}}
      })}
    }, 0);
  expect_journal_client_unregister(mock_image_ctx, "peer 1", 0);

  C_SaferCond ctx;
  auto req = new MockDisableRequest(&mock_image_ctx, false, true, &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockMirrorDisableRequest, JournalClientUnregisterError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);
  expect_snap_remove(mock_image_ctx, "snap 1", 0);
  expect_snap_remove(mock_image_ctx, "snap 2", 0);

  InSequence seq;

  MockGetInfoRequest mock_get_info_request;
  expect_get_mirror_info(
    mock_image_ctx, mock_get_info_request,
    {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "global id",
     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, PROMOTION_STATE_PRIMARY, 0);
  MockImageStateUpdateRequest mock_image_state_update_request;
  expect_mirror_image_state_update(
    mock_image_ctx, mock_image_state_update_request, 0);
  expect_journal_client_list(
    mock_image_ctx, {
      {"", encode(journal::ClientData{journal::ImageClientMeta{}})},
      {"peer 1", encode(journal::ClientData{journal::MirrorPeerClientMeta{}})},
      {"peer 2", encode(journal::ClientData{journal::MirrorPeerClientMeta{
        "remote image id", {{cls::rbd::UserSnapshotNamespace(), "snap 1", boost::optional<uint64_t>(0)},
                            {cls::rbd::UserSnapshotNamespace(), "snap 2", boost::optional<uint64_t>(0)}}}
      })}
    }, 0);
  expect_journal_client_unregister(mock_image_ctx, "peer 1", -EINVAL);
  expect_journal_client_unregister(mock_image_ctx, "peer 2", 0);

  C_SaferCond ctx;
  auto req = new MockDisableRequest(&mock_image_ctx, false, true, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorDisableRequest, SnapshotPromoteError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockSnapshotPromoteRequest mock_promote_request;

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  MockGetInfoRequest mock_get_info_request;
  expect_get_mirror_info(
    mock_image_ctx, mock_get_info_request,
    {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, "global id",
     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, PROMOTION_STATE_NON_PRIMARY, 0);
  MockImageStateUpdateRequest mock_image_state_update_request;
  expect_mirror_image_state_update(
    mock_image_ctx, mock_image_state_update_request, 0);
  expect_snapshot_promote(mock_image_ctx, mock_promote_request, -EPERM);

  C_SaferCond ctx;
  auto req = new MockDisableRequest(&mock_image_ctx, true, true, &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockMirrorDisableRequest, RefreshError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockSnapshotPromoteRequest mock_promote_request;

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  MockGetInfoRequest mock_get_info_request;
  expect_get_mirror_info(
    mock_image_ctx, mock_get_info_request,
    {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, "global id",
     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, PROMOTION_STATE_NON_PRIMARY, 0);
  MockImageStateUpdateRequest mock_image_state_update_request;
  expect_mirror_image_state_update(
    mock_image_ctx, mock_image_state_update_request, 0);
  expect_snapshot_promote(mock_image_ctx, mock_promote_request, 0);
  expect_is_refresh_required(mock_image_ctx, true);
  expect_refresh_image(mock_image_ctx, -EPERM);

  C_SaferCond ctx;
  auto req = new MockDisableRequest(&mock_image_ctx, true, true, &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockMirrorDisableRequest, MirrorImageRemoveError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  MockGetInfoRequest mock_get_info_request;
  expect_get_mirror_info(
    mock_image_ctx, mock_get_info_request,
    {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "global id",
     cls::rbd::MIRROR_IMAGE_STATE_ENABLED}, PROMOTION_STATE_PRIMARY, 0);
  MockImageStateUpdateRequest mock_image_state_update_request;
  expect_mirror_image_state_update(
    mock_image_ctx, mock_image_state_update_request, 0);
  expect_journal_client_list(mock_image_ctx, {}, 0);
  MockImageRemoveRequest mock_image_remove_request;
  expect_mirror_image_remove(
    mock_image_ctx, mock_image_remove_request, -EINVAL);

  C_SaferCond ctx;
  auto req = new MockDisableRequest(&mock_image_ctx, false, true, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace mirror
} // namespace librbd
