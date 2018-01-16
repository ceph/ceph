// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockOperations.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "common/Mutex.h"
#include "librbd/MirroringWatcher.h"
#include "librbd/journal/PromoteRequest.h"
#include "librbd/mirror/DisableRequest.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx& image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

template <>
struct Journal<librbd::MockTestImageCtx> {
  static Journal *s_instance;
  static void is_tag_owner(librbd::MockTestImageCtx *, bool *is_primary,
                           Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->is_tag_owner(is_primary, on_finish);
  }

  Journal() {
    s_instance = this;
  }

  MOCK_METHOD2(is_tag_owner, void(bool*, Context*));
};

Journal<librbd::MockTestImageCtx> *Journal<librbd::MockTestImageCtx>::s_instance = nullptr;

template <>
struct MirroringWatcher<librbd::MockTestImageCtx> {
  static MirroringWatcher *s_instance;
  static void notify_image_updated(librados::IoCtx &io_ctx,
                                   cls::rbd::MirrorImageState mirror_image_state,
                                   const std::string &image_id,
                                   const std::string &global_image_id,
                                   Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->notify_image_updated(mirror_image_state, image_id,
                                     global_image_id, on_finish);
  }

  MirroringWatcher() {
    s_instance = this;
  }

  MOCK_METHOD4(notify_image_updated, void(cls::rbd::MirrorImageState,
                                          const std::string &,
                                          const std::string &,
                                          Context *));
};

MirroringWatcher<librbd::MockTestImageCtx> *MirroringWatcher<librbd::MockTestImageCtx>::s_instance = nullptr;

namespace journal {

template <>
struct PromoteRequest<librbd::MockTestImageCtx> {
  Context *on_finish = nullptr;
  static PromoteRequest *s_instance;
  static PromoteRequest *create(librbd::MockTestImageCtx *, bool force,
                                Context *on_finish) {
    assert(s_instance != nullptr);
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

} // namespace librbd

// template definitions
#include "librbd/mirror/DisableRequest.cc"
template class librbd::mirror::DisableRequest<librbd::MockTestImageCtx>;

namespace librbd {
namespace mirror {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockMirrorDisableRequest : public TestMockFixture {
public:
  typedef DisableRequest<MockTestImageCtx> MockDisableRequest;
  typedef Journal<MockTestImageCtx> MockJournal;
  typedef MirroringWatcher<MockTestImageCtx> MockMirroringWatcher;
  typedef journal::PromoteRequest<MockTestImageCtx> MockPromoteRequest;

  void expect_get_mirror_image(MockTestImageCtx &mock_image_ctx,
                               const cls::rbd::MirrorImage &mirror_image,
                               int r) {
    using ceph::encode;
    bufferlist bl;
    encode(mirror_image, bl);

    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(RBD_MIRRORING, _, StrEq("rbd"), StrEq("mirror_image_get"),
                     _, _, _))
      .WillOnce(DoAll(WithArg<5>(CopyInBufferlist(bl)),
                      Return(r)));
  }

  void expect_is_tag_owner(MockTestImageCtx &mock_image_ctx,
                           MockJournal &mock_journal,
                           bool is_primary, int r) {
    EXPECT_CALL(mock_journal, is_tag_owner(_, _))
      .WillOnce(DoAll(SetArgPointee<0>(is_primary),
                      WithArg<1>(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue))));
  }

  void expect_set_mirror_image(MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(RBD_MIRRORING, _, StrEq("rbd"), StrEq("mirror_image_set"),
                     _, _, _))
      .WillOnce(Return(r));
  }

  void expect_remove_mirror_image(MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(RBD_MIRRORING, _, StrEq("rbd"), StrEq("mirror_image_remove"),
                     _, _, _))
      .WillOnce(Return(r));
  }

  void expect_notify_image_updated(MockTestImageCtx &mock_image_ctx,
                                   MockMirroringWatcher &mock_mirroring_watcher,
                                   cls::rbd::MirrorImageState state,
                                   const std::string &global_id, int r) {
    EXPECT_CALL(mock_mirroring_watcher,
                notify_image_updated(state, mock_image_ctx.id, global_id, _))
      .WillOnce(WithArg<3>(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue)));
  }

  void expect_journal_client_list(MockTestImageCtx &mock_image_ctx,
                                  const std::set<cls::journal::Client> &clients,
                                  int r) {
    bufferlist bl;
    using ceph::encode;
    encode(clients, bl);

    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(::journal::Journaler::header_oid(mock_image_ctx.id),
                     _, StrEq("journal"), StrEq("client_list"), _, _, _))
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
                     ContentsEqual(bl), _, _))
      .WillOnce(Return(r));
  }

  void expect_journal_promote(MockTestImageCtx &mock_image_ctx,
                              MockPromoteRequest &mock_promote_request, int r) {
    EXPECT_CALL(mock_promote_request, send())
      .WillOnce(FinishRequest(&mock_promote_request, r, &mock_image_ctx));
  }

  void expect_snap_remove(MockTestImageCtx &mock_image_ctx,
                          const std::string &snap_name, int r) {
    EXPECT_CALL(*mock_image_ctx.operations, execute_snap_remove(_, StrEq(snap_name), _))
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
  MockJournal mock_journal;
  MockMirroringWatcher mock_mirroring_watcher;

  expect_op_work_queue(mock_image_ctx);
  expect_snap_remove(mock_image_ctx, "snap 1", 0);
  expect_snap_remove(mock_image_ctx, "snap 2", 0);

  InSequence seq;
  expect_get_mirror_image(mock_image_ctx,
                          {"global id", cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                          0);
  expect_is_tag_owner(mock_image_ctx, mock_journal, true, 0);
  expect_set_mirror_image(mock_image_ctx, 0);
  expect_notify_image_updated(mock_image_ctx, mock_mirroring_watcher,
                              cls::rbd::MIRROR_IMAGE_STATE_DISABLING,
                              "global id", -ESHUTDOWN);
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
  expect_remove_mirror_image(mock_image_ctx, 0);
  expect_notify_image_updated(mock_image_ctx, mock_mirroring_watcher,
                              cls::rbd::MIRROR_IMAGE_STATE_DISABLED,
                              "global id", -ETIMEDOUT);

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
  MockJournal mock_journal;
  MockMirroringWatcher mock_mirroring_watcher;

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_get_mirror_image(mock_image_ctx,
                          {"global id", cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                          0);
  expect_is_tag_owner(mock_image_ctx, mock_journal, true, 0);
  expect_set_mirror_image(mock_image_ctx, 0);
  expect_notify_image_updated(mock_image_ctx, mock_mirroring_watcher,
                              cls::rbd::MIRROR_IMAGE_STATE_DISABLING,
                              "global id", 0);
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
  MockJournal mock_journal;
  MockMirroringWatcher mock_mirroring_watcher;
  MockPromoteRequest mock_promote_request;

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_get_mirror_image(mock_image_ctx,
                          {"global id", cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                          0);
  expect_is_tag_owner(mock_image_ctx, mock_journal, false, 0);
  expect_set_mirror_image(mock_image_ctx, 0);
  expect_notify_image_updated(mock_image_ctx, mock_mirroring_watcher,
                              cls::rbd::MIRROR_IMAGE_STATE_DISABLING,
                              "global id", 0);
  expect_journal_promote(mock_image_ctx, mock_promote_request, 0);
  expect_journal_client_list(mock_image_ctx, {}, 0);
  expect_remove_mirror_image(mock_image_ctx, 0);
  expect_notify_image_updated(mock_image_ctx, mock_mirroring_watcher,
                              cls::rbd::MIRROR_IMAGE_STATE_DISABLED,
                              "global id", 0);

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
  MockJournal mock_journal;
  MockMirroringWatcher mock_mirroring_watcher;

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_get_mirror_image(mock_image_ctx,
                          {"global id", cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                          0);
  expect_is_tag_owner(mock_image_ctx, mock_journal, false, 0);

  C_SaferCond ctx;
  auto req = new MockDisableRequest(&mock_image_ctx, false, false, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorDisableRequest, MirrorImageGetError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal;

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_get_mirror_image(mock_image_ctx, {}, -EBADMSG);

  C_SaferCond ctx;
  auto req = new MockDisableRequest(&mock_image_ctx, false, true, &ctx);
  req->send();
  ASSERT_EQ(-EBADMSG, ctx.wait());
}

TEST_F(TestMockMirrorDisableRequest, IsTagOwnerError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal;

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_get_mirror_image(mock_image_ctx,
                          {"global id", cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                          0);
  expect_is_tag_owner(mock_image_ctx, mock_journal, true, -EBADMSG);

  C_SaferCond ctx;
  auto req = new MockDisableRequest(&mock_image_ctx, false, true, &ctx);
  req->send();
  ASSERT_EQ(-EBADMSG, ctx.wait());
}

TEST_F(TestMockMirrorDisableRequest, MirrorImageSetError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal;

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_get_mirror_image(mock_image_ctx,
                          {"global id", cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                          0);
  expect_is_tag_owner(mock_image_ctx, mock_journal, true, 0);
  expect_set_mirror_image(mock_image_ctx, -ENOENT);

  C_SaferCond ctx;
  auto req = new MockDisableRequest(&mock_image_ctx, false, true, &ctx);
  req->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockMirrorDisableRequest, JournalPromoteError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal;
  MockMirroringWatcher mock_mirroring_watcher;
  MockPromoteRequest mock_promote_request;

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_get_mirror_image(mock_image_ctx,
                          {"global id", cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                          0);
  expect_is_tag_owner(mock_image_ctx, mock_journal, false, 0);
  expect_set_mirror_image(mock_image_ctx, 0);
  expect_notify_image_updated(mock_image_ctx, mock_mirroring_watcher,
                              cls::rbd::MIRROR_IMAGE_STATE_DISABLING,
                              "global id", 0);
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
  MockJournal mock_journal;
  MockMirroringWatcher mock_mirroring_watcher;

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_get_mirror_image(mock_image_ctx,
                          {"global id", cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                          0);
  expect_is_tag_owner(mock_image_ctx, mock_journal, true, 0);
  expect_set_mirror_image(mock_image_ctx, 0);
  expect_notify_image_updated(mock_image_ctx, mock_mirroring_watcher,
                              cls::rbd::MIRROR_IMAGE_STATE_DISABLING,
                              "global id", 0);
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
  MockJournal mock_journal;
  MockMirroringWatcher mock_mirroring_watcher;

  expect_op_work_queue(mock_image_ctx);
  expect_snap_remove(mock_image_ctx, "snap 1", 0);
  expect_snap_remove(mock_image_ctx, "snap 2", -EPERM);

  InSequence seq;
  expect_get_mirror_image(mock_image_ctx,
                          {"global id", cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                          0);
  expect_is_tag_owner(mock_image_ctx, mock_journal, true, 0);
  expect_set_mirror_image(mock_image_ctx, 0);
  expect_notify_image_updated(mock_image_ctx, mock_mirroring_watcher,
                              cls::rbd::MIRROR_IMAGE_STATE_DISABLING,
                              "global id", 0);
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
  MockJournal mock_journal;
  MockMirroringWatcher mock_mirroring_watcher;

  expect_op_work_queue(mock_image_ctx);
  expect_snap_remove(mock_image_ctx, "snap 1", 0);
  expect_snap_remove(mock_image_ctx, "snap 2", 0);

  InSequence seq;
  expect_get_mirror_image(mock_image_ctx,
                          {"global id", cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                          0);
  expect_is_tag_owner(mock_image_ctx, mock_journal, true, 0);
  expect_set_mirror_image(mock_image_ctx, 0);
  expect_notify_image_updated(mock_image_ctx, mock_mirroring_watcher,
                              cls::rbd::MIRROR_IMAGE_STATE_DISABLING,
                              "global id", 0);
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

TEST_F(TestMockMirrorDisableRequest, MirrorImageRemoveError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal;
  MockMirroringWatcher mock_mirroring_watcher;

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_get_mirror_image(mock_image_ctx,
                          {"global id", cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                          0);
  expect_is_tag_owner(mock_image_ctx, mock_journal, true, 0);
  expect_set_mirror_image(mock_image_ctx, 0);
  expect_notify_image_updated(mock_image_ctx, mock_mirroring_watcher,
                              cls::rbd::MIRROR_IMAGE_STATE_DISABLING,
                              "global id", 0);
  expect_journal_client_list(mock_image_ctx, {}, 0);
  expect_remove_mirror_image(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  auto req = new MockDisableRequest(&mock_image_ctx, false, true, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace mirror
} // namespace librbd

