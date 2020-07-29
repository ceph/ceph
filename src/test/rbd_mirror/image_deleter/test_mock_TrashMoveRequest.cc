// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/TrashWatcher.h"
#include "librbd/journal/ResetRequest.h"
#include "librbd/mirror/GetInfoRequest.h"
#include "librbd/trash/MoveRequest.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_deleter/TrashMoveRequest.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockExclusiveLock.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockImageState.h"
#include "test/librbd/mock/MockOperations.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  static MockTestImageCtx *s_instance;
  static MockTestImageCtx *create(const std::string &image_name,
                                  const std::string &image_id,
                                  const char *snap, librados::IoCtx& p,
                                  bool read_only) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  MockTestImageCtx(librbd::ImageCtx &image_ctx)
      : librbd::MockImageCtx(image_ctx) {
    s_instance = this;
  }
};

MockTestImageCtx *MockTestImageCtx::s_instance = nullptr;

} // anonymous namespace

template<>
struct TrashWatcher<MockTestImageCtx> {
  static TrashWatcher* s_instance;
  static void notify_image_added(librados::IoCtx&, const std::string& image_id,
                                 const cls::rbd::TrashImageSpec& spec,
                                 Context *ctx) {
    ceph_assert(s_instance != nullptr);
    s_instance->notify_image_added(image_id, spec, ctx);
  }

  MOCK_METHOD3(notify_image_added, void(const std::string&,
                                        const cls::rbd::TrashImageSpec&,
                                        Context*));

  TrashWatcher() {
    s_instance = this;
  }
};

TrashWatcher<MockTestImageCtx>* TrashWatcher<MockTestImageCtx>::s_instance = nullptr;

namespace journal {

template <>
struct ResetRequest<MockTestImageCtx> {
  static ResetRequest* s_instance;
  Context* on_finish = nullptr;

  static ResetRequest* create(librados::IoCtx &io_ctx,
                              const std::string &image_id,
                              const std::string &client_id,
                              const std::string &mirror_uuid,
                              ContextWQ *op_work_queue,
                              Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    EXPECT_EQ(librbd::Journal<>::LOCAL_MIRROR_UUID, mirror_uuid);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  ResetRequest() {
    s_instance = this;
  }
};

ResetRequest<MockTestImageCtx>* ResetRequest<MockTestImageCtx>::s_instance = nullptr;

} // namespace journal

namespace mirror {

template<>
struct GetInfoRequest<librbd::MockTestImageCtx> {
  static GetInfoRequest* s_instance;
  cls::rbd::MirrorImage *mirror_image;
  PromotionState *promotion_state;
  std::string *primary_mirror_uuid;
  Context *on_finish = nullptr;

  static GetInfoRequest* create(librados::IoCtx& io_ctx,
                                librbd::asio::ContextWQ* context_wq,
                                const std::string& image_id,
                                cls::rbd::MirrorImage *mirror_image,
                                PromotionState *promotion_state,
                                std::string* primary_mirror_uuid,
                                Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->mirror_image = mirror_image;
    s_instance->promotion_state = promotion_state;
    s_instance->primary_mirror_uuid = primary_mirror_uuid;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  GetInfoRequest() {
    ceph_assert(s_instance == nullptr);
    s_instance = this;
  }
  ~GetInfoRequest() {
    s_instance = nullptr;
  }

  MOCK_METHOD0(send, void());
};

GetInfoRequest<librbd::MockTestImageCtx>* GetInfoRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace mirror
namespace trash {

template <>
struct MoveRequest<MockTestImageCtx> {
  static MoveRequest* s_instance;
  Context* on_finish = nullptr;

  typedef boost::optional<utime_t> DefermentEndTime;

  static MoveRequest* create(librados::IoCtx& io_ctx,
                             const std::string& image_id,
                             const cls::rbd::TrashImageSpec& trash_image_spec,
                             Context* on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->construct(image_id, trash_image_spec);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD2(construct, void(const std::string&,
                               const cls::rbd::TrashImageSpec&));
  MOCK_METHOD0(send, void());

  MoveRequest() {
    s_instance = this;
  }
};

MoveRequest<MockTestImageCtx>* MoveRequest<MockTestImageCtx>::s_instance = nullptr;

} // namespace trash
} // namespace librbd

#include "tools/rbd_mirror/image_deleter/TrashMoveRequest.cc"

namespace rbd {
namespace mirror {
namespace image_deleter {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;
using ::testing::WithArgs;

class TestMockImageDeleterTrashMoveRequest : public TestMockFixture {
public:
  typedef TrashMoveRequest<librbd::MockTestImageCtx> MockTrashMoveRequest;
  typedef librbd::journal::ResetRequest<librbd::MockTestImageCtx> MockJournalResetRequest;
  typedef librbd::mirror::GetInfoRequest<librbd::MockTestImageCtx> MockGetMirrorInfoRequest;
  typedef librbd::trash::MoveRequest<librbd::MockTestImageCtx> MockLibrbdTrashMoveRequest;
  typedef librbd::TrashWatcher<librbd::MockTestImageCtx> MockTrashWatcher;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &m_local_image_ctx));
  }

  void expect_mirror_image_get_image_id(const std::string& image_id, int r) {
    bufferlist bl;
    encode(image_id, bl);

    EXPECT_CALL(get_mock_io_ctx(m_local_io_ctx),
                exec(RBD_MIRRORING, _, StrEq("rbd"),
                     StrEq("mirror_image_get_image_id"), _, _, _, _))
      .WillOnce(DoAll(WithArg<5>(Invoke([bl](bufferlist *out_bl) {
                                          *out_bl = bl;
                                        })),
                      Return(r)));
  }

  void expect_get_mirror_info(
      MockGetMirrorInfoRequest &mock_get_mirror_info_request,
      const cls::rbd::MirrorImage &mirror_image,
      librbd::mirror::PromotionState promotion_state,
      const std::string& primary_mirror_uuid, int r) {
    EXPECT_CALL(mock_get_mirror_info_request, send())
      .WillOnce(Invoke([this, &mock_get_mirror_info_request, mirror_image,
                        promotion_state, primary_mirror_uuid, r]() {
          *mock_get_mirror_info_request.mirror_image = mirror_image;
          *mock_get_mirror_info_request.promotion_state = promotion_state;
          *mock_get_mirror_info_request.primary_mirror_uuid =
            primary_mirror_uuid;
          m_threads->work_queue->queue(
            mock_get_mirror_info_request.on_finish, r);
        }));
  }

  void expect_set_journal_policy(librbd::MockTestImageCtx &mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, set_journal_policy(_))
      .WillOnce(Invoke([](librbd::journal::Policy* policy) {
                  delete policy;
                }));
  }

  void expect_open(librbd::MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.state, open(true, _))
      .WillOnce(WithArg<1>(Invoke([this, &mock_image_ctx, r](Context* ctx) {
                             EXPECT_EQ(0U, mock_image_ctx.read_only_mask &
                                             librbd::IMAGE_READ_ONLY_FLAG_NON_PRIMARY);
                             m_threads->work_queue->queue(ctx, r);
                           })));
  }

  void expect_close(librbd::MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.state, close(_))
      .WillOnce(Invoke([this, r](Context* ctx) {
                  m_threads->work_queue->queue(ctx, r);
                }));
  }

  void expect_block_requests(librbd::MockTestImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.exclusive_lock, block_requests(0)).Times(1);
  }

  void expect_acquire_lock(librbd::MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.exclusive_lock, acquire_lock(_))
      .WillOnce(Invoke([this, r](Context* ctx) {
                  m_threads->work_queue->queue(ctx, r);
                }));
  }

  void expect_mirror_image_set(const std::string& image_id,
                               const cls::rbd::MirrorImage& mirror_image,
                               int r) {
    bufferlist bl;
    encode(image_id, bl);
    encode(mirror_image, bl);

    EXPECT_CALL(get_mock_io_ctx(m_local_io_ctx),
                exec(RBD_MIRRORING, _, StrEq("rbd"),
                     StrEq("mirror_image_set"), ContentsEqual(bl), _, _, _))
      .WillOnce(Return(r));
  }

  void expect_mirror_image_remove(librados::IoCtx &ioctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(ioctx),
                exec(StrEq("rbd_mirroring"), _, StrEq("rbd"),
                     StrEq("mirror_image_remove"), _, _, _, _))
      .WillOnce(Return(r));
  }

  void expect_journal_reset(MockJournalResetRequest& mock_journal_reset_request,
                            int r) {
    EXPECT_CALL(mock_journal_reset_request, send())
      .WillOnce(Invoke([this, &mock_journal_reset_request, r]() {
                  m_threads->work_queue->queue(mock_journal_reset_request.on_finish, r);
                }));
  }

  void expect_trash_move(MockLibrbdTrashMoveRequest& mock_trash_move_request,
                         const std::string& image_name,
                         const std::string& image_id,
                         const boost::optional<uint32_t>& delay, int r) {
    EXPECT_CALL(mock_trash_move_request, construct(image_id, _))
      .WillOnce(WithArg<1>(Invoke([image_name, delay](const cls::rbd::TrashImageSpec& spec) {
                             ASSERT_EQ(cls::rbd::TRASH_IMAGE_SOURCE_MIRRORING,
                                       spec.source);
                             ASSERT_EQ(image_name, spec.name);
                             if (delay) {
                               utime_t time{spec.deletion_time};
                               time += *delay;
                               ASSERT_TRUE(time == spec.deferment_end_time);
                             } else {
                               ASSERT_EQ(spec.deletion_time, spec.deferment_end_time);
                             }
                           })));
    EXPECT_CALL(mock_trash_move_request, send())
      .WillOnce(Invoke([this, &mock_trash_move_request, r]() {
                  m_threads->work_queue->queue(mock_trash_move_request.on_finish, r);
                }));
  }

  void expect_notify_image_added(MockTrashWatcher& mock_trash_watcher,
                                 const std::string& image_id) {
    EXPECT_CALL(mock_trash_watcher, notify_image_added(image_id, _, _))
      .WillOnce(WithArg<2>(Invoke([this](Context *ctx) {
                             m_threads->work_queue->queue(ctx, 0);
                           })));
  }

  librbd::ImageCtx *m_local_image_ctx;
};

TEST_F(TestMockImageDeleterTrashMoveRequest, SuccessJournal) {
  librbd::MockTestImageCtx mock_image_ctx(*m_local_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  InSequence seq;
  expect_mirror_image_get_image_id("image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_ORPHAN,
                         "remote mirror uuid", 0);

  expect_mirror_image_set("image id",
                          {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                           "global image id",
                           cls::rbd::MIRROR_IMAGE_STATE_DISABLING}, 0);

  expect_set_journal_policy(mock_image_ctx);
  expect_open(mock_image_ctx, 0);

  MockJournalResetRequest mock_journal_reset_request;
  expect_journal_reset(mock_journal_reset_request, 0);

  expect_block_requests(mock_image_ctx);
  expect_acquire_lock(mock_image_ctx, 0);

  MockLibrbdTrashMoveRequest mock_librbd_trash_move_request;
  expect_trash_move(mock_librbd_trash_move_request, m_image_name, "image id",
                    {}, 0);
  expect_mirror_image_remove(m_local_io_ctx, 0);

  expect_close(mock_image_ctx, 0);

  MockTrashWatcher mock_trash_watcher;
  expect_notify_image_added(mock_trash_watcher, "image id");

  C_SaferCond ctx;
  auto req = MockTrashMoveRequest::create(m_local_io_ctx, "global image id",
                                          true,
                                          m_local_image_ctx->op_work_queue,
                                          &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashMoveRequest, SuccessSnapshot) {
  librbd::MockTestImageCtx mock_image_ctx(*m_local_image_ctx);

  InSequence seq;
  expect_mirror_image_get_image_id("image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_NON_PRIMARY,
                         "remote mirror uuid", 0);

  expect_mirror_image_set("image id",
                          {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
                           "global image id",
                           cls::rbd::MIRROR_IMAGE_STATE_DISABLING}, 0);

  expect_set_journal_policy(mock_image_ctx);
  expect_open(mock_image_ctx, 0);

  MockLibrbdTrashMoveRequest mock_librbd_trash_move_request;
  expect_trash_move(mock_librbd_trash_move_request, m_image_name, "image id",
                    {}, 0);
  expect_mirror_image_remove(m_local_io_ctx, 0);

  expect_close(mock_image_ctx, 0);

  MockTrashWatcher mock_trash_watcher;
  expect_notify_image_added(mock_trash_watcher, "image id");

  C_SaferCond ctx;
  auto req = MockTrashMoveRequest::create(m_local_io_ctx, "global image id",
                                          false,
                                          m_local_image_ctx->op_work_queue,
                                          &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashMoveRequest, GetImageIdDNE) {
  InSequence seq;
  expect_mirror_image_get_image_id("image id", -ENOENT);

  C_SaferCond ctx;
  auto req = MockTrashMoveRequest::create(m_local_io_ctx, "global image id",
                                          true,
                                          m_local_image_ctx->op_work_queue,
                                          &ctx);
  req->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashMoveRequest, GetImageIdError) {
  InSequence seq;
  expect_mirror_image_get_image_id("image id", -EINVAL);

  C_SaferCond ctx;
  auto req = MockTrashMoveRequest::create(m_local_io_ctx, "global image id",
                                          true,
                                          m_local_image_ctx->op_work_queue,
                                          &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashMoveRequest, GetMirrorInfoLocalPrimary) {
  InSequence seq;
  expect_mirror_image_get_image_id("image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_PRIMARY,
                         "remote mirror uuid", 0);

  C_SaferCond ctx;
  auto req = MockTrashMoveRequest::create(m_local_io_ctx, "global image id",
                                          true,
                                          m_local_image_ctx->op_work_queue,
                                          &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashMoveRequest, GetMirrorInfoOrphan) {
  InSequence seq;
  expect_mirror_image_get_image_id("image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_ORPHAN,
                         "remote mirror uuid", 0);

  C_SaferCond ctx;
  auto req = MockTrashMoveRequest::create(m_local_io_ctx, "global image id",
                                          false,
                                          m_local_image_ctx->op_work_queue,
                                          &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashMoveRequest, GetMirrorInfoDNE) {
  librbd::MockTestImageCtx mock_image_ctx(*m_local_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  InSequence seq;
  expect_mirror_image_get_image_id("image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_ORPHAN,
                         "remote mirror uuid", -ENOENT);

  C_SaferCond ctx;
  auto req = MockTrashMoveRequest::create(m_local_io_ctx, "global image id",
                                          true,
                                          m_local_image_ctx->op_work_queue,
                                          &ctx);
  req->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashMoveRequest, GetMirrorInfoError) {
  InSequence seq;
  expect_mirror_image_get_image_id("image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_ORPHAN,
                         "remote mirror uuid", -EINVAL);

  C_SaferCond ctx;
  auto req = MockTrashMoveRequest::create(m_local_io_ctx, "global image id",
                                          true,
                                          m_local_image_ctx->op_work_queue,
                                          &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashMoveRequest, DisableMirrorImageError) {
  InSequence seq;
  expect_mirror_image_get_image_id("image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_ORPHAN,
                         "remote mirror uuid", 0);

  expect_mirror_image_set("image id",
                          {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                           "global image id",
                           cls::rbd::MIRROR_IMAGE_STATE_DISABLING}, -EINVAL);

  C_SaferCond ctx;
  auto req = MockTrashMoveRequest::create(m_local_io_ctx, "global image id",
                                          true,
                                          m_local_image_ctx->op_work_queue,
                                          &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashMoveRequest, OpenImageError) {
  librbd::MockTestImageCtx mock_image_ctx(*m_local_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  InSequence seq;
  expect_mirror_image_get_image_id("image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_ORPHAN,
                         "remote mirror uuid", 0);

  expect_mirror_image_set("image id",
                          {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                           "global image id",
                           cls::rbd::MIRROR_IMAGE_STATE_DISABLING}, 0);

  expect_set_journal_policy(mock_image_ctx);
  expect_open(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  auto req = MockTrashMoveRequest::create(m_local_io_ctx, "global image id",
                                          true,
                                          m_local_image_ctx->op_work_queue,
                                          &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashMoveRequest, ResetJournalError) {
  librbd::MockTestImageCtx mock_image_ctx(*m_local_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  InSequence seq;
  expect_mirror_image_get_image_id("image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_ORPHAN,
                         "remote mirror uuid", 0);

  expect_mirror_image_set("image id",
                          {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                           "global image id",
                           cls::rbd::MIRROR_IMAGE_STATE_DISABLING}, 0);

  expect_set_journal_policy(mock_image_ctx);
  expect_open(mock_image_ctx, 0);

  MockJournalResetRequest mock_journal_reset_request;
  expect_journal_reset(mock_journal_reset_request, -EINVAL);

  expect_close(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto req = MockTrashMoveRequest::create(m_local_io_ctx, "global image id",
                                          true,
                                          m_local_image_ctx->op_work_queue,
                                          &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashMoveRequest, AcquireLockError) {
  librbd::MockTestImageCtx mock_image_ctx(*m_local_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  InSequence seq;
  expect_mirror_image_get_image_id("image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_ORPHAN,
                         "remote mirror uuid", 0);

  expect_mirror_image_set("image id",
                          {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                           "global image id",
                           cls::rbd::MIRROR_IMAGE_STATE_DISABLING}, 0);

  expect_set_journal_policy(mock_image_ctx);
  expect_open(mock_image_ctx, 0);

  MockJournalResetRequest mock_journal_reset_request;
  expect_journal_reset(mock_journal_reset_request, 0);

  expect_block_requests(mock_image_ctx);
  expect_acquire_lock(mock_image_ctx, -EINVAL);

  expect_close(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto req = MockTrashMoveRequest::create(m_local_io_ctx, "global image id",
                                          true,
                                          m_local_image_ctx->op_work_queue,
                                          &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashMoveRequest, TrashMoveError) {
  librbd::MockTestImageCtx mock_image_ctx(*m_local_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  InSequence seq;
  expect_mirror_image_get_image_id("image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_ORPHAN,
                         "remote mirror uuid", 0);

  expect_mirror_image_set("image id",
                          {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                           "global image id",
                           cls::rbd::MIRROR_IMAGE_STATE_DISABLING}, 0);

  expect_set_journal_policy(mock_image_ctx);
  expect_open(mock_image_ctx, 0);

  MockJournalResetRequest mock_journal_reset_request;
  expect_journal_reset(mock_journal_reset_request, 0);

  expect_block_requests(mock_image_ctx);
  expect_acquire_lock(mock_image_ctx, 0);

  MockLibrbdTrashMoveRequest mock_librbd_trash_move_request;
  expect_trash_move(mock_librbd_trash_move_request, m_image_name, "image id",
                    {}, -EINVAL);

  expect_close(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto req = MockTrashMoveRequest::create(m_local_io_ctx, "global image id",
                                          true,
                                          m_local_image_ctx->op_work_queue,
                                          &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashMoveRequest, RemoveMirrorImageError) {
  librbd::MockTestImageCtx mock_image_ctx(*m_local_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  InSequence seq;
  expect_mirror_image_get_image_id("image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_ORPHAN,
                         "remote mirror uuid", 0);

  expect_mirror_image_set("image id",
                          {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                           "global image id",
                           cls::rbd::MIRROR_IMAGE_STATE_DISABLING}, 0);

  expect_set_journal_policy(mock_image_ctx);
  expect_open(mock_image_ctx, 0);

  MockJournalResetRequest mock_journal_reset_request;
  expect_journal_reset(mock_journal_reset_request, 0);

  expect_block_requests(mock_image_ctx);
  expect_acquire_lock(mock_image_ctx, 0);

  MockLibrbdTrashMoveRequest mock_librbd_trash_move_request;
  expect_trash_move(mock_librbd_trash_move_request, m_image_name, "image id",
                    {}, 0);
  expect_mirror_image_remove(m_local_io_ctx, -EINVAL);

  expect_close(mock_image_ctx, 0);

  MockTrashWatcher mock_trash_watcher;
  expect_notify_image_added(mock_trash_watcher, "image id");

  C_SaferCond ctx;
  auto req = MockTrashMoveRequest::create(m_local_io_ctx, "global image id",
                                          true,
                                          m_local_image_ctx->op_work_queue,
                                          &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashMoveRequest, CloseImageError) {
  librbd::MockTestImageCtx mock_image_ctx(*m_local_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  InSequence seq;
  expect_mirror_image_get_image_id("image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_ORPHAN,
                         "remote mirror uuid", 0);

  expect_mirror_image_set("image id",
                          {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                           "global image id",
                           cls::rbd::MIRROR_IMAGE_STATE_DISABLING}, 0);

  expect_set_journal_policy(mock_image_ctx);
  expect_open(mock_image_ctx, 0);

  MockJournalResetRequest mock_journal_reset_request;
  expect_journal_reset(mock_journal_reset_request, 0);

  expect_block_requests(mock_image_ctx);
  expect_acquire_lock(mock_image_ctx, 0);

  MockLibrbdTrashMoveRequest mock_librbd_trash_move_request;
  expect_trash_move(mock_librbd_trash_move_request, m_image_name, "image id",
                    {}, 0);
  expect_mirror_image_remove(m_local_io_ctx, 0);

  expect_close(mock_image_ctx, -EINVAL);

  MockTrashWatcher mock_trash_watcher;
  expect_notify_image_added(mock_trash_watcher, "image id");

  C_SaferCond ctx;
  auto req = MockTrashMoveRequest::create(m_local_io_ctx, "global image id",
                                          true,
                                          m_local_image_ctx->op_work_queue,
                                          &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDeleterTrashMoveRequest, DelayedDelation) {
  librbd::MockTestImageCtx mock_image_ctx(*m_local_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.config.set_val("rbd_mirroring_delete_delay", "600");
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  InSequence seq;
  expect_mirror_image_get_image_id("image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_NON_PRIMARY,
                         "remote mirror uuid", 0);

  expect_mirror_image_set("image id",
                          {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                           "global image id",
                           cls::rbd::MIRROR_IMAGE_STATE_DISABLING}, 0);

  expect_set_journal_policy(mock_image_ctx);
  expect_open(mock_image_ctx, 0);

  MockJournalResetRequest mock_journal_reset_request;
  expect_journal_reset(mock_journal_reset_request, 0);

  expect_block_requests(mock_image_ctx);
  expect_acquire_lock(mock_image_ctx, 0);

  MockLibrbdTrashMoveRequest mock_librbd_trash_move_request;
  expect_trash_move(mock_librbd_trash_move_request, m_image_name, "image id",
                    600, 0);

  expect_mirror_image_remove(m_local_io_ctx, 0);
  expect_close(mock_image_ctx, 0);

  MockTrashWatcher mock_trash_watcher;
  expect_notify_image_added(mock_trash_watcher, "image id");

  C_SaferCond ctx;
  auto req = MockTrashMoveRequest::create(m_local_io_ctx, "global image id",
                                          true,
                                          m_local_image_ctx->op_work_queue,
                                          &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

} // namespace image_deleter
} // namespace mirror
} // namespace rbd
