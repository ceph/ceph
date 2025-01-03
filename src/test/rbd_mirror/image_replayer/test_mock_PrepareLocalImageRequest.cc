// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/journal/TypeTraits.h"
#include "librbd/mirror/GetInfoRequest.h"
#include "tools/rbd_mirror/ImageDeleter.h"
#include "tools/rbd_mirror/image_replayer/GetMirrorImageIdRequest.h"
#include "tools/rbd_mirror/image_replayer/PrepareLocalImageRequest.h"
#include "tools/rbd_mirror/image_replayer/StateBuilder.h"
#include "tools/rbd_mirror/image_replayer/journal/StateBuilder.h"
#include "tools/rbd_mirror/image_replayer/snapshot/StateBuilder.h"
#include "test/journal/mock/MockJournaler.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockJournal.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

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
} // namespace librbd

namespace rbd {
namespace mirror {

template <>
struct ImageDeleter<librbd::MockTestImageCtx> {
  static ImageDeleter* s_instance;

  static void trash_move(librados::IoCtx& local_io_ctx,
                         const std::string& global_image_id, bool resync,
                         librbd::asio::ContextWQ* work_queue,
                         Context* on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->trash_move(global_image_id, resync, on_finish);
  }

  MOCK_METHOD3(trash_move, void(const std::string&, bool, Context*));

  ImageDeleter() {
    s_instance = this;
  }
};

ImageDeleter<librbd::MockTestImageCtx>* ImageDeleter<librbd::MockTestImageCtx>::s_instance = nullptr;

namespace image_replayer {

template <>
struct GetMirrorImageIdRequest<librbd::MockTestImageCtx> {
  static GetMirrorImageIdRequest* s_instance;
  std::string* image_id = nullptr;
  Context* on_finish = nullptr;

  static GetMirrorImageIdRequest* create(librados::IoCtx& io_ctx,
                                         const std::string& global_image_id,
                                         std::string* image_id,
                                         Context* on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->image_id = image_id;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  GetMirrorImageIdRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

template<>
struct StateBuilder<librbd::MockTestImageCtx> {
  virtual ~StateBuilder() {}

  std::string local_image_id;
  librbd::mirror::PromotionState local_promotion_state;
};

GetMirrorImageIdRequest<librbd::MockTestImageCtx>* GetMirrorImageIdRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

namespace journal {

template<>
struct StateBuilder<librbd::MockTestImageCtx>
  : public image_replayer::StateBuilder<librbd::MockTestImageCtx> {
  static StateBuilder* s_instance;

  cls::rbd::MirrorImageMode mirror_image_mode =
    cls::rbd::MIRROR_IMAGE_MODE_JOURNAL;

  std::string local_primary_mirror_uuid;

  static StateBuilder* create(const std::string&) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  StateBuilder() {
    s_instance = this;
  }
};

StateBuilder<librbd::MockTestImageCtx>* StateBuilder<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace journal

namespace snapshot {

template<>
struct StateBuilder<librbd::MockTestImageCtx>
  : public image_replayer::StateBuilder<librbd::MockTestImageCtx> {
  static StateBuilder* s_instance;

  cls::rbd::MirrorImageMode mirror_image_mode =
    cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT;

  static StateBuilder* create(const std::string&) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  StateBuilder() {
    s_instance = this;
  }
};

StateBuilder<librbd::MockTestImageCtx>* StateBuilder<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace snapshot
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

// template definitions
#include "tools/rbd_mirror/image_replayer/PrepareLocalImageRequest.cc"

namespace rbd {
namespace mirror {
namespace image_replayer {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;
using ::testing::WithArgs;

class TestMockImageReplayerPrepareLocalImageRequest : public TestMockFixture {
public:
  typedef ImageDeleter<librbd::MockTestImageCtx> MockImageDeleter;
  typedef PrepareLocalImageRequest<librbd::MockTestImageCtx> MockPrepareLocalImageRequest;
  typedef GetMirrorImageIdRequest<librbd::MockTestImageCtx> MockGetMirrorImageIdRequest;
  typedef StateBuilder<librbd::MockTestImageCtx> MockStateBuilder;
  typedef journal::StateBuilder<librbd::MockTestImageCtx> MockJournalStateBuilder;
  typedef snapshot::StateBuilder<librbd::MockTestImageCtx> MockSnapshotStateBuilder;
  typedef librbd::mirror::GetInfoRequest<librbd::MockTestImageCtx> MockGetMirrorInfoRequest;

  void expect_get_mirror_image_id(MockGetMirrorImageIdRequest& mock_get_mirror_image_id_request,
                                  const std::string& image_id, int r) {
    EXPECT_CALL(mock_get_mirror_image_id_request, send())
      .WillOnce(Invoke([&mock_get_mirror_image_id_request, image_id, r]() {
                  *mock_get_mirror_image_id_request.image_id = image_id;
                  mock_get_mirror_image_id_request.on_finish->complete(r);
                }));
  }

  void expect_dir_get_name(librados::IoCtx &io_ctx,
                           const std::string &image_name, int r) {
    bufferlist bl;
    encode(image_name, bl);

    EXPECT_CALL(get_mock_io_ctx(io_ctx),
                exec(RBD_DIRECTORY, _, StrEq("rbd"), StrEq("dir_get_name"), _,
                     _, _, _))
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

  void expect_trash_move(MockImageDeleter& mock_image_deleter,
                         const std::string& global_image_id,
                         bool ignore_orphan, int r) {
    EXPECT_CALL(mock_image_deleter,
                trash_move(global_image_id, ignore_orphan, _))
      .WillOnce(WithArg<2>(Invoke([this, r](Context* ctx) {
                             m_threads->work_queue->queue(ctx, r);
                           })));
  }

};

TEST_F(TestMockImageReplayerPrepareLocalImageRequest, SuccessJournal) {
  InSequence seq;
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request, "local image id",
                             0);
  expect_dir_get_name(m_local_io_ctx, "local image name", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_NON_PRIMARY,
                         "remote mirror uuid", 0);

  MockJournalStateBuilder mock_journal_state_builder;
  MockStateBuilder* mock_state_builder = nullptr;
  std::string local_image_name;
  C_SaferCond ctx;
  auto req = MockPrepareLocalImageRequest::create(m_local_io_ctx,
                                                  "global image id",
                                                  &local_image_name,
                                                  &mock_state_builder,
                                                  m_threads->work_queue,
                                                  &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
  ASSERT_TRUE(mock_state_builder != nullptr);
  ASSERT_EQ(std::string("local image name"), local_image_name);
  ASSERT_EQ(std::string("local image id"),
            mock_journal_state_builder.local_image_id);
  ASSERT_EQ(cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
            mock_journal_state_builder.mirror_image_mode);
  ASSERT_EQ(librbd::mirror::PROMOTION_STATE_NON_PRIMARY,
            mock_journal_state_builder.local_promotion_state);
  ASSERT_EQ(std::string("remote mirror uuid"),
            mock_journal_state_builder.local_primary_mirror_uuid);
}

TEST_F(TestMockImageReplayerPrepareLocalImageRequest, SuccessSnapshot) {
  InSequence seq;
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request, "local image id",
                             0);
  expect_dir_get_name(m_local_io_ctx, "local image name", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_NON_PRIMARY,
                         "remote mirror uuid", 0);

  MockSnapshotStateBuilder mock_journal_state_builder;
  MockStateBuilder* mock_state_builder = nullptr;
  std::string local_image_name;
  C_SaferCond ctx;
  auto req = MockPrepareLocalImageRequest::create(m_local_io_ctx,
                                                  "global image id",
                                                  &local_image_name,
                                                  &mock_state_builder,
                                                  m_threads->work_queue,
                                                  &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
  ASSERT_TRUE(mock_state_builder != nullptr);
  ASSERT_EQ(std::string("local image name"), local_image_name);
  ASSERT_EQ(std::string("local image id"),
            mock_journal_state_builder.local_image_id);
  ASSERT_EQ(cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
            mock_journal_state_builder.mirror_image_mode);
  ASSERT_EQ(librbd::mirror::PROMOTION_STATE_NON_PRIMARY,
            mock_journal_state_builder.local_promotion_state);
}

TEST_F(TestMockImageReplayerPrepareLocalImageRequest, MirrorImageIdError) {
  InSequence seq;
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request, "", -EINVAL);

  MockStateBuilder* mock_state_builder = nullptr;
  std::string local_image_name;
  C_SaferCond ctx;
  auto req = MockPrepareLocalImageRequest::create(m_local_io_ctx,
                                                  "global image id",
                                                  &local_image_name,
                                                  &mock_state_builder,
                                                  m_threads->work_queue,
                                                  &ctx);
  req->send();

  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerPrepareLocalImageRequest, DirGetNameDNE) {
  InSequence seq;
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request, "local image id",
                             0);
  expect_dir_get_name(m_local_io_ctx, "", -ENOENT);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_NON_PRIMARY,
                         "remote mirror uuid", 0);

  MockJournalStateBuilder mock_journal_state_builder;
  MockStateBuilder* mock_state_builder = nullptr;
  std::string local_image_name;
  C_SaferCond ctx;
  auto req = MockPrepareLocalImageRequest::create(m_local_io_ctx,
                                                  "global image id",
                                                  &local_image_name,
                                                  &mock_state_builder,
                                                  m_threads->work_queue,
                                                  &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageReplayerPrepareLocalImageRequest, DirGetNameError) {
  InSequence seq;
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request, "local image id",
                             0);
  expect_dir_get_name(m_local_io_ctx, "", -EPERM);

  MockStateBuilder* mock_state_builder = nullptr;
  std::string local_image_name;
  C_SaferCond ctx;
  auto req = MockPrepareLocalImageRequest::create(m_local_io_ctx,
                                                  "global image id",
                                                  &local_image_name,
                                                  &mock_state_builder,
                                                  m_threads->work_queue,
                                                  &ctx);
  req->send();

  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockImageReplayerPrepareLocalImageRequest, MirrorImageInfoError) {
  InSequence seq;
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request, "local image id",
                             0);
  expect_dir_get_name(m_local_io_ctx, "local image name", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_NON_PRIMARY,
                         "remote mirror uuid", -EINVAL);

  MockStateBuilder* mock_state_builder = nullptr;
  std::string local_image_name;
  C_SaferCond ctx;
  auto req = MockPrepareLocalImageRequest::create(m_local_io_ctx,
                                                  "global image id",
                                                  &local_image_name,
                                                  &mock_state_builder,
                                                  m_threads->work_queue,
                                                  &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerPrepareLocalImageRequest, ImageCreating) {
  InSequence seq;
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request, "local image id",
                             0);
  expect_dir_get_name(m_local_io_ctx, "local image name", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_CREATING},
                         librbd::mirror::PROMOTION_STATE_NON_PRIMARY,
                         "remote mirror uuid", 0);

  MockImageDeleter mock_image_deleter;
  expect_trash_move(mock_image_deleter, "global image id", false, 0);

  MockSnapshotStateBuilder mock_journal_state_builder;
  MockStateBuilder* mock_state_builder = nullptr;
  std::string local_image_name;
  C_SaferCond ctx;
  auto req = MockPrepareLocalImageRequest::create(m_local_io_ctx,
                                                  "global image id",
                                                  &local_image_name,
                                                  &mock_state_builder,
                                                  m_threads->work_queue,
                                                  &ctx);
  req->send();

  ASSERT_EQ(-ENOENT, ctx.wait());
  ASSERT_TRUE(mock_state_builder == nullptr);
}

TEST_F(TestMockImageReplayerPrepareLocalImageRequest, ImageDisabling) {
  InSequence seq;
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request, "local image id",
                             0);
  expect_dir_get_name(m_local_io_ctx, "local image name", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_DISABLING},
                         librbd::mirror::PROMOTION_STATE_NON_PRIMARY,
                         "remote mirror uuid", 0);

  MockSnapshotStateBuilder mock_journal_state_builder;
  MockStateBuilder* mock_state_builder = nullptr;
  std::string local_image_name;
  C_SaferCond ctx;
  auto req = MockPrepareLocalImageRequest::create(m_local_io_ctx,
                                                  "global image id",
                                                  &local_image_name,
                                                  &mock_state_builder,
                                                  m_threads->work_queue,
                                                  &ctx);
  req->send();

  ASSERT_EQ(-ERESTART, ctx.wait());
  ASSERT_TRUE(mock_state_builder == nullptr);
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd
