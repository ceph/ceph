// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/journal/TypeTraits.h"
#include "librbd/mirror/GetInfoRequest.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_replayer/GetMirrorImageIdRequest.h"
#include "tools/rbd_mirror/image_replayer/PrepareRemoteImageRequest.h"
#include "tools/rbd_mirror/image_replayer/StateBuilder.h"
#include "tools/rbd_mirror/image_replayer/journal/StateBuilder.h"
#include "tools/rbd_mirror/image_replayer/snapshot/StateBuilder.h"
#include "test/journal/mock/MockJournaler.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace journal {

template <>
struct TypeTraits<MockTestImageCtx> {
  typedef ::journal::MockJournalerProxy Journaler;
};

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
} // namespace librbd

namespace rbd {
namespace mirror {

template <>
struct Threads<librbd::MockTestImageCtx> {
  ceph::mutex &timer_lock;
  SafeTimer *timer;
  librbd::asio::ContextWQ *work_queue;

  Threads(Threads<librbd::ImageCtx> *threads)
    : timer_lock(threads->timer_lock), timer(threads->timer),
      work_queue(threads->work_queue) {
  }
};

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
  std::string local_image_id;
  librbd::mirror::PromotionState local_promotion_state =
    librbd::mirror::PROMOTION_STATE_NON_PRIMARY;
  std::string remote_image_id;
  std::string remote_mirror_uuid;
  librbd::mirror::PromotionState remote_promotion_state;

  virtual ~StateBuilder() {}

  MOCK_CONST_METHOD0(get_mirror_image_mode, cls::rbd::MirrorImageMode());
};

GetMirrorImageIdRequest<librbd::MockTestImageCtx>* GetMirrorImageIdRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

namespace journal {

template<>
struct StateBuilder<librbd::MockTestImageCtx>
  : public image_replayer::StateBuilder<librbd::MockTestImageCtx> {
  static StateBuilder* s_instance;

  cls::rbd::MirrorImageMode mirror_image_mode =
    cls::rbd::MIRROR_IMAGE_MODE_JOURNAL;

  ::journal::MockJournalerProxy* remote_journaler = nullptr;
  cls::journal::ClientState remote_client_state;
  librbd::journal::MirrorPeerClientMeta remote_client_meta;

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

  std::string remote_mirror_peer_uuid;

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
#include "tools/rbd_mirror/image_replayer/PrepareRemoteImageRequest.cc"

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

class TestMockImageReplayerPrepareRemoteImageRequest : public TestMockFixture {
public:
  typedef Threads<librbd::MockTestImageCtx> MockThreads;
  typedef PrepareRemoteImageRequest<librbd::MockTestImageCtx> MockPrepareRemoteImageRequest;
  typedef GetMirrorImageIdRequest<librbd::MockTestImageCtx> MockGetMirrorImageIdRequest;
  typedef StateBuilder<librbd::MockTestImageCtx> MockStateBuilder;
  typedef journal::StateBuilder<librbd::MockTestImageCtx> MockJournalStateBuilder;
  typedef snapshot::StateBuilder<librbd::MockTestImageCtx> MockSnapshotStateBuilder;
  typedef librbd::mirror::GetInfoRequest<librbd::MockTestImageCtx> MockGetMirrorInfoRequest;

  void expect_get_mirror_image_mode(MockStateBuilder& mock_state_builder,
                                    cls::rbd::MirrorImageMode mirror_image_mode) {
    EXPECT_CALL(mock_state_builder, get_mirror_image_mode())
      .WillOnce(Return(mirror_image_mode));
  }

  void expect_get_mirror_image_id(MockGetMirrorImageIdRequest& mock_get_mirror_image_id_request,
                                  const std::string& image_id, int r) {
    EXPECT_CALL(mock_get_mirror_image_id_request, send())
      .WillOnce(Invoke([&mock_get_mirror_image_id_request, image_id, r]() {
                  *mock_get_mirror_image_id_request.image_id = image_id;
                  mock_get_mirror_image_id_request.on_finish->complete(r);
                }));
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

  void expect_journaler_get_client(::journal::MockJournaler &mock_journaler,
                                   const std::string &client_id,
                                   cls::journal::Client &client, int r) {
    EXPECT_CALL(mock_journaler, get_client(StrEq(client_id), _, _))
      .WillOnce(DoAll(WithArg<1>(Invoke([client](cls::journal::Client *out_client) {
                                          *out_client = client;
                                        })),
                      WithArg<2>(Invoke([this, r](Context *on_finish) {
                                          m_threads->work_queue->queue(on_finish, r);
                                        }))));
  }

  void expect_journaler_register_client(::journal::MockJournaler &mock_journaler,
                                        const librbd::journal::ClientData &client_data,
                                        int r) {
    bufferlist bl;
    encode(client_data, bl);

    EXPECT_CALL(mock_journaler, register_client(ContentsEqual(bl), _))
      .WillOnce(WithArg<1>(Invoke([this, r](Context *on_finish) {
                                    m_threads->work_queue->queue(on_finish, r);
                                  })));
  }
};

TEST_F(TestMockImageReplayerPrepareRemoteImageRequest, SuccessJournal) {
  ::journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);

  InSequence seq;
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request,
                             "remote image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_PRIMARY,
                         "remote mirror uuid", 0);

  EXPECT_CALL(mock_remote_journaler, construct());

  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta;
  mirror_peer_client_meta.image_id = "local image id";
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_SYNCING;
  librbd::journal::ClientData client_data{mirror_peer_client_meta};
  cls::journal::Client client;
  client.state = cls::journal::CLIENT_STATE_DISCONNECTED;
  encode(client_data, client.data);
  expect_journaler_get_client(mock_remote_journaler, "local mirror uuid",
                              client, 0);

  MockJournalStateBuilder mock_journal_state_builder;
  MockStateBuilder* mock_state_builder = nullptr;
  C_SaferCond ctx;
  auto req = MockPrepareRemoteImageRequest::create(&mock_threads,
                                                   m_local_io_ctx,
                                                   m_remote_io_ctx,
                                                   "global image id",
                                                   "local mirror uuid",
                                                   {"remote mirror uuid", ""},
                                                   nullptr,
                                                   &mock_state_builder,
                                                   &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
  ASSERT_TRUE(mock_state_builder != nullptr);
  ASSERT_EQ(cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
           mock_journal_state_builder.mirror_image_mode);
  ASSERT_EQ(std::string("remote mirror uuid"),
            mock_journal_state_builder.remote_mirror_uuid);
  ASSERT_EQ(std::string("remote image id"),
            mock_journal_state_builder.remote_image_id);
  ASSERT_EQ(librbd::mirror::PROMOTION_STATE_PRIMARY,
            mock_journal_state_builder.remote_promotion_state);
  ASSERT_TRUE(mock_journal_state_builder.remote_journaler != nullptr);
  ASSERT_EQ(cls::journal::CLIENT_STATE_DISCONNECTED,
            mock_journal_state_builder.remote_client_state);
  // owned by StateBuilder, normally freed in StateBuilder::close()
  delete mock_journal_state_builder.remote_journaler;
}

TEST_F(TestMockImageReplayerPrepareRemoteImageRequest, SuccessSnapshot) {
  MockThreads mock_threads(m_threads);

  InSequence seq;
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request,
                             "remote image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_PRIMARY,
                         "remote mirror uuid", 0);

  MockSnapshotStateBuilder mock_snapshot_state_builder;
  MockStateBuilder* mock_state_builder = nullptr;
  C_SaferCond ctx;
  auto req = MockPrepareRemoteImageRequest::create(&mock_threads,
                                                   m_local_io_ctx,
                                                   m_remote_io_ctx,
                                                   "global image id",
                                                   "local mirror uuid",
                                                   {"remote mirror uuid",
                                                    "remote mirror peer uuid"},
                                                   nullptr,
                                                   &mock_state_builder,
                                                   &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
  ASSERT_TRUE(mock_state_builder != nullptr);
  ASSERT_EQ(cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
           mock_snapshot_state_builder.mirror_image_mode);
  ASSERT_EQ(std::string("remote mirror uuid"),
            mock_snapshot_state_builder.remote_mirror_uuid);
  ASSERT_EQ(std::string("remote mirror peer uuid"),
            mock_snapshot_state_builder.remote_mirror_peer_uuid);
  ASSERT_EQ(std::string("remote image id"),
            mock_snapshot_state_builder.remote_image_id);
  ASSERT_EQ(librbd::mirror::PROMOTION_STATE_PRIMARY,
            mock_snapshot_state_builder.remote_promotion_state);
}

TEST_F(TestMockImageReplayerPrepareRemoteImageRequest, SuccessNotRegistered) {
  ::journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);

  InSequence seq;
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request,
                             "remote image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_PRIMARY,
                         "remote mirror uuid", 0);

  MockJournalStateBuilder mock_journal_state_builder;
  expect_get_mirror_image_mode(mock_journal_state_builder,
                               cls::rbd::MIRROR_IMAGE_MODE_JOURNAL);

  EXPECT_CALL(mock_remote_journaler, construct());

  cls::journal::Client client;
  expect_journaler_get_client(mock_remote_journaler, "local mirror uuid",
                              client, -ENOENT);

  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta;
  mirror_peer_client_meta.image_id = "local image id";
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;
  librbd::journal::ClientData client_data{mirror_peer_client_meta};
  expect_journaler_register_client(mock_remote_journaler, client_data, 0);

  mock_journal_state_builder.local_image_id = "local image id";
  MockStateBuilder* mock_state_builder = &mock_journal_state_builder;
  C_SaferCond ctx;
  auto req = MockPrepareRemoteImageRequest::create(&mock_threads,
                                                   m_local_io_ctx,
                                                   m_remote_io_ctx,
                                                   "global image id",
                                                   "local mirror uuid",
                                                   {"remote mirror uuid", ""},
                                                   nullptr,
                                                   &mock_state_builder,
                                                   &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
  ASSERT_TRUE(mock_state_builder != nullptr);
  ASSERT_EQ(std::string("remote image id"),
            mock_journal_state_builder.remote_image_id);
  ASSERT_EQ(librbd::mirror::PROMOTION_STATE_PRIMARY,
            mock_journal_state_builder.remote_promotion_state);
  ASSERT_TRUE(mock_journal_state_builder.remote_journaler != nullptr);
  ASSERT_EQ(cls::journal::CLIENT_STATE_CONNECTED,
            mock_journal_state_builder.remote_client_state);
  // owned by StateBuilder, normally freed in StateBuilder::close()
  delete mock_journal_state_builder.remote_journaler;
}

TEST_F(TestMockImageReplayerPrepareRemoteImageRequest, GetMirrorImageIdError) {
  MockThreads mock_threads(m_threads);

  InSequence seq;
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request, "", -EINVAL);

  MockJournalStateBuilder mock_journal_state_builder;
  MockStateBuilder* mock_state_builder = &mock_journal_state_builder;
  C_SaferCond ctx;
  auto req = MockPrepareRemoteImageRequest::create(&mock_threads,
                                                   m_local_io_ctx,
                                                   m_remote_io_ctx,
                                                   "global image id",
                                                   "local mirror uuid",
                                                   {"remote mirror uuid", ""},
                                                   nullptr,
                                                   &mock_state_builder,
                                                   &ctx);
  req->send();

  ASSERT_EQ(-EINVAL, ctx.wait());
  ASSERT_TRUE(mock_journal_state_builder.remote_journaler == nullptr);
}

TEST_F(TestMockImageReplayerPrepareRemoteImageRequest, GetMirrorInfoError) {
  MockThreads mock_threads(m_threads);

  InSequence seq;
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request,
                             "remote image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_PRIMARY,
                         "remote mirror uuid", -EINVAL);

  MockJournalStateBuilder mock_journal_state_builder;
  MockStateBuilder* mock_state_builder = nullptr;
  C_SaferCond ctx;
  auto req = MockPrepareRemoteImageRequest::create(&mock_threads,
                                                   m_local_io_ctx,
                                                   m_remote_io_ctx,
                                                   "global image id",
                                                   "local mirror uuid",
                                                   {"remote mirror uuid", ""},
                                                   nullptr,
                                                   &mock_state_builder,
                                                   &ctx);
  req->send();

  ASSERT_EQ(-EINVAL, ctx.wait());
  ASSERT_TRUE(mock_state_builder == nullptr);
}

TEST_F(TestMockImageReplayerPrepareRemoteImageRequest, GetClientError) {
  ::journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);

  InSequence seq;
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request,
                             "remote image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_PRIMARY,
                         "remote mirror uuid", 0);

  EXPECT_CALL(mock_remote_journaler, construct());

  cls::journal::Client client;
  expect_journaler_get_client(mock_remote_journaler, "local mirror uuid",
                              client, -EINVAL);

  MockJournalStateBuilder mock_journal_state_builder;
  MockStateBuilder* mock_state_builder = nullptr;
  C_SaferCond ctx;
  auto req = MockPrepareRemoteImageRequest::create(&mock_threads,
                                                   m_local_io_ctx,
                                                   m_remote_io_ctx,
                                                   "global image id",
                                                   "local mirror uuid",
                                                   {"remote mirror uuid", ""},
                                                   nullptr,
                                                   &mock_state_builder,
                                                   &ctx);
  req->send();

  ASSERT_EQ(-EINVAL, ctx.wait());
  ASSERT_TRUE(mock_state_builder == nullptr);
}

TEST_F(TestMockImageReplayerPrepareRemoteImageRequest, RegisterClientError) {
  ::journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);

  InSequence seq;
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request,
                             "remote image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_PRIMARY,
                         "remote mirror uuid", 0);

  MockJournalStateBuilder mock_journal_state_builder;
  expect_get_mirror_image_mode(mock_journal_state_builder,
                               cls::rbd::MIRROR_IMAGE_MODE_JOURNAL);

  EXPECT_CALL(mock_remote_journaler, construct());

  cls::journal::Client client;
  expect_journaler_get_client(mock_remote_journaler, "local mirror uuid",
                              client, -ENOENT);

  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta;
  mirror_peer_client_meta.image_id = "local image id";
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;
  librbd::journal::ClientData client_data{mirror_peer_client_meta};
  expect_journaler_register_client(mock_remote_journaler, client_data, -EINVAL);

  mock_journal_state_builder.local_image_id = "local image id";
  MockStateBuilder* mock_state_builder = &mock_journal_state_builder;
  C_SaferCond ctx;
  auto req = MockPrepareRemoteImageRequest::create(&mock_threads,
                                                   m_local_io_ctx,
                                                   m_remote_io_ctx,
                                                   "global image id",
                                                   "local mirror uuid",
                                                   {"remote mirror uuid", ""},
                                                   nullptr,
                                                   &mock_state_builder,
                                                   &ctx);
  req->send();

  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerPrepareRemoteImageRequest, MirrorImageIdDNEJournal) {
  MockThreads mock_threads(m_threads);

  InSequence seq;
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request, "", -ENOENT);

  MockJournalStateBuilder mock_journal_state_builder;
  MockStateBuilder* mock_state_builder = &mock_journal_state_builder;
  C_SaferCond ctx;
  auto req = MockPrepareRemoteImageRequest::create(&mock_threads,
                                                   m_local_io_ctx,
                                                   m_remote_io_ctx,
                                                   "global image id",
                                                   "local mirror uuid",
                                                   {"remote mirror uuid", ""},
                                                   nullptr,
                                                   &mock_state_builder,
                                                   &ctx);
  req->send();

  ASSERT_EQ(-ENOENT, ctx.wait());
  ASSERT_EQ(cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
           mock_journal_state_builder.mirror_image_mode);
  ASSERT_EQ("remote mirror uuid",
            mock_journal_state_builder.remote_mirror_uuid);
  ASSERT_EQ("", mock_journal_state_builder.remote_image_id);
}

TEST_F(TestMockImageReplayerPrepareRemoteImageRequest, MirrorImageIdDNESnapshot) {
  MockThreads mock_threads(m_threads);

  InSequence seq;
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request, "", -ENOENT);

  MockSnapshotStateBuilder mock_snapshot_state_builder;
  MockStateBuilder* mock_state_builder = &mock_snapshot_state_builder;
  C_SaferCond ctx;
  auto req = MockPrepareRemoteImageRequest::create(&mock_threads,
                                                   m_local_io_ctx,
                                                   m_remote_io_ctx,
                                                   "global image id",
                                                   "local mirror uuid",
                                                   {"remote mirror uuid",
                                                    "remote mirror peer uuid"},
                                                   nullptr,
                                                   &mock_state_builder,
                                                   &ctx);
  req->send();

  ASSERT_EQ(-ENOENT, ctx.wait());
  ASSERT_EQ(cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
           mock_snapshot_state_builder.mirror_image_mode);
  ASSERT_EQ("remote mirror uuid",
            mock_snapshot_state_builder.remote_mirror_uuid);
  ASSERT_EQ("remote mirror peer uuid",
            mock_snapshot_state_builder.remote_mirror_peer_uuid);
  ASSERT_EQ("", mock_snapshot_state_builder.remote_image_id);
}

TEST_F(TestMockImageReplayerPrepareRemoteImageRequest, MirrorInfoDNEJournal) {
  MockThreads mock_threads(m_threads);

  InSequence seq;
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request,
                             "remote image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_PRIMARY,
                         "remote mirror uuid", -ENOENT);

  MockJournalStateBuilder mock_journal_state_builder;
  MockStateBuilder* mock_state_builder = &mock_journal_state_builder;
  C_SaferCond ctx;
  auto req = MockPrepareRemoteImageRequest::create(&mock_threads,
                                                   m_local_io_ctx,
                                                   m_remote_io_ctx,
                                                   "global image id",
                                                   "local mirror uuid",
                                                   {"remote mirror uuid", ""},
                                                   nullptr,
                                                   &mock_state_builder,
                                                   &ctx);
  req->send();

  ASSERT_EQ(-ENOENT, ctx.wait());
  ASSERT_EQ(cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
           mock_journal_state_builder.mirror_image_mode);
  ASSERT_EQ("remote mirror uuid",
            mock_journal_state_builder.remote_mirror_uuid);
  ASSERT_EQ("", mock_journal_state_builder.remote_image_id);
}

TEST_F(TestMockImageReplayerPrepareRemoteImageRequest, MirrorInfoDNESnapshot) {
  MockThreads mock_threads(m_threads);

  InSequence seq;
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request,
                             "remote image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                         librbd::mirror::PROMOTION_STATE_PRIMARY,
                         "remote mirror uuid", -ENOENT);

  MockSnapshotStateBuilder mock_snapshot_state_builder;
  MockStateBuilder* mock_state_builder = &mock_snapshot_state_builder;
  C_SaferCond ctx;
  auto req = MockPrepareRemoteImageRequest::create(&mock_threads,
                                                   m_local_io_ctx,
                                                   m_remote_io_ctx,
                                                   "global image id",
                                                   "local mirror uuid",
                                                   {"remote mirror uuid",
                                                    "remote mirror peer uuid"},
                                                   nullptr,
                                                   &mock_state_builder,
                                                   &ctx);
  req->send();

  ASSERT_EQ(-ENOENT, ctx.wait());
  ASSERT_EQ(cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
           mock_snapshot_state_builder.mirror_image_mode);
  ASSERT_EQ("remote mirror uuid",
            mock_snapshot_state_builder.remote_mirror_uuid);
  ASSERT_EQ("remote mirror peer uuid",
            mock_snapshot_state_builder.remote_mirror_peer_uuid);
  ASSERT_EQ("", mock_snapshot_state_builder.remote_image_id);
}

TEST_F(TestMockImageReplayerPrepareRemoteImageRequest, MirrorInfoDisablingJournal) {
  MockThreads mock_threads(m_threads);

  InSequence seq;
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request,
                             "remote image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_DISABLING},
                         librbd::mirror::PROMOTION_STATE_PRIMARY,
                         "remote mirror uuid", 0);

  MockJournalStateBuilder mock_journal_state_builder;
  expect_get_mirror_image_mode(mock_journal_state_builder,
                               cls::rbd::MIRROR_IMAGE_MODE_JOURNAL);
  MockStateBuilder* mock_state_builder = &mock_journal_state_builder;
  C_SaferCond ctx;
  auto req = MockPrepareRemoteImageRequest::create(&mock_threads,
                                                   m_local_io_ctx,
                                                   m_remote_io_ctx,
                                                   "global image id",
                                                   "local mirror uuid",
                                                   {"remote mirror uuid", ""},
                                                   nullptr,
                                                   &mock_state_builder,
                                                   &ctx);
  req->send();

  ASSERT_EQ(-ENOENT, ctx.wait());
  ASSERT_EQ(cls::rbd::MIRROR_IMAGE_MODE_JOURNAL,
           mock_journal_state_builder.mirror_image_mode);
  ASSERT_EQ("remote mirror uuid",
            mock_journal_state_builder.remote_mirror_uuid);
  ASSERT_EQ("", mock_journal_state_builder.remote_image_id);
}

TEST_F(TestMockImageReplayerPrepareRemoteImageRequest, MirrorInfoDisablingSnapshot) {
  MockThreads mock_threads(m_threads);

  InSequence seq;
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request,
                             "remote image id", 0);

  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_mirror_info(mock_get_mirror_info_request,
                         {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
                          "global image id",
                          cls::rbd::MIRROR_IMAGE_STATE_DISABLING},
                         librbd::mirror::PROMOTION_STATE_PRIMARY,
                         "remote mirror uuid", 0);

  MockSnapshotStateBuilder mock_snapshot_state_builder;
  expect_get_mirror_image_mode(mock_snapshot_state_builder,
                               cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT);
  MockStateBuilder* mock_state_builder = &mock_snapshot_state_builder;
  C_SaferCond ctx;
  auto req = MockPrepareRemoteImageRequest::create(&mock_threads,
                                                   m_local_io_ctx,
                                                   m_remote_io_ctx,
                                                   "global image id",
                                                   "local mirror uuid",
                                                   {"remote mirror uuid",
                                                    "remote mirror peer uuid"},
                                                   nullptr,
                                                   &mock_state_builder,
                                                   &ctx);
  req->send();

  ASSERT_EQ(-ENOENT, ctx.wait());
  ASSERT_EQ(cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
           mock_snapshot_state_builder.mirror_image_mode);
  ASSERT_EQ("remote mirror uuid",
            mock_snapshot_state_builder.remote_mirror_uuid);
  ASSERT_EQ("remote mirror peer uuid",
            mock_snapshot_state_builder.remote_mirror_peer_uuid);
  ASSERT_EQ("", mock_snapshot_state_builder.remote_image_id);
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd
