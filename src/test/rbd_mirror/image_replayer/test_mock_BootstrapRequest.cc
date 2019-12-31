// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "librbd/journal/TypeTraits.h"
#include "librbd/mirror/GetInfoRequest.h"
#include "tools/rbd_mirror/InstanceWatcher.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_replayer/BootstrapRequest.h"
#include "tools/rbd_mirror/image_replayer/CloseImageRequest.h"
#include "tools/rbd_mirror/image_replayer/OpenImageRequest.h"
#include "tools/rbd_mirror/image_replayer/OpenLocalImageRequest.h"
#include "tools/rbd_mirror/image_replayer/PrepareLocalImageRequest.h"
#include "tools/rbd_mirror/image_replayer/PrepareRemoteImageRequest.h"
#include "tools/rbd_mirror/image_replayer/journal/CreateLocalImageRequest.h"
#include "tools/rbd_mirror/image_replayer/journal/PrepareReplayRequest.h"
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

namespace journal {

template <>
struct TypeTraits<librbd::MockTestImageCtx> {
  typedef ::journal::MockJournaler Journaler;
};

} // namespace journal

namespace mirror {

template<>
struct GetInfoRequest<librbd::MockTestImageCtx> {
  static GetInfoRequest* s_instance;
  cls::rbd::MirrorImage *mirror_image;
  PromotionState *promotion_state;
  Context *on_finish = nullptr;

  static GetInfoRequest* create(librbd::MockTestImageCtx &image_ctx,
                                cls::rbd::MirrorImage *mirror_image,
                                PromotionState *promotion_state,
                                Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->mirror_image = mirror_image;
    s_instance->promotion_state = promotion_state;
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

class ProgressContext;

template <>
struct Threads<librbd::MockTestImageCtx> {
  ceph::mutex &timer_lock;
  SafeTimer *timer;
  ContextWQ *work_queue;

  Threads(Threads<librbd::ImageCtx> *threads)
    : timer_lock(threads->timer_lock), timer(threads->timer),
      work_queue(threads->work_queue) {
  }
};

template<>
struct ImageSync<librbd::MockTestImageCtx> {
  static ImageSync* s_instance;
  Context *on_finish = nullptr;

  static ImageSync* create(
      librbd::MockTestImageCtx *local_image_ctx,
      librbd::MockTestImageCtx *remote_image_ctx,
      SafeTimer *timer, ceph::mutex *timer_lock,
      const std::string &mirror_uuid, ::journal::MockJournaler *journaler,
      librbd::journal::MirrorPeerClientMeta *client_meta, ContextWQ *work_queue,
      InstanceWatcher<librbd::MockTestImageCtx> *instance_watcher,
      Context *on_finish, ProgressContext *progress_ctx) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  ImageSync() {
    ceph_assert(s_instance == nullptr);
    s_instance = this;
  }
  ~ImageSync() {
    s_instance = nullptr;
  }

  MOCK_METHOD0(get, void());
  MOCK_METHOD0(put, void());
  MOCK_METHOD0(send, void());
  MOCK_METHOD0(cancel, void());
};

ImageSync<librbd::MockTestImageCtx>*
  ImageSync<librbd::MockTestImageCtx>::s_instance = nullptr;

template<>
struct InstanceWatcher<librbd::MockTestImageCtx> {
};

namespace image_replayer {

template<>
struct CloseImageRequest<librbd::MockTestImageCtx> {
  static CloseImageRequest* s_instance;
  librbd::MockTestImageCtx **image_ctx = nullptr;
  Context *on_finish = nullptr;

  static CloseImageRequest* create(librbd::MockTestImageCtx **image_ctx,
                                   Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->image_ctx = image_ctx;
    s_instance->on_finish = on_finish;
    s_instance->construct(*image_ctx);
    return s_instance;
  }

  CloseImageRequest() {
    ceph_assert(s_instance == nullptr);
    s_instance = this;
  }
  ~CloseImageRequest() {
    s_instance = nullptr;
  }

  MOCK_METHOD1(construct, void(librbd::MockTestImageCtx *image_ctx));
  MOCK_METHOD0(send, void());
};

template<>
struct OpenImageRequest<librbd::MockTestImageCtx> {
  static OpenImageRequest* s_instance;
  librbd::MockTestImageCtx **image_ctx = nullptr;
  Context *on_finish = nullptr;

  static OpenImageRequest* create(librados::IoCtx &io_ctx,
                                  librbd::MockTestImageCtx **image_ctx,
                                  const std::string &image_id,
                                  bool read_only, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->image_ctx = image_ctx;
    s_instance->on_finish = on_finish;
    s_instance->construct(io_ctx, image_id);
    return s_instance;
  }

  OpenImageRequest() {
    ceph_assert(s_instance == nullptr);
    s_instance = this;
  }
  ~OpenImageRequest() {
    s_instance = nullptr;
  }

  MOCK_METHOD2(construct, void(librados::IoCtx &io_ctx,
                               const std::string &image_id));
  MOCK_METHOD0(send, void());
};

template<>
struct OpenLocalImageRequest<librbd::MockTestImageCtx> {
  static OpenLocalImageRequest* s_instance;
  librbd::MockTestImageCtx **image_ctx = nullptr;
  Context *on_finish = nullptr;

  static OpenLocalImageRequest* create(librados::IoCtx &local_io_ctx,
                                       librbd::MockTestImageCtx **local_image_ctx,
                                       const std::string &local_image_id,
                                       ContextWQ *work_queue,
                                       Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->image_ctx = local_image_ctx;
    s_instance->on_finish = on_finish;
    s_instance->construct(local_io_ctx, local_image_id);
    return s_instance;
  }

  OpenLocalImageRequest() {
    ceph_assert(s_instance == nullptr);
    s_instance = this;
  }
  ~OpenLocalImageRequest() {
    s_instance = nullptr;
  }

  MOCK_METHOD2(construct, void(librados::IoCtx &io_ctx,
                               const std::string &image_id));
  MOCK_METHOD0(send, void());
};

template<>
struct PrepareLocalImageRequest<librbd::MockTestImageCtx> {
  static PrepareLocalImageRequest* s_instance;
  std::string *local_image_id = nullptr;
  std::string *local_image_name = nullptr;
  std::string *tag_owner = nullptr;
  Context *on_finish = nullptr;

  static PrepareLocalImageRequest* create(librados::IoCtx &,
                                          const std::string &global_image_id,
                                          std::string *local_image_id,
                                          std::string *local_image_name,
                                          std::string *tag_owner,
                                          ContextWQ *work_queue,
                                          Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->local_image_id = local_image_id;
    s_instance->local_image_name = local_image_name;
    s_instance->tag_owner = tag_owner;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  PrepareLocalImageRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

template<>
struct PrepareRemoteImageRequest<librbd::MockTestImageCtx> {
  static PrepareRemoteImageRequest* s_instance;
  std::string *remote_mirror_uuid = nullptr;
  std::string *remote_image_id = nullptr;
  ::journal::MockJournaler **remote_journaler = nullptr;
  cls::journal::ClientState *client_state;
  librbd::journal::MirrorPeerClientMeta *client_meta = nullptr;
  Context *on_finish = nullptr;

  static PrepareRemoteImageRequest* create(Threads<librbd::MockTestImageCtx> *threads,
                                           librados::IoCtx &,
                                           const std::string &global_image_id,
                                           const std::string &local_mirror_uuid,
                                           const std::string &local_image_id,
                                           const ::journal::Settings &settings,
                                           ::journal::CacheManagerHandler *cache_manager_handler,
                                           std::string *remote_mirror_uuid,
                                           std::string *remote_image_id,
                                           ::journal::MockJournaler **remote_journaler,
                                           cls::journal::ClientState *client_state,
                                           librbd::journal::MirrorPeerClientMeta *client_meta,
                                           Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->remote_mirror_uuid = remote_mirror_uuid;
    s_instance->remote_image_id = remote_image_id;
    s_instance->remote_journaler = remote_journaler;
    s_instance->client_state = client_state;
    s_instance->client_meta = client_meta;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  PrepareRemoteImageRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

CloseImageRequest<librbd::MockTestImageCtx>*
  CloseImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
OpenImageRequest<librbd::MockTestImageCtx>*
  OpenImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
OpenLocalImageRequest<librbd::MockTestImageCtx>*
  OpenLocalImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
PrepareLocalImageRequest<librbd::MockTestImageCtx>*
  PrepareLocalImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
PrepareRemoteImageRequest<librbd::MockTestImageCtx>*
  PrepareRemoteImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

namespace journal {

template<>
struct CreateLocalImageRequest<librbd::MockTestImageCtx> {
  static CreateLocalImageRequest* s_instance;

  librbd::journal::MirrorPeerClientMeta* client_meta = nullptr;
  std::string* local_image_id;
  Context *on_finish = nullptr;

  static CreateLocalImageRequest* create(
      Threads<librbd::MockTestImageCtx>* mock_threads,
      librados::IoCtx &local_io_ctx,
      librbd::MockTestImageCtx* mock_remote_image_ctx,
      ::journal::MockJournaler *mock_remote_journaler,
      const std::string &global_image_id,
      const std::string &remote_mirror_uuid,
      librbd::journal::MirrorPeerClientMeta* client_meta,
      ProgressContext* progress_ctx,
      std::string* local_image_id, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->client_meta = client_meta;
    s_instance->local_image_id = local_image_id;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  CreateLocalImageRequest() {
    ceph_assert(s_instance == nullptr);
    s_instance = this;
  }
  ~CreateLocalImageRequest() {
    s_instance = nullptr;
  }

  MOCK_METHOD0(send, void());
};

template<>
struct PrepareReplayRequest<librbd::MockTestImageCtx> {
  static PrepareReplayRequest* s_instance;

  bool* resync_requested = nullptr;
  bool* syncing = nullptr;
  Context *on_finish = nullptr;

  static PrepareReplayRequest* create(
      librbd::MockTestImageCtx* local_image_ctx,
      ::journal::MockJournaler* remote_journaler,
      librbd::mirror::PromotionState remote_promotion_state,
      const std::string& local_mirror_uuid,
      const std::string& remote_mirror_uuid,
      librbd::journal::MirrorPeerClientMeta* client_meta,
      ProgressContext* progress_ctx,
      bool* resync_requested, bool* syncing, Context* on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->resync_requested = resync_requested;
    s_instance->syncing = syncing;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  PrepareReplayRequest() {
    ceph_assert(s_instance == nullptr);
    s_instance = this;
  }
  ~PrepareReplayRequest() {
    s_instance = nullptr;
  }

  MOCK_METHOD0(send, void());
};

CreateLocalImageRequest<librbd::MockTestImageCtx>*
  CreateLocalImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
PrepareReplayRequest<librbd::MockTestImageCtx>*
  PrepareReplayRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace journal
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

// template definitions
#include "tools/rbd_mirror/image_replayer/BootstrapRequest.cc"

namespace rbd {
namespace mirror {
namespace image_replayer {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::StrEq;
using ::testing::WithArg;

MATCHER_P(IsSameIoCtx, io_ctx, "") {
  return &get_mock_io_ctx(arg) == &get_mock_io_ctx(*io_ctx);
}

class TestMockImageReplayerBootstrapRequest : public TestMockFixture {
public:
  typedef Threads<librbd::MockTestImageCtx> MockThreads;
  typedef BootstrapRequest<librbd::MockTestImageCtx> MockBootstrapRequest;
  typedef CloseImageRequest<librbd::MockTestImageCtx> MockCloseImageRequest;
  typedef ImageSync<librbd::MockTestImageCtx> MockImageSync;
  typedef InstanceWatcher<librbd::MockTestImageCtx> MockInstanceWatcher;
  typedef OpenImageRequest<librbd::MockTestImageCtx> MockOpenImageRequest;
  typedef OpenLocalImageRequest<librbd::MockTestImageCtx> MockOpenLocalImageRequest;
  typedef PrepareLocalImageRequest<librbd::MockTestImageCtx> MockPrepareLocalImageRequest;
  typedef PrepareRemoteImageRequest<librbd::MockTestImageCtx> MockPrepareRemoteImageRequest;
  typedef journal::CreateLocalImageRequest<librbd::MockTestImageCtx> MockCreateLocalImageRequest;
  typedef journal::PrepareReplayRequest<librbd::MockTestImageCtx> MockPrepareReplayRequest;
  typedef librbd::mirror::GetInfoRequest<librbd::MockTestImageCtx> MockGetMirrorInfoRequest;
  typedef std::list<cls::journal::Tag> Tags;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_remote_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_remote_io_ctx, m_image_name, &m_remote_image_ctx));

    ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &m_local_image_ctx));
  }

  void expect_send(MockPrepareLocalImageRequest &mock_request,
                   const std::string &local_image_id,
                   const std::string &local_image_name,
                   const std::string &tag_owner,
                   int r) {
    EXPECT_CALL(mock_request, send())
      .WillOnce(Invoke([&mock_request, local_image_id, local_image_name,
                        tag_owner, r]() {
          if (r == 0) {
            *mock_request.local_image_id = local_image_id;
            *mock_request.local_image_name = local_image_name;
            *mock_request.tag_owner = tag_owner;
          }
          mock_request.on_finish->complete(r);
        }));
  }

  void expect_send(MockPrepareRemoteImageRequest& mock_request,
                   ::journal::MockJournaler& mock_remote_journaler,
                   const std::string& mirror_uuid, const std::string& image_id,
                   cls::journal::ClientState client_state,
                   const librbd::journal::MirrorPeerClientMeta& mirror_peer_client_meta,
                   int r) {
    EXPECT_CALL(mock_request, send())
      .WillOnce(Invoke([&mock_request, &mock_remote_journaler, image_id,
                        mirror_uuid, client_state, mirror_peer_client_meta,
                        r]() {
                  if (r >= 0) {
                    *mock_request.remote_journaler = &mock_remote_journaler;
                    *mock_request.client_state = client_state;
                    *mock_request.client_meta = mirror_peer_client_meta;
                  }

                  *mock_request.remote_mirror_uuid = mirror_uuid;
                  *mock_request.remote_image_id = image_id;
                  mock_request.on_finish->complete(r);
                }));
  }

  void expect_open_image(MockOpenImageRequest &mock_open_image_request,
                         librados::IoCtx &io_ctx, const std::string &image_id,
                         librbd::MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(mock_open_image_request,
                construct(IsSameIoCtx(&io_ctx), image_id));
    EXPECT_CALL(mock_open_image_request, send())
      .WillOnce(Invoke([this, &mock_open_image_request, &mock_image_ctx, r]() {
          *mock_open_image_request.image_ctx = &mock_image_ctx;
          m_threads->work_queue->queue(mock_open_image_request.on_finish, r);
        }));
  }

  void expect_open_local_image(MockOpenLocalImageRequest &mock_open_local_image_request,
                               librados::IoCtx &io_ctx, const std::string &image_id,
                               librbd::MockTestImageCtx *mock_image_ctx, int r) {
    EXPECT_CALL(mock_open_local_image_request,
                construct(IsSameIoCtx(&io_ctx), image_id));
    EXPECT_CALL(mock_open_local_image_request, send())
      .WillOnce(Invoke([this, &mock_open_local_image_request, mock_image_ctx, r]() {
          if (r >= 0) {
            *mock_open_local_image_request.image_ctx = mock_image_ctx;
          }
          m_threads->work_queue->queue(mock_open_local_image_request.on_finish,
                                       r);
        }));
  }

  void expect_close_image(MockCloseImageRequest &mock_close_image_request,
                          librbd::MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(mock_close_image_request, construct(&mock_image_ctx));
    EXPECT_CALL(mock_close_image_request, send())
      .WillOnce(Invoke([this, &mock_close_image_request, r]() {
          *mock_close_image_request.image_ctx = nullptr;
          m_threads->work_queue->queue(mock_close_image_request.on_finish, r);
        }));
  }

  void expect_get_remote_mirror_info(
      MockGetMirrorInfoRequest &mock_get_mirror_info_request,
      const cls::rbd::MirrorImage &mirror_image,
      librbd::mirror::PromotionState promotion_state, int r) {
    EXPECT_CALL(mock_get_mirror_info_request, send())
      .WillOnce(Invoke([this, &mock_get_mirror_info_request, mirror_image,
                        promotion_state, r]() {
          *mock_get_mirror_info_request.mirror_image = mirror_image;
          *mock_get_mirror_info_request.promotion_state = promotion_state;
          m_threads->work_queue->queue(
            mock_get_mirror_info_request.on_finish, r);
        }));
  }

  void expect_create_local_image(
      MockCreateLocalImageRequest &mock_create_local_image_request,
      const std::string& local_image_id, int r) {
    EXPECT_CALL(mock_create_local_image_request, send())
      .WillOnce(Invoke([this, &mock_create_local_image_request, local_image_id,
                        r]() {
          if (r >= 0) {
            mock_create_local_image_request.client_meta->state =
              librbd::journal::MIRROR_PEER_STATE_SYNCING;
            mock_create_local_image_request.client_meta->image_id =
              local_image_id;
            *mock_create_local_image_request.local_image_id = local_image_id;
          }
          m_threads->work_queue->queue(
            mock_create_local_image_request.on_finish, r);
        }));
  }

  void expect_prepare_replay(
      MockPrepareReplayRequest& mock_prepare_replay_request,
      bool resync_requested, bool syncing, int r) {
    EXPECT_CALL(mock_prepare_replay_request, send())
      .WillOnce(Invoke([this, &mock_prepare_replay_request, resync_requested,
                        syncing, r]() {
          if (r >= 0) {
            *mock_prepare_replay_request.resync_requested = resync_requested;
            *mock_prepare_replay_request.syncing = syncing;
          }
          m_threads->work_queue->queue(mock_prepare_replay_request.on_finish,
                                       r);
        }));
  }

  void expect_image_sync(MockImageSync &mock_image_sync, int r) {
    EXPECT_CALL(mock_image_sync, get());
    EXPECT_CALL(mock_image_sync, send())
      .WillOnce(Invoke([this, &mock_image_sync, r]() {
            m_threads->work_queue->queue(mock_image_sync.on_finish, r);
          }));
    EXPECT_CALL(mock_image_sync, put());
  }

  MockBootstrapRequest *create_request(MockThreads* mock_threads,
                                       MockInstanceWatcher *mock_instance_watcher,
                                       const std::string &remote_image_id,
                                       const std::string &global_image_id,
                                       const std::string &local_mirror_uuid,
                                       Context *on_finish) {
    return new MockBootstrapRequest(mock_threads,
                                    m_local_io_ctx,
                                    m_remote_io_ctx,
                                    mock_instance_watcher,
                                    remote_image_id,
                                    global_image_id,
                                    local_mirror_uuid,
                                    nullptr, nullptr,
                                    &m_local_test_image_ctx,
                                    &m_local_image_id,
                                    &m_remote_mirror_uuid,
                                    &m_mock_remote_journaler,
                                    &m_do_resync, on_finish);
  }

  librbd::ImageCtx *m_remote_image_ctx;
  librbd::ImageCtx *m_local_image_ctx = nullptr;

  librbd::MockTestImageCtx *m_local_test_image_ctx = nullptr;
  std::string m_local_image_id;
  std::string m_remote_mirror_uuid;
  ::journal::MockJournaler *m_mock_remote_journaler = nullptr;
  bool m_do_resync = false;
};

TEST_F(TestMockImageReplayerBootstrapRequest, Success) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  expect_send(mock_prepare_local_image_request, m_local_image_ctx->id,
              m_local_image_ctx->name, "remote mirror uuid", 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  ::journal::MockJournaler mock_remote_journaler;
  cls::journal::ClientState client_state = cls::journal::CLIENT_STATE_CONNECTED;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta{
    m_local_image_ctx->id};
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_SYNCING;
  expect_send(mock_prepare_remote_image_request, mock_remote_journaler,
              "remote mirror uuid", m_remote_image_ctx->id, client_state,
              mirror_peer_client_meta, 0);

  // open the remote image
  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // test if remote image is primary
  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_remote_mirror_info(mock_get_mirror_info_request,
                                {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "uuid",
                                 cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                                librbd::mirror::PROMOTION_STATE_PRIMARY, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx, 0);

  // prepare replay
  MockPrepareReplayRequest mock_prepare_replay_request;
  expect_prepare_replay(mock_prepare_replay_request, false, false, 0);

  // close remote image
  MockCloseImageRequest mock_close_image_request;
  expect_close_image(mock_close_image_request, mock_remote_image_ctx, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, mock_remote_image_ctx.id,
    "global image id", "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageReplayerBootstrapRequest, OpenLocalImageError) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  expect_send(mock_prepare_local_image_request, m_local_image_ctx->id,
              m_local_image_ctx->name, "remote mirror uuid", 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  ::journal::MockJournaler mock_remote_journaler;
  cls::journal::ClientState client_state = cls::journal::CLIENT_STATE_CONNECTED;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta{
    m_local_image_ctx->id};
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_SYNCING;
  expect_send(mock_prepare_remote_image_request, mock_remote_journaler,
              "remote mirror uuid", m_remote_image_ctx->id, client_state,
              mirror_peer_client_meta, 0);

  // open the remote image
  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // test if remote image is primary
  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_remote_mirror_info(mock_get_mirror_info_request,
                                {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "uuid",
                                 cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                                librbd::mirror::PROMOTION_STATE_PRIMARY, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx,
                          -EINVAL);

  // close remote image
  MockCloseImageRequest mock_close_image_request;
  expect_close_image(mock_close_image_request, mock_remote_image_ctx, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, mock_remote_image_ctx.id,
    "global image id", "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerBootstrapRequest, OpenLocalImageDNE) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  expect_send(mock_prepare_local_image_request, m_local_image_ctx->id,
              m_local_image_ctx->name, "remote mirror uuid", 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  ::journal::MockJournaler mock_remote_journaler;
  cls::journal::ClientState client_state = cls::journal::CLIENT_STATE_CONNECTED;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta{
    m_local_image_ctx->id};
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_SYNCING;
  expect_send(mock_prepare_remote_image_request, mock_remote_journaler,
              "remote mirror uuid", m_remote_image_ctx->id, client_state,
              mirror_peer_client_meta, 0);

  // open the remote image
  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // test if remote image is primary
  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_remote_mirror_info(mock_get_mirror_info_request,
                                {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "uuid",
                                 cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                                librbd::mirror::PROMOTION_STATE_PRIMARY, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx,
                          -ENOENT);

  // create local image
  MockCreateLocalImageRequest mock_create_local_image_request;
  expect_create_local_image(mock_create_local_image_request, "local image id",
                            0);

  // re-open the local image
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          "local image id", &mock_local_image_ctx, 0);

  // prepare replay
  MockPrepareReplayRequest mock_prepare_replay_request;
  expect_prepare_replay(mock_prepare_replay_request, false, false, 0);

  // close remote image
  MockCloseImageRequest mock_close_image_request;
  expect_close_image(mock_close_image_request, mock_remote_image_ctx, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, mock_remote_image_ctx.id,
    "global image id", "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageReplayerBootstrapRequest, OpenLocalImagePrimary) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  expect_send(mock_prepare_local_image_request, m_local_image_ctx->id,
              m_local_image_ctx->name, "remote mirror uuid", 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  ::journal::MockJournaler mock_remote_journaler;
  cls::journal::ClientState client_state = cls::journal::CLIENT_STATE_CONNECTED;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta{
    m_local_image_ctx->id};
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_SYNCING;
  expect_send(mock_prepare_remote_image_request, mock_remote_journaler,
              "remote mirror uuid", m_remote_image_ctx->id, client_state,
              mirror_peer_client_meta, 0);

  // open the remote image
  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // test if remote image is primary
  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_remote_mirror_info(mock_get_mirror_info_request,
                                {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "uuid",
                                 cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                                librbd::mirror::PROMOTION_STATE_PRIMARY, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx,
                          -EREMOTEIO);

  // close remote image
  MockCloseImageRequest mock_close_image_request;
  expect_close_image(mock_close_image_request, mock_remote_image_ctx, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, mock_remote_image_ctx.id,
    "global image id", "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(-EREMOTEIO, ctx.wait());
}

TEST_F(TestMockImageReplayerBootstrapRequest, CreateLocalImageError) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  expect_send(mock_prepare_local_image_request, "", "", "", 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  ::journal::MockJournaler mock_remote_journaler;
  cls::journal::ClientState client_state = cls::journal::CLIENT_STATE_CONNECTED;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta;
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_SYNCING;
  expect_send(mock_prepare_remote_image_request, mock_remote_journaler,
              "remote mirror uuid", m_remote_image_ctx->id, client_state,
              mirror_peer_client_meta, 0);

  // open the remote image
  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // test if remote image is primary
  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_remote_mirror_info(mock_get_mirror_info_request,
                                {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "uuid",
                                 cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                                librbd::mirror::PROMOTION_STATE_PRIMARY, 0);

  // create local image
  MockCreateLocalImageRequest mock_create_local_image_request;
  expect_create_local_image(mock_create_local_image_request, "local image id",
                            -EINVAL);

  // close remote image
  MockCloseImageRequest mock_close_image_request;
  expect_close_image(mock_close_image_request, mock_remote_image_ctx, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, mock_remote_image_ctx.id,
    "global image id", "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerBootstrapRequest, PrepareReplayError) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  expect_send(mock_prepare_local_image_request, m_local_image_ctx->id,
              m_local_image_ctx->name, "remote mirror uuid", 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  ::journal::MockJournaler mock_remote_journaler;
  cls::journal::ClientState client_state = cls::journal::CLIENT_STATE_CONNECTED;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta{
    m_local_image_ctx->id};
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_SYNCING;
  expect_send(mock_prepare_remote_image_request, mock_remote_journaler,
              "remote mirror uuid", m_remote_image_ctx->id, client_state,
              mirror_peer_client_meta, 0);

  // open the remote image
  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // test if remote image is primary
  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_remote_mirror_info(mock_get_mirror_info_request,
                                {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "uuid",
                                 cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                                librbd::mirror::PROMOTION_STATE_PRIMARY, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx, 0);

  // prepare replay
  MockPrepareReplayRequest mock_prepare_replay_request;
  expect_prepare_replay(mock_prepare_replay_request, false, false, -EINVAL);

  // close local image
  MockCloseImageRequest mock_close_image_request;
  expect_close_image(mock_close_image_request, mock_local_image_ctx, 0);

  // close remote image
  expect_close_image(mock_close_image_request, mock_remote_image_ctx, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, mock_remote_image_ctx.id,
    "global image id", "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerBootstrapRequest, PrepareReplayResyncRequested) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  expect_send(mock_prepare_local_image_request, m_local_image_ctx->id,
              m_local_image_ctx->name, "remote mirror uuid", 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  ::journal::MockJournaler mock_remote_journaler;
  cls::journal::ClientState client_state = cls::journal::CLIENT_STATE_CONNECTED;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta{
    m_local_image_ctx->id};
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_SYNCING;
  expect_send(mock_prepare_remote_image_request, mock_remote_journaler,
              "remote mirror uuid", m_remote_image_ctx->id, client_state,
              mirror_peer_client_meta, 0);

  // open the remote image
  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // test if remote image is primary
  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_remote_mirror_info(mock_get_mirror_info_request,
                                {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "uuid",
                                 cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                                librbd::mirror::PROMOTION_STATE_PRIMARY, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx, 0);

  // prepare replay
  MockPrepareReplayRequest mock_prepare_replay_request;
  expect_prepare_replay(mock_prepare_replay_request, true, false, 0);

  // close remote image
  MockCloseImageRequest mock_close_image_request;
  expect_close_image(mock_close_image_request, mock_remote_image_ctx, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, mock_remote_image_ctx.id,
    "global image id", "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_TRUE(m_do_resync);
}

TEST_F(TestMockImageReplayerBootstrapRequest, PrepareReplaySyncing) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  expect_send(mock_prepare_local_image_request, m_local_image_ctx->id,
              m_local_image_ctx->name, "remote mirror uuid", 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  ::journal::MockJournaler mock_remote_journaler;
  cls::journal::ClientState client_state = cls::journal::CLIENT_STATE_CONNECTED;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta{
    m_local_image_ctx->id};
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_SYNCING;
  expect_send(mock_prepare_remote_image_request, mock_remote_journaler,
              "remote mirror uuid", m_remote_image_ctx->id, client_state,
              mirror_peer_client_meta, 0);

  // open the remote image
  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // test if remote image is primary
  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_remote_mirror_info(mock_get_mirror_info_request,
                                {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "uuid",
                                 cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                                librbd::mirror::PROMOTION_STATE_PRIMARY, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx, 0);

  // prepare replay
  MockPrepareReplayRequest mock_prepare_replay_request;
  expect_prepare_replay(mock_prepare_replay_request, false, true, 0);

  // image sync
  MockImageSync mock_image_sync;
  expect_image_sync(mock_image_sync, 0);

  // close remote image
  MockCloseImageRequest mock_close_image_request;
  expect_close_image(mock_close_image_request, mock_remote_image_ctx, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, mock_remote_image_ctx.id,
    "global image id", "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageReplayerBootstrapRequest, PrepareReplayDisconnected) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  expect_send(mock_prepare_local_image_request, m_local_image_ctx->id,
              m_local_image_ctx->name, "remote mirror uuid", 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  ::journal::MockJournaler mock_remote_journaler;
  cls::journal::ClientState client_state = cls::journal::CLIENT_STATE_CONNECTED;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta{
    m_local_image_ctx->id};
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_SYNCING;
  expect_send(mock_prepare_remote_image_request, mock_remote_journaler,
              "remote mirror uuid", m_remote_image_ctx->id, client_state,
              mirror_peer_client_meta, 0);

  // open the remote image
  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // test if remote image is primary
  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_remote_mirror_info(mock_get_mirror_info_request,
                                {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "uuid",
                                 cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                                librbd::mirror::PROMOTION_STATE_PRIMARY, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx, 0);

  // prepare replay
  MockPrepareReplayRequest mock_prepare_replay_request;
  expect_prepare_replay(mock_prepare_replay_request, false, false, 0);

  // close remote image
  MockCloseImageRequest mock_close_image_request;
  expect_close_image(mock_close_image_request, mock_remote_image_ctx, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, mock_remote_image_ctx.id,
    "global image id", "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageReplayerBootstrapRequest, ImageSyncError) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  expect_send(mock_prepare_local_image_request, m_local_image_ctx->id,
              m_local_image_ctx->name, "remote mirror uuid", 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  ::journal::MockJournaler mock_remote_journaler;
  cls::journal::ClientState client_state = cls::journal::CLIENT_STATE_CONNECTED;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta{
    m_local_image_ctx->id};
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_SYNCING;
  expect_send(mock_prepare_remote_image_request, mock_remote_journaler,
              "remote mirror uuid", m_remote_image_ctx->id, client_state,
              mirror_peer_client_meta, 0);

  // open the remote image
  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // test if remote image is primary
  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_remote_mirror_info(mock_get_mirror_info_request,
                                {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "uuid",
                                 cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                                librbd::mirror::PROMOTION_STATE_PRIMARY, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx, 0);

  // prepare replay
  MockPrepareReplayRequest mock_prepare_replay_request;
  expect_prepare_replay(mock_prepare_replay_request, false, true, 0);

  // image sync
  MockImageSync mock_image_sync;
  expect_image_sync(mock_image_sync, -EINVAL);

  // close remote image
  MockCloseImageRequest mock_close_image_request;
  expect_close_image(mock_close_image_request, mock_remote_image_ctx, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, mock_remote_image_ctx.id,
    "global image id", "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerBootstrapRequest, ImageSyncCanceled) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  expect_send(mock_prepare_local_image_request, m_local_image_ctx->id,
              m_local_image_ctx->name, "remote mirror uuid", 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  ::journal::MockJournaler mock_remote_journaler;
  cls::journal::ClientState client_state = cls::journal::CLIENT_STATE_CONNECTED;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta{
    m_local_image_ctx->id};
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_SYNCING;
  expect_send(mock_prepare_remote_image_request, mock_remote_journaler,
              "remote mirror uuid", m_remote_image_ctx->id, client_state,
              mirror_peer_client_meta, 0);

  // open the remote image
  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // test if remote image is primary
  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_remote_mirror_info(mock_get_mirror_info_request,
                                {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "uuid",
                                 cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                                librbd::mirror::PROMOTION_STATE_PRIMARY, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx, 0);

  // prepare replay
  MockPrepareReplayRequest mock_prepare_replay_request;
  expect_prepare_replay(mock_prepare_replay_request, false, true, 0);

  // close remote image
  MockCloseImageRequest mock_close_image_request;
  expect_close_image(mock_close_image_request, mock_remote_image_ctx, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, mock_remote_image_ctx.id,
    "global image id", "local mirror uuid", &ctx);
  request->cancel();
  request->send();
  ASSERT_EQ(-ECANCELED, ctx.wait());
}

TEST_F(TestMockImageReplayerBootstrapRequest, CloseLocalImageError) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  expect_send(mock_prepare_local_image_request, m_local_image_ctx->id,
              m_local_image_ctx->name, "remote mirror uuid", 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  ::journal::MockJournaler mock_remote_journaler;
  cls::journal::ClientState client_state = cls::journal::CLIENT_STATE_CONNECTED;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta{
    m_local_image_ctx->id};
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_SYNCING;
  expect_send(mock_prepare_remote_image_request, mock_remote_journaler,
              "remote mirror uuid", m_remote_image_ctx->id, client_state,
              mirror_peer_client_meta, 0);

  // open the remote image
  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // test if remote image is primary
  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_remote_mirror_info(mock_get_mirror_info_request,
                                {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "uuid",
                                 cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                                librbd::mirror::PROMOTION_STATE_PRIMARY, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx, 0);

  // prepare replay
  MockPrepareReplayRequest mock_prepare_replay_request;
  expect_prepare_replay(mock_prepare_replay_request, false, false, -EREMOTEIO);

  // close local image
  MockCloseImageRequest mock_close_image_request;
  expect_close_image(mock_close_image_request, mock_local_image_ctx, -EINVAL);

  // close remote image
  expect_close_image(mock_close_image_request, mock_remote_image_ctx, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, mock_remote_image_ctx.id,
    "global image id", "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(-EREMOTEIO, ctx.wait());
}

TEST_F(TestMockImageReplayerBootstrapRequest, CloseRemoteImageError) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  expect_send(mock_prepare_local_image_request, m_local_image_ctx->id,
              m_local_image_ctx->name, "remote mirror uuid", 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  ::journal::MockJournaler mock_remote_journaler;
  cls::journal::ClientState client_state = cls::journal::CLIENT_STATE_CONNECTED;
  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta{
    m_local_image_ctx->id};
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_SYNCING;
  expect_send(mock_prepare_remote_image_request, mock_remote_journaler,
              "remote mirror uuid", m_remote_image_ctx->id, client_state,
              mirror_peer_client_meta, 0);

  // open the remote image
  librbd::MockJournal mock_journal;
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // test if remote image is primary
  MockGetMirrorInfoRequest mock_get_mirror_info_request;
  expect_get_remote_mirror_info(mock_get_mirror_info_request,
                                {cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "uuid",
                                 cls::rbd::MIRROR_IMAGE_STATE_ENABLED},
                                librbd::mirror::PROMOTION_STATE_PRIMARY, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.journal = &mock_journal;
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx, 0);

  // prepare replay
  MockPrepareReplayRequest mock_prepare_replay_request;
  expect_prepare_replay(mock_prepare_replay_request, false, false, 0);

  MockCloseImageRequest mock_close_image_request;
  expect_close_image(mock_close_image_request, mock_remote_image_ctx, -EINVAL);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, mock_remote_image_ctx.id,
    "global image id", "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd
