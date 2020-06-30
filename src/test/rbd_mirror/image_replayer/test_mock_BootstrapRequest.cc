// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "librbd/journal/TypeTraits.h"
#include "tools/rbd_mirror/BaseRequest.h"
#include "tools/rbd_mirror/InstanceWatcher.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_replayer/BootstrapRequest.h"
#include "tools/rbd_mirror/image_replayer/OpenImageRequest.h"
#include "tools/rbd_mirror/image_replayer/OpenLocalImageRequest.h"
#include "tools/rbd_mirror/image_replayer/PrepareLocalImageRequest.h"
#include "tools/rbd_mirror/image_replayer/PrepareRemoteImageRequest.h"
#include "tools/rbd_mirror/image_replayer/StateBuilder.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/rbd_mirror/mock/image_sync/MockSyncPointHandler.h"
#include "test/rbd_mirror/mock/MockBaseRequest.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace
} // namespace librbd

namespace rbd {
namespace mirror {

class ProgressContext;

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

template<>
struct ImageSync<librbd::MockTestImageCtx> {
  static ImageSync* s_instance;
  Context *on_finish = nullptr;

  static ImageSync* create(
      Threads<librbd::MockTestImageCtx>* threads,
      librbd::MockTestImageCtx *local_image_ctx,
      librbd::MockTestImageCtx *remote_image_ctx,
      const std::string &local_mirror_uuid,
      image_sync::SyncPointHandler* sync_point_handler,
      InstanceWatcher<librbd::MockTestImageCtx> *instance_watcher,
      ProgressContext *progress_ctx, Context *on_finish) {
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
                                       librbd::asio::ContextWQ *work_queue,
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
  std::string *local_image_name = nullptr;
  StateBuilder<librbd::MockTestImageCtx>** state_builder = nullptr;
  Context *on_finish = nullptr;

  static PrepareLocalImageRequest* create(librados::IoCtx &,
                                          const std::string &global_image_id,
                                          std::string *local_image_name,
                                          StateBuilder<librbd::MockTestImageCtx>** state_builder,
                                          librbd::asio::ContextWQ *work_queue,
                                          Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->local_image_name = local_image_name;
    s_instance->state_builder = state_builder;
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
  StateBuilder<librbd::MockTestImageCtx>** state_builder = nullptr;
  Context *on_finish = nullptr;

  static PrepareRemoteImageRequest* create(Threads<librbd::MockTestImageCtx> *threads,
                                           librados::IoCtx &,
                                           librados::IoCtx &,
                                           const std::string &global_image_id,
                                           const std::string &local_mirror_uuid,
                                           const RemotePoolMeta& remote_pool_meta,
                                           ::journal::CacheManagerHandler *cache_manager_handler,
                                           StateBuilder<librbd::MockTestImageCtx>** state_builder,
                                           Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->state_builder = state_builder;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  PrepareRemoteImageRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

template<>
struct StateBuilder<librbd::MockTestImageCtx> {
  static StateBuilder* s_instance;

  image_sync::MockSyncPointHandler mock_sync_point_handler;
  MockBaseRequest mock_base_request;

  librbd::MockTestImageCtx* local_image_ctx = nullptr;
  librbd::MockTestImageCtx* remote_image_ctx = nullptr;
  std::string local_image_id;
  std::string remote_mirror_uuid;
  std::string remote_image_id;

  static StateBuilder* create(const std::string&) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  image_sync::MockSyncPointHandler* create_sync_point_handler() {
    return &mock_sync_point_handler;
  }

  StateBuilder() {
    s_instance = this;
  }

  MOCK_CONST_METHOD0(is_disconnected, bool());
  MOCK_CONST_METHOD0(is_local_primary, bool());
  MOCK_CONST_METHOD0(is_linked, bool());

  MOCK_CONST_METHOD0(replay_requires_remote_image, bool());
  MOCK_METHOD1(close_remote_image, void(Context*));

  MOCK_METHOD6(create_local_image_request,
               BaseRequest*(Threads<librbd::MockTestImageCtx>*,
                            librados::IoCtx&,
                            const std::string&,
                            PoolMetaCache*,
                            ProgressContext*,
                            Context*));
  MOCK_METHOD5(create_prepare_replay_request,
               BaseRequest*(const std::string&,
                            ProgressContext*,
                            bool*, bool*, Context*));

  void destroy_sync_point_handler() {
  }
  void destroy() {
  }
};

OpenImageRequest<librbd::MockTestImageCtx>*
  OpenImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
OpenLocalImageRequest<librbd::MockTestImageCtx>*
  OpenLocalImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
PrepareLocalImageRequest<librbd::MockTestImageCtx>*
  PrepareLocalImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
PrepareRemoteImageRequest<librbd::MockTestImageCtx>*
  PrepareRemoteImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
StateBuilder<librbd::MockTestImageCtx>*
  StateBuilder<librbd::MockTestImageCtx>::s_instance = nullptr;

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
using ::testing::WithArgs;

MATCHER_P(IsSameIoCtx, io_ctx, "") {
  return &get_mock_io_ctx(arg) == &get_mock_io_ctx(*io_ctx);
}

class TestMockImageReplayerBootstrapRequest : public TestMockFixture {
public:
  typedef Threads<librbd::MockTestImageCtx> MockThreads;
  typedef BootstrapRequest<librbd::MockTestImageCtx> MockBootstrapRequest;
  typedef ImageSync<librbd::MockTestImageCtx> MockImageSync;
  typedef InstanceWatcher<librbd::MockTestImageCtx> MockInstanceWatcher;
  typedef OpenImageRequest<librbd::MockTestImageCtx> MockOpenImageRequest;
  typedef OpenLocalImageRequest<librbd::MockTestImageCtx> MockOpenLocalImageRequest;
  typedef PrepareLocalImageRequest<librbd::MockTestImageCtx> MockPrepareLocalImageRequest;
  typedef PrepareRemoteImageRequest<librbd::MockTestImageCtx> MockPrepareRemoteImageRequest;
  typedef StateBuilder<librbd::MockTestImageCtx> MockStateBuilder;
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
                   MockStateBuilder& mock_state_builder,
                   const std::string& local_image_id,
                   const std::string& local_image_name, int r) {
    EXPECT_CALL(mock_request, send())
      .WillOnce(Invoke([&mock_request, &mock_state_builder, local_image_id,
                        local_image_name, r]() {
          if (r == 0) {
            *mock_request.state_builder = &mock_state_builder;
            mock_state_builder.local_image_id = local_image_id;
            *mock_request.local_image_name = local_image_name;
          }
          mock_request.on_finish->complete(r);
        }));
  }

  void expect_send(MockPrepareRemoteImageRequest& mock_request,
                   MockStateBuilder& mock_state_builder,
                   const std::string& remote_mirror_uuid,
                   const std::string& remote_image_id,
                   int r) {
    EXPECT_CALL(mock_request, send())
      .WillOnce(Invoke([&mock_request, &mock_state_builder, remote_mirror_uuid,
                        remote_image_id, r]() {
                  if (r >= 0) {
                    *mock_request.state_builder = &mock_state_builder;
                    mock_state_builder.remote_image_id = remote_image_id;
                  }

                  mock_state_builder.remote_mirror_uuid = remote_mirror_uuid;
                  mock_request.on_finish->complete(r);
                }));
  }

  void expect_is_local_primary(MockStateBuilder& mock_state_builder,
                               bool is_primary) {
    EXPECT_CALL(mock_state_builder, is_local_primary())
      .WillOnce(Return(is_primary));
  }

  void expect_is_disconnected(MockStateBuilder& mock_state_builder,
                              bool is_disconnected) {
    EXPECT_CALL(mock_state_builder, is_disconnected())
      .WillOnce(Return(is_disconnected));
  }

  void expect_replay_requires_remote_image(MockStateBuilder& mock_state_builder,
                                           bool requires_image) {
    EXPECT_CALL(mock_state_builder, replay_requires_remote_image())
      .WillOnce(Return(requires_image));
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

  void expect_close_remote_image(
      MockStateBuilder& mock_state_builder, int r) {
    EXPECT_CALL(mock_state_builder, close_remote_image(_))
      .WillOnce(Invoke([&mock_state_builder, r]
                       (Context* on_finish) {
          mock_state_builder.remote_image_ctx = nullptr;
          on_finish->complete(r);
        }));
  }

  void expect_create_local_image(MockStateBuilder& mock_state_builder,
                                 const std::string& local_image_id, int r) {
    EXPECT_CALL(mock_state_builder,
                create_local_image_request(_, _, _, _, _, _))
      .WillOnce(WithArg<5>(
        Invoke([&mock_state_builder, local_image_id, r](Context* ctx) {
          if (r >= 0) {
            mock_state_builder.local_image_id = local_image_id;
          }
          mock_state_builder.mock_base_request.on_finish = ctx;
          return &mock_state_builder.mock_base_request;
        })));
    EXPECT_CALL(mock_state_builder.mock_base_request, send())
      .WillOnce(Invoke([this, &mock_state_builder, r]() {
          m_threads->work_queue->queue(
            mock_state_builder.mock_base_request.on_finish, r);
        }));
  }

  void expect_prepare_replay(MockStateBuilder& mock_state_builder,
                             bool resync_requested, bool syncing, int r) {
    EXPECT_CALL(mock_state_builder,
                create_prepare_replay_request(_, _, _, _, _))
      .WillOnce(WithArgs<2, 3, 4>(
        Invoke([&mock_state_builder, resync_requested, syncing, r]
               (bool* resync, bool* sync, Context* ctx) {
          if (r >= 0) {
            *resync = resync_requested;
            *sync = syncing;
          }
          mock_state_builder.mock_base_request.on_finish = ctx;
          return &mock_state_builder.mock_base_request;
        })));
    EXPECT_CALL(mock_state_builder.mock_base_request, send())
      .WillOnce(Invoke([this, &mock_state_builder, r]() {
          m_threads->work_queue->queue(
            mock_state_builder.mock_base_request.on_finish, r);
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
                                       const std::string &global_image_id,
                                       const std::string &local_mirror_uuid,
                                       Context *on_finish) {
    return new MockBootstrapRequest(mock_threads,
                                    m_local_io_ctx,
                                    m_remote_io_ctx,
                                    mock_instance_watcher,
                                    global_image_id,
                                    local_mirror_uuid,
                                    {"remote mirror uuid",
                                     "remote mirror peer uuid"},
                                    nullptr, nullptr, nullptr,
                                    &m_mock_state_builder,
                                    &m_do_resync, on_finish);
  }

  librbd::ImageCtx *m_remote_image_ctx;
  librbd::ImageCtx *m_local_image_ctx = nullptr;

  MockStateBuilder* m_mock_state_builder = nullptr;
  bool m_do_resync = false;
};

TEST_F(TestMockImageReplayerBootstrapRequest, Success) {
  InSequence seq;

  // prepare local image
  MockStateBuilder mock_state_builder;
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  expect_send(mock_prepare_local_image_request, mock_state_builder,
              m_local_image_ctx->id, m_local_image_ctx->name, 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  expect_send(mock_prepare_remote_image_request, mock_state_builder,
              "remote mirror uuid", m_remote_image_ctx->id, 0);
  expect_is_local_primary(mock_state_builder, false);

  // open the remote image
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx, 0);

  // prepare replay
  expect_prepare_replay(mock_state_builder, false, false, 0);
  expect_is_disconnected(mock_state_builder, false);

  // close remote image
  expect_replay_requires_remote_image(mock_state_builder, false);
  expect_close_remote_image(mock_state_builder, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, "global image id",
    "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageReplayerBootstrapRequest, OpenLocalImageError) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockStateBuilder mock_state_builder;
  expect_send(mock_prepare_local_image_request, mock_state_builder,
              m_local_image_ctx->id, m_local_image_ctx->name, 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  expect_send(mock_prepare_remote_image_request, mock_state_builder,
              "remote mirror uuid", m_remote_image_ctx->id, 0);
  expect_is_local_primary(mock_state_builder, false);

  // open the remote image
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx,
                          -EINVAL);

  // close remote image
  expect_replay_requires_remote_image(mock_state_builder, false);
  expect_close_remote_image(mock_state_builder, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, "global image id",
    "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerBootstrapRequest, OpenLocalImageDNE) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockStateBuilder mock_state_builder;
  expect_send(mock_prepare_local_image_request, mock_state_builder,
              m_local_image_ctx->id, m_local_image_ctx->name, 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  expect_send(mock_prepare_remote_image_request, mock_state_builder,
              "remote mirror uuid", m_remote_image_ctx->id, 0);
  expect_is_local_primary(mock_state_builder, false);

  // open the remote image
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx,
                          -ENOENT);

  // create local image
  expect_create_local_image(mock_state_builder, "local image id", 0);

  // re-open the local image
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          "local image id", &mock_local_image_ctx, 0);

  // prepare replay
  expect_prepare_replay(mock_state_builder, false, false, 0);
  expect_is_disconnected(mock_state_builder, false);

  // close remote image
  expect_replay_requires_remote_image(mock_state_builder, false);
  expect_close_remote_image(mock_state_builder, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, "global image id",
    "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageReplayerBootstrapRequest, OpenLocalImagePrimary) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockStateBuilder mock_state_builder;
  expect_send(mock_prepare_local_image_request, mock_state_builder,
              m_local_image_ctx->id, m_local_image_ctx->name, 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  expect_send(mock_prepare_remote_image_request, mock_state_builder,
              "remote mirror uuid", m_remote_image_ctx->id, 0);
  expect_is_local_primary(mock_state_builder, false);

  // open the remote image
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx,
                          -EREMOTEIO);

  // close remote image
  expect_replay_requires_remote_image(mock_state_builder, false);
  expect_close_remote_image(mock_state_builder, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, "global image id",
    "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(-EREMOTEIO, ctx.wait());
}

TEST_F(TestMockImageReplayerBootstrapRequest, CreateLocalImageError) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockStateBuilder mock_state_builder;
  expect_send(mock_prepare_local_image_request, mock_state_builder, "", "",
              -ENOENT);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  expect_send(mock_prepare_remote_image_request, mock_state_builder,
              "remote mirror uuid", m_remote_image_ctx->id, 0);
  expect_is_local_primary(mock_state_builder, false);

  // open the remote image
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // create local image
  expect_create_local_image(mock_state_builder, "local image id", -EINVAL);

  // close remote image
  expect_replay_requires_remote_image(mock_state_builder, false);
  expect_close_remote_image(mock_state_builder, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, "global image id",
    "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerBootstrapRequest, PrepareReplayError) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockStateBuilder mock_state_builder;
  expect_send(mock_prepare_local_image_request, mock_state_builder,
              m_local_image_ctx->id, m_local_image_ctx->name, 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  expect_send(mock_prepare_remote_image_request, mock_state_builder,
              "remote mirror uuid", m_remote_image_ctx->id, 0);
  expect_is_local_primary(mock_state_builder, false);

  // open the remote image
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx, 0);

  // prepare replay
  expect_prepare_replay(mock_state_builder, false, false, -EINVAL);

  // close remote image
  expect_replay_requires_remote_image(mock_state_builder, false);
  expect_close_remote_image(mock_state_builder, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, "global image id",
    "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerBootstrapRequest, PrepareReplayResyncRequested) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockStateBuilder mock_state_builder;
  expect_send(mock_prepare_local_image_request, mock_state_builder,
              m_local_image_ctx->id, m_local_image_ctx->name, 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  expect_send(mock_prepare_remote_image_request, mock_state_builder,
              "remote mirror uuid", m_remote_image_ctx->id, 0);
  expect_is_local_primary(mock_state_builder, false);

  // open the remote image
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx, 0);

  // prepare replay
  expect_prepare_replay(mock_state_builder, true, false, 0);

  // close remote image
  expect_replay_requires_remote_image(mock_state_builder, false);
  expect_close_remote_image(mock_state_builder, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, "global image id",
    "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_TRUE(m_do_resync);
}

TEST_F(TestMockImageReplayerBootstrapRequest, PrepareReplaySyncing) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockStateBuilder mock_state_builder;
  expect_send(mock_prepare_local_image_request, mock_state_builder,
              m_local_image_ctx->id, m_local_image_ctx->name, 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  expect_send(mock_prepare_remote_image_request, mock_state_builder,
              "remote mirror uuid", m_remote_image_ctx->id, 0);
  expect_is_local_primary(mock_state_builder, false);

  // open the remote image
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx, 0);

  // prepare replay
  expect_prepare_replay(mock_state_builder, false, true, 0);
  expect_is_disconnected(mock_state_builder, false);

  // image sync
  MockImageSync mock_image_sync;
  expect_image_sync(mock_image_sync, 0);

  // close remote image
  expect_replay_requires_remote_image(mock_state_builder, false);
  expect_close_remote_image(mock_state_builder, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, "global image id",
    "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageReplayerBootstrapRequest, PrepareReplayDisconnected) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockStateBuilder mock_state_builder;
  expect_send(mock_prepare_local_image_request, mock_state_builder,
              m_local_image_ctx->id, m_local_image_ctx->name, 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  expect_send(mock_prepare_remote_image_request, mock_state_builder,
              "remote mirror uuid", m_remote_image_ctx->id, 0);
  expect_is_local_primary(mock_state_builder, false);

  // open the remote image
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx, 0);

  // prepare replay
  expect_prepare_replay(mock_state_builder, false, false, 0);
  expect_is_disconnected(mock_state_builder, false);

  // close remote image
  expect_replay_requires_remote_image(mock_state_builder, false);
  expect_close_remote_image(mock_state_builder, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, "global image id",
    "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageReplayerBootstrapRequest, ImageSyncError) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockStateBuilder mock_state_builder;
  expect_send(mock_prepare_local_image_request, mock_state_builder,
              m_local_image_ctx->id, m_local_image_ctx->name, 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  expect_send(mock_prepare_remote_image_request, mock_state_builder,
              "remote mirror uuid", m_remote_image_ctx->id, 0);
  expect_is_local_primary(mock_state_builder, false);

  // open the remote image
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx, 0);

  // prepare replay
  expect_prepare_replay(mock_state_builder, false, true, 0);
  expect_is_disconnected(mock_state_builder, false);

  // image sync
  MockImageSync mock_image_sync;
  expect_image_sync(mock_image_sync, -EINVAL);

  // close remote image
  expect_replay_requires_remote_image(mock_state_builder, false);
  expect_close_remote_image(mock_state_builder, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, "global image id",
    "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerBootstrapRequest, ImageSyncCanceled) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockStateBuilder mock_state_builder;
  expect_send(mock_prepare_local_image_request, mock_state_builder,
              m_local_image_ctx->id, m_local_image_ctx->name, 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  expect_send(mock_prepare_remote_image_request, mock_state_builder,
              "remote mirror uuid", m_remote_image_ctx->id, 0);
  expect_is_local_primary(mock_state_builder, false);

  // open the remote image
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx, 0);

  // prepare replay
  expect_prepare_replay(mock_state_builder, false, true, 0);
  expect_is_disconnected(mock_state_builder, false);

  // close remote image
  expect_replay_requires_remote_image(mock_state_builder, false);
  expect_close_remote_image(mock_state_builder, 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, "global image id",
    "local mirror uuid", &ctx);
  request->cancel();
  request->send();
  ASSERT_EQ(-ECANCELED, ctx.wait());
}

TEST_F(TestMockImageReplayerBootstrapRequest, CloseRemoteImageError) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockStateBuilder mock_state_builder;
  expect_send(mock_prepare_local_image_request, mock_state_builder,
              m_local_image_ctx->id, m_local_image_ctx->name, 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  expect_send(mock_prepare_remote_image_request, mock_state_builder,
              "remote mirror uuid", m_remote_image_ctx->id, 0);
  expect_is_local_primary(mock_state_builder, false);

  // open the remote image
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx, 0);

  // prepare replay
  expect_prepare_replay(mock_state_builder, false, false, 0);
  expect_is_disconnected(mock_state_builder, false);

  // attempt to close remote image
  expect_replay_requires_remote_image(mock_state_builder, false);
  expect_close_remote_image(mock_state_builder, -EINVAL);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, "global image id",
    "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageReplayerBootstrapRequest, ReplayRequiresRemoteImage) {
  InSequence seq;

  // prepare local image
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockStateBuilder mock_state_builder;
  expect_send(mock_prepare_local_image_request, mock_state_builder,
              m_local_image_ctx->id, m_local_image_ctx->name, 0);

  // prepare remote image
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  expect_send(mock_prepare_remote_image_request, mock_state_builder,
              "remote mirror uuid", m_remote_image_ctx->id, 0);
  expect_is_local_primary(mock_state_builder, false);

  // open the remote image
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockOpenImageRequest mock_open_image_request;
  expect_open_image(mock_open_image_request, m_remote_io_ctx,
                    mock_remote_image_ctx.id, mock_remote_image_ctx, 0);

  // open the local image
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  MockOpenLocalImageRequest mock_open_local_image_request;
  expect_open_local_image(mock_open_local_image_request, m_local_io_ctx,
                          mock_local_image_ctx.id, &mock_local_image_ctx, 0);

  // prepare replay
  expect_prepare_replay(mock_state_builder, false, false, 0);
  expect_is_disconnected(mock_state_builder, false);

  // remote image is left open
  expect_replay_requires_remote_image(mock_state_builder, true);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockInstanceWatcher mock_instance_watcher;
  MockBootstrapRequest *request = create_request(
    &mock_threads, &mock_instance_watcher, "global image id",
    "local mirror uuid", &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd
