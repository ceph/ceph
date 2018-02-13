// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/journal/TypeTraits.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_replayer/GetMirrorImageIdRequest.h"
#include "tools/rbd_mirror/image_replayer/PrepareRemoteImageRequest.h"
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
} // namespace librbd

namespace rbd {
namespace mirror {

template <>
struct Threads<librbd::MockTestImageCtx> {
  Mutex &timer_lock;
  SafeTimer *timer;
  ContextWQ *work_queue;

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
    assert(s_instance != nullptr);
    s_instance->image_id = image_id;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  GetMirrorImageIdRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

GetMirrorImageIdRequest<librbd::MockTestImageCtx>* GetMirrorImageIdRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

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

  void expect_get_mirror_image_id(MockGetMirrorImageIdRequest& mock_get_mirror_image_id_request,
                                  const std::string& image_id, int r) {
    EXPECT_CALL(mock_get_mirror_image_id_request, send())
      .WillOnce(Invoke([&mock_get_mirror_image_id_request, image_id, r]() {
                  *mock_get_mirror_image_id_request.image_id = image_id;
                  mock_get_mirror_image_id_request.on_finish->complete(r);
                }));
  }

  void expect_mirror_uuid_get(librados::IoCtx &io_ctx,
                              const std::string &mirror_uuid, int r) {
    bufferlist bl;
    encode(mirror_uuid, bl);

    EXPECT_CALL(get_mock_io_ctx(io_ctx),
                exec(RBD_MIRRORING, _, StrEq("rbd"), StrEq("mirror_uuid_get"), _, _, _))
      .WillOnce(DoAll(WithArg<5>(Invoke([bl](bufferlist *out_bl) {
                                          *out_bl = bl;
                                        })),
                      Return(r)));
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

TEST_F(TestMockImageReplayerPrepareRemoteImageRequest, Success) {
  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);

  InSequence seq;
  expect_mirror_uuid_get(m_remote_io_ctx, "remote mirror uuid", 0);
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request,
                             "remote image id", 0);

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

  std::string remote_mirror_uuid;
  std::string remote_image_id;
  journal::MockJournalerProxy *remote_journaler = nullptr;
  cls::journal::ClientState client_state;
  librbd::journal::MirrorPeerClientMeta client_meta;
  C_SaferCond ctx;
  auto req = MockPrepareRemoteImageRequest::create(&mock_threads,
                                                   m_remote_io_ctx,
                                                   "global image id",
                                                   "local mirror uuid",
                                                   "local image id",
                                                   &remote_mirror_uuid,
                                                   &remote_image_id,
                                                   &remote_journaler,
                                                   &client_state, &client_meta,
                                                   &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(std::string("remote mirror uuid"), remote_mirror_uuid);
  ASSERT_EQ(std::string("remote image id"), remote_image_id);
  ASSERT_TRUE(remote_journaler != nullptr);
  ASSERT_EQ(cls::journal::CLIENT_STATE_DISCONNECTED, client_state);
  delete remote_journaler;
}

TEST_F(TestMockImageReplayerPrepareRemoteImageRequest, SuccessNotRegistered) {
  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);

  InSequence seq;
  expect_mirror_uuid_get(m_remote_io_ctx, "remote mirror uuid", 0);
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request,
                             "remote image id", 0);

  EXPECT_CALL(mock_remote_journaler, construct());

  cls::journal::Client client;
  expect_journaler_get_client(mock_remote_journaler, "local mirror uuid",
                              client, -ENOENT);

  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta;
  mirror_peer_client_meta.image_id = "local image id";
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;
  librbd::journal::ClientData client_data{mirror_peer_client_meta};
  expect_journaler_register_client(mock_remote_journaler, client_data, 0);

  std::string remote_mirror_uuid;
  std::string remote_image_id;
  journal::MockJournalerProxy *remote_journaler = nullptr;
  cls::journal::ClientState client_state;
  librbd::journal::MirrorPeerClientMeta client_meta;
  C_SaferCond ctx;
  auto req = MockPrepareRemoteImageRequest::create(&mock_threads,
                                                   m_remote_io_ctx,
                                                   "global image id",
                                                   "local mirror uuid",
                                                   "local image id",
                                                   &remote_mirror_uuid,
                                                   &remote_image_id,
                                                   &remote_journaler,
                                                   &client_state, &client_meta,
                                                   &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(std::string("remote mirror uuid"), remote_mirror_uuid);
  ASSERT_EQ(std::string("remote image id"), remote_image_id);
  ASSERT_TRUE(remote_journaler != nullptr);
  ASSERT_EQ(cls::journal::CLIENT_STATE_CONNECTED, client_state);
  delete remote_journaler;
}

TEST_F(TestMockImageReplayerPrepareRemoteImageRequest, MirrorUuidError) {
  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);

  InSequence seq;
  expect_mirror_uuid_get(m_remote_io_ctx, "", -EINVAL);

  std::string remote_mirror_uuid;
  std::string remote_image_id;
  journal::MockJournalerProxy *remote_journaler = nullptr;
  cls::journal::ClientState client_state;
  librbd::journal::MirrorPeerClientMeta client_meta;
  C_SaferCond ctx;
  auto req = MockPrepareRemoteImageRequest::create(&mock_threads,
                                                   m_remote_io_ctx,
                                                   "global image id",
                                                   "local mirror uuid",
                                                   "",
                                                   &remote_mirror_uuid,
                                                   &remote_image_id,
                                                   &remote_journaler,
                                                   &client_state, &client_meta,
                                                   &ctx);
  req->send();

  ASSERT_EQ(-EINVAL, ctx.wait());
  ASSERT_EQ(std::string(""), remote_mirror_uuid);
  ASSERT_TRUE(remote_journaler == nullptr);
}

TEST_F(TestMockImageReplayerPrepareRemoteImageRequest, MirrorImageIdError) {
  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);

  InSequence seq;
  expect_mirror_uuid_get(m_remote_io_ctx, "remote mirror uuid", 0);
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request, "", -EINVAL);

  std::string remote_mirror_uuid;
  std::string remote_image_id;
  journal::MockJournalerProxy *remote_journaler = nullptr;
  cls::journal::ClientState client_state;
  librbd::journal::MirrorPeerClientMeta client_meta;
  C_SaferCond ctx;
  auto req = MockPrepareRemoteImageRequest::create(&mock_threads,
                                                   m_remote_io_ctx,
                                                   "global image id",
                                                   "local mirror uuid",
                                                   "",
                                                   &remote_mirror_uuid,
                                                   &remote_image_id,
                                                   &remote_journaler,
                                                   &client_state, &client_meta,
                                                   &ctx);
  req->send();

  ASSERT_EQ(-EINVAL, ctx.wait());
  ASSERT_EQ(std::string("remote mirror uuid"), remote_mirror_uuid);
  ASSERT_TRUE(remote_journaler == nullptr);
}

TEST_F(TestMockImageReplayerPrepareRemoteImageRequest, GetClientError) {
  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);

  InSequence seq;
  expect_mirror_uuid_get(m_remote_io_ctx, "remote mirror uuid", 0);
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request,
                             "remote image id", 0);

  EXPECT_CALL(mock_remote_journaler, construct());

  cls::journal::Client client;
  expect_journaler_get_client(mock_remote_journaler, "local mirror uuid",
                              client, -EINVAL);

  std::string remote_mirror_uuid;
  std::string remote_image_id;
  journal::MockJournalerProxy *remote_journaler = nullptr;
  cls::journal::ClientState client_state;
  librbd::journal::MirrorPeerClientMeta client_meta;
  C_SaferCond ctx;
  auto req = MockPrepareRemoteImageRequest::create(&mock_threads,
                                                   m_remote_io_ctx,
                                                   "global image id",
                                                   "local mirror uuid",
                                                   "local image id",
                                                   &remote_mirror_uuid,
                                                   &remote_image_id,
                                                   &remote_journaler,
                                                   &client_state, &client_meta,
                                                   &ctx);
  req->send();

  ASSERT_EQ(-EINVAL, ctx.wait());
  ASSERT_EQ(std::string("remote mirror uuid"), remote_mirror_uuid);
  ASSERT_EQ(std::string("remote image id"), remote_image_id);
  ASSERT_TRUE(remote_journaler == nullptr);
}

TEST_F(TestMockImageReplayerPrepareRemoteImageRequest, RegisterClientError) {
  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);

  InSequence seq;
  expect_mirror_uuid_get(m_remote_io_ctx, "remote mirror uuid", 0);
  MockGetMirrorImageIdRequest mock_get_mirror_image_id_request;
  expect_get_mirror_image_id(mock_get_mirror_image_id_request,
                             "remote image id", 0);

  EXPECT_CALL(mock_remote_journaler, construct());

  cls::journal::Client client;
  expect_journaler_get_client(mock_remote_journaler, "local mirror uuid",
                              client, -ENOENT);

  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta;
  mirror_peer_client_meta.image_id = "local image id";
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_REPLAYING;
  librbd::journal::ClientData client_data{mirror_peer_client_meta};
  expect_journaler_register_client(mock_remote_journaler, client_data, -EINVAL);

  std::string remote_mirror_uuid;
  std::string remote_image_id;
  journal::MockJournalerProxy *remote_journaler = nullptr;
  cls::journal::ClientState client_state;
  librbd::journal::MirrorPeerClientMeta client_meta;
  C_SaferCond ctx;
  auto req = MockPrepareRemoteImageRequest::create(&mock_threads,
                                                   m_remote_io_ctx,
                                                   "global image id",
                                                   "local mirror uuid",
                                                   "local image id",
                                                   &remote_mirror_uuid,
                                                   &remote_image_id,
                                                   &remote_journaler,
                                                   &client_state, &client_meta,
                                                   &ctx);
  req->send();

  ASSERT_EQ(-EINVAL, ctx.wait());
  ASSERT_EQ(std::string("remote mirror uuid"), remote_mirror_uuid);
  ASSERT_EQ(std::string("remote image id"), remote_image_id);
  ASSERT_TRUE(remote_journaler == nullptr);
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd
