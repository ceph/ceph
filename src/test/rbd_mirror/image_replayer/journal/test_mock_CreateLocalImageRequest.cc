// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_replayer/CreateImageRequest.h"
#include "tools/rbd_mirror/image_replayer/journal/CreateLocalImageRequest.h"
#include "tools/rbd_mirror/image_replayer/journal/StateBuilder.h"
#include "test/journal/mock/MockJournaler.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/rbd_mirror/mock/MockContextWQ.h"
#include "test/rbd_mirror/mock/MockSafeTimer.h"
#include <boost/intrusive_ptr.hpp>

namespace librbd {

namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  explicit MockTestImageCtx(librbd::ImageCtx &image_ctx)
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

namespace util {

static std::string s_image_id;

template <>
std::string generate_image_id<MockTestImageCtx>(librados::IoCtx&) {
  ceph_assert(!s_image_id.empty());
  return s_image_id;
}

} // namespace util
} // namespace librbd

namespace rbd {
namespace mirror {

template <>
struct Threads<librbd::MockTestImageCtx> {
};

namespace image_replayer {

template<>
struct CreateImageRequest<librbd::MockTestImageCtx> {
  static CreateImageRequest* s_instance;
  Context *on_finish = nullptr;

  static CreateImageRequest* create(Threads<librbd::MockTestImageCtx>* threads,
                                    librados::IoCtx &local_io_ctx,
                                    const std::string &global_image_id,
                                    const std::string &remote_mirror_uuid,
                                    const std::string &local_image_name,
				    const std::string &local_image_id,
                                    librbd::MockTestImageCtx *remote_image_ctx,
                                    PoolMetaCache<librbd::MockTestImageCtx>* pool_meta_cache,
                                    cls::rbd::MirrorImageMode mirror_image_mode,
                                    Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    s_instance->construct(local_image_id);
    return s_instance;
  }

  CreateImageRequest() {
    ceph_assert(s_instance == nullptr);
    s_instance = this;
  }
  ~CreateImageRequest() {
    s_instance = nullptr;
  }

  MOCK_METHOD1(construct, void(const std::string&));
  MOCK_METHOD0(send, void());
};

CreateImageRequest<librbd::MockTestImageCtx>*
  CreateImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

namespace journal {

template<>
struct StateBuilder<librbd::MockTestImageCtx> {
  std::string local_image_id;

  std::string remote_mirror_uuid;
  ::journal::MockJournalerProxy* remote_journaler = nullptr;
  cls::journal::ClientState remote_client_state;
  librbd::journal::MirrorPeerClientMeta remote_client_meta;
};

} // namespace journal
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

#include "tools/rbd_mirror/image_replayer/journal/CreateLocalImageRequest.cc"

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::WithArg;

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace journal {

class TestMockImageReplayerJournalCreateLocalImageRequest : public TestMockFixture {
public:
  typedef CreateLocalImageRequest<librbd::MockTestImageCtx> MockCreateLocalImageRequest;
  typedef Threads<librbd::MockTestImageCtx> MockThreads;
  typedef CreateImageRequest<librbd::MockTestImageCtx> MockCreateImageRequest;
  typedef StateBuilder<librbd::MockTestImageCtx> MockStateBuilder;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_remote_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_remote_io_ctx, m_image_name, &m_remote_image_ctx));
    m_mock_remote_image_ctx = new librbd::MockTestImageCtx(*m_remote_image_ctx);
  }

  void TearDown() override {
    delete m_mock_remote_image_ctx;
    TestMockFixture::TearDown();
  }

  void expect_journaler_register_client(
      ::journal::MockJournaler& mock_journaler,
      const librbd::journal::ClientData& client_data, int r) {
    bufferlist bl;
    encode(client_data, bl);

    EXPECT_CALL(mock_journaler, register_client(ContentsEqual(bl), _))
      .WillOnce(WithArg<1>(Invoke([this, r](Context *on_finish) {
                                    m_threads->work_queue->queue(on_finish, r);
                                  })));
  }

  void expect_journaler_unregister_client(
      ::journal::MockJournaler& mock_journaler, int r) {
    EXPECT_CALL(mock_journaler, unregister_client(_))
      .WillOnce(Invoke([this, r](Context *on_finish) {
                  m_threads->work_queue->queue(on_finish, r);
                }));
  }

  void expect_journaler_update_client(
      ::journal::MockJournaler& mock_journaler,
      const librbd::journal::ClientData& client_data, int r) {
    bufferlist bl;
    encode(client_data, bl);

    EXPECT_CALL(mock_journaler, update_client(ContentsEqual(bl), _))
      .WillOnce(WithArg<1>(Invoke([this, r](Context *on_finish) {
                                    m_threads->work_queue->queue(on_finish, r);
                                  })));
  }

  void expect_create_image(MockCreateImageRequest& mock_create_image_request,
                           const std::string& image_id, int r) {
    EXPECT_CALL(mock_create_image_request, construct(image_id));
    EXPECT_CALL(mock_create_image_request, send())
      .WillOnce(Invoke([this, &mock_create_image_request, r]() {
          m_threads->work_queue->queue(mock_create_image_request.on_finish, r);
        }));
  }

  MockCreateLocalImageRequest* create_request(
      MockThreads& mock_threads,
      MockStateBuilder& mock_state_builder,
      const std::string& global_image_id,
      Context* on_finish) {
    return new MockCreateLocalImageRequest(
      &mock_threads, m_local_io_ctx, m_mock_remote_image_ctx,
      global_image_id, nullptr, nullptr, &mock_state_builder,
      on_finish);
  }

  librbd::ImageCtx *m_remote_image_ctx;
  librbd::MockTestImageCtx *m_mock_remote_image_ctx = nullptr;
};

TEST_F(TestMockImageReplayerJournalCreateLocalImageRequest, Success) {
  InSequence seq;

  // re-register the client
  ::journal::MockJournaler mock_journaler;
  expect_journaler_unregister_client(mock_journaler, 0);

  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta;
  librbd::util::s_image_id = "local image id";
  mirror_peer_client_meta.image_id = "local image id";
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_SYNCING;
  librbd::journal::ClientData client_data;
  client_data.client_meta = mirror_peer_client_meta;
  expect_journaler_register_client(mock_journaler, client_data, 0);

  // create the missing local image
  MockCreateImageRequest mock_create_image_request;
  expect_create_image(mock_create_image_request, "local image id", 0);

  C_SaferCond ctx;
  MockThreads mock_threads;
  MockStateBuilder mock_state_builder;
  auto request = create_request(
    mock_threads, mock_state_builder, "global image id", &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ("local image id", mock_state_builder.local_image_id);
  ASSERT_EQ("local image id", mock_state_builder.remote_client_meta.image_id);
  ASSERT_EQ(librbd::journal::MIRROR_PEER_STATE_SYNCING,
            mock_state_builder.remote_client_meta.state);
}

TEST_F(TestMockImageReplayerJournalCreateLocalImageRequest, UnregisterError) {
  InSequence seq;

  // re-register the client
  ::journal::MockJournaler mock_journaler;
  expect_journaler_unregister_client(mock_journaler, -EINVAL);

  C_SaferCond ctx;
  MockThreads mock_threads;
  MockStateBuilder mock_state_builder;
  auto request = create_request(
    mock_threads, mock_state_builder, "global image id", &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerJournalCreateLocalImageRequest, RegisterError) {
  InSequence seq;

  // re-register the client
  ::journal::MockJournaler mock_journaler;
  expect_journaler_unregister_client(mock_journaler, 0);

  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta;
  librbd::util::s_image_id = "local image id";
  mirror_peer_client_meta.image_id = "local image id";
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_SYNCING;
  librbd::journal::ClientData client_data;
  client_data.client_meta = mirror_peer_client_meta;
  expect_journaler_register_client(mock_journaler, client_data, -EINVAL);

  C_SaferCond ctx;
  MockThreads mock_threads;
  MockStateBuilder mock_state_builder;
  auto request = create_request(
    mock_threads, mock_state_builder, "global image id", &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerJournalCreateLocalImageRequest, CreateImageError) {
  InSequence seq;

  // re-register the client
  ::journal::MockJournaler mock_journaler;
  expect_journaler_unregister_client(mock_journaler, 0);

  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta;
  librbd::util::s_image_id = "local image id";
  mirror_peer_client_meta.image_id = "local image id";
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_SYNCING;
  librbd::journal::ClientData client_data;
  client_data.client_meta = mirror_peer_client_meta;
  expect_journaler_register_client(mock_journaler, client_data, 0);

  // create the missing local image
  MockCreateImageRequest mock_create_image_request;
  expect_create_image(mock_create_image_request, "local image id", -EINVAL);

  C_SaferCond ctx;
  MockThreads mock_threads;
  MockStateBuilder mock_state_builder;
  auto request = create_request(
    mock_threads, mock_state_builder, "global image id", &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerJournalCreateLocalImageRequest, CreateImageDuplicate) {
  InSequence seq;

  // re-register the client
  ::journal::MockJournaler mock_journaler;
  expect_journaler_unregister_client(mock_journaler, 0);

  librbd::journal::MirrorPeerClientMeta mirror_peer_client_meta;
  librbd::util::s_image_id = "local image id";
  mirror_peer_client_meta.image_id = "local image id";
  mirror_peer_client_meta.state = librbd::journal::MIRROR_PEER_STATE_SYNCING;
  librbd::journal::ClientData client_data;
  client_data.client_meta = mirror_peer_client_meta;
  expect_journaler_register_client(mock_journaler, client_data, 0);

  // create the missing local image
  MockCreateImageRequest mock_create_image_request;
  expect_create_image(mock_create_image_request, "local image id", -EBADF);

  // re-register the client
  expect_journaler_unregister_client(mock_journaler, 0);
  expect_journaler_register_client(mock_journaler, client_data, 0);

  // re-create the local image
  expect_create_image(mock_create_image_request, "local image id", 0);

  C_SaferCond ctx;
  MockThreads mock_threads;
  MockStateBuilder mock_state_builder;
  auto request = create_request(
    mock_threads, mock_state_builder, "global image id", &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

} // namespace journal
} // namespace image_replayer
} // namespace mirror
} // namespace rbd
