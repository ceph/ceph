// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "librbd/internal.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "tools/rbd_mirror/PoolMetaCache.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_replayer/CreateImageRequest.h"
#include "tools/rbd_mirror/image_replayer/snapshot/CreateLocalImageRequest.h"
#include "tools/rbd_mirror/image_replayer/snapshot/StateBuilder.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
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
  ceph::mutex &timer_lock;
  SafeTimer *timer;
  librbd::asio::ContextWQ *work_queue;

  Threads(Threads<librbd::ImageCtx> *threads)
    : timer_lock(threads->timer_lock), timer(threads->timer),
      work_queue(threads->work_queue) {
  }
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
                                    PoolMetaCache* pool_meta_cache,
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

namespace snapshot {

template<>
struct StateBuilder<librbd::MockTestImageCtx> {
  std::string local_image_id;
  std::string remote_mirror_uuid;
};

} // namespace snapshot
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

#include "tools/rbd_mirror/image_replayer/snapshot/CreateLocalImageRequest.cc"

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace snapshot {

class TestMockImageReplayerSnapshotCreateLocalImageRequest : public TestMockFixture {
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

  void snap_create(librbd::ImageCtx *image_ctx, const std::string &snap_name) {
    librbd::NoOpProgressContext prog_ctx;
    ASSERT_EQ(0, image_ctx->operations->snap_create(cls::rbd::UserSnapshotNamespace(),
					            snap_name, 0, prog_ctx));
    ASSERT_EQ(0, image_ctx->operations->snap_protect(cls::rbd::UserSnapshotNamespace(),
						     snap_name));
    ASSERT_EQ(0, image_ctx->state->refresh());
  }

  int clone_image(librbd::ImageCtx *parent_image_ctx,
                  const std::string &snap_name, const std::string &clone_name) {
    snap_create(parent_image_ctx, snap_name);

    int order = 0;
    return librbd::clone(m_remote_io_ctx, parent_image_ctx->name.c_str(),
                         snap_name.c_str(), m_remote_io_ctx,
                         clone_name.c_str(), parent_image_ctx->features,
                         &order, 0, 0);
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

  void expect_mirror_image_remove(const std::string& image_id, int r) {
    bufferlist bl;
    encode(image_id, bl);

    EXPECT_CALL(get_mock_io_ctx(m_local_io_ctx),
                exec(StrEq("rbd_mirroring"), _, StrEq("rbd"),
                     StrEq("mirror_image_remove"),
                     ContentsEqual(bl), _, _, _))
      .WillOnce(Return(r));
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
      global_image_id, &m_pool_meta_cache, nullptr, &mock_state_builder,
      on_finish);
  }

  PoolMetaCache m_pool_meta_cache{g_ceph_context};

  librbd::ImageCtx *m_remote_image_ctx;
  librbd::MockTestImageCtx *m_mock_remote_image_ctx = nullptr;
};

TEST_F(TestMockImageReplayerSnapshotCreateLocalImageRequest, Success) {
  InSequence seq;

  librbd::util::s_image_id = "local image id";
  expect_mirror_image_set("local image id",
                          {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
                           "global image id",
                           cls::rbd::MIRROR_IMAGE_STATE_CREATING}, 0);

  MockCreateImageRequest mock_create_image_request;
  expect_create_image(mock_create_image_request, "local image id", 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockStateBuilder mock_state_builder;
  auto request = create_request(
    mock_threads, mock_state_builder, "global image id", &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ("local image id", mock_state_builder.local_image_id);
}

TEST_F(TestMockImageReplayerSnapshotCreateLocalImageRequest, AddMirrorImageError) {
  InSequence seq;

  librbd::util::s_image_id = "local image id";
  expect_mirror_image_set("local image id",
                          {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
                           "global image id",
                           cls::rbd::MIRROR_IMAGE_STATE_CREATING}, -EINVAL);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockStateBuilder mock_state_builder;
  auto request = create_request(
    mock_threads, mock_state_builder, "global image id", &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerSnapshotCreateLocalImageRequest, CreateImageError) {
  InSequence seq;

  librbd::util::s_image_id = "local image id";
  expect_mirror_image_set("local image id",
                          {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
                           "global image id",
                           cls::rbd::MIRROR_IMAGE_STATE_CREATING}, 0);

  MockCreateImageRequest mock_create_image_request;
  expect_create_image(mock_create_image_request, "local image id", -EINVAL);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockStateBuilder mock_state_builder;
  auto request = create_request(
    mock_threads, mock_state_builder, "global image id", &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerSnapshotCreateLocalImageRequest, CreateImageDuplicate) {
  InSequence seq;

  librbd::util::s_image_id = "local image id";
  expect_mirror_image_set("local image id",
                          {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
                           "global image id",
                           cls::rbd::MIRROR_IMAGE_STATE_CREATING}, 0);

  MockCreateImageRequest mock_create_image_request;
  expect_create_image(mock_create_image_request, "local image id", -EBADF);

  expect_mirror_image_set("local image id",
                          {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
                           "global image id",
                           cls::rbd::MIRROR_IMAGE_STATE_DISABLING}, 0);

  expect_mirror_image_remove("local image id", 0);

  expect_mirror_image_set("local image id",
                          {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
                           "global image id",
                           cls::rbd::MIRROR_IMAGE_STATE_CREATING}, 0);

  expect_create_image(mock_create_image_request, "local image id", 0);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockStateBuilder mock_state_builder;
  auto request = create_request(
    mock_threads, mock_state_builder, "global image id", &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ("local image id", mock_state_builder.local_image_id);
}

TEST_F(TestMockImageReplayerSnapshotCreateLocalImageRequest, DisableMirrorImageError) {
  InSequence seq;

  librbd::util::s_image_id = "local image id";
  expect_mirror_image_set("local image id",
                          {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
                           "global image id",
                           cls::rbd::MIRROR_IMAGE_STATE_DISABLING}, -EINVAL);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockStateBuilder mock_state_builder;
  mock_state_builder.local_image_id = "local image id";
  auto request = create_request(
    mock_threads, mock_state_builder, "global image id", &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageReplayerSnapshotCreateLocalImageRequest, RemoveMirrorImageError) {
  InSequence seq;

  librbd::util::s_image_id = "local image id";
  expect_mirror_image_set("local image id",
                          {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
                           "global image id",
                           cls::rbd::MIRROR_IMAGE_STATE_DISABLING}, 0);

  expect_mirror_image_remove("local image id", -EINVAL);

  C_SaferCond ctx;
  MockThreads mock_threads(m_threads);
  MockStateBuilder mock_state_builder;
  mock_state_builder.local_image_id = "local image id";
  auto request = create_request(
    mock_threads, mock_state_builder, "global image id", &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace journal
} // namespace image_replayer
} // namespace mirror
} // namespace rbd
