// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/rbd_mirror/mock/MockJournaler.h"
#include "tools/rbd_mirror/image_sync/ImageCopyRequest.h"
#include "tools/rbd_mirror/image_sync/ObjectCopyRequest.h"
#include "tools/rbd_mirror/Threads.h"
#include <boost/scope_exit.hpp>

namespace rbd {
namespace mirror {
namespace image_sync {

template <>
struct ObjectCopyRequest<librbd::MockImageCtx> {
  static ObjectCopyRequest* s_instance;
  static ObjectCopyRequest* create(librbd::MockImageCtx *local_image_ctx,
                                   librbd::MockImageCtx *remote_image_ctx,
                                   const ImageCopyRequest<librbd::MockImageCtx>::SnapMap *snap_map,
                                   uint64_t object_number, Context *on_finish) {
    assert(s_instance != nullptr);
    Mutex::Locker locker(s_instance->lock);
    s_instance->snap_map = snap_map;
    s_instance->object_contexts[object_number] = on_finish;
    s_instance->cond.Signal();
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  Mutex lock;
  Cond cond;

  const ImageCopyRequest<librbd::MockImageCtx>::SnapMap *snap_map;
  std::map<uint64_t, Context *> object_contexts;

  ObjectCopyRequest() : lock("lock") {
    s_instance = this;
  }
};

ObjectCopyRequest<librbd::MockImageCtx>* ObjectCopyRequest<librbd::MockImageCtx>::s_instance = nullptr;

} // namespace image_sync
} // namespace mirror
} // namespace rbd

// template definitions
#include "tools/rbd_mirror/image_sync/ImageCopyRequest.cc"
template class rbd::mirror::image_sync::ImageCopyRequest<librbd::MockImageCtx>;

namespace rbd {
namespace mirror {
namespace image_sync {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArg;

class TestMockImageSyncImageCopyRequest : public TestMockFixture {
public:
  typedef ImageCopyRequest<librbd::MockImageCtx> MockImageCopyRequest;
  typedef ObjectCopyRequest<librbd::MockImageCtx> MockObjectCopyRequest;

  virtual void SetUp() {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_remote_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_remote_io_ctx, m_image_name, &m_remote_image_ctx));

    ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &m_local_image_ctx));

    m_threads = new rbd::mirror::Threads(reinterpret_cast<CephContext*>(
      m_local_io_ctx.cct()));
  }

  virtual void TearDown() {
    delete m_threads;
    TestMockFixture::TearDown();
  }

  void expect_get_snap_id(librbd::MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, get_snap_id(_))
      .WillRepeatedly(Invoke([&mock_image_ctx](std::string snap_name) {
        RWLock::RLocker snap_locker(mock_image_ctx.image_ctx->snap_lock);
        return mock_image_ctx.image_ctx->get_snap_id(snap_name);
      }));
  }

  void expect_get_object_count(librbd::MockImageCtx &mock_image_ctx,
                               uint64_t count) {
    EXPECT_CALL(mock_image_ctx, get_object_count(_))
      .WillOnce(Return(count)).RetiresOnSaturation();
  }

  void expect_update_client(journal::MockJournaler &mock_journaler, int r) {
    EXPECT_CALL(mock_journaler, update_client(_, _))
      .WillOnce(WithArg<1>(CompleteContext(r)));
  }

  void expect_object_copy_send(MockObjectCopyRequest &mock_object_copy_request) {
    EXPECT_CALL(mock_object_copy_request, send());
  }

  bool complete_object_copy(MockObjectCopyRequest &mock_object_copy_request,
                            uint64_t object_num, int r) {
    Mutex::Locker locker(mock_object_copy_request.lock);
    while (mock_object_copy_request.object_contexts.count(object_num) == 0) {
      if (mock_object_copy_request.cond.WaitInterval(m_local_image_ctx->cct,
                                                     mock_object_copy_request.lock,
                                                     utime_t(10, 0)) != 0) {
        return false;
      }
    }

    m_threads->work_queue->queue(mock_object_copy_request.object_contexts[object_num], r);
    return true;
  }

  MockImageCopyRequest::SnapMap wait_for_snap_map(MockObjectCopyRequest &mock_object_copy_request) {
    Mutex::Locker locker(mock_object_copy_request.lock);
    while (mock_object_copy_request.snap_map == nullptr) {
      if (mock_object_copy_request.cond.WaitInterval(m_local_image_ctx->cct,
                                                     mock_object_copy_request.lock,
                                                     utime_t(10, 0)) != 0) {
        return MockImageCopyRequest::SnapMap();
      }
    }
    return *mock_object_copy_request.snap_map;
  }

  MockImageCopyRequest *create_request(librbd::MockImageCtx &mock_remote_image_ctx,
                                       librbd::MockImageCtx &mock_local_image_ctx,
                                       journal::MockJournaler &mock_journaler,
                                       librbd::journal::MirrorPeerSyncPoint &sync_point,
                                       Context *ctx) {
    return new MockImageCopyRequest(&mock_local_image_ctx,
                                    &mock_remote_image_ctx,
                                    m_threads->timer, &m_threads->timer_lock,
                                    &mock_journaler, &m_client_meta,
                                    &sync_point, ctx);
  }

  int create_snap(librbd::ImageCtx *image_ctx, const char* snap_name,
                  librados::snap_t *snap_id) {
    int r = image_ctx->operations->snap_create(snap_name);
    if (r < 0) {
      return r;
    }

    r = image_ctx->state->refresh();
    if (r < 0) {
      return r;
    }

    if (image_ctx->snap_ids.count(snap_name) == 0) {
      return -ENOENT;
    }
    *snap_id = image_ctx->snap_ids[snap_name];
    return 0;
  }

  int create_snap(const char* snap_name) {
    librados::snap_t remote_snap_id;
    int r = create_snap(m_remote_image_ctx, snap_name, &remote_snap_id);
    if (r < 0) {
      return r;
    }

    librados::snap_t local_snap_id;
    r = create_snap(m_local_image_ctx, snap_name, &local_snap_id);
    if (r < 0) {
      return r;
    }

    // collection of all existing snaps in local image
    MockImageCopyRequest::SnapIds local_snap_ids({local_snap_id});
    if (!m_snap_map.empty()) {
      local_snap_ids.insert(local_snap_ids.end(),
                            m_snap_map.rbegin()->second.begin(),
                            m_snap_map.rbegin()->second.end());
    }
    m_snap_map[remote_snap_id] = local_snap_ids;
    m_client_meta.snap_seqs[remote_snap_id] = local_snap_id;
    return 0;
  }

  librbd::ImageCtx *m_remote_image_ctx;
  librbd::ImageCtx *m_local_image_ctx;
  librbd::journal::MirrorPeerClientMeta m_client_meta;
  MockImageCopyRequest::SnapMap m_snap_map;

  rbd::mirror::Threads *m_threads = nullptr;
};

TEST_F(TestMockImageSyncImageCopyRequest, SimpleImage) {
  ASSERT_EQ(0, create_snap("snap1"));
  m_client_meta.sync_points = {{"snap1", boost::none}};

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;
  MockObjectCopyRequest mock_object_copy_request;

  expect_get_snap_id(mock_remote_image_ctx);

  InSequence seq;
  expect_get_object_count(mock_remote_image_ctx, 1);
  expect_get_object_count(mock_remote_image_ctx, 0);
  expect_update_client(mock_journaler, 0);
  expect_object_copy_send(mock_object_copy_request);
  expect_update_client(mock_journaler, 0);

  C_SaferCond ctx;
  MockImageCopyRequest *request = create_request(mock_remote_image_ctx,
                                                 mock_local_image_ctx,
                                                 mock_journaler,
                                                 m_client_meta.sync_points.front(),
                                                 &ctx);
  request->send();

  ASSERT_EQ(m_snap_map, wait_for_snap_map(mock_object_copy_request));
  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 0, 0));
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageSyncImageCopyRequest, Throttled) {
  ASSERT_EQ(0, create_snap("snap1"));
  m_client_meta.sync_points = {{"snap1", boost::none}};

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;
  MockObjectCopyRequest mock_object_copy_request;

  expect_get_snap_id(mock_remote_image_ctx);

  InSequence seq;
  expect_get_object_count(mock_remote_image_ctx, 50);
  expect_get_object_count(mock_remote_image_ctx, 0);
  expect_update_client(mock_journaler, 0);
  for (int i = 0; i < 50; ++i) {
    expect_object_copy_send(mock_object_copy_request);
  }
  expect_update_client(mock_journaler, 0);

  C_SaferCond ctx;
  MockImageCopyRequest *request = create_request(mock_remote_image_ctx,
                                                 mock_local_image_ctx,
                                                 mock_journaler,
                                                 m_client_meta.sync_points.front(),
                                                 &ctx);
  request->send();

  ASSERT_EQ(m_snap_map, wait_for_snap_map(mock_object_copy_request));
  for (uint64_t i = 0; i < 50; ++i) {
    ASSERT_TRUE(complete_object_copy(mock_object_copy_request, i, 0));
  }
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageSyncImageCopyRequest, SnapshotSubset) {
  ASSERT_EQ(0, create_snap("snap1"));
  ASSERT_EQ(0, create_snap("snap2"));
  ASSERT_EQ(0, create_snap("snap3"));
  m_client_meta.sync_points = {{"snap3", "snap2", boost::none}};

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;
  MockObjectCopyRequest mock_object_copy_request;

  expect_get_snap_id(mock_remote_image_ctx);

  InSequence seq;
  expect_get_object_count(mock_remote_image_ctx, 1);
  expect_get_object_count(mock_remote_image_ctx, 0);
  expect_get_object_count(mock_remote_image_ctx, 1);
  expect_get_object_count(mock_remote_image_ctx, 1);
  expect_update_client(mock_journaler, 0);
  expect_object_copy_send(mock_object_copy_request);
  expect_update_client(mock_journaler, 0);

  C_SaferCond ctx;
  MockImageCopyRequest *request = create_request(mock_remote_image_ctx,
                                                 mock_local_image_ctx,
                                                 mock_journaler,
                                                 m_client_meta.sync_points.front(),
                                                 &ctx);
  request->send();

  MockImageCopyRequest::SnapMap snap_map(m_snap_map);
  snap_map.erase(snap_map.begin());
  ASSERT_EQ(snap_map, wait_for_snap_map(mock_object_copy_request));

  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 0, 0));
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageSyncImageCopyRequest, RestartCatchup) {
  ASSERT_EQ(0, create_snap("snap1"));
  ASSERT_EQ(0, create_snap("snap2"));
  m_client_meta.sync_points = {{"snap1", boost::none},
                               {"snap2", "snap1", boost::none}};

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;
  MockObjectCopyRequest mock_object_copy_request;

  expect_get_snap_id(mock_remote_image_ctx);

  InSequence seq;
  expect_get_object_count(mock_remote_image_ctx, 1);
  expect_get_object_count(mock_remote_image_ctx, 0);
  expect_get_object_count(mock_remote_image_ctx, 0);
  expect_update_client(mock_journaler, 0);
  expect_object_copy_send(mock_object_copy_request);
  expect_update_client(mock_journaler, 0);

  C_SaferCond ctx;
  MockImageCopyRequest *request = create_request(mock_remote_image_ctx,
                                                 mock_local_image_ctx,
                                                 mock_journaler,
                                                 m_client_meta.sync_points.back(),
                                                 &ctx);
  request->send();

  ASSERT_EQ(m_snap_map, wait_for_snap_map(mock_object_copy_request));
  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 0, 0));
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageSyncImageCopyRequest, RestartPartialSync) {
  ASSERT_EQ(0, create_snap("snap1"));
  m_client_meta.sync_points = {{"snap1", 0}};

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;
  MockObjectCopyRequest mock_object_copy_request;

  expect_get_snap_id(mock_remote_image_ctx);

  InSequence seq;
  expect_get_object_count(mock_remote_image_ctx, 1);
  expect_get_object_count(mock_remote_image_ctx, 2);
  expect_update_client(mock_journaler, 0);
  expect_object_copy_send(mock_object_copy_request);
  expect_update_client(mock_journaler, 0);

  C_SaferCond ctx;
  MockImageCopyRequest *request = create_request(mock_remote_image_ctx,
                                                 mock_local_image_ctx,
                                                 mock_journaler,
                                                 m_client_meta.sync_points.front(),
                                                 &ctx);
  request->send();

  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 1, 0));
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageSyncImageCopyRequest, Cancel) {
  std::string max_ops_str;
  ASSERT_EQ(0, _rados.conf_get("rbd_concurrent_management_ops", max_ops_str));
  ASSERT_EQ(0, _rados.conf_set("rbd_concurrent_management_ops", "1"));
  BOOST_SCOPE_EXIT( (max_ops_str) ) {
    ASSERT_EQ(0, _rados.conf_set("rbd_concurrent_management_ops", max_ops_str.c_str()));
  } BOOST_SCOPE_EXIT_END;

  ASSERT_EQ(0, create_snap("snap1"));
  m_client_meta.sync_points = {{"snap1", boost::none}};

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;
  MockObjectCopyRequest mock_object_copy_request;

  expect_get_snap_id(mock_remote_image_ctx);

  InSequence seq;
  expect_get_object_count(mock_remote_image_ctx, 2);
  expect_get_object_count(mock_remote_image_ctx, 2);
  expect_update_client(mock_journaler, 0);
  expect_object_copy_send(mock_object_copy_request);
  expect_update_client(mock_journaler, 0);

  C_SaferCond ctx;
  MockImageCopyRequest *request = create_request(mock_remote_image_ctx,
                                                 mock_local_image_ctx,
                                                 mock_journaler,
                                                 m_client_meta.sync_points.front(),
                                                 &ctx);
  request->send();

  ASSERT_EQ(m_snap_map, wait_for_snap_map(mock_object_copy_request));
  request->cancel();

  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 0, 0));
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageSyncImageCopyRequest, MissingSnap) {
  ASSERT_EQ(0, create_snap("snap1"));
  m_client_meta.sync_points = {{"missing-snap", boost::none}};

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;

  expect_get_snap_id(mock_remote_image_ctx);

  C_SaferCond ctx;
  MockImageCopyRequest *request = create_request(mock_remote_image_ctx,
                                                 mock_local_image_ctx,
                                                 mock_journaler,
                                                 m_client_meta.sync_points.front(),
                                                 &ctx);
  request->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockImageSyncImageCopyRequest, MissingFromSnap) {
  ASSERT_EQ(0, create_snap("snap1"));
  m_client_meta.sync_points = {{"snap1", "missing-snap", boost::none}};

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;

  expect_get_snap_id(mock_remote_image_ctx);

  C_SaferCond ctx;
  MockImageCopyRequest *request = create_request(mock_remote_image_ctx,
                                                 mock_local_image_ctx,
                                                 mock_journaler,
                                                 m_client_meta.sync_points.front(),
                                                 &ctx);
  request->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockImageSyncImageCopyRequest, EmptySnapMap) {
  ASSERT_EQ(0, create_snap("snap1"));
  ASSERT_EQ(0, create_snap("snap2"));
  m_client_meta.snap_seqs = {{0, 0}};
  m_client_meta.sync_points = {{"snap2", "snap1", boost::none}};

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;

  expect_get_snap_id(mock_remote_image_ctx);

  C_SaferCond ctx;
  MockImageCopyRequest *request = create_request(mock_remote_image_ctx,
                                                 mock_local_image_ctx,
                                                 mock_journaler,
                                                 m_client_meta.sync_points.front(),
                                                 &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace image_sync
} // namespace mirror
} // namespace rbd
