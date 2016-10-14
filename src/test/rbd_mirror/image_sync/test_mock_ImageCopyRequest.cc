// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/journal/TypeTraits.h"
#include "test/journal/mock/MockJournaler.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "tools/rbd_mirror/image_sync/ImageCopyRequest.h"
#include "tools/rbd_mirror/image_sync/ObjectCopyRequest.h"
#include "tools/rbd_mirror/Threads.h"
#include <boost/scope_exit.hpp>

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
} // namespace librbd

namespace rbd {
namespace mirror {
namespace image_sync {

template <>
struct ObjectCopyRequest<librbd::MockTestImageCtx> {
  static ObjectCopyRequest* s_instance;
  static ObjectCopyRequest* create(librbd::MockTestImageCtx *local_image_ctx,
                                   librbd::MockTestImageCtx *remote_image_ctx,
                                   const ImageCopyRequest<librbd::MockTestImageCtx>::SnapMap *snap_map,
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

  const ImageCopyRequest<librbd::MockTestImageCtx>::SnapMap *snap_map = nullptr;
  std::map<uint64_t, Context *> object_contexts;

  ObjectCopyRequest() : lock("lock") {
    s_instance = this;
  }
};

ObjectCopyRequest<librbd::MockTestImageCtx>* ObjectCopyRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace image_sync
} // namespace mirror
} // namespace rbd

// template definitions
#include "tools/rbd_mirror/image_sync/ImageCopyRequest.cc"
template class rbd::mirror::image_sync::ImageCopyRequest<librbd::MockTestImageCtx>;

namespace rbd {
namespace mirror {
namespace image_sync {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArg;
using ::testing::InvokeWithoutArgs;

class TestMockImageSyncImageCopyRequest : public TestMockFixture {
public:
  typedef ImageCopyRequest<librbd::MockTestImageCtx> MockImageCopyRequest;
  typedef ObjectCopyRequest<librbd::MockTestImageCtx> MockObjectCopyRequest;

  virtual void SetUp() {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_remote_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_remote_io_ctx, m_image_name, &m_remote_image_ctx));

    ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &m_local_image_ctx));
  }

  void expect_get_snap_id(librbd::MockTestImageCtx &mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, get_snap_id(_))
      .WillRepeatedly(Invoke([&mock_image_ctx](std::string snap_name) {
        assert(mock_image_ctx.image_ctx->snap_lock.is_locked());
        return mock_image_ctx.image_ctx->get_snap_id(snap_name);
      }));
  }

  void expect_get_object_count(librbd::MockTestImageCtx &mock_image_ctx,
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
                               uint64_t object_num, int r,
                               std::function<void()> fn = []() {}) {
    Mutex::Locker locker(mock_object_copy_request.lock);
    while (mock_object_copy_request.object_contexts.count(object_num) == 0) {
      if (mock_object_copy_request.cond.WaitInterval(m_local_image_ctx->cct,
                                                     mock_object_copy_request.lock,
                                                     utime_t(10, 0)) != 0) {
        return false;
      }
    }

    FunctionContext *wrapper_ctx = new FunctionContext(
      [&mock_object_copy_request, object_num, fn] (int r) {
        fn();
        mock_object_copy_request.object_contexts[object_num]->complete(r);
      });
    m_threads->work_queue->queue(wrapper_ctx, r);
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

  MockImageCopyRequest *create_request(librbd::MockTestImageCtx &mock_remote_image_ctx,
                                       librbd::MockTestImageCtx &mock_local_image_ctx,
                                       journal::MockJournaler &mock_journaler,
                                       librbd::journal::MirrorPeerSyncPoint &sync_point,
                                       Context *ctx) {
    return new MockImageCopyRequest(&mock_local_image_ctx,
                                    &mock_remote_image_ctx,
                                    m_threads->timer, &m_threads->timer_lock,
                                    &mock_journaler, &m_client_meta,
                                    &sync_point, ctx);
  }

  using TestFixture::create_snap;
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
};

TEST_F(TestMockImageSyncImageCopyRequest, SimpleImage) {
  ASSERT_EQ(0, create_snap("snap1"));
  m_client_meta.sync_points = {{"snap1", boost::none}};

  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
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

  std::string update_sync_age;;
  ASSERT_EQ(0, _rados->conf_get("rbd_mirror_sync_point_update_age", update_sync_age));
  ASSERT_EQ(0, _rados->conf_set("rbd_mirror_sync_point_update_age", "1"));
  BOOST_SCOPE_EXIT( (update_sync_age) ) {
    ASSERT_EQ(0, _rados->conf_set("rbd_mirror_sync_point_update_age", update_sync_age.c_str()));
  } BOOST_SCOPE_EXIT_END;


  std::string max_ops_str;
  ASSERT_EQ(0, _rados->conf_get("rbd_concurrent_management_ops", max_ops_str));
  int max_ops = std::stoi(max_ops_str);

  uint64_t object_count = 55;

  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;
  MockObjectCopyRequest mock_object_copy_request;

  expect_get_snap_id(mock_remote_image_ctx);

  expect_get_object_count(mock_remote_image_ctx, object_count);
  expect_get_object_count(mock_remote_image_ctx, 0);

  EXPECT_CALL(mock_object_copy_request, send()).Times(object_count);

  boost::optional<uint64_t> expected_object_number(boost::none);
  EXPECT_CALL(mock_journaler, update_client(_, _))
    .WillRepeatedly(
        Invoke([&expected_object_number, max_ops, object_count, this]
               (bufferlist data, Context *ctx) {
          ASSERT_EQ(expected_object_number,
                    m_client_meta.sync_points.front().object_number);
          if (!expected_object_number) {
            expected_object_number = (max_ops - 1);
          } else {
            expected_object_number = expected_object_number.get() + max_ops;
          }

          if (expected_object_number.get() > (object_count - 1)) {
            expected_object_number = (object_count - 1);
          }

          m_threads->work_queue->queue(ctx, 0);
      }));


  C_SaferCond ctx;
  MockImageCopyRequest *request = create_request(mock_remote_image_ctx,
                                                 mock_local_image_ctx,
                                                 mock_journaler,
                                                 m_client_meta.sync_points.front(),
                                                 &ctx);
  request->send();

  std::function<void()> sleep_fn = [request]() {
    sleep(2);
  };

  ASSERT_EQ(m_snap_map, wait_for_snap_map(mock_object_copy_request));
  for (uint64_t i = 0; i < object_count; ++i) {
    if (i % 10 == 0) {
      ASSERT_TRUE(complete_object_copy(mock_object_copy_request, i, 0, sleep_fn));
    } else {
      ASSERT_TRUE(complete_object_copy(mock_object_copy_request, i, 0));
    }
  }
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageSyncImageCopyRequest, SnapshotSubset) {
  ASSERT_EQ(0, create_snap("snap1"));
  ASSERT_EQ(0, create_snap("snap2"));
  ASSERT_EQ(0, create_snap("snap3"));
  m_client_meta.sync_points = {{"snap3", "snap2", boost::none}};

  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
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

  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
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
  m_client_meta.sync_points = {{"snap1", librbd::journal::MirrorPeerSyncPoint::ObjectNumber{0U}}};

  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
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
  ASSERT_EQ(0, _rados->conf_get("rbd_concurrent_management_ops", max_ops_str));
  ASSERT_EQ(0, _rados->conf_set("rbd_concurrent_management_ops", "1"));
  BOOST_SCOPE_EXIT( (max_ops_str) ) {
    ASSERT_EQ(0, _rados->conf_set("rbd_concurrent_management_ops", max_ops_str.c_str()));
  } BOOST_SCOPE_EXIT_END;

  ASSERT_EQ(0, create_snap("snap1"));
  m_client_meta.sync_points = {{"snap1", boost::none}};

  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;
  MockObjectCopyRequest mock_object_copy_request;

  expect_get_snap_id(mock_remote_image_ctx);

  InSequence seq;
  expect_get_object_count(mock_remote_image_ctx, 2);
  expect_get_object_count(mock_remote_image_ctx, 2);
  expect_update_client(mock_journaler, 0);
  expect_object_copy_send(mock_object_copy_request);

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
  ASSERT_EQ(-ECANCELED, ctx.wait());
}

TEST_F(TestMockImageSyncImageCopyRequest, Cancel_Inflight_Sync) {
  std::string update_sync_age;;
  ASSERT_EQ(0, _rados->conf_get("rbd_mirror_sync_point_update_age", update_sync_age));
  ASSERT_EQ(0, _rados->conf_set("rbd_mirror_sync_point_update_age", "1"));
  BOOST_SCOPE_EXIT( (update_sync_age) ) {
    ASSERT_EQ(0, _rados->conf_set("rbd_mirror_sync_point_update_age", update_sync_age.c_str()));
  } BOOST_SCOPE_EXIT_END;

  std::string max_ops_str;
  ASSERT_EQ(0, _rados->conf_get("rbd_concurrent_management_ops", max_ops_str));
  ASSERT_EQ(0, _rados->conf_set("rbd_concurrent_management_ops", "3"));
  BOOST_SCOPE_EXIT( (max_ops_str) ) {
    ASSERT_EQ(0, _rados->conf_set("rbd_concurrent_management_ops", max_ops_str.c_str()));
  } BOOST_SCOPE_EXIT_END;

  ASSERT_EQ(0, create_snap("snap1"));
  m_client_meta.sync_points = {{"snap1", boost::none}};

  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;
  MockObjectCopyRequest mock_object_copy_request;

  expect_get_snap_id(mock_remote_image_ctx);

  expect_get_object_count(mock_remote_image_ctx, 10);
  expect_get_object_count(mock_remote_image_ctx, 0);

  EXPECT_CALL(mock_object_copy_request, send()).Times(6);

  EXPECT_CALL(mock_journaler, update_client(_, _))
    .WillRepeatedly(Invoke([this] (bufferlist data, Context *ctx) {
          m_threads->work_queue->queue(ctx, 0);
      }));


  C_SaferCond ctx;
  MockImageCopyRequest *request = create_request(mock_remote_image_ctx,
                                                 mock_local_image_ctx,
                                                 mock_journaler,
                                                 m_client_meta.sync_points.front(),
                                                 &ctx);
  request->send();

  ASSERT_EQ(m_snap_map, wait_for_snap_map(mock_object_copy_request));

  std::function<void()> cancel_fn = [request]() {
    sleep(2);
    request->cancel();
  };

  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 0, 0));
  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 1, 0));
  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 2, 0));
  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 3, 0, cancel_fn));
  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 4, 0));
  ASSERT_TRUE(complete_object_copy(mock_object_copy_request, 5, 0));

  ASSERT_EQ(-ECANCELED, ctx.wait());
  ASSERT_EQ(5u, m_client_meta.sync_points.front().object_number.get());
}

TEST_F(TestMockImageSyncImageCopyRequest, Cancel1) {
  ASSERT_EQ(0, create_snap("snap1"));
  m_client_meta.sync_points = {{"snap1", boost::none}};

  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;
  MockObjectCopyRequest mock_object_copy_request;

  C_SaferCond ctx;
  MockImageCopyRequest *request = create_request(mock_remote_image_ctx,
                                                 mock_local_image_ctx,
                                                 mock_journaler,
                                                 m_client_meta.sync_points.front(),
                                                 &ctx);

  expect_get_snap_id(mock_remote_image_ctx);

  InSequence seq;
  expect_get_object_count(mock_remote_image_ctx, 1);
  expect_get_object_count(mock_remote_image_ctx, 0);
  EXPECT_CALL(mock_journaler, update_client(_, _))
    .WillOnce(DoAll(InvokeWithoutArgs([request]() {
	    request->cancel();
	  }),
	WithArg<1>(CompleteContext(0))));

  request->send();
  ASSERT_EQ(-ECANCELED, ctx.wait());
}

TEST_F(TestMockImageSyncImageCopyRequest, MissingSnap) {
  ASSERT_EQ(0, create_snap("snap1"));
  m_client_meta.sync_points = {{"missing-snap", boost::none}};

  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
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

  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
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

  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
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

TEST_F(TestMockImageSyncImageCopyRequest, EmptySnapSeqs) {
  ASSERT_EQ(0, create_snap("snap1"));
  ASSERT_EQ(0, create_snap("snap2"));
  m_client_meta.snap_seqs = {};
  m_client_meta.sync_points = {{"snap2", "snap1", boost::none}};

  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
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
