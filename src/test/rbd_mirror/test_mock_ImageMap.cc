// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/rbd_mirror/mock/MockContextWQ.h"
#include "test/rbd_mirror/mock/MockSafeTimer.h"
#include "librbd/MirroringWatcher.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/ImageMap.h"
#include "tools/rbd_mirror/image_map/LoadRequest.h"
#include "tools/rbd_mirror/image_map/UpdateRequest.h"
#include "tools/rbd_mirror/image_map/Types.h"
#include "include/stringify.h"

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

template <>
struct Threads<librbd::MockTestImageCtx> {
  MockSafeTimer *timer;
  ceph::mutex &timer_lock;

  MockContextWQ *work_queue;

  Threads(Threads<librbd::ImageCtx> *threads)
    : timer(new MockSafeTimer()),
      timer_lock(threads->timer_lock),
      work_queue(new MockContextWQ()) {
  }
  ~Threads() {
    delete timer;
    delete work_queue;
  }
};

namespace image_map {

template <>
struct LoadRequest<librbd::MockTestImageCtx> {
  std::map<std::string, cls::rbd::MirrorImageMap> *image_map;
  Context *on_finish = nullptr;

  static LoadRequest *s_instance;
  static LoadRequest *create(librados::IoCtx &ioctx,
                             std::map<std::string, cls::rbd::MirrorImageMap> *image_map,
                             Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->image_map = image_map;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  LoadRequest() {
    s_instance = this;
  }
};

template <>
struct UpdateRequest<librbd::MockTestImageCtx> {
  Context *on_finish = nullptr;
  static UpdateRequest *s_instance;
  static UpdateRequest *create(librados::IoCtx &ioctx,
                               std::map<std::string, cls::rbd::MirrorImageMap> &&update_mapping,
                               std::set<std::string> &&global_image_ids,
                               Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  UpdateRequest() {
    s_instance = this;
  }
};

LoadRequest<librbd::MockTestImageCtx> *
LoadRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
UpdateRequest<librbd::MockTestImageCtx> *
UpdateRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace image_map

} // namespace mirror
} // namespace rbd

// template definitions
#include "tools/rbd_mirror/ImageMap.cc"

namespace rbd {
namespace mirror {

using ::testing::_;
using ::testing::DoAll;
using ::testing::WithArg;
using ::testing::AtLeast;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::ReturnArg;
using ::testing::StrEq;

using image_map::Listener;
using image_map::LoadRequest;
using image_map::UpdateRequest;

using ::rbd::mirror::Threads;

class TestMockImageMap : public TestMockFixture {
public:
  typedef Threads<librbd::MockTestImageCtx> MockThreads;
  typedef ImageMap<librbd::MockTestImageCtx> MockImageMap;
  typedef LoadRequest<librbd::MockTestImageCtx> MockLoadRequest;
  typedef UpdateRequest<librbd::MockTestImageCtx> MockUpdateRequest;

  struct MockListener : Listener {
    TestMockImageMap *test_mock_image_map;

    MockListener(TestMockImageMap *test_mock_image_map)
      : test_mock_image_map(test_mock_image_map) {
    }

    MOCK_METHOD2(mock_acquire_image, void(const std::string &, Context*));
    MOCK_METHOD2(mock_release_image, void(const std::string &, Context*));
    MOCK_METHOD3(mock_remove_image, void(const std::string &,
                                         const std::string &, Context*));

    void acquire_image(const std::string &global_image_id,
                       const std::string &instance_id, Context* on_finish) {
      mock_acquire_image(global_image_id, on_finish);
    }

    void release_image(const std::string &global_image_id,
                       const std::string &instance_id, Context* on_finish) {
      mock_release_image(global_image_id, on_finish);
    }

    void remove_image(const std::string &mirror_uuid,
                      const std::string &global_image_id,
                      const std::string &instance_id, Context* on_finish) {
      mock_remove_image(mirror_uuid, global_image_id, on_finish);
    }
  };

  TestMockImageMap() = default;

  void SetUp() override {
    TestFixture::SetUp();

    m_local_instance_id = stringify(m_local_io_ctx.get_instance_id());

    EXPECT_EQ(0, _rados->conf_set("rbd_mirror_image_policy_migration_throttle",
                                  "0"));
    EXPECT_EQ(0, _rados->conf_set("rbd_mirror_image_policy_type", "simple"));
  }

  void TearDown() override {
    EXPECT_EQ(0, _rados->conf_set("rbd_mirror_image_policy_type", "none"));

    TestFixture::TearDown();
  }

  void expect_work_queue(MockThreads &mock_threads) {
    EXPECT_CALL(*mock_threads.work_queue, queue(_, _))
      .WillRepeatedly(Invoke([this](Context *ctx, int r) {
            m_threads->work_queue->queue(ctx, r);
          }));
  }

  void expect_add_event(MockThreads &mock_threads) {
    EXPECT_CALL(*mock_threads.timer, add_event_after(_,_))
      .WillOnce(DoAll(WithArg<1>(Invoke([this](Context *ctx) {
             auto wrapped_ctx = new LambdaContext([this, ctx](int r) {
                    std::lock_guard timer_locker{m_threads->timer_lock};
                    ctx->complete(r);
                  });
                m_threads->work_queue->queue(wrapped_ctx, 0);
              })), ReturnArg<1>()));
  }

  void expect_rebalance_event(MockThreads &mock_threads) {
    EXPECT_CALL(*mock_threads.timer, add_event_after(_,_))
      .WillOnce(DoAll(WithArg<1>(Invoke([this](Context *ctx) {
                // disable rebalance so as to not reschedule it again
                CephContext *cct = reinterpret_cast<CephContext *>(m_local_io_ctx.cct());
                cct->_conf.set_val("rbd_mirror_image_policy_rebalance_timeout", "0");

                auto wrapped_ctx = new LambdaContext([this, ctx](int r) {
                    std::lock_guard timer_locker{m_threads->timer_lock};
                    ctx->complete(r);
                  });
                m_threads->work_queue->queue(wrapped_ctx, 0);
              })), ReturnArg<1>()));
  }

  void expect_load_request(MockLoadRequest &request, int r) {
    EXPECT_CALL(request, send())
      .WillOnce(Invoke([&request, r]() {
            request.on_finish->complete(r);
          }));
  }

  void expect_update_request(MockUpdateRequest &request, int r) {
    EXPECT_CALL(request, send())
      .WillOnce(Invoke([this, &request, r]() {
            request.on_finish->complete(r);
            if (r == 0) {
              std::lock_guard locker{m_lock};
              ++m_map_update_count;
              m_cond.notify_all();
            }
          }));
  }

  void expect_listener_acquire_image(MockListener &mock_listener,
                                     const std::string &global_image_id,
                                     std::map<std::string, Context*> *peer_ack_ctxs) {
    EXPECT_CALL(mock_listener, mock_acquire_image(global_image_id, _))
      .WillOnce(WithArg<1>(Invoke([this, global_image_id, peer_ack_ctxs](Context* ctx) {
              std::lock_guard locker{m_lock};
              peer_ack_ctxs->insert({global_image_id, ctx});
              ++m_notify_update_count;
              m_cond.notify_all();
            })));
  }

  void expect_listener_release_image(MockListener &mock_listener,
                                     const std::string &global_image_id,
                                     std::map<std::string, Context*> *peer_ack_ctxs) {
    EXPECT_CALL(mock_listener, mock_release_image(global_image_id, _))
      .WillOnce(WithArg<1>(Invoke([this, global_image_id, peer_ack_ctxs](Context* ctx) {
              std::lock_guard locker{m_lock};
              peer_ack_ctxs->insert({global_image_id, ctx});
              ++m_notify_update_count;
              m_cond.notify_all();
            })));
  }

  void expect_listener_remove_image(MockListener &mock_listener,
                                    const std::string &mirror_uuid,
                                    const std::string &global_image_id,
                                    std::map<std::string, Context*> *peer_ack_ctxs) {
    EXPECT_CALL(mock_listener,
                mock_remove_image(mirror_uuid, global_image_id, _))
      .WillOnce(WithArg<2>(Invoke([this, global_image_id, peer_ack_ctxs](Context* ctx) {
              std::lock_guard locker{m_lock};
              peer_ack_ctxs->insert({global_image_id, ctx});
              ++m_notify_update_count;
              m_cond.notify_all();
            })));
  }

  void expect_listener_images_unmapped(MockListener &mock_listener, size_t count,
                                       std::set<std::string> *global_image_ids,
                                       std::map<std::string, Context*> *peer_ack_ctxs) {
    EXPECT_CALL(mock_listener, mock_release_image(_, _))
      .Times(count)
      .WillRepeatedly(Invoke([this, global_image_ids, peer_ack_ctxs](std::string global_image_id, Context* ctx) {
              std::lock_guard locker{m_lock};
              global_image_ids->emplace(global_image_id);
              peer_ack_ctxs->insert({global_image_id, ctx});
              ++m_notify_update_count;
              m_cond.notify_all();
            }));
  }

  void remote_peer_ack_nowait(MockImageMap *image_map,
                              const std::set<std::string> &global_image_ids,
                              int ret,
                              std::map<std::string, Context*> *peer_ack_ctxs) {
    for (auto& global_image_id : global_image_ids) {
      auto it = peer_ack_ctxs->find(global_image_id);
      ASSERT_TRUE(it != peer_ack_ctxs->end());
      auto ack_ctx = it->second;
      peer_ack_ctxs->erase(it);
      ack_ctx->complete(ret);
      wait_for_scheduled_task();
    }
  }

  void remote_peer_ack_wait(MockImageMap *image_map,
                            const std::set<std::string> &global_image_ids,
                            int ret,
                            std::map<std::string, Context*> *peer_ack_ctxs) {
    for (auto& global_image_id : global_image_ids) {
      auto it = peer_ack_ctxs->find(global_image_id);
      ASSERT_TRUE(it != peer_ack_ctxs->end());
      auto ack_ctx = it->second;
      peer_ack_ctxs->erase(it);
      ack_ctx->complete(ret);
      wait_for_scheduled_task();
      ASSERT_TRUE(wait_for_map_update(1));
    }
  }

  void remote_peer_ack_listener_wait(MockImageMap *image_map,
                                     const std::set<std::string> &global_image_ids,
                                     int ret,
                                     std::map<std::string, Context*> *peer_ack_ctxs) {
    for (auto& global_image_id : global_image_ids) {
      auto it = peer_ack_ctxs->find(global_image_id);
      ASSERT_TRUE(it != peer_ack_ctxs->end());
      auto ack_ctx = it->second;
      peer_ack_ctxs->erase(it);
      ack_ctx->complete(ret);
      ASSERT_TRUE(wait_for_map_update(1));
      ASSERT_TRUE(wait_for_listener_notify(1));
    }
  }

  void update_map_and_acquire(MockThreads &mock_threads,
                              MockUpdateRequest &mock_update_request,
                              MockListener &mock_listener,
                              const std::set<std::string> &global_image_ids,
                              int ret,
                              std::map<std::string, Context*> *peer_ack_ctxs) {
    for (auto const &global_image_id : global_image_ids) {
      expect_add_event(mock_threads);
      expect_update_request(mock_update_request, ret);
      expect_add_event(mock_threads);
      expect_listener_acquire_image(mock_listener, global_image_id,
                                    peer_ack_ctxs);
    }
  }

  void update_map_request(MockThreads &mock_threads,
                          MockUpdateRequest &mock_update_request,
                          const std::set<std::string> &global_image_ids, int ret) {
    for (uint32_t i = 0; i < global_image_ids.size(); ++i) {
      expect_add_event(mock_threads);
      expect_update_request(mock_update_request, ret);
    }
  }

  void wait_for_scheduled_task() {
    m_threads->work_queue->drain();
  }

  bool wait_for_listener_notify(uint32_t count) {
    std::unique_lock locker{m_lock};
    while (m_notify_update_count < count) {
      if (m_cond.wait_for(locker, 10s) == std::cv_status::timeout) {
        break;
      }
    }

    if (m_notify_update_count < count) {
      return false;
    }

    m_notify_update_count -= count;
    return true;
  }

  bool wait_for_map_update(uint32_t count) {
    std::unique_lock locker{m_lock};
    while (m_map_update_count < count) {
      if (m_cond.wait_for(locker, 10s) == std::cv_status::timeout) {
        break;
      }
    }

    if (m_map_update_count < count) {
      return false;
    }

    m_map_update_count -= count;
    return true;
  }

  int when_shut_down(MockImageMap *image_map) {
    C_SaferCond ctx;
    image_map->shut_down(&ctx);
    return ctx.wait();
  }

  void listener_acquire_images(MockListener &mock_listener,
                               const std::set<std::string> &global_image_ids,
                               std::map<std::string, Context*> *peer_ack_ctxs) {
    for (auto const &global_image_id : global_image_ids) {
      expect_listener_acquire_image(mock_listener, global_image_id,
                                    peer_ack_ctxs);
    }
  }

  void listener_release_images(MockListener &mock_listener,
                               const std::set<std::string> &global_image_ids,
                               std::map<std::string, Context*> *peer_ack_ctxs) {
    for (auto const &global_image_id : global_image_ids) {
      expect_listener_release_image(mock_listener, global_image_id,
                                    peer_ack_ctxs);
    }
  }

  void listener_remove_images(MockListener &mock_listener,
                              const std::string &mirror_uuid,
                              std::set<std::string> &global_image_ids,
                              std::map<std::string, Context*> *peer_ack_ctxs) {
    for (auto const &global_image_id : global_image_ids) {
      expect_listener_remove_image(mock_listener, mirror_uuid, global_image_id,
                                   peer_ack_ctxs);
    }
  }

  ceph::mutex m_lock = ceph::make_mutex("TestMockImageMap::m_lock");
  ceph::condition_variable m_cond;
  uint32_t m_notify_update_count = 0;
  uint32_t m_map_update_count = 0;
  std::string m_local_instance_id;
};

TEST_F(TestMockImageMap, SetLocalImages) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;

  MockLoadRequest mock_load_request;
  expect_load_request(mock_load_request, 0);

  MockListener mock_listener(this);

  std::unique_ptr<MockImageMap> mock_image_map{
    MockImageMap::create(m_local_io_ctx, &mock_threads, m_local_instance_id,
                         mock_listener)};

  C_SaferCond cond;
  mock_image_map->init(&cond);
  ASSERT_EQ(0, cond.wait());

  std::set<std::string> global_image_ids{
    "global id 1", "global id 2"
  };
  std::set<std::string> global_image_ids_ack(global_image_ids);

  // UPDATE_MAPPING+ACQUIRE
  expect_add_event(mock_threads);
  MockUpdateRequest mock_update_request;
  expect_update_request(mock_update_request, 0);
  expect_add_event(mock_threads);
  std::map<std::string, Context*> peer_ack_ctxs;
  listener_acquire_images(mock_listener, global_image_ids, &peer_ack_ctxs);

  // initial image list
  mock_image_map->update_images("", std::move(global_image_ids), {});

  ASSERT_TRUE(wait_for_map_update(1));
  ASSERT_TRUE(wait_for_listener_notify(global_image_ids_ack.size()));

  // remote peer ACKs image acquire request
  remote_peer_ack_nowait(mock_image_map.get(), global_image_ids_ack, 0,
                         &peer_ack_ctxs);

  wait_for_scheduled_task();
  ASSERT_EQ(0, when_shut_down(mock_image_map.get()));
}

TEST_F(TestMockImageMap, AddRemoveLocalImage) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;

  MockLoadRequest mock_load_request;
  expect_load_request(mock_load_request, 0);

  MockListener mock_listener(this);

  std::unique_ptr<MockImageMap> mock_image_map{
    MockImageMap::create(m_local_io_ctx, &mock_threads, m_local_instance_id,
                         mock_listener)};

  C_SaferCond cond;
  mock_image_map->init(&cond);
  ASSERT_EQ(0, cond.wait());

  std::set<std::string> initial_global_image_ids{
    "global id 1", "global id 2"
  };
  std::set<std::string> initial_global_image_ids_ack(initial_global_image_ids);

  std::set<std::string> remove_global_image_ids{
    "global id 1", "global id 2"
  };
  std::set<std::string> remove_global_image_ids_ack(remove_global_image_ids);

  // UPDATE_MAPPING+ACQUIRE
  expect_add_event(mock_threads);
  MockUpdateRequest mock_update_request;
  expect_update_request(mock_update_request, 0);
  expect_add_event(mock_threads);
  std::map<std::string, Context*> peer_ack_ctxs;
  listener_acquire_images(mock_listener, initial_global_image_ids,
                          &peer_ack_ctxs);

  // initial image list
  mock_image_map->update_images("", std::move(initial_global_image_ids), {});

  ASSERT_TRUE(wait_for_map_update(1));
  ASSERT_TRUE(wait_for_listener_notify(initial_global_image_ids_ack.size()));

  // remote peer ACKs image acquire request
  remote_peer_ack_nowait(mock_image_map.get(), initial_global_image_ids_ack, 0,
                         &peer_ack_ctxs);

  // RELEASE+REMOVE_MAPPING
  expect_add_event(mock_threads);
  listener_release_images(mock_listener, remove_global_image_ids,
                          &peer_ack_ctxs);
  update_map_request(mock_threads, mock_update_request, remove_global_image_ids,
                     0);

  // remove images
  mock_image_map->update_images("", {}, std::move(remove_global_image_ids));
  ASSERT_TRUE(wait_for_listener_notify(remove_global_image_ids_ack.size()));

  remote_peer_ack_wait(mock_image_map.get(), remove_global_image_ids_ack, 0,
                       &peer_ack_ctxs);

  wait_for_scheduled_task();
  ASSERT_EQ(0, when_shut_down(mock_image_map.get()));
}

TEST_F(TestMockImageMap, AddRemoveRemoteImage) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;

  MockLoadRequest mock_load_request;
  expect_load_request(mock_load_request, 0);

  MockListener mock_listener(this);

  std::unique_ptr<MockImageMap> mock_image_map{
    MockImageMap::create(m_local_io_ctx, &mock_threads, m_local_instance_id,
                         mock_listener)};

  C_SaferCond cond;
  mock_image_map->init(&cond);
  ASSERT_EQ(0, cond.wait());

  std::set<std::string> initial_global_image_ids{
    "global id 1", "global id 2"
  };
  std::set<std::string> initial_global_image_ids_ack(initial_global_image_ids);

  std::set<std::string> remove_global_image_ids{
    "global id 1", "global id 2"
  };
  std::set<std::string> remove_global_image_ids_ack(remove_global_image_ids);

  // UPDATE_MAPPING+ACQUIRE
  expect_add_event(mock_threads);
  MockUpdateRequest mock_update_request;
  expect_update_request(mock_update_request, 0);
  expect_add_event(mock_threads);
  std::map<std::string, Context*> peer_ack_ctxs;
  listener_acquire_images(mock_listener, initial_global_image_ids,
                          &peer_ack_ctxs);

  // initial image list
  mock_image_map->update_images("uuid1", std::move(initial_global_image_ids),
                                {});

  ASSERT_TRUE(wait_for_map_update(1));
  ASSERT_TRUE(wait_for_listener_notify(initial_global_image_ids_ack.size()));

  // remote peer ACKs image acquire request
  remote_peer_ack_nowait(mock_image_map.get(), initial_global_image_ids_ack, 0,
                         &peer_ack_ctxs);

  // RELEASE+REMOVE_MAPPING
  std::map<std::string, Context*> peer_remove_ack_ctxs;
  listener_remove_images(mock_listener, "uuid1", remove_global_image_ids,
                         &peer_remove_ack_ctxs);
  expect_add_event(mock_threads);
  listener_release_images(mock_listener, remove_global_image_ids,
                          &peer_ack_ctxs);
  update_map_request(mock_threads, mock_update_request, remove_global_image_ids,
                     0);

  // remove images
  mock_image_map->update_images("uuid1", {}, std::move(remove_global_image_ids));
  ASSERT_TRUE(wait_for_listener_notify(remove_global_image_ids_ack.size() * 2));

  remote_peer_ack_nowait(mock_image_map.get(), remove_global_image_ids_ack, 0,
                         &peer_remove_ack_ctxs);
  remote_peer_ack_wait(mock_image_map.get(), remove_global_image_ids_ack, 0,
                       &peer_ack_ctxs);

  wait_for_scheduled_task();
  ASSERT_EQ(0, when_shut_down(mock_image_map.get()));
}

TEST_F(TestMockImageMap, AddRemoveRemoteImageDuplicateNotification) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;

  MockLoadRequest mock_load_request;
  expect_load_request(mock_load_request, 0);

  MockListener mock_listener(this);

  std::unique_ptr<MockImageMap> mock_image_map{
    MockImageMap::create(m_local_io_ctx, &mock_threads, m_local_instance_id,
                         mock_listener)};

  C_SaferCond cond;
  mock_image_map->init(&cond);
  ASSERT_EQ(0, cond.wait());

  std::set<std::string> initial_global_image_ids{
    "global id 1", "global id 2"
  };
  std::set<std::string> initial_global_image_ids_dup(initial_global_image_ids);
  std::set<std::string> initial_global_image_ids_ack(initial_global_image_ids);

  std::set<std::string> remove_global_image_ids{
    "global id 1", "global id 2"
  };
  std::set<std::string> remove_global_image_ids_dup(remove_global_image_ids);
  std::set<std::string> remove_global_image_ids_ack(remove_global_image_ids);

  // UPDATE_MAPPING+ACQUIRE
  expect_add_event(mock_threads);
  MockUpdateRequest mock_update_request;
  expect_update_request(mock_update_request, 0);
  expect_add_event(mock_threads);
  std::map<std::string, Context*> peer_ack_ctxs;
  listener_acquire_images(mock_listener, initial_global_image_ids,
                          &peer_ack_ctxs);

  // initial image list
  mock_image_map->update_images("uuid1", std::move(initial_global_image_ids), {});

  ASSERT_TRUE(wait_for_map_update(1));
  ASSERT_TRUE(wait_for_listener_notify(initial_global_image_ids_ack.size()));

  // trigger duplicate "add" event
  wait_for_scheduled_task();
  mock_image_map->update_images("uuid1", std::move(initial_global_image_ids_dup), {});

  // remote peer ACKs image acquire request
  remote_peer_ack_nowait(mock_image_map.get(), initial_global_image_ids_ack, 0,
                         &peer_ack_ctxs);

  // RELEASE+REMOVE_MAPPING
  std::map<std::string, Context*> peer_remove_ack_ctxs;
  listener_remove_images(mock_listener, "uuid1", remove_global_image_ids,
                         &peer_remove_ack_ctxs);
  expect_add_event(mock_threads);
  listener_release_images(mock_listener, remove_global_image_ids,
                          &peer_ack_ctxs);
  update_map_request(mock_threads, mock_update_request, remove_global_image_ids, 0);

  // remove images
  mock_image_map->update_images("uuid1", {}, std::move(remove_global_image_ids));
  ASSERT_TRUE(wait_for_listener_notify(remove_global_image_ids_ack.size() * 2));

  remote_peer_ack_nowait(mock_image_map.get(), remove_global_image_ids_ack, 0,
                         &peer_remove_ack_ctxs);
  remote_peer_ack_wait(mock_image_map.get(), remove_global_image_ids_ack, 0,
                       &peer_ack_ctxs);

  // trigger duplicate "remove" notification
  mock_image_map->update_images("uuid1", {}, std::move(remove_global_image_ids_dup));

  wait_for_scheduled_task();
  ASSERT_EQ(0, when_shut_down(mock_image_map.get()));
}

TEST_F(TestMockImageMap, AcquireImageErrorRetry) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;

  MockLoadRequest mock_load_request;
  expect_load_request(mock_load_request, 0);

  MockListener mock_listener(this);

  std::unique_ptr<MockImageMap> mock_image_map{
    MockImageMap::create(m_local_io_ctx, &mock_threads, m_local_instance_id,
                         mock_listener)};

  C_SaferCond cond;
  mock_image_map->init(&cond);
  ASSERT_EQ(0, cond.wait());

  std::set<std::string> initial_global_image_ids{
    "global id 1", "global id 2"
  };
  std::set<std::string> initial_global_image_ids_ack(initial_global_image_ids);

  // UPDATE_MAPPING failure
  expect_add_event(mock_threads);
  MockUpdateRequest mock_update_request;
  expect_update_request(mock_update_request, -EIO);

  // UPDATE_MAPPING+ACQUIRE
  expect_add_event(mock_threads);
  expect_update_request(mock_update_request, 0);
  expect_add_event(mock_threads);
  std::map<std::string, Context*> peer_ack_ctxs;
  listener_acquire_images(mock_listener, initial_global_image_ids,
                          &peer_ack_ctxs);

  // initial image list
  mock_image_map->update_images("uuid1", std::move(initial_global_image_ids), {});

  ASSERT_TRUE(wait_for_map_update(1));
  ASSERT_TRUE(wait_for_listener_notify(initial_global_image_ids_ack.size()));

  // remote peer ACKs image acquire request
  remote_peer_ack_nowait(mock_image_map.get(), initial_global_image_ids_ack, 0,
                         &peer_ack_ctxs);

  wait_for_scheduled_task();
  ASSERT_EQ(0, when_shut_down(mock_image_map.get()));
}

TEST_F(TestMockImageMap, RemoveRemoteAndLocalImage) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;

  MockLoadRequest mock_load_request;
  expect_load_request(mock_load_request, 0);

  MockListener mock_listener(this);

  std::unique_ptr<MockImageMap> mock_image_map{
    MockImageMap::create(m_local_io_ctx, &mock_threads, m_local_instance_id,
                         mock_listener)};

  C_SaferCond cond;
  mock_image_map->init(&cond);
  ASSERT_EQ(0, cond.wait());

  // remote image set
  std::set<std::string> initial_remote_global_image_ids{
    "global id 1"
  };
  std::set<std::string> initial_remote_global_image_ids_ack(initial_remote_global_image_ids);

  // local image set
  std::set<std::string> initial_local_global_image_ids{
    "global id 1"
  };

  // remote/local images to remove
  std::set<std::string> remote_remove_global_image_ids{
    "global id 1"
  };
  std::set<std::string> remote_remove_global_image_ids_ack(remote_remove_global_image_ids);

  std::set<std::string> local_remove_global_image_ids{
    "global id 1"
  };
  std::set<std::string> local_remove_global_image_ids_ack(local_remove_global_image_ids);

  // UPDATE_MAPPING+ACQUIRE
  expect_add_event(mock_threads);
  MockUpdateRequest mock_update_request;
  expect_update_request(mock_update_request, 0);
  expect_add_event(mock_threads);
  std::map<std::string, Context*> peer_ack_ctxs;
  listener_acquire_images(mock_listener, initial_remote_global_image_ids,
                          &peer_ack_ctxs);

  // initial remote image list
  mock_image_map->update_images("uuid1", std::move(initial_remote_global_image_ids), {});

  ASSERT_TRUE(wait_for_map_update(1));
  ASSERT_TRUE(wait_for_listener_notify(initial_remote_global_image_ids_ack.size()));

  // remote peer ACKs image acquire request
  remote_peer_ack_nowait(mock_image_map.get(),
                         initial_remote_global_image_ids_ack, 0,
                         &peer_ack_ctxs);

  // set initial local image list -- this is a no-op from policy pov
  mock_image_map->update_images("", std::move(initial_local_global_image_ids), {});

  // remove remote images -- this should be a no-op from policy pov
  // except the listener notification
  std::map<std::string, Context*> peer_ack_remove_ctxs;
  listener_remove_images(mock_listener, "uuid1", remote_remove_global_image_ids,
                         &peer_ack_remove_ctxs);

  mock_image_map->update_images("uuid1", {}, std::move(remote_remove_global_image_ids));
  ASSERT_TRUE(wait_for_listener_notify(remote_remove_global_image_ids_ack.size()));

  // RELEASE+REMOVE_MAPPING
  expect_add_event(mock_threads);
  listener_release_images(mock_listener, local_remove_global_image_ids,
                          &peer_ack_ctxs);
  update_map_request(mock_threads, mock_update_request, local_remove_global_image_ids, 0);

  // remove local images
  mock_image_map->update_images("", {}, std::move(local_remove_global_image_ids));
  ASSERT_TRUE(wait_for_listener_notify(local_remove_global_image_ids_ack.size()));

  remote_peer_ack_nowait(mock_image_map.get(), local_remove_global_image_ids_ack,
                         0,  &peer_ack_remove_ctxs);
  remote_peer_ack_wait(mock_image_map.get(), local_remove_global_image_ids_ack,
                       0, &peer_ack_ctxs);

  wait_for_scheduled_task();
  ASSERT_EQ(0, when_shut_down(mock_image_map.get()));
}

TEST_F(TestMockImageMap, AddInstance) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;

  MockLoadRequest mock_load_request;
  expect_load_request(mock_load_request, 0);

  MockListener mock_listener(this);

  std::unique_ptr<MockImageMap> mock_image_map{
    MockImageMap::create(m_local_io_ctx, &mock_threads, m_local_instance_id,
                         mock_listener)};

  C_SaferCond cond;
  mock_image_map->init(&cond);
  ASSERT_EQ(0, cond.wait());

  std::set<std::string> global_image_ids{
    "global id 1", "global id 2", "global id 3", "global id 4", "global id 5"
  };
  std::set<std::string> global_image_ids_ack(global_image_ids);

  // UPDATE_MAPPING+ACQUIRE
  expect_add_event(mock_threads);
  MockUpdateRequest mock_update_request;
  expect_update_request(mock_update_request, 0);
  expect_add_event(mock_threads);
  std::map<std::string, Context*> peer_ack_ctxs;
  listener_acquire_images(mock_listener, global_image_ids,
                          &peer_ack_ctxs);

  // initial image list
  mock_image_map->update_images("uuid1", std::move(global_image_ids), {});

  ASSERT_TRUE(wait_for_map_update(1));
  ASSERT_TRUE(wait_for_listener_notify(global_image_ids_ack.size()));

  // remote peer ACKs image acquire request
  remote_peer_ack_nowait(mock_image_map.get(), global_image_ids_ack, 0,
                         &peer_ack_ctxs);
  wait_for_scheduled_task();

  mock_image_map->update_instances_added({m_local_instance_id});

  std::set<std::string> shuffled_global_image_ids;

  // RELEASE+UPDATE_MAPPING+ACQUIRE
  expect_add_event(mock_threads);
  expect_listener_images_unmapped(mock_listener, 3, &shuffled_global_image_ids,
                                  &peer_ack_ctxs);

  mock_image_map->update_instances_added({"9876"});

  wait_for_scheduled_task();
  ASSERT_TRUE(wait_for_listener_notify(shuffled_global_image_ids.size()));

  update_map_and_acquire(mock_threads, mock_update_request,
                         mock_listener, shuffled_global_image_ids, 0,
                         &peer_ack_ctxs);
  remote_peer_ack_listener_wait(mock_image_map.get(), shuffled_global_image_ids,
                                0, &peer_ack_ctxs);

  // completion shuffle action for now (re)mapped images
  remote_peer_ack_nowait(mock_image_map.get(), shuffled_global_image_ids, 0,
                         &peer_ack_ctxs);

  wait_for_scheduled_task();
  ASSERT_EQ(0, when_shut_down(mock_image_map.get()));
}

TEST_F(TestMockImageMap, RemoveInstance) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;

  MockLoadRequest mock_load_request;
  expect_load_request(mock_load_request, 0);

  MockListener mock_listener(this);

  std::unique_ptr<MockImageMap> mock_image_map{
    MockImageMap::create(m_local_io_ctx, &mock_threads, m_local_instance_id,
                         mock_listener)};

  C_SaferCond cond;
  mock_image_map->init(&cond);
  ASSERT_EQ(0, cond.wait());

  std::set<std::string> global_image_ids{
    "global id 1", "global id 2", "global id 3", "global id 4", "global id 5"
  };
  std::set<std::string> global_image_ids_ack(global_image_ids);

  expect_add_event(mock_threads);

  // UPDATE_MAPPING+ACQUIRE
  MockUpdateRequest mock_update_request;
  expect_update_request(mock_update_request, 0);
  expect_add_event(mock_threads);
  std::map<std::string, Context*> peer_ack_ctxs;
  listener_acquire_images(mock_listener, global_image_ids,
                          &peer_ack_ctxs);

  // set initial image list
  mock_image_map->update_images("uuid1", std::move(global_image_ids), {});

  ASSERT_TRUE(wait_for_map_update(1));
  ASSERT_TRUE(wait_for_listener_notify(global_image_ids_ack.size()));

  // remote peer ACKs image acquire request -- completing action
  remote_peer_ack_nowait(mock_image_map.get(), global_image_ids_ack, 0,
                         &peer_ack_ctxs);
  wait_for_scheduled_task();

  mock_image_map->update_instances_added({m_local_instance_id});

  std::set<std::string> shuffled_global_image_ids;

  // RELEASE+UPDATE_MAPPING+ACQUIRE
  expect_add_event(mock_threads);
  expect_listener_images_unmapped(mock_listener, 3, &shuffled_global_image_ids,
                                  &peer_ack_ctxs);

  mock_image_map->update_instances_added({"9876"});

  wait_for_scheduled_task();
  ASSERT_TRUE(wait_for_listener_notify(shuffled_global_image_ids.size()));

  update_map_and_acquire(mock_threads, mock_update_request,
                         mock_listener, shuffled_global_image_ids, 0,
                         &peer_ack_ctxs);
  remote_peer_ack_listener_wait(mock_image_map.get(), shuffled_global_image_ids,
                                0, &peer_ack_ctxs);

  // completion shuffle action for now (re)mapped images
  remote_peer_ack_nowait(mock_image_map.get(), shuffled_global_image_ids, 0,
                         &peer_ack_ctxs);
  wait_for_scheduled_task();

  shuffled_global_image_ids.clear();

  // remove added instance
  expect_add_event(mock_threads);
  expect_listener_images_unmapped(mock_listener, 2, &shuffled_global_image_ids,
                                  &peer_ack_ctxs);

  mock_image_map->update_instances_removed({"9876"});

  wait_for_scheduled_task();
  ASSERT_TRUE(wait_for_listener_notify(shuffled_global_image_ids.size()));

  update_map_and_acquire(mock_threads, mock_update_request,
                         mock_listener, shuffled_global_image_ids, 0,
                         &peer_ack_ctxs);
  remote_peer_ack_listener_wait(mock_image_map.get(), shuffled_global_image_ids,
                                0, &peer_ack_ctxs);

  // completion shuffle action for now (re)mapped images
  remote_peer_ack_nowait(mock_image_map.get(), shuffled_global_image_ids, 0,
                         &peer_ack_ctxs);

  wait_for_scheduled_task();
  ASSERT_EQ(0, when_shut_down(mock_image_map.get()));
}

TEST_F(TestMockImageMap, AddInstancePingPongImageTest) {
  EXPECT_EQ(0, _rados->conf_set("rbd_mirror_image_policy_migration_throttle", "600"));

  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;

  std::set<std::string> global_image_ids{
    "global id 1", "global id 2", "global id 3", "global id 4", "global id 5",
    "global id 6", "global id 7", "global id 8", "global id 9", "global id 10",
    "global id 11", "global id 12", "global id 13", "global id 14"
  };

  std::map<std::string, cls::rbd::MirrorImageMap> image_mapping;
  for (auto& global_image_id : global_image_ids) {
    image_mapping[global_image_id] = {m_local_instance_id, {}, {}};
  }

  // ACQUIRE
  MockLoadRequest mock_load_request;
  EXPECT_CALL(mock_load_request, send()).WillOnce(
    Invoke([&mock_load_request, &image_mapping]() {
      *mock_load_request.image_map = image_mapping;
      mock_load_request.on_finish->complete(0);
    }));

  expect_add_event(mock_threads);
  MockListener mock_listener(this);
  std::map<std::string, Context*> peer_ack_ctxs;
  listener_acquire_images(mock_listener, global_image_ids,
                          &peer_ack_ctxs);

  std::unique_ptr<MockImageMap> mock_image_map{
    MockImageMap::create(m_local_io_ctx, &mock_threads, m_local_instance_id,
                         mock_listener)};

  C_SaferCond cond;
  mock_image_map->init(&cond);
  ASSERT_EQ(0, cond.wait());

  mock_image_map->update_instances_added({m_local_instance_id});

  std::set<std::string> global_image_ids_ack(global_image_ids);

  // remote peer ACKs image acquire request -- completing action
  ASSERT_TRUE(wait_for_listener_notify(global_image_ids_ack.size()));
  remote_peer_ack_nowait(mock_image_map.get(), global_image_ids_ack, 0,
                         &peer_ack_ctxs);
  wait_for_scheduled_task();

  // RELEASE+UPDATE_MAPPING+ACQUIRE
  expect_add_event(mock_threads);
  MockUpdateRequest mock_update_request;
  expect_update_request(mock_update_request, 0);
  expect_add_event(mock_threads);
  listener_acquire_images(mock_listener, global_image_ids,
                          &peer_ack_ctxs);

  // set initial image list
  mock_image_map->update_images("uuid1", std::move(global_image_ids), {});

  ASSERT_TRUE(wait_for_map_update(1));
  ASSERT_TRUE(wait_for_listener_notify(global_image_ids_ack.size()));

  // remote peer ACKs image acquire request -- completing action
  remote_peer_ack_nowait(mock_image_map.get(), global_image_ids_ack, 0,
                         &peer_ack_ctxs);
  wait_for_scheduled_task();

  std::set<std::string> shuffled_global_image_ids;

  // RELEASE+UPDATE_MAPPING+ACQUIRE
  expect_add_event(mock_threads);
  expect_listener_images_unmapped(mock_listener, 7, &shuffled_global_image_ids,
                                  &peer_ack_ctxs);

  mock_image_map->update_instances_added({"9876"});

  wait_for_scheduled_task();
  ASSERT_TRUE(wait_for_listener_notify(shuffled_global_image_ids.size()));

  update_map_and_acquire(mock_threads, mock_update_request,
                         mock_listener, shuffled_global_image_ids, 0,
                         &peer_ack_ctxs);
  remote_peer_ack_listener_wait(mock_image_map.get(), shuffled_global_image_ids,
                                0, &peer_ack_ctxs);

  // completion shuffle action for now (re)mapped images
  remote_peer_ack_nowait(mock_image_map.get(), shuffled_global_image_ids, 0,
                         &peer_ack_ctxs);
  wait_for_scheduled_task();

  std::set<std::string> migrated_global_image_ids(shuffled_global_image_ids);
  shuffled_global_image_ids.clear();

  // RELEASE+UPDATE_MAPPING+ACQUIRE
  expect_add_event(mock_threads);
  expect_listener_images_unmapped(mock_listener, 3, &shuffled_global_image_ids,
                                  &peer_ack_ctxs);

  // add another instance
  mock_image_map->update_instances_added({"5432"});

  wait_for_scheduled_task();
  ASSERT_TRUE(wait_for_listener_notify(shuffled_global_image_ids.size()));

  update_map_and_acquire(mock_threads, mock_update_request,
                         mock_listener, shuffled_global_image_ids, 0,
                         &peer_ack_ctxs);
  remote_peer_ack_listener_wait(mock_image_map.get(), shuffled_global_image_ids,
                                0, &peer_ack_ctxs);

  // completion shuffle action for now (re)mapped images
  remote_peer_ack_nowait(mock_image_map.get(), shuffled_global_image_ids, 0,
                         &peer_ack_ctxs);

  // shuffle set should be distinct
  std::set<std::string> reshuffled;
  std::set_intersection(migrated_global_image_ids.begin(), migrated_global_image_ids.end(),
                        shuffled_global_image_ids.begin(), shuffled_global_image_ids.end(),
                        std::inserter(reshuffled, reshuffled.begin()));
  ASSERT_TRUE(reshuffled.empty());

  wait_for_scheduled_task();
  ASSERT_EQ(0, when_shut_down(mock_image_map.get()));
}

TEST_F(TestMockImageMap, RemoveInstanceWithRemoveImage) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;

  MockLoadRequest mock_load_request;
  expect_load_request(mock_load_request, 0);

  MockListener mock_listener(this);

  std::unique_ptr<MockImageMap> mock_image_map{
    MockImageMap::create(m_local_io_ctx, &mock_threads, m_local_instance_id,
                         mock_listener)};

  C_SaferCond cond;
  mock_image_map->init(&cond);
  ASSERT_EQ(0, cond.wait());

  std::set<std::string> global_image_ids{
    "global id 1", "global id 2", "global id 3", "remote id 4",
  };
  std::set<std::string> global_image_ids_ack(global_image_ids);

  std::set<std::string> remove_global_image_ids{
    "global id 1"
  };
  std::set<std::string> remove_global_image_ids_ack(remove_global_image_ids);

  expect_add_event(mock_threads);
  // UPDATE_MAPPING+ACQUIRE
  MockUpdateRequest mock_update_request;
  expect_update_request(mock_update_request, 0);
  expect_add_event(mock_threads);
  std::map<std::string, Context*> peer_ack_ctxs;
  listener_acquire_images(mock_listener, global_image_ids,
                          &peer_ack_ctxs);

  // initial image list
  mock_image_map->update_images("uuid1", std::move(global_image_ids), {});

  ASSERT_TRUE(wait_for_map_update(1));
  ASSERT_TRUE(wait_for_listener_notify(global_image_ids_ack.size()));

  remote_peer_ack_nowait(mock_image_map.get(), global_image_ids_ack, 0,
                         &peer_ack_ctxs);
  wait_for_scheduled_task();

  mock_image_map->update_instances_added({m_local_instance_id});

  std::set<std::string> shuffled_global_image_ids;

  // RELEASE+UPDATE_MAPPING+ACQUIRE
  expect_add_event(mock_threads);
  expect_listener_images_unmapped(mock_listener, 2, &shuffled_global_image_ids,
                                  &peer_ack_ctxs);

  mock_image_map->update_instances_added({"9876"});

  wait_for_scheduled_task();
  ASSERT_TRUE(wait_for_listener_notify(shuffled_global_image_ids.size()));

  update_map_and_acquire(mock_threads, mock_update_request,
                         mock_listener, shuffled_global_image_ids, 0,
                         &peer_ack_ctxs);
  remote_peer_ack_listener_wait(mock_image_map.get(), shuffled_global_image_ids,
                                0, &peer_ack_ctxs);

  // completion shuffle action for now (re)mapped images
  remote_peer_ack_nowait(mock_image_map.get(), shuffled_global_image_ids, 0,
                         &peer_ack_ctxs);
  wait_for_scheduled_task();

  std::set<std::string> shuffled_global_image_ids_ack(shuffled_global_image_ids);

  // RELEASE

  std::map<std::string, Context*> peer_ack_remove_ctxs;
  listener_remove_images(mock_listener, "uuid1", shuffled_global_image_ids,
                         &peer_ack_remove_ctxs);
  expect_add_event(mock_threads);
  listener_release_images(mock_listener, shuffled_global_image_ids,
                          &peer_ack_ctxs);
  expect_add_event(mock_threads);
  expect_update_request(mock_update_request, 0);
  expect_add_event(mock_threads);
  expect_update_request(mock_update_request, 0);

  mock_image_map->update_images("uuid1", {}, std::move(shuffled_global_image_ids));
  ASSERT_TRUE(wait_for_listener_notify(shuffled_global_image_ids_ack.size() * 2));

  // instance failed -- update policy for instance removal
  mock_image_map->update_instances_removed({"9876"});

  remote_peer_ack_nowait(mock_image_map.get(), shuffled_global_image_ids,
                         -ENOENT, &peer_ack_remove_ctxs);
  remote_peer_ack_wait(mock_image_map.get(), shuffled_global_image_ids,
                       -EBLACKLISTED, &peer_ack_ctxs);

  wait_for_scheduled_task();
  ASSERT_EQ(0, when_shut_down(mock_image_map.get()));
}

TEST_F(TestMockImageMap, AddErrorAndRemoveImage) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;

  MockLoadRequest mock_load_request;
  expect_load_request(mock_load_request, 0);

  MockListener mock_listener(this);

  std::unique_ptr<MockImageMap> mock_image_map{
    MockImageMap::create(m_local_io_ctx, &mock_threads, m_local_instance_id,
                         mock_listener)};

  C_SaferCond cond;
  mock_image_map->init(&cond);
  ASSERT_EQ(0, cond.wait());

  mock_image_map->update_instances_added({m_local_instance_id});

  std::set<std::string> global_image_ids{
    "global id 1", "global id 2", "global id 3", "remote id 4",
  };
  std::set<std::string> global_image_ids_ack(global_image_ids);

  // UPDATE_MAPPING+ACQUIRE
  expect_add_event(mock_threads);
  MockUpdateRequest mock_update_request;
  expect_update_request(mock_update_request, 0);
  expect_add_event(mock_threads);
  std::map<std::string, Context*> peer_ack_ctxs;
  listener_acquire_images(mock_listener, global_image_ids,
                          &peer_ack_ctxs);

  // initial image list
  mock_image_map->update_images("uuid1", std::move(global_image_ids), {});

  ASSERT_TRUE(wait_for_map_update(1));
  ASSERT_TRUE(wait_for_listener_notify(global_image_ids_ack.size()));

  // remote peer ACKs image acquire request
  remote_peer_ack_nowait(mock_image_map.get(), global_image_ids_ack, 0,
                         &peer_ack_ctxs);
  wait_for_scheduled_task();

  std::set<std::string> shuffled_global_image_ids;

  // RELEASE+UPDATE_MAPPING+ACQUIRE
  expect_add_event(mock_threads);
  expect_listener_images_unmapped(mock_listener, 2, &shuffled_global_image_ids,
                                  &peer_ack_ctxs);

  mock_image_map->update_instances_added({"9876"});

  wait_for_scheduled_task();
  ASSERT_TRUE(wait_for_listener_notify(shuffled_global_image_ids.size()));

  update_map_and_acquire(mock_threads, mock_update_request,
                         mock_listener, shuffled_global_image_ids, 0,
                         &peer_ack_ctxs);
  remote_peer_ack_listener_wait(mock_image_map.get(), shuffled_global_image_ids,
                                0, &peer_ack_ctxs);
  wait_for_scheduled_task();

  mock_image_map->update_instances_removed({"9876"});

  std::set<std::string> released_global_image_ids;
  std::map<std::string, Context*> release_peer_ack_ctxs;
  expect_add_event(mock_threads);
  expect_listener_images_unmapped(mock_listener, 1, &released_global_image_ids,
                                  &release_peer_ack_ctxs);
  expect_add_event(mock_threads);
  expect_listener_images_unmapped(mock_listener, 1, &released_global_image_ids,
                                  &release_peer_ack_ctxs);

  // instance blacklisted -- ACQUIRE request fails
  remote_peer_ack_nowait(mock_image_map.get(), shuffled_global_image_ids,
                         -EBLACKLISTED, &peer_ack_ctxs);
  ASSERT_TRUE(wait_for_listener_notify(shuffled_global_image_ids.size()));

  std::map<std::string, Context*> remap_peer_ack_ctxs;
  update_map_and_acquire(mock_threads, mock_update_request,
                         mock_listener, shuffled_global_image_ids, 0,
                         &remap_peer_ack_ctxs);

  // instance blacklisted -- RELEASE request fails
  remote_peer_ack_listener_wait(mock_image_map.get(), shuffled_global_image_ids,
                                -ENOENT, &release_peer_ack_ctxs);
  wait_for_scheduled_task();

  // new peer acks acquire request
  remote_peer_ack_nowait(mock_image_map.get(), shuffled_global_image_ids, 0,
                         &remap_peer_ack_ctxs);
  wait_for_scheduled_task();

  std::set<std::string> shuffled_global_image_ids_ack(shuffled_global_image_ids);

  // remove image
  std::map<std::string, Context*> peer_ack_remove_ctxs;
  listener_remove_images(mock_listener, "uuid1", shuffled_global_image_ids,
                         &peer_ack_remove_ctxs);
  expect_add_event(mock_threads);
  listener_release_images(mock_listener, shuffled_global_image_ids,
                          &peer_ack_ctxs);
  update_map_request(mock_threads, mock_update_request, shuffled_global_image_ids, 0);

  mock_image_map->update_images("uuid1", {}, std::move(shuffled_global_image_ids));
  ASSERT_TRUE(wait_for_listener_notify(shuffled_global_image_ids_ack.size() * 2));

  remote_peer_ack_nowait(mock_image_map.get(), shuffled_global_image_ids_ack, 0,
                         &peer_ack_remove_ctxs);
  remote_peer_ack_wait(mock_image_map.get(), shuffled_global_image_ids_ack, 0,
                       &peer_ack_ctxs);

  wait_for_scheduled_task();
  ASSERT_EQ(0, when_shut_down(mock_image_map.get()));
}

TEST_F(TestMockImageMap, MirrorUUIDUpdated) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;

  MockLoadRequest mock_load_request;
  expect_load_request(mock_load_request, 0);

  MockListener mock_listener(this);

  std::unique_ptr<MockImageMap> mock_image_map{
    MockImageMap::create(m_local_io_ctx, &mock_threads, m_local_instance_id,
                         mock_listener)};

  C_SaferCond cond;
  mock_image_map->init(&cond);
  ASSERT_EQ(0, cond.wait());

  // remote image set
  std::set<std::string> initial_remote_global_image_ids{
    "global id 1", "global id 2", "global id 3"
  };
  std::set<std::string> initial_remote_global_image_ids_ack(initial_remote_global_image_ids);

  // remote/local images to remove
  std::set<std::string> remote_removed_global_image_ids{
    "global id 1", "global id 2", "global id 3"
  };
  std::set<std::string> remote_removed_global_image_ids_ack(remote_removed_global_image_ids);

  std::set<std::string> remote_added_global_image_ids{
    "global id 1", "global id 2", "global id 3"
  };
  std::set<std::string> remote_added_global_image_ids_ack(remote_added_global_image_ids);

  // UPDATE_MAPPING+ACQUIRE
  expect_add_event(mock_threads);
  MockUpdateRequest mock_update_request;
  expect_update_request(mock_update_request, 0);
  expect_add_event(mock_threads);
  std::map<std::string, Context*> peer_ack_ctxs;
  listener_acquire_images(mock_listener, initial_remote_global_image_ids,
                          &peer_ack_ctxs);

  // initial remote image list
  mock_image_map->update_images("uuid1", std::move(initial_remote_global_image_ids), {});

  ASSERT_TRUE(wait_for_map_update(1));
  ASSERT_TRUE(wait_for_listener_notify(initial_remote_global_image_ids_ack.size()));

  // remote peer ACKs image acquire request
  remote_peer_ack_nowait(mock_image_map.get(),
                         initial_remote_global_image_ids_ack, 0,
                         &peer_ack_ctxs);
  wait_for_scheduled_task();

  // RELEASE+REMOVE_MAPPING
  std::map<std::string, Context*> peer_remove_ack_ctxs;
  listener_remove_images(mock_listener, "uuid1", remote_removed_global_image_ids,
                         &peer_remove_ack_ctxs);
  expect_add_event(mock_threads);
  listener_release_images(mock_listener, remote_removed_global_image_ids,
                          &peer_ack_ctxs);
  update_map_request(mock_threads, mock_update_request, remote_removed_global_image_ids, 0);

  mock_image_map->update_images("uuid1", {}, std::move(remote_removed_global_image_ids));
  ASSERT_TRUE(wait_for_listener_notify(remote_removed_global_image_ids_ack.size() * 2));

  remote_peer_ack_nowait(mock_image_map.get(),
                         remote_removed_global_image_ids_ack, 0,
                         &peer_remove_ack_ctxs);
  remote_peer_ack_wait(mock_image_map.get(),
                       remote_removed_global_image_ids_ack, 0,
                       &peer_ack_ctxs);

  // UPDATE_MAPPING+ACQUIRE
  expect_add_event(mock_threads);
  expect_update_request(mock_update_request, 0);
  expect_add_event(mock_threads);
  listener_acquire_images(mock_listener, remote_added_global_image_ids,
                          &peer_ack_ctxs);

  mock_image_map->update_images("uuid2", std::move(remote_added_global_image_ids), {});

  ASSERT_TRUE(wait_for_map_update(1));
  ASSERT_TRUE(wait_for_listener_notify(remote_added_global_image_ids_ack.size()));

  // remote peer ACKs image acquire request
  remote_peer_ack_nowait(mock_image_map.get(),
                         remote_added_global_image_ids_ack, 0,
                         &peer_ack_ctxs);

  wait_for_scheduled_task();
  ASSERT_EQ(0, when_shut_down(mock_image_map.get()));
}

TEST_F(TestMockImageMap, RebalanceImageMap) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;

  MockLoadRequest mock_load_request;
  expect_load_request(mock_load_request, 0);

  MockListener mock_listener(this);

  std::unique_ptr<MockImageMap> mock_image_map{
    MockImageMap::create(m_local_io_ctx, &mock_threads, m_local_instance_id,
                         mock_listener)};

  C_SaferCond cond;
  mock_image_map->init(&cond);
  ASSERT_EQ(0, cond.wait());

  std::set<std::string> global_image_ids{
    "global id 1", "global id 2", "global id 3", "global id 4", "global id 5",
    "global id 6", "global id 7", "global id 8", "global id 9", "global id 10",
  };
  std::set<std::string> global_image_ids_ack(global_image_ids);

  // UPDATE_MAPPING+ACQUIRE
  expect_add_event(mock_threads);
  MockUpdateRequest mock_update_request;
  expect_update_request(mock_update_request, 0);
  expect_add_event(mock_threads);
  std::map<std::string, Context*> peer_ack_ctxs;
  listener_acquire_images(mock_listener, global_image_ids,
                          &peer_ack_ctxs);

  // initial image list
  mock_image_map->update_images("", std::move(global_image_ids), {});

  ASSERT_TRUE(wait_for_map_update(1));
  ASSERT_TRUE(wait_for_listener_notify(global_image_ids_ack.size()));

  // remote peer ACKs image acquire request
  remote_peer_ack_nowait(mock_image_map.get(), global_image_ids_ack, 0,
                         &peer_ack_ctxs);
  wait_for_scheduled_task();

  mock_image_map->update_instances_added({m_local_instance_id});

  std::set<std::string> shuffled_global_image_ids;

  // RELEASE+UPDATE_MAPPING+ACQUIRE
  expect_add_event(mock_threads);
  expect_listener_images_unmapped(mock_listener, 5, &shuffled_global_image_ids,
                                  &peer_ack_ctxs);

  mock_image_map->update_instances_added({"9876"});

  wait_for_scheduled_task();
  ASSERT_TRUE(wait_for_listener_notify(shuffled_global_image_ids.size()));

  update_map_and_acquire(mock_threads, mock_update_request,
                         mock_listener, shuffled_global_image_ids, 0,
                         &peer_ack_ctxs);
  remote_peer_ack_listener_wait(mock_image_map.get(), shuffled_global_image_ids,
                                0, &peer_ack_ctxs);

  // completion shuffle action for now (re)mapped images
  remote_peer_ack_nowait(mock_image_map.get(), shuffled_global_image_ids, 0,
                         &peer_ack_ctxs);

  wait_for_scheduled_task();

  // remove all shuffled images -- make way for rebalance
  std::set<std::string> shuffled_global_image_ids_ack(shuffled_global_image_ids);

  // RELEASE+REMOVE_MAPPING
  expect_add_event(mock_threads);
  listener_release_images(mock_listener, shuffled_global_image_ids,
                          &peer_ack_ctxs);
  update_map_request(mock_threads, mock_update_request, shuffled_global_image_ids,
                     0);

  mock_image_map->update_images("", {}, std::move(shuffled_global_image_ids));
  ASSERT_TRUE(wait_for_listener_notify(shuffled_global_image_ids_ack.size()));

  remote_peer_ack_wait(mock_image_map.get(), shuffled_global_image_ids_ack, 0,
                       &peer_ack_ctxs);
  wait_for_scheduled_task();

  shuffled_global_image_ids.clear();
  shuffled_global_image_ids_ack.clear();

  std::set<std::string> new_global_image_ids = {
    "global id 11"
  };
  std::set<std::string> new_global_image_ids_ack(new_global_image_ids);

  expect_add_event(mock_threads);
  expect_update_request(mock_update_request, 0);
  expect_add_event(mock_threads);
  listener_acquire_images(mock_listener, new_global_image_ids, &peer_ack_ctxs);

  expect_rebalance_event(mock_threads); // rebalance task
  expect_add_event(mock_threads);       // update task scheduled by
                                        // rebalance task
  expect_listener_images_unmapped(mock_listener, 2, &shuffled_global_image_ids,
                                  &peer_ack_ctxs);

  mock_image_map->update_images("", std::move(new_global_image_ids), {});

  ASSERT_TRUE(wait_for_map_update(1));
  ASSERT_TRUE(wait_for_listener_notify(new_global_image_ids_ack.size()));

  // set rebalance interval
  CephContext *cct = reinterpret_cast<CephContext *>(m_local_io_ctx.cct());
  cct->_conf.set_val("rbd_mirror_image_policy_rebalance_timeout", "5");
  remote_peer_ack_nowait(mock_image_map.get(), new_global_image_ids_ack, 0,
                         &peer_ack_ctxs);

  wait_for_scheduled_task();
  ASSERT_TRUE(wait_for_listener_notify(shuffled_global_image_ids.size()));

  update_map_and_acquire(mock_threads, mock_update_request,
                         mock_listener, shuffled_global_image_ids, 0,
                         &peer_ack_ctxs);
  remote_peer_ack_listener_wait(mock_image_map.get(), shuffled_global_image_ids,
                                0, &peer_ack_ctxs);

  // completion shuffle action for now (re)mapped images
  remote_peer_ack_nowait(mock_image_map.get(), shuffled_global_image_ids, 0,
                         &peer_ack_ctxs);

  wait_for_scheduled_task();
  ASSERT_EQ(0, when_shut_down(mock_image_map.get()));
}

} // namespace mirror
} // namespace rbd
