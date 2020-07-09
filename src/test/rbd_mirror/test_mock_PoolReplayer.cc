// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Config.h"
#include "librbd/api/Namespace.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librados_test_stub/MockTestMemCluster.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "test/rbd_mirror/test_mock_fixture.h"
#include "test/rbd_mirror/mock/MockContextWQ.h"
#include "test/rbd_mirror/mock/MockSafeTimer.h"
#include "tools/rbd_mirror/Throttler.h"
#include "tools/rbd_mirror/LeaderWatcher.h"
#include "tools/rbd_mirror/NamespaceReplayer.h"
#include "tools/rbd_mirror/PoolMetaCache.h"
#include "tools/rbd_mirror/PoolReplayer.h"
#include "tools/rbd_mirror/RemotePoolPoller.h"
#include "tools/rbd_mirror/ServiceDaemon.h"
#include "tools/rbd_mirror/Threads.h"
#include "common/Formatter.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace api {

template <>
class Config<MockTestImageCtx> {
public:
  static void apply_pool_overrides(librados::IoCtx& io_ctx,
                                   ConfigProxy* config_proxy) {
  }
};

template <>
class Namespace<MockTestImageCtx> {
public:
  static Namespace* s_instance;

  static int list(librados::IoCtx& io_ctx, std::vector<std::string> *names) {
    if (s_instance) {
      return s_instance->list(names);
    }

    return 0;
  }

  Namespace() {
    s_instance = this;
  }

  void add(const std::string &name) {
    std::lock_guard locker{m_lock};

    m_names.insert(name);
  }

  void remove(const std::string &name) {
    std::lock_guard locker{m_lock};

    m_names.erase(name);
  }

  void clear() {
    std::lock_guard locker{m_lock};

    m_names.clear();
  }

private:
  ceph::mutex m_lock = ceph::make_mutex("Namespace");
  std::set<std::string> m_names;

  int list(std::vector<std::string> *names) {
    std::lock_guard locker{m_lock};

    names->clear();
    names->insert(names->begin(), m_names.begin(), m_names.end());
    return 0;
  }
};

Namespace<librbd::MockTestImageCtx>* Namespace<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace api

} // namespace librbd

namespace rbd {
namespace mirror {

template <>
struct Throttler<librbd::MockTestImageCtx> {
  static Throttler* s_instance;

  static Throttler *create(
      CephContext *cct,
      const std::string &max_concurrent_ops_config_param_name) {
    return s_instance;
  }

  Throttler() {
    ceph_assert(s_instance == nullptr);
    s_instance = this;
  }

  virtual ~Throttler() {
    ceph_assert(s_instance == this);
    s_instance = nullptr;
  }

  MOCK_METHOD1(print_status, void(Formatter*));
};

Throttler<librbd::MockTestImageCtx>* Throttler<librbd::MockTestImageCtx>::s_instance = nullptr;

template <>
struct NamespaceReplayer<librbd::MockTestImageCtx> {
  static std::map<std::string, NamespaceReplayer *> s_instances;

  static NamespaceReplayer *create(
      const std::string &name,
      librados::IoCtx &local_ioctx,
      librados::IoCtx &remote_ioctx,
      const std::string &local_mirror_uuid,
      const std::string& local_mirror_peer_uuid,
      const RemotePoolMeta& remote_pool_meta,
      Threads<librbd::MockTestImageCtx> *threads,
      Throttler<librbd::MockTestImageCtx> *image_sync_throttler,
      Throttler<librbd::MockTestImageCtx> *image_deletion_throttler,
      ServiceDaemon<librbd::MockTestImageCtx> *service_daemon,
      journal::CacheManagerHandler *cache_manager_handler,
      PoolMetaCache* pool_meta_cache) {
    ceph_assert(s_instances.count(name));
    auto namespace_replayer = s_instances[name];
    s_instances.erase(name);
    return namespace_replayer;
  }

  MOCK_METHOD0(is_blacklisted, bool());
  MOCK_METHOD0(get_instance_id, std::string());

  MOCK_METHOD1(init, void(Context*));
  MOCK_METHOD1(shut_down, void(Context*));

  MOCK_METHOD1(handle_acquire_leader, void(Context *));
  MOCK_METHOD1(handle_release_leader, void(Context *));
  MOCK_METHOD1(handle_update_leader, void(const std::string &));
  MOCK_METHOD1(handle_instances_added, void(const std::vector<std::string> &));
  MOCK_METHOD1(handle_instances_removed, void(const std::vector<std::string> &));

  MOCK_METHOD1(print_status, void(Formatter*));
  MOCK_METHOD0(start, void());
  MOCK_METHOD0(stop, void());
  MOCK_METHOD0(restart, void());
  MOCK_METHOD0(flush, void());

  NamespaceReplayer(const std::string &name = "") {
    ceph_assert(!s_instances.count(name));
    s_instances[name] = this;
  }
};

std::map<std::string, NamespaceReplayer<librbd::MockTestImageCtx> *> NamespaceReplayer<librbd::MockTestImageCtx>::s_instances;

template<>
struct LeaderWatcher<librbd::MockTestImageCtx> {
  static LeaderWatcher* s_instance;
  leader_watcher::Listener* listener = nullptr;

  static LeaderWatcher *create(Threads<librbd::MockTestImageCtx> *threads,
                               librados::IoCtx &ioctx,
                               leader_watcher::Listener* listener) {
    ceph_assert(s_instance != nullptr);
    s_instance->listener = listener;
    return s_instance;
  }

  MOCK_METHOD0(is_blacklisted, bool());
  MOCK_METHOD0(is_leader, bool());
  MOCK_METHOD0(release_leader, void());

  MOCK_METHOD1(get_leader_instance_id, bool(std::string*));
  MOCK_METHOD1(list_instances, void(std::vector<std::string>*));

  MOCK_METHOD0(init, int());
  MOCK_METHOD0(shut_down, int());

  LeaderWatcher() {
    s_instance = this;
  }

};

LeaderWatcher<librbd::MockTestImageCtx>* LeaderWatcher<librbd::MockTestImageCtx>::s_instance = nullptr;

template<>
struct RemotePoolPoller<librbd::MockTestImageCtx> {
  static RemotePoolPoller* s_instance;

  remote_pool_poller::Listener* listener = nullptr;

  static RemotePoolPoller* create(
      Threads<librbd::MockTestImageCtx>* threads,
      librados::IoCtx& remote_io_ctx,
      const std::string& local_site_name,
      const std::string& local_mirror_uuid,
      remote_pool_poller::Listener& listener) {
    ceph_assert(s_instance != nullptr);
    s_instance->listener = &listener;
    return s_instance;
  }

  MOCK_METHOD1(init, void(Context*));
  MOCK_METHOD1(shut_down, void(Context*));

  RemotePoolPoller() {
    s_instance = this;
  }
};

RemotePoolPoller<librbd::MockTestImageCtx>* RemotePoolPoller<librbd::MockTestImageCtx>::s_instance = nullptr;

template<>
struct ServiceDaemon<librbd::MockTestImageCtx> {
  MOCK_METHOD2(add_namespace, void(int64_t, const std::string &));
  MOCK_METHOD2(remove_namespace, void(int64_t, const std::string &));

  MOCK_METHOD3(add_or_update_attribute,
               void(int64_t, const std::string&,
                    const service_daemon::AttributeValue&));
  MOCK_METHOD2(remove_attribute,
               void(int64_t, const std::string&));

  MOCK_METHOD4(add_or_update_callout, uint64_t(int64_t, uint64_t,
                                               service_daemon::CalloutLevel,
                                               const std::string&));
  MOCK_METHOD2(remove_callout, void(int64_t, uint64_t));
};

template <>
struct Threads<librbd::MockTestImageCtx> {
  MockSafeTimer *timer;
  ceph::mutex &timer_lock;
  ceph::condition_variable timer_cond;

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

} // namespace mirror
} // namespace rbd

// template definitions
#include "tools/rbd_mirror/PoolReplayer.cc"

namespace rbd {
namespace mirror {

using ::testing::_;
using ::testing::AtLeast;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockPoolReplayer : public TestMockFixture {
public:
  typedef librbd::api::Namespace<librbd::MockTestImageCtx> MockNamespace;
  typedef PoolReplayer<librbd::MockTestImageCtx> MockPoolReplayer;
  typedef Throttler<librbd::MockTestImageCtx> MockThrottler;
  typedef NamespaceReplayer<librbd::MockTestImageCtx> MockNamespaceReplayer;
  typedef RemotePoolPoller<librbd::MockTestImageCtx> MockRemotePoolPoller;
  typedef LeaderWatcher<librbd::MockTestImageCtx> MockLeaderWatcher;
  typedef ServiceDaemon<librbd::MockTestImageCtx> MockServiceDaemon;
  typedef Threads<librbd::MockTestImageCtx> MockThreads;

  void expect_work_queue(MockThreads &mock_threads) {
    EXPECT_CALL(*mock_threads.work_queue, queue(_, _))
      .WillRepeatedly(Invoke([this](Context *ctx, int r) {
            m_threads->work_queue->queue(ctx, r);
          }));
  }

  void expect_connect(librados::MockTestMemCluster& mock_cluster,
                      librados::MockTestMemRadosClient* mock_rados_client,
                      const std::string& cluster_name, CephContext** cct_ref) {
    EXPECT_CALL(mock_cluster, create_rados_client(_))
      .WillOnce(Invoke([cluster_name, mock_rados_client, cct_ref](CephContext* cct) {
                  EXPECT_EQ(cluster_name, cct->_conf->cluster);
                  if (cct_ref != nullptr) {
                    cct->get();
                    *cct_ref = cct;
                  }
                  return mock_rados_client;
                }));
  }

  void expect_create_ioctx(librados::MockTestMemRadosClient* mock_rados_client,
                           librados::MockTestMemIoCtxImpl* mock_io_ctx_impl) {
    EXPECT_CALL(*mock_rados_client, create_ioctx(_, _))
      .WillOnce(Invoke([mock_io_ctx_impl](int64_t id, const std::string& name) {
                  return mock_io_ctx_impl;
                }));
  }

  void expect_mirror_uuid_get(librados::MockTestMemIoCtxImpl *io_ctx_impl,
                              const std::string &uuid, int r) {
    bufferlist out_bl;
    encode(uuid, out_bl);

    EXPECT_CALL(*io_ctx_impl,
                exec(RBD_MIRRORING, _, StrEq("rbd"), StrEq("mirror_uuid_get"),
                     _, _, _, _))
      .WillOnce(DoAll(WithArg<5>(Invoke([out_bl](bufferlist *bl) {
                          *bl = out_bl;
                        })),
                      Return(r)));
  }

  void expect_mirror_mode_get(librados::MockTestMemIoCtxImpl *io_ctx_impl,
                              cls::rbd::MirrorMode mirror_mode, int r) {
    bufferlist out_bl;
    encode(mirror_mode, out_bl);

    EXPECT_CALL(*io_ctx_impl,
                exec(RBD_MIRRORING, _, StrEq("rbd"), StrEq("mirror_mode_get"),
                     _, _, _, _))
      .WillOnce(DoAll(WithArg<5>(Invoke([out_bl](bufferlist *bl) {
                                          *bl = out_bl;
                                        })),
          Return(r)));
  }

  void expect_mirror_mode_get(librados::MockTestMemIoCtxImpl *io_ctx_impl) {
    EXPECT_CALL(*io_ctx_impl,
                exec(RBD_MIRRORING, _, StrEq("rbd"), StrEq("mirror_mode_get"),
                     _, _, _, _))
      .WillRepeatedly(DoAll(WithArg<5>(Invoke([](bufferlist *bl) {
                encode(cls::rbd::MIRROR_MODE_POOL, *bl);
              })),
          Return(0)));
  }

  void expect_leader_watcher_init(MockLeaderWatcher& mock_leader_watcher,
                                  int r) {
    EXPECT_CALL(mock_leader_watcher, init())
      .WillOnce(Return(r));
  }

  void expect_leader_watcher_shut_down(MockLeaderWatcher& mock_leader_watcher) {
    EXPECT_CALL(mock_leader_watcher, shut_down());
  }

  void expect_leader_watcher_get_leader_instance_id(
      MockLeaderWatcher& mock_leader_watcher) {
    EXPECT_CALL(mock_leader_watcher, get_leader_instance_id(_))
      .WillRepeatedly(Return(true));
  }

  void expect_leader_watcher_list_instances(
      MockLeaderWatcher& mock_leader_watcher) {
    EXPECT_CALL(mock_leader_watcher, list_instances(_))
      .Times(AtLeast(0));
  }

  void expect_remote_pool_poller_init(
      MockRemotePoolPoller& mock_remote_pool_poller,
      const RemotePoolMeta& remote_pool_meta, int r) {
    EXPECT_CALL(mock_remote_pool_poller, init(_))
      .WillOnce(Invoke(
                  [this, &mock_remote_pool_poller, remote_pool_meta, r]
                  (Context* ctx) {
                    if (r >= 0) {
                      mock_remote_pool_poller.listener->handle_updated(
                        remote_pool_meta);
                    }

                    m_threads->work_queue->queue(ctx, r);
                }));
  }

  void expect_remote_pool_poller_shut_down(
      MockRemotePoolPoller& mock_remote_pool_poller, int r) {
    EXPECT_CALL(mock_remote_pool_poller, shut_down(_))
      .WillOnce(Invoke(
                  [this, r](Context* ctx) {
                    m_threads->work_queue->queue(ctx, r);
                }));
  }

  void expect_leader_watcher_is_blacklisted(
      MockLeaderWatcher &mock_leader_watcher, bool blacklisted) {
    EXPECT_CALL(mock_leader_watcher, is_blacklisted())
      .WillRepeatedly(Return(blacklisted));
  }

  void expect_namespace_replayer_is_blacklisted(
      MockNamespaceReplayer &mock_namespace_replayer,
      bool blacklisted) {
    EXPECT_CALL(mock_namespace_replayer, is_blacklisted())
      .WillRepeatedly(Return(blacklisted));
  }

  void expect_namespace_replayer_get_instance_id(
      MockNamespaceReplayer &mock_namespace_replayer,
      const std::string &instance_id) {
    EXPECT_CALL(mock_namespace_replayer, get_instance_id())
      .WillOnce(Return(instance_id));
  }

  void expect_namespace_replayer_init(
      MockNamespaceReplayer &mock_namespace_replayer, int r,
      Context *on_init = nullptr) {

    EXPECT_CALL(mock_namespace_replayer, init(_))
      .WillOnce(Invoke([this, r, on_init](Context* ctx) {
                         m_threads->work_queue->queue(ctx, r);
                         if (on_init != nullptr) {
                           m_threads->work_queue->queue(on_init, r);
                         }
                       }));
  }

  void expect_namespace_replayer_shut_down(
      MockNamespaceReplayer &mock_namespace_replayer,
      Context *on_shut_down = nullptr) {
    EXPECT_CALL(mock_namespace_replayer, shut_down(_))
      .WillOnce(Invoke([this, on_shut_down](Context* ctx) {
                         m_threads->work_queue->queue(ctx);
                         if (on_shut_down != nullptr) {
                           m_threads->work_queue->queue(on_shut_down);
                         }
                       }));
  }

  void expect_namespace_replayer_handle_acquire_leader(
      MockNamespaceReplayer &mock_namespace_replayer, int r,
      Context *on_acquire = nullptr) {
    EXPECT_CALL(mock_namespace_replayer, handle_acquire_leader(_))
      .WillOnce(Invoke([this, r, on_acquire](Context* ctx) {
                         m_threads->work_queue->queue(ctx, r);
                         if (on_acquire != nullptr) {
                           m_threads->work_queue->queue(on_acquire, r);
                         }
                       }));
  }

  void expect_namespace_replayer_handle_release_leader(
      MockNamespaceReplayer &mock_namespace_replayer, int r,
      Context *on_release = nullptr) {
    EXPECT_CALL(mock_namespace_replayer, handle_release_leader(_))
      .WillOnce(Invoke([this, r, on_release](Context* ctx) {
                         m_threads->work_queue->queue(ctx, r);
                         if (on_release != nullptr) {
                           m_threads->work_queue->queue(on_release, r);
                         }
                       }));
  }

  void expect_namespace_replayer_handle_update_leader(
      MockNamespaceReplayer &mock_namespace_replayer,
      const std::string &leader_instance_id,
      Context *on_update = nullptr) {
    EXPECT_CALL(mock_namespace_replayer,
                handle_update_leader(leader_instance_id))
      .WillOnce(Invoke([on_update](const std::string &) {
                         if (on_update != nullptr) {
                           on_update->complete(0);
                         }
                       }));
  }

  void expect_namespace_replayer_handle_instances_added(
      MockNamespaceReplayer &mock_namespace_replayer) {
    EXPECT_CALL(mock_namespace_replayer, handle_instances_added(_));
  }

  void expect_namespace_replayer_handle_instances_removed(
      MockNamespaceReplayer &mock_namespace_replayer) {
    EXPECT_CALL(mock_namespace_replayer, handle_instances_removed(_));
  }

  void expect_service_daemon_add_namespace(
      MockServiceDaemon &mock_service_daemon,
      const std::string& namespace_name) {
    EXPECT_CALL(mock_service_daemon,
                add_namespace(m_local_io_ctx.get_id(), namespace_name));
  }

  void expect_service_daemon_remove_namespace(
      MockServiceDaemon &mock_service_daemon,
      const std::string& namespace_name) {
    EXPECT_CALL(mock_service_daemon,
                remove_namespace(m_local_io_ctx.get_id(), namespace_name));
  }

  void expect_service_daemon_add_or_update_attribute(
      MockServiceDaemon &mock_service_daemon, const std::string& key,
      const service_daemon::AttributeValue& value) {
    EXPECT_CALL(mock_service_daemon, add_or_update_attribute(_, key, value));
  }

  void expect_service_daemon_remove_attribute(
      MockServiceDaemon &mock_service_daemon, const std::string& key) {
    EXPECT_CALL(mock_service_daemon, remove_attribute(_, key));
  }

  void expect_service_daemon_add_or_update_instance_id_attribute(
      MockServiceDaemon &mock_service_daemon, const std::string &instance_id) {
    expect_service_daemon_add_or_update_attribute(
        mock_service_daemon, "instance_id", {instance_id});
  }

  PoolMetaCache m_pool_meta_cache{g_ceph_context};
};

TEST_F(TestMockPoolReplayer, ConfigKeyOverride) {
  PeerSpec peer_spec{"uuid", "cluster name", "client.name"};
  peer_spec.mon_host = "123";
  peer_spec.key = "234";

  auto mock_default_namespace_replayer = new MockNamespaceReplayer();
  expect_namespace_replayer_is_blacklisted(*mock_default_namespace_replayer,
                                           false);

  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  auto mock_leader_watcher = new MockLeaderWatcher();
  expect_leader_watcher_get_leader_instance_id(*mock_leader_watcher);
  expect_leader_watcher_is_blacklisted(*mock_leader_watcher, false);

  InSequence seq;

  auto& mock_cluster = get_mock_cluster();
  auto mock_local_rados_client = mock_cluster.do_create_rados_client(
    g_ceph_context);
  expect_connect(mock_cluster, mock_local_rados_client, "ceph", nullptr);

  auto mock_remote_rados_client = mock_cluster.do_create_rados_client(
    g_ceph_context);
  CephContext* remote_cct = nullptr;
  expect_connect(mock_cluster, mock_remote_rados_client, "cluster name",
                 &remote_cct);

  auto mock_local_io_ctx = mock_local_rados_client->do_create_ioctx(
    m_local_io_ctx.get_id(), m_local_io_ctx.get_pool_name());
  expect_create_ioctx(mock_local_rados_client, mock_local_io_ctx);

  expect_mirror_uuid_get(mock_local_io_ctx, "uuid", 0);
  auto mock_remote_pool_poller = new MockRemotePoolPoller();
  expect_remote_pool_poller_init(*mock_remote_pool_poller,
                                 {"remote mirror uuid", ""}, 0);
  expect_namespace_replayer_init(*mock_default_namespace_replayer, 0);
  expect_leader_watcher_init(*mock_leader_watcher, 0);

  MockServiceDaemon mock_service_daemon;
  std::string instance_id = stringify(mock_local_io_ctx->get_instance_id());
  expect_service_daemon_add_or_update_instance_id_attribute(
    mock_service_daemon, instance_id);

  MockPoolReplayer pool_replayer(&mock_threads, &mock_service_daemon, nullptr,
                                 &m_pool_meta_cache,
                                 m_local_io_ctx.get_id(), peer_spec, {});
  pool_replayer.init("siteA");

  ASSERT_TRUE(remote_cct != nullptr);
  ASSERT_EQ("123", remote_cct->_conf.get_val<std::string>("mon_host"));
  ASSERT_EQ("234", remote_cct->_conf.get_val<std::string>("key"));
  remote_cct->put();

  expect_leader_watcher_shut_down(*mock_leader_watcher);
  expect_namespace_replayer_shut_down(*mock_default_namespace_replayer);
  expect_remote_pool_poller_shut_down(*mock_remote_pool_poller, 0);

  pool_replayer.shut_down();
}

TEST_F(TestMockPoolReplayer, AcquireReleaseLeader) {
  PeerSpec peer_spec{"uuid", "cluster name", "client.name"};
  peer_spec.mon_host = "123";
  peer_spec.key = "234";

  auto mock_default_namespace_replayer = new MockNamespaceReplayer();
  expect_namespace_replayer_is_blacklisted(*mock_default_namespace_replayer,
                                           false);

  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  auto mock_leader_watcher = new MockLeaderWatcher();
  expect_leader_watcher_get_leader_instance_id(*mock_leader_watcher);
  expect_leader_watcher_list_instances(*mock_leader_watcher);
  expect_leader_watcher_is_blacklisted(*mock_leader_watcher, false);

  InSequence seq;

  auto& mock_cluster = get_mock_cluster();
  auto mock_local_rados_client = mock_cluster.do_create_rados_client(
    g_ceph_context);
  expect_connect(mock_cluster, mock_local_rados_client, "ceph", nullptr);

  auto mock_remote_rados_client = mock_cluster.do_create_rados_client(
    g_ceph_context);
  expect_connect(mock_cluster, mock_remote_rados_client, "cluster name",
                 nullptr);

  auto mock_local_io_ctx = mock_local_rados_client->do_create_ioctx(
    m_local_io_ctx.get_id(), m_local_io_ctx.get_pool_name());
  expect_create_ioctx(mock_local_rados_client, mock_local_io_ctx);

  expect_mirror_uuid_get(mock_local_io_ctx, "uuid", 0);
  auto mock_remote_pool_poller = new MockRemotePoolPoller();
  expect_remote_pool_poller_init(*mock_remote_pool_poller,
                                 {"remote mirror uuid", ""}, 0);
  expect_namespace_replayer_init(*mock_default_namespace_replayer, 0);
  expect_leader_watcher_init(*mock_leader_watcher, 0);

  MockServiceDaemon mock_service_daemon;
  std::string instance_id = stringify(mock_local_io_ctx->get_instance_id());
  expect_service_daemon_add_or_update_instance_id_attribute(
    mock_service_daemon, instance_id);

  MockPoolReplayer pool_replayer(&mock_threads, &mock_service_daemon, nullptr,
                                 &m_pool_meta_cache,
                                 m_local_io_ctx.get_id(), peer_spec, {});
  pool_replayer.init("siteA");

  expect_service_daemon_add_or_update_attribute(
      mock_service_daemon, SERVICE_DAEMON_LEADER_KEY, true);
  expect_namespace_replayer_handle_acquire_leader(
      *mock_default_namespace_replayer, 0);

  C_SaferCond on_acquire;
  mock_leader_watcher->listener->post_acquire_handler(&on_acquire);
  ASSERT_EQ(0, on_acquire.wait());

  expect_service_daemon_remove_attribute(mock_service_daemon,
                                         SERVICE_DAEMON_LEADER_KEY);
  expect_namespace_replayer_handle_release_leader(
      *mock_default_namespace_replayer, 0);

  C_SaferCond on_release;
  mock_leader_watcher->listener->pre_release_handler(&on_release);
  ASSERT_EQ(0, on_release.wait());

  expect_leader_watcher_shut_down(*mock_leader_watcher);
  expect_namespace_replayer_shut_down(*mock_default_namespace_replayer);
  expect_remote_pool_poller_shut_down(*mock_remote_pool_poller, 0);

  pool_replayer.shut_down();
}

TEST_F(TestMockPoolReplayer, Namespaces) {
  PeerSpec peer_spec{"uuid", "cluster name", "client.name"};
  peer_spec.mon_host = "123";
  peer_spec.key = "234";

  g_ceph_context->_conf.set_val(
      "rbd_mirror_pool_replayers_refresh_interval", "1");

  MockNamespace mock_namespace;

  auto mock_default_namespace_replayer = new MockNamespaceReplayer();
  expect_namespace_replayer_is_blacklisted(*mock_default_namespace_replayer,
                                           false);

  auto mock_ns1_namespace_replayer = new MockNamespaceReplayer("ns1");
  expect_namespace_replayer_is_blacklisted(*mock_ns1_namespace_replayer,
                                           false);

  auto mock_ns2_namespace_replayer = new MockNamespaceReplayer("ns2");
  expect_namespace_replayer_is_blacklisted(*mock_ns2_namespace_replayer,
                                           false);

  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  auto mock_leader_watcher = new MockLeaderWatcher();
  expect_leader_watcher_get_leader_instance_id(*mock_leader_watcher);
  expect_leader_watcher_list_instances(*mock_leader_watcher);
  expect_leader_watcher_is_blacklisted(*mock_leader_watcher, false);

  auto& mock_cluster = get_mock_cluster();
  auto mock_local_rados_client = mock_cluster.do_create_rados_client(
      g_ceph_context);
  auto mock_local_io_ctx = mock_local_rados_client->do_create_ioctx(
      m_local_io_ctx.get_id(), m_local_io_ctx.get_pool_name());
  auto mock_remote_rados_client = mock_cluster.do_create_rados_client(
      g_ceph_context);

  expect_mirror_mode_get(mock_local_io_ctx);

  InSequence seq;

  expect_connect(mock_cluster, mock_local_rados_client, "ceph", nullptr);
  expect_connect(mock_cluster, mock_remote_rados_client, "cluster name",
                 nullptr);
  expect_create_ioctx(mock_local_rados_client, mock_local_io_ctx);
  expect_mirror_uuid_get(mock_local_io_ctx, "uuid", 0);
  auto mock_remote_pool_poller = new MockRemotePoolPoller();
  expect_remote_pool_poller_init(*mock_remote_pool_poller,
                                 {"remote mirror uuid", ""}, 0);
  expect_namespace_replayer_init(*mock_default_namespace_replayer, 0);
  expect_leader_watcher_init(*mock_leader_watcher, 0);

  MockServiceDaemon mock_service_daemon;
  std::string instance_id = stringify(mock_local_io_ctx->get_instance_id());
  expect_service_daemon_add_or_update_instance_id_attribute(
    mock_service_daemon, instance_id);

  MockPoolReplayer pool_replayer(&mock_threads, &mock_service_daemon, nullptr,
                                 &m_pool_meta_cache,
                                 m_local_io_ctx.get_id(), peer_spec, {});
  pool_replayer.init("siteA");

  C_SaferCond on_ns1_init;
  expect_namespace_replayer_init(*mock_ns1_namespace_replayer, 0);
  expect_service_daemon_add_namespace(mock_service_daemon, "ns1");
  expect_namespace_replayer_handle_update_leader(*mock_ns1_namespace_replayer,
                                                 "", &on_ns1_init);

  mock_namespace.add("ns1");
  ASSERT_EQ(0, on_ns1_init.wait());

  expect_service_daemon_add_or_update_attribute(
      mock_service_daemon, SERVICE_DAEMON_LEADER_KEY, true);
  expect_namespace_replayer_handle_acquire_leader(
      *mock_default_namespace_replayer, 0);
  expect_namespace_replayer_handle_acquire_leader(
      *mock_ns1_namespace_replayer, 0);

  C_SaferCond on_acquire;
  mock_leader_watcher->listener->post_acquire_handler(&on_acquire);
  ASSERT_EQ(0, on_acquire.wait());

  expect_namespace_replayer_init(*mock_ns2_namespace_replayer, 0);
  expect_service_daemon_add_namespace(mock_service_daemon, "ns2");
  C_SaferCond on_ns2_acquire;
  expect_namespace_replayer_handle_acquire_leader(
      *mock_ns2_namespace_replayer, 0, &on_ns2_acquire);
  expect_namespace_replayer_handle_instances_added(
      *mock_ns2_namespace_replayer);

  mock_namespace.add("ns2");
  ASSERT_EQ(0, on_ns2_acquire.wait());

  C_SaferCond on_ns2_shut_down;
  expect_service_daemon_remove_namespace(mock_service_daemon, "ns2");
  expect_namespace_replayer_shut_down(*mock_ns2_namespace_replayer,
                                      &on_ns2_shut_down);
  mock_namespace.remove("ns2");
  ASSERT_EQ(0, on_ns2_shut_down.wait());

  expect_service_daemon_remove_attribute(mock_service_daemon,
                                         SERVICE_DAEMON_LEADER_KEY);
  expect_namespace_replayer_handle_release_leader(
      *mock_default_namespace_replayer, 0);
  expect_namespace_replayer_handle_release_leader(
      *mock_ns1_namespace_replayer, 0);

  C_SaferCond on_release;
  mock_leader_watcher->listener->pre_release_handler(&on_release);
  ASSERT_EQ(0, on_release.wait());

  expect_service_daemon_remove_namespace(mock_service_daemon, "ns1");
  expect_namespace_replayer_shut_down(*mock_ns1_namespace_replayer);
  expect_leader_watcher_shut_down(*mock_leader_watcher);
  expect_namespace_replayer_shut_down(*mock_default_namespace_replayer);
  expect_remote_pool_poller_shut_down(*mock_remote_pool_poller, 0);

  pool_replayer.shut_down();
}

TEST_F(TestMockPoolReplayer, NamespacesError) {
  PeerSpec peer_spec{"uuid", "cluster name", "client.name"};
  peer_spec.mon_host = "123";
  peer_spec.key = "234";

  g_ceph_context->_conf.set_val(
      "rbd_mirror_pool_replayers_refresh_interval", "1");

  MockNamespace mock_namespace;

  auto mock_default_namespace_replayer = new MockNamespaceReplayer();
  expect_namespace_replayer_is_blacklisted(*mock_default_namespace_replayer,
                                           false);
  auto mock_ns1_namespace_replayer = new MockNamespaceReplayer("ns1");
  auto mock_ns2_namespace_replayer = new MockNamespaceReplayer("ns2");
  expect_namespace_replayer_is_blacklisted(*mock_ns2_namespace_replayer,
                                           false);
  auto mock_ns3_namespace_replayer = new MockNamespaceReplayer("ns3");

  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  auto mock_leader_watcher = new MockLeaderWatcher();
  expect_leader_watcher_get_leader_instance_id(*mock_leader_watcher);
  expect_leader_watcher_list_instances(*mock_leader_watcher);
  expect_leader_watcher_is_blacklisted(*mock_leader_watcher, false);

  auto& mock_cluster = get_mock_cluster();
  auto mock_local_rados_client = mock_cluster.do_create_rados_client(
      g_ceph_context);
  auto mock_local_io_ctx = mock_local_rados_client->do_create_ioctx(
      m_local_io_ctx.get_id(), m_local_io_ctx.get_pool_name());
  auto mock_remote_rados_client = mock_cluster.do_create_rados_client(
      g_ceph_context);

  expect_mirror_mode_get(mock_local_io_ctx);

  InSequence seq;

  expect_connect(mock_cluster, mock_local_rados_client, "ceph", nullptr);
  expect_connect(mock_cluster, mock_remote_rados_client, "cluster name",
                 nullptr);
  expect_create_ioctx(mock_local_rados_client, mock_local_io_ctx);
  expect_mirror_uuid_get(mock_local_io_ctx, "uuid", 0);
  auto mock_remote_pool_poller = new MockRemotePoolPoller();
  expect_remote_pool_poller_init(*mock_remote_pool_poller,
                                 {"remote mirror uuid", ""}, 0);
  expect_namespace_replayer_init(*mock_default_namespace_replayer, 0);
  expect_leader_watcher_init(*mock_leader_watcher, 0);

  MockServiceDaemon mock_service_daemon;
  std::string instance_id = stringify(mock_local_io_ctx->get_instance_id());
  expect_service_daemon_add_or_update_instance_id_attribute(
    mock_service_daemon, instance_id);

  MockPoolReplayer pool_replayer(&mock_threads, &mock_service_daemon, nullptr,
                                 &m_pool_meta_cache,
                                 m_local_io_ctx.get_id(), peer_spec, {});
  pool_replayer.init("siteA");

  // test namespace replayer init fails for non leader

  C_SaferCond on_ns1_init;
  Context* ctx = new LambdaContext(
      [&mock_namespace, &on_ns1_init](int r) {
        mock_namespace.remove("ns1");
        on_ns1_init.complete(r);
      });
  expect_namespace_replayer_init(*mock_ns1_namespace_replayer, -EINVAL, ctx);
  mock_namespace.add("ns1");
  ASSERT_EQ(-EINVAL, on_ns1_init.wait());

  // test acquire leader fails when default namespace replayer fails

  expect_service_daemon_add_or_update_attribute(
    mock_service_daemon, SERVICE_DAEMON_LEADER_KEY, true);
  expect_namespace_replayer_handle_acquire_leader(
    *mock_default_namespace_replayer, -EINVAL);

  C_SaferCond on_acquire1;
  mock_leader_watcher->listener->post_acquire_handler(&on_acquire1);
  ASSERT_EQ(-EINVAL, on_acquire1.wait());

  // test acquire leader succeeds when non-default namespace replayer fails

  C_SaferCond on_ns2_init;
  expect_namespace_replayer_init(*mock_ns2_namespace_replayer, 0);
  expect_service_daemon_add_namespace(mock_service_daemon, "ns2");
  expect_namespace_replayer_handle_update_leader(*mock_ns2_namespace_replayer,
                                                 "", &on_ns2_init);
  mock_namespace.add("ns2");
  ASSERT_EQ(0, on_ns2_init.wait());

  expect_service_daemon_add_or_update_attribute(
      mock_service_daemon, SERVICE_DAEMON_LEADER_KEY, true);
  expect_namespace_replayer_handle_acquire_leader(
      *mock_default_namespace_replayer, 0);

  expect_namespace_replayer_handle_acquire_leader(*mock_ns2_namespace_replayer,
                                                  -EINVAL);
  ctx = new LambdaContext(
      [&mock_namespace](int) {
        mock_namespace.remove("ns2");
      });
  expect_service_daemon_remove_namespace(mock_service_daemon, "ns2");
  expect_namespace_replayer_shut_down(*mock_ns2_namespace_replayer, ctx);
  mock_namespace.add("ns2");

  C_SaferCond on_acquire2;
  mock_leader_watcher->listener->post_acquire_handler(&on_acquire2);
  ASSERT_EQ(0, on_acquire2.wait());

  // test namespace replayer init fails on acquire leader

  C_SaferCond on_ns3_shut_down;
  ctx = new LambdaContext(
      [&mock_namespace, &on_ns3_shut_down](int) {
        mock_namespace.remove("ns3");
        on_ns3_shut_down.complete(0);
      });
  expect_namespace_replayer_init(*mock_ns3_namespace_replayer, 0);
  expect_service_daemon_add_namespace(mock_service_daemon, "ns3");
  expect_namespace_replayer_handle_acquire_leader(*mock_ns3_namespace_replayer,
                                                  -EINVAL);
  expect_service_daemon_remove_namespace(mock_service_daemon, "ns3");
  expect_namespace_replayer_shut_down(*mock_ns3_namespace_replayer, ctx);
  mock_namespace.add("ns3");
  ASSERT_EQ(0, on_ns3_shut_down.wait());

  expect_leader_watcher_shut_down(*mock_leader_watcher);
  expect_namespace_replayer_shut_down(*mock_default_namespace_replayer);
  expect_remote_pool_poller_shut_down(*mock_remote_pool_poller, 0);

  pool_replayer.shut_down();
}

} // namespace mirror
} // namespace rbd
