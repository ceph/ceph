// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Config.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librados_test_stub/MockTestMemCluster.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "test/rbd_mirror/test_mock_fixture.h"
#include "test/rbd_mirror/mock/MockContextWQ.h"
#include "test/rbd_mirror/mock/MockSafeTimer.h"
#include "tools/rbd_mirror/PoolReplayer.h"
#include "tools/rbd_mirror/ImageDeleter.h"
#include "tools/rbd_mirror/ImageMap.h"
#include "tools/rbd_mirror/InstanceWatcher.h"
#include "tools/rbd_mirror/InstanceReplayer.h"
#include "tools/rbd_mirror/LeaderWatcher.h"
#include "tools/rbd_mirror/PoolWatcher.h"
#include "tools/rbd_mirror/ServiceDaemon.h"
#include "tools/rbd_mirror/Threads.h"

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

}

} // namespace librbd

namespace rbd {
namespace mirror {

template <>
struct ImageDeleter<librbd::MockTestImageCtx> {
  static ImageDeleter* s_instance;

  static ImageDeleter* create(librados::IoCtx &ioctx,
                              Threads<librbd::MockTestImageCtx> *threads,
                              ServiceDaemon<librbd::MockTestImageCtx> *service_daemon) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  MOCK_METHOD1(init, void(Context*));
  MOCK_METHOD1(shut_down, void(Context*));
  MOCK_METHOD2(print_status, void(Formatter*, std::stringstream*));

  ImageDeleter() {
    s_instance = this;
  }
};

ImageDeleter<librbd::MockTestImageCtx>* ImageDeleter<librbd::MockTestImageCtx>::s_instance = nullptr;

template<>
struct ImageMap<librbd::MockTestImageCtx> {
  static ImageMap* s_instance;

  static ImageMap *create(librados::IoCtx &ioctx,
                          Threads<librbd::MockTestImageCtx> *threads,
                          const std::string& instance_id,
                          image_map::Listener &listener) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  MOCK_METHOD1(init, void(Context*));
  MOCK_METHOD1(shut_down, void(Context*));

  MOCK_METHOD1(update_instances_added, void(const std::vector<std::string>&));
  MOCK_METHOD1(update_instances_removed, void(const std::vector<std::string>&));

  MOCK_METHOD3(update_images_mock, void(const std::string&,
                                        const std::set<std::string>&,
                                        const std::set<std::string>&));
  void update_images(const std::string& mirror_uuid,
                     std::set<std::string>&& added,
                     std::set<std::string>&& removed) {
    update_images_mock(mirror_uuid, added, removed);
  }

  ImageMap() {
    s_instance = this;
  }
};

ImageMap<librbd::MockTestImageCtx>* ImageMap<librbd::MockTestImageCtx>::s_instance = nullptr;

template<>
struct InstanceReplayer<librbd::MockTestImageCtx> {
  static InstanceReplayer* s_instance;

  static InstanceReplayer* create(Threads<librbd::MockTestImageCtx> *threads,
                                  ServiceDaemon<librbd::MockTestImageCtx> *service_daemon,
                                  RadosRef rados, const std::string& uuid,
                                  int64_t pool_id) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  MOCK_METHOD0(start, void());
  MOCK_METHOD0(stop, void());
  MOCK_METHOD0(restart, void());
  MOCK_METHOD0(flush, void());

  MOCK_METHOD2(print_status, void(Formatter*, std::stringstream*));

  MOCK_METHOD2(add_peer, void(const std::string&, librados::IoCtx&));

  MOCK_METHOD0(init, void());
  MOCK_METHOD0(shut_down, void());
  MOCK_METHOD1(release_all, void(Context*));

  InstanceReplayer() {
    s_instance = this;
  }
};

InstanceReplayer<librbd::MockTestImageCtx>* InstanceReplayer<librbd::MockTestImageCtx>::s_instance = nullptr;

template<>
struct InstanceWatcher<librbd::MockTestImageCtx> {
  static InstanceWatcher* s_instance;

  static InstanceWatcher* create(librados::IoCtx &ioctx,
                                 MockContextWQ* work_queue,
                                 InstanceReplayer<librbd::MockTestImageCtx>* instance_replayer) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  MOCK_METHOD0(handle_acquire_leader, void());
  MOCK_METHOD0(handle_release_leader, void());

  MOCK_METHOD0(get_instance_id, std::string());

  MOCK_METHOD2(print_sync_status, void(Formatter*, std::stringstream*));

  MOCK_METHOD0(init, int());
  MOCK_METHOD0(shut_down, void());

  MOCK_METHOD3(notify_image_acquire, void(const std::string&,
                                          const std::string&,
                                          Context*));
  MOCK_METHOD3(notify_image_release, void(const std::string&,
                                          const std::string&,
                                          Context*));
  MOCK_METHOD4(notify_peer_image_removed, void(const std::string&,
                                               const std::string&,
                                               const std::string&,
                                               Context*));

  MOCK_METHOD1(handle_update_leader, void(const std::string&));

  InstanceWatcher() {
    s_instance = this;
  }

};

InstanceWatcher<librbd::MockTestImageCtx>* InstanceWatcher<librbd::MockTestImageCtx>::s_instance = nullptr;

template<>
struct LeaderWatcher<librbd::MockTestImageCtx> {
  static LeaderWatcher* s_instance;

  static LeaderWatcher *create(Threads<librbd::MockTestImageCtx> *threads,
                               librados::IoCtx &ioctx,
                               leader_watcher::Listener* listener) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  MOCK_METHOD0(is_leader, bool());
  MOCK_METHOD0(release_leader, void());

  MOCK_METHOD1(get_leader_instance_id, void(std::string*));
  MOCK_METHOD1(list_instances, void(std::vector<std::string>*));

  MOCK_METHOD0(init, int());
  MOCK_METHOD0(shut_down, int());

  LeaderWatcher() {
    s_instance = this;
  }

};

LeaderWatcher<librbd::MockTestImageCtx>* LeaderWatcher<librbd::MockTestImageCtx>::s_instance = nullptr;

template<>
struct PoolWatcher<librbd::MockTestImageCtx> {
  static PoolWatcher* s_instance;

  static PoolWatcher *create(Threads<librbd::MockTestImageCtx> *threads,
                             librados::IoCtx &ioctx,
                             pool_watcher::Listener& listener) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  MOCK_METHOD0(is_blacklisted, bool());

  MOCK_METHOD0(get_image_count, uint64_t());

  MOCK_METHOD1(init, void(Context*));
  MOCK_METHOD1(shut_down, void(Context*));

  PoolWatcher() {
    s_instance = this;
  }

};

PoolWatcher<librbd::MockTestImageCtx>* PoolWatcher<librbd::MockTestImageCtx>::s_instance = nullptr;

template<>
struct ServiceDaemon<librbd::MockTestImageCtx> {
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
  Mutex &timer_lock;
  Cond timer_cond;

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
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockPoolReplayer : public TestMockFixture {
public:
  typedef PoolReplayer<librbd::MockTestImageCtx> MockPoolReplayer;
  typedef ImageMap<librbd::MockTestImageCtx> MockImageMap;
  typedef InstanceReplayer<librbd::MockTestImageCtx> MockInstanceReplayer;
  typedef InstanceWatcher<librbd::MockTestImageCtx> MockInstanceWatcher;
  typedef LeaderWatcher<librbd::MockTestImageCtx> MockLeaderWatcher;
  typedef PoolWatcher<librbd::MockTestImageCtx> MockPoolWatcher;
  typedef ServiceDaemon<librbd::MockTestImageCtx> MockServiceDaemon;
  typedef Threads<librbd::MockTestImageCtx> MockThreads;

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
                     _, _, _))
      .WillOnce(DoAll(WithArg<5>(Invoke([out_bl](bufferlist *bl) {
                          *bl = out_bl;
                        })),
                      Return(r)));
  }

  void expect_instance_replayer_init(MockInstanceReplayer& mock_instance_replayer) {
    EXPECT_CALL(mock_instance_replayer, init());
  }

  void expect_instance_replayer_shut_down(MockInstanceReplayer& mock_instance_replayer) {
    EXPECT_CALL(mock_instance_replayer, shut_down());
  }

  void expect_instance_replayer_stop(MockInstanceReplayer& mock_instance_replayer) {
    EXPECT_CALL(mock_instance_replayer, stop());
  }

  void expect_instance_replayer_add_peer(MockInstanceReplayer& mock_instance_replayer,
                                        const std::string& uuid) {
    EXPECT_CALL(mock_instance_replayer, add_peer(uuid, _));
  }

  void expect_instance_watcher_get_instance_id(
      MockInstanceWatcher& mock_instance_watcher,
      const std::string &instance_id) {
    EXPECT_CALL(mock_instance_watcher, get_instance_id())
      .WillOnce(Return(instance_id));
  }

  void expect_instance_watcher_init(MockInstanceWatcher& mock_instance_watcher,
                                    int r) {
    EXPECT_CALL(mock_instance_watcher, init())
      .WillOnce(Return(r));
  }

  void expect_instance_watcher_shut_down(MockInstanceWatcher& mock_instance_watcher) {
    EXPECT_CALL(mock_instance_watcher, shut_down());
  }

  void expect_leader_watcher_init(MockLeaderWatcher& mock_leader_watcher,
                                  int r) {
    EXPECT_CALL(mock_leader_watcher, init())
      .WillOnce(Return(r));
  }

  void expect_leader_watcher_shut_down(MockLeaderWatcher& mock_leader_watcher) {
    EXPECT_CALL(mock_leader_watcher, shut_down());
  }

  void expect_service_daemon_add_or_update_attribute(
      MockServiceDaemon &mock_service_daemon, const std::string& key,
      const service_daemon::AttributeValue& value) {
    EXPECT_CALL(mock_service_daemon, add_or_update_attribute(_, _, _));
  }

  void expect_service_daemon_add_or_update_instance_id_attribute(
      MockInstanceWatcher& mock_instance_watcher,
      MockServiceDaemon &mock_service_daemon) {
    expect_instance_watcher_get_instance_id(mock_instance_watcher, "1234");
    expect_service_daemon_add_or_update_attribute(mock_service_daemon,
                                                  "instance_id", "1234");
  }
};

TEST_F(TestMockPoolReplayer, ConfigKeyOverride) {
  PeerSpec peer_spec{"uuid", "cluster name", "client.name"};
  peer_spec.mon_host = "123";
  peer_spec.key = "234";

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

  auto mock_instance_replayer = new MockInstanceReplayer();
  expect_instance_replayer_init(*mock_instance_replayer);
  expect_instance_replayer_add_peer(*mock_instance_replayer, "uuid");

  auto mock_instance_watcher = new MockInstanceWatcher();
  expect_instance_watcher_init(*mock_instance_watcher, 0);

  MockServiceDaemon mock_service_daemon;
  expect_service_daemon_add_or_update_instance_id_attribute(
      *mock_instance_watcher, mock_service_daemon);

  auto mock_leader_watcher = new MockLeaderWatcher();
  expect_leader_watcher_init(*mock_leader_watcher, 0);

  MockThreads mock_threads(m_threads);
  MockPoolReplayer pool_replayer(&mock_threads, &mock_service_daemon,
                                 m_local_io_ctx.get_id(), peer_spec, {});
  pool_replayer.init();

  ASSERT_TRUE(remote_cct != nullptr);
  ASSERT_EQ("123", remote_cct->_conf.get_val<std::string>("mon_host"));
  ASSERT_EQ("234", remote_cct->_conf.get_val<std::string>("key"));
  remote_cct->put();

  expect_instance_replayer_stop(*mock_instance_replayer);
  expect_leader_watcher_shut_down(*mock_leader_watcher);
  expect_instance_watcher_shut_down(*mock_instance_watcher);
  expect_instance_replayer_shut_down(*mock_instance_replayer);

  pool_replayer.shut_down();
}

} // namespace mirror
} // namespace rbd
