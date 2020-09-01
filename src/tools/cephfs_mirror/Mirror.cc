// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/Cond.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"
#include "include/types.h"
#include "json_spirit/json_spirit.h"
#include "mon/MonClient.h"
#include "msg/Messenger.h"
#include "aio_utils.h"
#include "Mirror.h"


#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_cephfs_mirror
#undef dout_prefix
#define dout_prefix *_dout << "cephfs::mirror::Mirror " << __func__

namespace cephfs {
namespace mirror {

namespace {

class SafeTimerSingleton : public SafeTimer {
public:
  SafeTimer *timer;
  ceph::mutex timer_lock = ceph::make_mutex("cephfs::mirror::timer_lock");

  explicit SafeTimerSingleton(CephContext *cct)
    : SafeTimer(cct, timer_lock, true) {
    init();
  }

  ~SafeTimerSingleton() {
    std::scoped_lock locker(timer_lock);
    shutdown();
  }
};

class ThreadPoolSingleton : public ThreadPool {
public:
  ContextWQ *work_queue = nullptr;

  explicit ThreadPoolSingleton(CephContext *cct)
    : ThreadPool(cct, "Mirror::thread_pool", "tp_mirror", 1) {
    work_queue = new ContextWQ("Mirror::work_queue", ceph::make_timespan(60), this);

    start();
  }

  ~ThreadPoolSingleton() override {
    work_queue->drain();
    delete work_queue;

    stop();
  }
};

} // anonymous namespace

Mirror::Mirror(CephContext *cct, const std::vector<const char*> &args,
                MonClient *monc, Messenger *msgr)
  : m_cct(cct),
    m_args(args),
    m_monc(monc),
    m_msgr(msgr),
    m_listener(this) {
  auto thread_pool = &(cct->lookup_or_create_singleton_object<ThreadPoolSingleton>(
                         "cephfs::mirror::thread_pool", false, cct));
  auto safe_timer = &(cct->lookup_or_create_singleton_object<SafeTimerSingleton>(
                        "cephfs::mirror::safe_timer", false, cct));
  m_work_queue = thread_pool->work_queue;
  m_timer = safe_timer->timer;
}

Mirror::~Mirror() {
  dout(10) << dendl;
}

int Mirror::init_mon_client() {
  dout(20) << dendl;

  m_monc->set_messenger(m_msgr);
  m_monc->set_want_keys(CEPH_ENTITY_TYPE_MON);

  int r = m_monc->init();
  if (r < 0) {
    derr << ": failed to init mon client: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = m_monc->authenticate(m_cct->_conf->client_mount_timeout);
  if (r < 0) {
    derr << ": failed to authenticate to monitor: " << cpp_strerror(r) << dendl;
    return r;
  }

  client_t me = m_monc->get_global_id();
  m_msgr->set_myname(entity_name_t::CLIENT(me.v));
  return 0;
}

int Mirror::init(std::string &reason) {
  dout(20) << dendl;

  std::scoped_lock locker(m_lock);
  int r = init_mon_client();
  if (r < 0) {
    return r;
  }

  return 0;
}

void Mirror::shutdown() {
  dout(20) << dendl;

  std::unique_lock locker(m_lock);
  if (m_fs_mirrors.empty()) {
    return;
  }

  m_stopping = true;
  m_cond.notify_all();
  m_cond.wait(locker, [this] {return m_stopped;});
}

void Mirror::handle_signal(int signum) {
  dout(10) << ": signal=" << signum << dendl;
  ceph_assert(signum == SIGTERM || signum == SIGINT);
  shutdown();
  ::exit(0);
}

void Mirror::handle_mirroring_enabled(const std::string &fs_name, int r) {
  dout(20) << ": fs_name=" << fs_name << ", r=" << r << dendl;

  std::scoped_lock locker(m_lock);
  if (r < 0) {
    derr << ": failed to initialize FSMirror for filesystem=" << fs_name
         << ": " << cpp_strerror(r) << dendl;
    if (!m_stopping) {
      m_fs_mirrors.erase(fs_name);
    }

    return;
  }

  dout(10) << ": Initialized FSMirror for filesystem=" << fs_name << dendl;
}

void Mirror::mirroring_enabled(const std::string &fs_name, uint64_t local_pool_id) {
  dout(10) << ": fs_name=" << fs_name << ", pool_id=" << local_pool_id << dendl;

  std::scoped_lock locker(m_lock);
  if (m_stopping) {
    return;
  }

  // TODO: handle consecutive overlapping enable/disable calls
  ceph_assert(m_fs_mirrors.find(fs_name) == m_fs_mirrors.end());

  dout(10) << ": starting FSMirror: fs_name=" << fs_name << dendl;
  std::unique_ptr<FSMirror> fs_mirror(new FSMirror(m_cct, fs_name, local_pool_id,
                                                   m_args, m_work_queue));
  Context *on_finish = new LambdaContext([this, fs_name](int r) {
                                           handle_mirroring_enabled(fs_name, r);
                                         });
  fs_mirror->init(new C_AsyncCallback<ContextWQ>(m_work_queue, on_finish));
  m_fs_mirrors.emplace(fs_name, std::move(fs_mirror));
}

void Mirror::mirroring_disabled(const std::string &fs_name) {
  dout(10) << ": fs_name=" << fs_name << dendl;

  std::scoped_lock locker(m_lock);
  if (!m_fs_mirrors.count(fs_name)) {
    dout(5) << ": fs mirror not found -- init failure(?) for " << fs_name
            << dendl;
    return;
  }

  if (m_stopping) {
    dout(5) << "shutting down" << dendl;
    return;
  }

  auto &fs_mirror = m_fs_mirrors.at(fs_name);
  if (!fs_mirror->is_stopping()) {
    Context *on_finish = new LambdaContext([this, fs_name](int r) {
                                             handle_shutdown(fs_name, r);
                                           });
    fs_mirror->shutdown(new C_AsyncCallback<ContextWQ>(m_work_queue, on_finish));
  }
}

void Mirror::peer_added(const std::string &fs_name, const Peer &peer) {
  dout(20) << ": fs_name=" << fs_name << ", peer=" << peer << dendl;

  std::scoped_lock locker(m_lock);
  if (!m_fs_mirrors.count(fs_name)) {
    dout(5) << ": fs mirror not found -- init failure(?) for " << fs_name
            << dendl;
    return;
  }

  if (m_stopping) {
    dout(5) << "shutting down" << dendl;
    return;
  }

  auto &fs_mirror = m_fs_mirrors.at(fs_name);
  fs_mirror->add_peer(peer);
}

void Mirror::peer_removed(const std::string &fs_name, const Peer &peer) {
  dout(20) << ": fs_name=" << fs_name << ", peer=" << peer << dendl;

  std::scoped_lock locker(m_lock);
  if (!m_fs_mirrors.count(fs_name)) {
    dout(5) << ": fs mirror not found -- init failure(?) for " << fs_name
            << dendl;
    return;
  }

  if (m_stopping) {
    dout(5) << "shutting down" << dendl;
    return;
  }

  auto &fs_mirror = m_fs_mirrors.at(fs_name);
  fs_mirror->remove_peer(peer);
}

void Mirror::handle_shutdown(const std::string &fs_name, int r) {
  dout(10) << ": fs_name=" << fs_name << ", r=" << r << dendl;

  std::scoped_lock locker(m_lock);
  m_fs_mirrors.erase(fs_name);
  m_cond.notify_all();
}

void Mirror::run() {
  dout(20) << dendl;

  std::unique_lock locker(m_lock);
  m_cluster_watcher.reset(new ClusterWatcher(m_cct, m_monc, m_listener));
  m_msgr->add_dispatcher_tail(m_cluster_watcher.get());

  m_cluster_watcher->init();
  m_cond.wait(locker, [this]{return m_stopping;});

  for (auto &[fs_name, fs_mirror] : m_fs_mirrors) {
    dout(10) << ": shutting down mirror for fs_name=" << fs_name << dendl;
    if (fs_mirror->is_stopping()) {
      dout(10) << ": fs_name=" << fs_name << " is under shutdown" << dendl;
      continue;
    }

    Context *on_finish = new LambdaContext([this, fs_name = fs_name](int r) {
                                             handle_shutdown(fs_name, r);
                            });
    fs_mirror->shutdown(new C_AsyncCallback<ContextWQ>(m_work_queue, on_finish));
  }

  m_cond.wait(locker, [this] {return m_fs_mirrors.empty();});

  m_stopped = true;
  m_cond.notify_all();
}

} // namespace mirror
} // namespace cephfs

