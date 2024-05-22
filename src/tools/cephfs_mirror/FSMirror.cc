// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/admin_socket.h"
#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "common/perf_counters.h"
#include "common/perf_counters_key.h"
#include "include/stringify.h"
#include "msg/Messenger.h"
#include "FSMirror.h"
#include "PeerReplayer.h"
#include "aio_utils.h"
#include "ServiceDaemon.h"
#include "Utils.h"

#include "common/Cond.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_cephfs_mirror
#undef dout_prefix
#define dout_prefix *_dout << "cephfs::mirror::FSMirror " << __func__

using namespace std;

// Performance Counters
enum {
  l_cephfs_mirror_fs_mirror_first = 5000,
  l_cephfs_mirror_fs_mirror_peers,
  l_cephfs_mirror_fs_mirror_dir_count,
  l_cephfs_mirror_fs_mirror_last,
};

namespace cephfs {
namespace mirror {

namespace {

const std::string SERVICE_DAEMON_DIR_COUNT_KEY("directory_count");
const std::string SERVICE_DAEMON_PEER_INIT_FAILED_KEY("peer_init_failed");

class MirrorAdminSocketCommand {
public:
  virtual ~MirrorAdminSocketCommand() {
  }
  virtual int call(Formatter *f) = 0;
};

class StatusCommand : public MirrorAdminSocketCommand {
public:
  explicit StatusCommand(FSMirror *fs_mirror)
    : fs_mirror(fs_mirror) {
  }

  int call(Formatter *f) override {
    fs_mirror->mirror_status(f);
    return 0;
  }

private:
  FSMirror *fs_mirror;
};

} // anonymous namespace

class MirrorAdminSocketHook : public AdminSocketHook {
public:
  MirrorAdminSocketHook(CephContext *cct, const Filesystem &filesystem, FSMirror *fs_mirror)
    : admin_socket(cct->get_admin_socket()) {
    int r;
    std::string cmd;

    // mirror status format is name@fscid
    cmd = "fs mirror status " + stringify(filesystem.fs_name) + "@" + stringify(filesystem.fscid);
    r = admin_socket->register_command(
      cmd, this, "get filesystem mirror status");
    if (r == 0) {
      commands[cmd] = new StatusCommand(fs_mirror);
    }
  }

  ~MirrorAdminSocketHook() override {
    admin_socket->unregister_commands(this);
    for (auto &[command, cmdptr] : commands) {
      delete cmdptr;
    }
  }

  int call(std::string_view command, const cmdmap_t& cmdmap,
           const bufferlist&,
           Formatter *f, std::ostream &errss, bufferlist &out) override {
    auto p = commands.at(std::string(command));
    return p->call(f);
  }

private:
  typedef std::map<std::string, MirrorAdminSocketCommand*, std::less<>> Commands;

  AdminSocket *admin_socket;
  Commands commands;
};

FSMirror::FSMirror(CephContext *cct, const Filesystem &filesystem, uint64_t pool_id,
                   ServiceDaemon *service_daemon, std::vector<const char*> args,
                   ContextWQ *work_queue)
  : m_cct(cct),
    m_filesystem(filesystem),
    m_pool_id(pool_id),
    m_service_daemon(service_daemon),
    m_args(args),
    m_work_queue(work_queue),
    m_snap_listener(this),
    m_asok_hook(new MirrorAdminSocketHook(cct, filesystem, this)) {
  m_service_daemon->add_or_update_fs_attribute(m_filesystem.fscid, SERVICE_DAEMON_DIR_COUNT_KEY,
                                               (uint64_t)0);

  std::string labels = ceph::perf_counters::key_create("cephfs_mirror_mirrored_filesystems",
						       {{"filesystem", m_filesystem.fs_name}});
  PerfCountersBuilder plb(m_cct, labels, l_cephfs_mirror_fs_mirror_first,
			  l_cephfs_mirror_fs_mirror_last);
  auto prio = m_cct->_conf.get_val<int64_t>("cephfs_mirror_perf_stats_prio");
  plb.add_u64(l_cephfs_mirror_fs_mirror_peers,
	      "mirroring_peers", "Mirroring Peers", "mpee", prio);
  plb.add_u64(l_cephfs_mirror_fs_mirror_dir_count,
	      "directory_count", "Directory Count", "dirc", prio);
  m_perf_counters = plb.create_perf_counters();
  m_cct->get_perfcounters_collection()->add(m_perf_counters);
}

FSMirror::~FSMirror() {
  dout(20) << dendl;

  {
    std::scoped_lock locker(m_lock);
    delete m_instance_watcher;
    delete m_mirror_watcher;
  }
  // outside the lock so that in-progress commands can acquire
  // lock and finish executing.
  delete m_asok_hook;
  PerfCounters *perf_counters = nullptr;
  std::swap(perf_counters, m_perf_counters);
  if (perf_counters != nullptr) {
    m_cct->get_perfcounters_collection()->remove(perf_counters);
    delete perf_counters;
  }
}

int FSMirror::init_replayer(PeerReplayer *peer_replayer) {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  return peer_replayer->init();
}

void FSMirror::shutdown_replayer(PeerReplayer *peer_replayer) {
  peer_replayer->shutdown();
}

void FSMirror::cleanup() {
  dout(20) << dendl;
  ceph_unmount(m_mount);
  ceph_release(m_mount);
  m_ioctx.close();
  m_cluster.reset();
}

void FSMirror::reopen_logs() {
  std::scoped_lock locker(m_lock);

  if (m_cluster) {
    reinterpret_cast<CephContext *>(m_cluster->cct())->reopen_logs();
  }
  for (auto &[peer, replayer] : m_peer_replayers) {
    replayer->reopen_logs();
  }
}

void FSMirror::init(Context *on_finish) {
  dout(20) << dendl;

  std::scoped_lock locker(m_lock);
  int r = connect(g_ceph_context->_conf->name.to_str(),
                  g_ceph_context->_conf->cluster, &m_cluster, "", "", m_args);
  if (r < 0) {
    m_init_failed = true;
    on_finish->complete(r);
    return;
  }

  r = m_cluster->ioctx_create2(m_pool_id, m_ioctx);
  if (r < 0) {
    m_init_failed = true;
    m_cluster.reset();
    derr << ": error accessing local pool (id=" << m_pool_id << "): "
         << cpp_strerror(r) << dendl;
    on_finish->complete(r);
    return;
  }

  r = mount(m_cluster, m_filesystem, true, &m_mount);
  if (r < 0) {
    m_init_failed = true;
    m_ioctx.close();
    m_cluster.reset();
    on_finish->complete(r);
    return;
  }

  m_addrs = m_cluster->get_addrs();
  dout(10) << ": rados addrs=" << m_addrs << dendl;

  init_instance_watcher(on_finish);
}

void FSMirror::shutdown(Context *on_finish) {
  dout(20) << dendl;

  {
    std::scoped_lock locker(m_lock);
    m_stopping = true;
    if (m_on_init_finish != nullptr) {
      dout(10) << ": delaying shutdown -- init in progress" << dendl;
      m_on_shutdown_finish = new LambdaContext([this, on_finish](int r) {
                                                 if (r < 0) {
                                                   on_finish->complete(0);
                                                   return;
                                                 }
                                                 m_on_shutdown_finish = on_finish;
                                                 shutdown_peer_replayers();
                                               });
      return;
    }

    m_on_shutdown_finish = on_finish;
  }

  shutdown_peer_replayers();
}

void FSMirror::shutdown_peer_replayers() {
  dout(20) << dendl;

  for (auto &[peer, peer_replayer] : m_peer_replayers) {
    dout(5) << ": shutting down replayer for peer=" << peer << dendl;
    shutdown_replayer(peer_replayer.get());
  }
  m_peer_replayers.clear();

  shutdown_mirror_watcher();
}

void FSMirror::init_instance_watcher(Context *on_finish) {
  dout(20) << dendl;

  m_on_init_finish = new LambdaContext([this, on_finish](int r) {
                                         {
                                           std::scoped_lock locker(m_lock);
                                           if (r < 0) {
                                             m_init_failed = true;
                                           }
                                         }
                                         on_finish->complete(r);
                                         if (m_on_shutdown_finish != nullptr) {
                                           m_on_shutdown_finish->complete(r);
                                         }
                                       });

  Context *ctx = new C_CallbackAdapter<
    FSMirror, &FSMirror::handle_init_instance_watcher>(this);
  m_instance_watcher = InstanceWatcher::create(m_ioctx, m_snap_listener, m_work_queue);
  m_instance_watcher->init(ctx);
}

void FSMirror::handle_init_instance_watcher(int r) {
  dout(20) << ": r=" << r << dendl;

  Context *on_init_finish = nullptr;
  {
    std::scoped_lock locker(m_lock);
    if (r < 0) {
      std::swap(on_init_finish, m_on_init_finish);
    }
  }

  if (on_init_finish != nullptr) {
    on_init_finish->complete(r);
    return;
  }

  init_mirror_watcher();
}

void FSMirror::init_mirror_watcher() {
  dout(20) << dendl;

  std::scoped_lock locker(m_lock);
  Context *ctx = new C_CallbackAdapter<
    FSMirror, &FSMirror::handle_init_mirror_watcher>(this);
  m_mirror_watcher = MirrorWatcher::create(m_ioctx, this, m_work_queue);
  m_mirror_watcher->init(ctx);
}

void FSMirror::handle_init_mirror_watcher(int r) {
  dout(20) << ": r=" << r << dendl;

  Context *on_init_finish = nullptr;
  {
    std::scoped_lock locker(m_lock);
    if (r == 0) {
      std::swap(on_init_finish, m_on_init_finish);
    }
  }

  if (on_init_finish != nullptr) {
    on_init_finish->complete(r);
    return;
  }

  m_retval = r; // save errcode for init context callback
  shutdown_instance_watcher();
}

void FSMirror::shutdown_mirror_watcher() {
  dout(20) << dendl;

  std::scoped_lock locker(m_lock);
  Context *ctx = new C_CallbackAdapter<
    FSMirror, &FSMirror::handle_shutdown_mirror_watcher>(this);
  m_mirror_watcher->shutdown(ctx);
}

void FSMirror::handle_shutdown_mirror_watcher(int r) {
  dout(20) << ": r=" << r << dendl;

  shutdown_instance_watcher();
}

void FSMirror::shutdown_instance_watcher() {
  dout(20) << dendl;

  std::scoped_lock locker(m_lock);
  Context *ctx = new C_CallbackAdapter<
    FSMirror, &FSMirror::handle_shutdown_instance_watcher>(this);
  m_instance_watcher->shutdown(new C_AsyncCallback<ContextWQ>(m_work_queue, ctx));
}

void FSMirror::handle_shutdown_instance_watcher(int r) {
  dout(20) << ": r=" << r << dendl;

  cleanup();

  Context *on_init_finish = nullptr;
  Context *on_shutdown_finish = nullptr;

  {
    std::scoped_lock locker(m_lock);
    std::swap(on_init_finish, m_on_init_finish);
    std::swap(on_shutdown_finish, m_on_shutdown_finish);
  }

  if (on_init_finish != nullptr) {
    on_init_finish->complete(m_retval);
  }
  if (on_shutdown_finish != nullptr) {
    on_shutdown_finish->complete(r);
  }
}

void FSMirror::handle_acquire_directory(string_view dir_path) {
  dout(5) << ": dir_path=" << dir_path << dendl;

  {
    std::scoped_lock locker(m_lock);
    m_directories.emplace(dir_path);
    m_service_daemon->add_or_update_fs_attribute(m_filesystem.fscid, SERVICE_DAEMON_DIR_COUNT_KEY,
                                                 m_directories.size());

    for (auto &[peer, peer_replayer] : m_peer_replayers) {
      dout(10) << ": peer=" << peer << dendl;
      peer_replayer->add_directory(dir_path);
    }
  }
  if (m_perf_counters) {
    m_perf_counters->set(l_cephfs_mirror_fs_mirror_dir_count, m_directories.size());
  }
}

void FSMirror::handle_release_directory(string_view dir_path) {
  dout(5) << ": dir_path=" << dir_path << dendl;

  {
    std::scoped_lock locker(m_lock);
    auto it = m_directories.find(dir_path);
    if (it != m_directories.end()) {
      m_directories.erase(it);
      m_service_daemon->add_or_update_fs_attribute(m_filesystem.fscid, SERVICE_DAEMON_DIR_COUNT_KEY,
                                                   m_directories.size());
      for (auto &[peer, peer_replayer] : m_peer_replayers) {
        dout(10) << ": peer=" << peer << dendl;
        peer_replayer->remove_directory(dir_path);
      }
    }
    if (m_perf_counters) {
      m_perf_counters->set(l_cephfs_mirror_fs_mirror_dir_count, m_directories.size());
    }
  }
}

void FSMirror::add_peer(const Peer &peer) {
  dout(10) << ": peer=" << peer << dendl;

  std::scoped_lock locker(m_lock);
  m_all_peers.emplace(peer);
  if (m_peer_replayers.find(peer) != m_peer_replayers.end()) {
    return;
  }

  auto replayer = std::make_unique<PeerReplayer>(
    m_cct, this, m_cluster, m_filesystem, peer, m_directories, m_mount, m_service_daemon);
  int r = init_replayer(replayer.get());
  if (r < 0) {
    m_service_daemon->add_or_update_peer_attribute(m_filesystem.fscid, peer,
                                                   SERVICE_DAEMON_PEER_INIT_FAILED_KEY,
                                                   true);
    return;
  }
  m_peer_replayers.emplace(peer, std::move(replayer));
  ceph_assert(m_peer_replayers.size() == 1); // support only a single peer
  if (m_perf_counters) {
    m_perf_counters->inc(l_cephfs_mirror_fs_mirror_peers);
  }
}

void FSMirror::remove_peer(const Peer &peer) {
  dout(10) << ": peer=" << peer << dendl;

  std::unique_ptr<PeerReplayer> replayer;
  {
    std::scoped_lock locker(m_lock);
    m_all_peers.erase(peer);
    auto it = m_peer_replayers.find(peer);
    if (it != m_peer_replayers.end()) {
      replayer = std::move(it->second);
      m_peer_replayers.erase(it);
    }
  }

  if (replayer) {
    dout(5) << ": shutting down replayers for peer=" << peer << dendl;
    shutdown_replayer(replayer.get());
  }
  if (m_perf_counters) {
    m_perf_counters->dec(l_cephfs_mirror_fs_mirror_peers);
  }
}

void FSMirror::mirror_status(Formatter *f) {
  std::scoped_lock locker(m_lock);
  f->open_object_section("status");
  if (m_init_failed) {
    f->dump_string("state", "failed");
  } else if (is_blocklisted(locker)) {
    f->dump_string("state", "blocklisted");
  } else {
    // dump rados addr for blocklist test
    f->dump_string("rados_inst", m_addrs);
    f->open_object_section("peers");
    for ([[maybe_unused]] auto &[peer, peer_replayer] : m_peer_replayers) {
      peer.dump(f);
    }
    f->close_section(); // peers
    f->open_object_section("snap_dirs");
    f->dump_int("dir_count", m_directories.size());
    f->close_section(); // snap_dirs
  }
  f->close_section(); // status
}


} // namespace mirror
} // namespace cephfs
