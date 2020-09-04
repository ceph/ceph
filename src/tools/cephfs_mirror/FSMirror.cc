// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/admin_socket.h"
#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "include/stringify.h"
#include "msg/Messenger.h"
#include "FSMirror.h"
#include "aio_utils.h"

#include "common/Cond.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_cephfs_mirror
#undef dout_prefix
#define dout_prefix *_dout << "cephfs::mirror::FSMirror " << __func__

namespace cephfs {
namespace mirror {

namespace {
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
  MirrorAdminSocketHook(CephContext *cct, std::string_view fs_name, FSMirror *fs_mirror)
    : admin_socket(cct->get_admin_socket()) {
    int r;
    std::string cmd;

    cmd = "fs mirror status " + std::string(fs_name);
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
           Formatter *f, std::ostream &errss, bufferlist &out) override {
    auto p = commands.at(std::string(command));
    return p->call(f);
  }

private:
  typedef std::map<std::string, MirrorAdminSocketCommand*, std::less<>> Commands;

  AdminSocket *admin_socket;
  Commands commands;
};

FSMirror::FSMirror(CephContext *cct, std::string_view fs_name, uint64_t pool_id,
                   std::vector<const char*> args, ContextWQ *work_queue)
  : m_fs_name(fs_name),
    m_pool_id(pool_id),
    m_args(args),
    m_work_queue(work_queue),
    m_snap_listener(this),
    m_asok_hook(new MirrorAdminSocketHook(cct, fs_name, this)) {
}

FSMirror::~FSMirror() {
  dout(20) << dendl;

  std::scoped_lock locker(m_lock);
  delete m_instance_watcher;
  delete m_mirror_watcher;
  m_cluster.reset();
  delete m_asok_hook;
}

int FSMirror::connect(std::string_view client_name, std::string_view cluster_name,
                      RadosRef *cluster) {
  dout(20) << ": connecting to cluster=" << cluster_name << ", client=" << client_name
           << dendl;

  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  if (client_name.empty() || !iparams.name.from_str(client_name)) {
    derr << ": error initializing cluster handle for " << cluster_name << dendl;
    return -EINVAL;
  }

  CephContext *cct = common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY,
                                    CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);
  cct->_conf->cluster = cluster_name;

  int r = cct->_conf.parse_config_files(nullptr, nullptr, 0);
  if (r < 0) {
    derr << ": could not read ceph conf: " << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  cct->_conf.parse_env(cct->get_module_type());

  std::vector<const char*> args;
  r = cct->_conf.parse_argv(args);
  if (r < 0) {
    derr << ": could not parse environment: " << cpp_strerror(r) << dendl;
    cct->put();
    return r;
  }
  cct->_conf.parse_env(cct->get_module_type());

  cluster->reset(new librados::Rados());

  r = (*cluster)->init_with_context(cct);
  ceph_assert(r == 0);
  cct->put();

  r = (*cluster)->connect();
  if (r < 0) {
    derr << ": error connecting to " << cluster_name << ": " << cpp_strerror(r)
         << dendl;
    return r;
  }

  dout(10) << ": connected to cluster=" << cluster_name << " using client="
           << client_name << dendl;

  return 0;
}

void FSMirror::run() {
  dout(20) << dendl;

  std::unique_lock locker(m_lock);
  while (true) {
    dout(20) << ": trying to pick from " << m_directories.size() << " directories" << dendl;
    m_cond.wait(locker, [this]{return m_directories.size() || is_stopping();});
    if (is_stopping()) {
      break;
    }

    locker.unlock();
    ::sleep(1);
    locker.lock();
  }
}

void FSMirror::init_replayers() {
  std::scoped_lock locker(m_lock);

  auto replayers = g_ceph_context->_conf.get_val<uint64_t>(
    "cephfs_mirror_max_concurrent_directory_syncs");
  dout(20) << ": spawning " << replayers << " snapshot replayer(s)" << dendl;

  while (replayers-- > 0) {
    std::unique_ptr<SnapshotReplayer> replayer(new SnapshotReplayer(this));
    std::string name("replayer-" + stringify(replayers));
    replayer->create(name.c_str());
    m_snapshot_replayers.push_back(std::move(replayer));
  }
}

void FSMirror::init(Context *on_finish) {
  dout(20) << dendl;

  std::scoped_lock locker(m_lock);
  int r = connect(g_ceph_context->_conf->name.to_str(),
                  g_ceph_context->_conf->cluster, &m_cluster);
  if (r < 0) {
    on_finish->complete(r);
    return;
  }

  m_addrs = m_cluster->get_addrs();
  dout(10) << ": rados addrs=" << m_addrs << dendl;

  r = m_cluster->ioctx_create2(m_pool_id, m_ioctx);
  if (r < 0) {
    derr << ": error accessing local pool (id=" << m_pool_id << "): "
         << cpp_strerror(r) << dendl;
    on_finish->complete(r);
    return;
  }

  init_instance_watcher(on_finish);
}

void FSMirror::shutdown(Context *on_finish) {
  dout(20) << dendl;

  {
    std::scoped_lock locker(m_lock);
    m_stopping = true;
    m_cond.notify_all();
    if (m_on_init_finish != nullptr) {
      dout(10) << ": delaying shutdown -- init in progress" << dendl;
      m_on_shutdown_finish = new LambdaContext([this, on_finish](int r) {
                                                 if (r < 0) {
                                                   on_finish->complete(0);
                                                   return;
                                                 }
                                                 m_on_shutdown_finish = on_finish;
                                                 shutdown_mirror_watcher();
                                               });
      return;
    }

    m_on_shutdown_finish = on_finish;
  }

  wait_for_replayers();
}

void FSMirror::init_instance_watcher(Context *on_finish) {
  dout(20) << dendl;

  m_on_init_finish = new LambdaContext([this, on_finish](int r) {
                                         if (r == 0 ) {
                                           init_replayers();
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
  m_mirror_watcher = MirrorWatcher::create(m_ioctx, m_addrs, m_work_queue);
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

void FSMirror::wait_for_replayers() {
  dout(20) << dendl;

  for (auto &replayer : m_snapshot_replayers) {
    replayer->join();
  }

  m_snapshot_replayers.clear();
  shutdown_mirror_watcher();
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
  m_instance_watcher->shutdown(ctx);
}

void FSMirror::handle_shutdown_instance_watcher(int r) {
  dout(20) << ": r=" << r << dendl;

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

  std::scoped_lock locker(m_lock);
  m_directories.emplace(dir_path);
  m_cond.notify_all();
}

void FSMirror::handle_release_directory(string_view dir_path) {
  dout(5) << ": dir_path=" << dir_path << dendl;

  std::scoped_lock locker(m_lock);
  auto it = m_directories.find(dir_path);
  if (it != m_directories.end()) {
    m_directories.erase(it);
  }
}

void FSMirror::add_peer(const Peer &peer) {
  dout(10) << ": peer=" << peer << dendl;

  std::scoped_lock locker(m_lock);
  m_peers.emplace(peer);
  ceph_assert(m_peers.size() == 1); // support only a single peer
}

void FSMirror::remove_peer(const Peer &peer) {
  dout(10) << ": peer=" << peer << dendl;

  std::scoped_lock locker(m_lock);
  m_peers.erase(peer);
}

void FSMirror::mirror_status(Formatter *f) {
  std::scoped_lock locker(m_lock);
  f->open_object_section("status");
  f->open_object_section("peers");
  for (auto &peer : m_peers) {
    peer.dump(f);
  }
  f->close_section(); // peers
  f->open_object_section("snap_dirs");
  f->dump_int("dir_count", m_directories.size());
  f->close_section(); // snap_dirs
  f->close_section(); // status
}


} // namespace mirror
} // namespace cephfs
