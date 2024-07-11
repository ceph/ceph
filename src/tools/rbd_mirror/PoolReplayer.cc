// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "PoolReplayer.h"
#include "common/Cond.h"
#include "common/Formatter.h"
#include "common/admin_socket.h"
#include "common/ceph_argparse.h"
#include "common/code_environment.h"
#include "common/common_init.h"
#include "common/debug.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "global/global_context.h"
#include "librbd/api/Config.h"
#include "librbd/api/Namespace.h"
#include "PoolMetaCache.h"
#include "RemotePoolPoller.h"
#include "ServiceDaemon.h"
#include "Threads.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::PoolReplayer: " \
                           << this << " " << __func__ << ": "

namespace rbd {
namespace mirror {

using ::operator<<;

namespace {

const std::string SERVICE_DAEMON_INSTANCE_ID_KEY("instance_id");
const std::string SERVICE_DAEMON_LEADER_KEY("leader");

const std::vector<std::string> UNIQUE_PEER_CONFIG_KEYS {
  {"monmap", "mon_host", "mon_dns_srv_name", "key", "keyfile", "keyring"}};

template <typename I>
class PoolReplayerAdminSocketCommand {
public:
  PoolReplayerAdminSocketCommand(PoolReplayer<I> *pool_replayer)
    : pool_replayer(pool_replayer) {
  }
  virtual ~PoolReplayerAdminSocketCommand() {}
  virtual int call(Formatter *f) = 0;
protected:
  PoolReplayer<I> *pool_replayer;
};

template <typename I>
class StatusCommand : public PoolReplayerAdminSocketCommand<I> {
public:
  explicit StatusCommand(PoolReplayer<I> *pool_replayer)
    : PoolReplayerAdminSocketCommand<I>(pool_replayer) {
  }

  int call(Formatter *f) override {
    this->pool_replayer->print_status(f);
    return 0;
  }
};

template <typename I>
class StartCommand : public PoolReplayerAdminSocketCommand<I> {
public:
  explicit StartCommand(PoolReplayer<I> *pool_replayer)
    : PoolReplayerAdminSocketCommand<I>(pool_replayer) {
  }

  int call(Formatter *f) override {
    this->pool_replayer->start();
    return 0;
  }
};

template <typename I>
class StopCommand : public PoolReplayerAdminSocketCommand<I> {
public:
  explicit StopCommand(PoolReplayer<I> *pool_replayer)
    : PoolReplayerAdminSocketCommand<I>(pool_replayer) {
  }

  int call(Formatter *f) override {
    this->pool_replayer->stop(true);
    return 0;
  }
};

template <typename I>
class RestartCommand : public PoolReplayerAdminSocketCommand<I> {
public:
  explicit RestartCommand(PoolReplayer<I> *pool_replayer)
    : PoolReplayerAdminSocketCommand<I>(pool_replayer) {
  }

  int call(Formatter *f) override {
    this->pool_replayer->restart();
    return 0;
  }
};

template <typename I>
class FlushCommand : public PoolReplayerAdminSocketCommand<I> {
public:
  explicit FlushCommand(PoolReplayer<I> *pool_replayer)
    : PoolReplayerAdminSocketCommand<I>(pool_replayer) {
  }

  int call(Formatter *f) override {
    this->pool_replayer->flush();
    return 0;
  }
};

template <typename I>
class LeaderReleaseCommand : public PoolReplayerAdminSocketCommand<I> {
public:
  explicit LeaderReleaseCommand(PoolReplayer<I> *pool_replayer)
    : PoolReplayerAdminSocketCommand<I>(pool_replayer) {
  }

  int call(Formatter *f) override {
    this->pool_replayer->release_leader();
    return 0;
  }
};

template <typename I>
class PoolReplayerAdminSocketHook : public AdminSocketHook {
public:
  PoolReplayerAdminSocketHook(CephContext *cct, const std::string &name,
                              PoolReplayer<I> *pool_replayer)
    : admin_socket(cct->get_admin_socket()) {
    std::string command;
    int r;

    command = "rbd mirror status " + name;
    r = admin_socket->register_command(command, this,
				       "get status for rbd mirror " + name);
    if (r == 0) {
      commands[command] = new StatusCommand<I>(pool_replayer);
    }

    command = "rbd mirror start " + name;
    r = admin_socket->register_command(command, this,
				       "start rbd mirror " + name);
    if (r == 0) {
      commands[command] = new StartCommand<I>(pool_replayer);
    }

    command = "rbd mirror stop " + name;
    r = admin_socket->register_command(command, this,
				       "stop rbd mirror " + name);
    if (r == 0) {
      commands[command] = new StopCommand<I>(pool_replayer);
    }

    command = "rbd mirror restart " + name;
    r = admin_socket->register_command(command, this,
				       "restart rbd mirror " + name);
    if (r == 0) {
      commands[command] = new RestartCommand<I>(pool_replayer);
    }

    command = "rbd mirror flush " + name;
    r = admin_socket->register_command(command, this,
				       "flush rbd mirror " + name);
    if (r == 0) {
      commands[command] = new FlushCommand<I>(pool_replayer);
    }

    command = "rbd mirror leader release " + name;
    r = admin_socket->register_command(command, this,
                                       "release rbd mirror leader " + name);
    if (r == 0) {
      commands[command] = new LeaderReleaseCommand<I>(pool_replayer);
    }
  }

  ~PoolReplayerAdminSocketHook() override {
    (void)admin_socket->unregister_commands(this);
    for (auto i = commands.begin(); i != commands.end(); ++i) {
      delete i->second;
    }
  }

  int call(std::string_view command, const cmdmap_t& cmdmap,
	   const bufferlist&,
	   Formatter *f,
	   std::ostream& ss,
	   bufferlist& out) override {
    auto i = commands.find(command);
    ceph_assert(i != commands.end());
    return i->second->call(f);
  }

private:
  typedef std::map<std::string, PoolReplayerAdminSocketCommand<I>*,
		   std::less<>> Commands;

  AdminSocket *admin_socket;
  Commands commands;
};

} // anonymous namespace

template <typename I>
struct PoolReplayer<I>::RemotePoolPollerListener
  : public remote_pool_poller::Listener {

  PoolReplayer<I>* m_pool_replayer;

  RemotePoolPollerListener(PoolReplayer<I>* pool_replayer)
    : m_pool_replayer(pool_replayer) {
  }

  void handle_updated(const RemotePoolMeta& remote_pool_meta) override {
    m_pool_replayer->handle_remote_pool_meta_updated(remote_pool_meta);
  }
};

template <typename I>
PoolReplayer<I>::PoolReplayer(
    Threads<I> *threads, ServiceDaemon<I> *service_daemon,
    journal::CacheManagerHandler *cache_manager_handler,
    PoolMetaCache* pool_meta_cache, int64_t local_pool_id,
    const PeerSpec &peer, const std::vector<const char*> &args) :
  m_threads(threads),
  m_service_daemon(service_daemon),
  m_cache_manager_handler(cache_manager_handler),
  m_pool_meta_cache(pool_meta_cache),
  m_local_pool_id(local_pool_id),
  m_peer(peer),
  m_args(args),
  m_lock(ceph::make_mutex("rbd::mirror::PoolReplayer " + stringify(peer))),
  m_pool_replayer_thread(this),
  m_leader_listener(this) {
}

template <typename I>
PoolReplayer<I>::~PoolReplayer()
{
  shut_down();

  // pool replayer instances are generally (unless the peer gets
  // updated) preserved across errors to reduce ping-ponging of callout
  // error notifications, so this can't be done in shut_down()
  if (m_callout_id != service_daemon::CALLOUT_ID_NONE) {
    m_service_daemon->remove_callout(m_local_pool_id, m_callout_id);
  }

  ceph_assert(m_asok_hook == nullptr);
}

template <typename I>
bool PoolReplayer<I>::is_blocklisted() const {
  std::lock_guard locker{m_lock};
  return m_blocklisted;
}

template <typename I>
bool PoolReplayer<I>::is_leader() const {
  std::lock_guard locker{m_lock};
  return m_leader_watcher && m_leader_watcher->is_leader();
}

template <typename I>
bool PoolReplayer<I>::is_running() const {
  return m_pool_replayer_thread.is_started() && !m_stopping;
}

template <typename I>
void PoolReplayer<I>::init(const std::string& site_name) {
  std::lock_guard locker{m_lock};

  ceph_assert(!m_pool_replayer_thread.is_started());

  // reset state
  m_stopping = false;
  m_blocklisted = false;
  m_site_name = site_name;

  dout(10) << "replaying for " << m_peer << dendl;
  int r = init_rados(g_ceph_context->_conf->cluster,
                     g_ceph_context->_conf->name.to_str(),
                     "", "", "local cluster", &m_local_rados, false);
  if (r < 0) {
    m_callout_id = m_service_daemon->add_or_update_callout(
      m_local_pool_id, m_callout_id, service_daemon::CALLOUT_LEVEL_ERROR,
      "unable to connect to local cluster");
    return;
  }

  r = init_rados(m_peer.cluster_name, m_peer.client_name,
                 m_peer.mon_host, m_peer.key,
                 std::string("remote peer ") + stringify(m_peer),
                 &m_remote_rados, true);
  if (r < 0) {
    m_callout_id = m_service_daemon->add_or_update_callout(
      m_local_pool_id, m_callout_id, service_daemon::CALLOUT_LEVEL_ERROR,
      "unable to connect to remote cluster");
    return;
  }

  r = m_local_rados->ioctx_create2(m_local_pool_id, m_local_io_ctx);
  if (r < 0) {
    derr << "error accessing local pool " << m_local_pool_id << ": "
         << cpp_strerror(r) << dendl;
    return;
  }

  auto cct = reinterpret_cast<CephContext *>(m_local_io_ctx.cct());
  librbd::api::Config<I>::apply_pool_overrides(m_local_io_ctx, &cct->_conf);

  r = librbd::cls_client::mirror_uuid_get(&m_local_io_ctx,
                                          &m_local_mirror_uuid);
  if (r < 0) {
    derr << "failed to retrieve local mirror uuid from pool "
         << m_local_io_ctx.get_pool_name() << ": " << cpp_strerror(r) << dendl;
    m_callout_id = m_service_daemon->add_or_update_callout(
      m_local_pool_id, m_callout_id, service_daemon::CALLOUT_LEVEL_ERROR,
      "unable to query local mirror uuid");
    return;
  }

  r = m_remote_rados->ioctx_create(m_local_io_ctx.get_pool_name().c_str(),
                                   m_remote_io_ctx);
  if (r < 0) {
    derr << "error accessing remote pool " << m_local_io_ctx.get_pool_name()
         << ": " << cpp_strerror(r) << dendl;
    m_callout_id = m_service_daemon->add_or_update_callout(
      m_local_pool_id, m_callout_id, service_daemon::CALLOUT_LEVEL_WARNING,
      "unable to access remote pool");
    return;
  }

  dout(10) << "connected to " << m_peer << dendl;

  m_image_sync_throttler.reset(
      Throttler<I>::create(cct, "rbd_mirror_concurrent_image_syncs"));

  m_image_deletion_throttler.reset(
      Throttler<I>::create(cct, "rbd_mirror_concurrent_image_deletions"));

  m_remote_pool_poller_listener.reset(new RemotePoolPollerListener(this));
  m_remote_pool_poller.reset(RemotePoolPoller<I>::create(
    m_threads, m_remote_io_ctx, m_site_name, m_local_mirror_uuid,
    *m_remote_pool_poller_listener));

  C_SaferCond on_pool_poller_init;
  m_remote_pool_poller->init(&on_pool_poller_init);
  r = on_pool_poller_init.wait();
  if (r < 0) {
    derr << "failed to initialize remote pool poller: " << cpp_strerror(r)
         << dendl;
    m_callout_id = m_service_daemon->add_or_update_callout(
      m_local_pool_id, m_callout_id, service_daemon::CALLOUT_LEVEL_ERROR,
      "unable to initialize remote pool poller");
    m_remote_pool_poller.reset();
    return;
  }
  ceph_assert(!m_remote_pool_meta.mirror_uuid.empty());
  m_pool_meta_cache->set_remote_pool_meta(
    m_remote_io_ctx.get_id(), m_remote_pool_meta);
  m_pool_meta_cache->set_local_pool_meta(
    m_local_io_ctx.get_id(), {m_local_mirror_uuid});

  m_default_namespace_replayer.reset(NamespaceReplayer<I>::create(
      "", m_local_io_ctx, m_remote_io_ctx, m_local_mirror_uuid, m_peer.uuid,
      m_remote_pool_meta, m_threads, m_image_sync_throttler.get(),
      m_image_deletion_throttler.get(), m_service_daemon,
      m_cache_manager_handler, m_pool_meta_cache));

  C_SaferCond on_init;
  m_default_namespace_replayer->init(&on_init);
  r = on_init.wait();
  if (r < 0) {
    derr << "error initializing default namespace replayer: " << cpp_strerror(r)
         << dendl;
    m_callout_id = m_service_daemon->add_or_update_callout(
      m_local_pool_id, m_callout_id, service_daemon::CALLOUT_LEVEL_ERROR,
      "unable to initialize default namespace replayer");
    m_default_namespace_replayer.reset();
    return;
  }

  m_leader_watcher.reset(LeaderWatcher<I>::create(m_threads, m_local_io_ctx,
                                                  &m_leader_listener));
  r = m_leader_watcher->init();
  if (r < 0) {
    derr << "error initializing leader watcher: " << cpp_strerror(r) << dendl;
    m_callout_id = m_service_daemon->add_or_update_callout(
      m_local_pool_id, m_callout_id, service_daemon::CALLOUT_LEVEL_ERROR,
      "unable to initialize leader messenger object");
    m_leader_watcher.reset();
    return;
  }

  if (m_callout_id != service_daemon::CALLOUT_ID_NONE) {
    m_service_daemon->remove_callout(m_local_pool_id, m_callout_id);
    m_callout_id = service_daemon::CALLOUT_ID_NONE;
  }

  m_service_daemon->add_or_update_attribute(
    m_local_io_ctx.get_id(), SERVICE_DAEMON_INSTANCE_ID_KEY,
    stringify(m_local_io_ctx.get_instance_id()));

  m_pool_replayer_thread.create("pool replayer");
}

template <typename I>
void PoolReplayer<I>::shut_down() {
  dout(20) << dendl;
  {
    std::lock_guard l{m_lock};
    m_stopping = true;
    m_cond.notify_all();
  }
  if (m_pool_replayer_thread.is_started()) {
    m_pool_replayer_thread.join();
  }

  if (m_leader_watcher) {
    m_leader_watcher->shut_down();
  }
  m_leader_watcher.reset();

  if (m_default_namespace_replayer) {
    C_SaferCond on_shut_down;
    m_default_namespace_replayer->shut_down(&on_shut_down);
    on_shut_down.wait();
  }
  m_default_namespace_replayer.reset();

  if (m_remote_pool_poller) {
    C_SaferCond ctx;
    m_remote_pool_poller->shut_down(&ctx);
    ctx.wait();

    m_pool_meta_cache->remove_remote_pool_meta(m_remote_io_ctx.get_id());
    m_pool_meta_cache->remove_local_pool_meta(m_local_io_ctx.get_id());
  }
  m_remote_pool_poller.reset();
  m_remote_pool_poller_listener.reset();

  m_image_sync_throttler.reset();
  m_image_deletion_throttler.reset();

  m_local_rados.reset();
  m_remote_rados.reset();
}

template <typename I>
int PoolReplayer<I>::init_rados(const std::string &cluster_name,
			        const std::string &client_name,
                                const std::string &mon_host,
                                const std::string &key,
			        const std::string &description,
			        RadosRef *rados_ref,
                                bool strip_cluster_overrides) {
  dout(10) << "cluster_name=" << cluster_name << ", client_name=" << client_name
	   << ", mon_host=" << mon_host << ", strip_cluster_overrides="
	   << strip_cluster_overrides << dendl;

  // NOTE: manually bootstrap a CephContext here instead of via
  // the librados API to avoid mixing global singletons between
  // the librados shared library and the daemon
  // TODO: eliminate intermingling of global singletons within Ceph APIs
  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  if (client_name.empty() || !iparams.name.from_str(client_name)) {
    derr << "error initializing cluster handle for " << description << dendl;
    return -EINVAL;
  }

  CephContext *cct = common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY,
                                    CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);
  cct->_conf->cluster = cluster_name;

  // librados::Rados::conf_read_file
  int r = cct->_conf.parse_config_files(nullptr, nullptr, 0);
  if (r < 0 && r != -ENOENT) {
    // do not treat this as fatal, it might still be able to connect
    derr << "could not read ceph conf for " << description << ": "
	 << cpp_strerror(r) << dendl;
  }

  // preserve cluster-specific config settings before applying environment/cli
  // overrides
  std::map<std::string, std::string> config_values;
  if (strip_cluster_overrides) {
    // remote peer connections shouldn't apply cluster-specific
    // configuration settings
    for (auto& key : UNIQUE_PEER_CONFIG_KEYS) {
      config_values[key] = cct->_conf.get_val<std::string>(key);
    }
  }

  cct->_conf.parse_env(cct->get_module_type());

  // librados::Rados::conf_parse_env
  std::vector<const char*> args;
  r = cct->_conf.parse_argv(args);
  if (r < 0) {
    derr << "could not parse environment for " << description << ":"
         << cpp_strerror(r) << dendl;
    cct->put();
    return r;
  }
  cct->_conf.parse_env(cct->get_module_type());

  if (!m_args.empty()) {
    // librados::Rados::conf_parse_argv
    args = m_args;
    r = cct->_conf.parse_argv(args);
    if (r < 0) {
      derr << "could not parse command line args for " << description << ": "
	   << cpp_strerror(r) << dendl;
      cct->put();
      return r;
    }
  }

  if (strip_cluster_overrides) {
    // remote peer connections shouldn't apply cluster-specific
    // configuration settings
    for (auto& pair : config_values) {
      auto value = cct->_conf.get_val<std::string>(pair.first);
      if (pair.second != value) {
        dout(0) << "reverting global config option override: "
                << pair.first << ": " << value << " -> " << pair.second
                << dendl;
        cct->_conf.set_val_or_die(pair.first, pair.second);
      }
    }
  }

  if (!g_ceph_context->_conf->admin_socket.empty()) {
    cct->_conf.set_val_or_die("admin_socket",
                               "$run_dir/$name.$pid.$cluster.$cctid.asok");
  }

  if (!mon_host.empty()) {
    r = cct->_conf.set_val("mon_host", mon_host);
    if (r < 0) {
      derr << "failed to set mon_host config for " << description << ": "
           << cpp_strerror(r) << dendl;
      cct->put();
      return r;
    }
  }

  if (!key.empty()) {
    r = cct->_conf.set_val("key", key);
    if (r < 0) {
      derr << "failed to set key config for " << description << ": "
           << cpp_strerror(r) << dendl;
      cct->put();
      return r;
    }
  }

  // disable unnecessary librbd cache
  cct->_conf.set_val_or_die("rbd_cache", "false");
  cct->_conf.apply_changes(nullptr);
  cct->_conf.complain_about_parse_error(cct);

  rados_ref->reset(new librados::Rados());

  r = (*rados_ref)->init_with_context(cct);
  ceph_assert(r == 0);
  cct->put();

  r = (*rados_ref)->connect();
  if (r < 0) {
    derr << "error connecting to " << description << ": "
	 << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

template <typename I>
void PoolReplayer<I>::run() {
  dout(20) << dendl;

  while (true) {
    std::string asok_hook_name = m_local_io_ctx.get_pool_name() + " " +
                                 m_peer.cluster_name;
    if (m_asok_hook_name != asok_hook_name || m_asok_hook == nullptr) {
      m_asok_hook_name = asok_hook_name;
      delete m_asok_hook;

      m_asok_hook = new PoolReplayerAdminSocketHook<I>(g_ceph_context,
						       m_asok_hook_name, this);
    }

    with_namespace_replayers([this]() { update_namespace_replayers(); });

    std::unique_lock locker{m_lock};

    if (m_leader_watcher->is_blocklisted() ||
        m_default_namespace_replayer->is_blocklisted()) {
      m_blocklisted = true;
      m_stopping = true;
    }

    for (auto &it : m_namespace_replayers) {
      if (it.second->is_blocklisted()) {
        m_blocklisted = true;
        m_stopping = true;
        break;
      }
    }

    if (m_stopping) {
      break;
    }

    auto seconds = g_ceph_context->_conf.get_val<uint64_t>(
        "rbd_mirror_pool_replayers_refresh_interval");
    m_cond.wait_for(locker, ceph::make_timespan(seconds));
  }

  // shut down namespace replayers
  with_namespace_replayers([this]() { update_namespace_replayers(); });

  delete m_asok_hook;
  m_asok_hook = nullptr;
}

template <typename I>
void PoolReplayer<I>::update_namespace_replayers() {
  dout(20) << dendl;

  ceph_assert(ceph_mutex_is_locked(m_lock));

  std::set<std::string> mirroring_namespaces;
  if (!m_stopping) {
    int r = list_mirroring_namespaces(&mirroring_namespaces);
    if (r < 0) {
      return;
    }
  }

  auto cct = reinterpret_cast<CephContext *>(m_local_io_ctx.cct());
  C_SaferCond cond;
  auto gather_ctx = new C_Gather(cct, &cond);
  for (auto it = m_namespace_replayers.begin();
       it != m_namespace_replayers.end(); ) {
    auto iter = mirroring_namespaces.find(it->first);
    if (iter == mirroring_namespaces.end()) {
      auto namespace_replayer = it->second;
      auto on_shut_down = new LambdaContext(
        [namespace_replayer, ctx=gather_ctx->new_sub()](int r) {
          delete namespace_replayer;
          ctx->complete(r);
        });
      m_service_daemon->remove_namespace(m_local_pool_id, it->first);
      namespace_replayer->shut_down(on_shut_down);
      it = m_namespace_replayers.erase(it);
    } else {
      mirroring_namespaces.erase(iter);
      it++;
    }
  }

  for (auto &name : mirroring_namespaces) {
    auto namespace_replayer = NamespaceReplayer<I>::create(
        name, m_local_io_ctx, m_remote_io_ctx, m_local_mirror_uuid, m_peer.uuid,
        m_remote_pool_meta, m_threads, m_image_sync_throttler.get(),
        m_image_deletion_throttler.get(), m_service_daemon,
        m_cache_manager_handler, m_pool_meta_cache);
    auto on_init = new LambdaContext(
        [this, namespace_replayer, name, &mirroring_namespaces,
         ctx=gather_ctx->new_sub()](int r) {
          std::lock_guard locker{m_lock};
          if (r < 0) {
            derr << "failed to initialize namespace replayer for namespace "
                 << name << ": " << cpp_strerror(r) << dendl;
            delete namespace_replayer;
            mirroring_namespaces.erase(name);
          } else {
            m_namespace_replayers[name] = namespace_replayer;
            m_service_daemon->add_namespace(m_local_pool_id, name);
          }
          ctx->complete(r);
        });
    namespace_replayer->init(on_init);
  }

  gather_ctx->activate();

  m_lock.unlock();
  cond.wait();
  m_lock.lock();

  if (m_leader) {
    C_SaferCond acquire_cond;
    auto acquire_gather_ctx = new C_Gather(cct, &acquire_cond);

    for (auto &name : mirroring_namespaces) {
      namespace_replayer_acquire_leader(name, acquire_gather_ctx->new_sub());
    }
    acquire_gather_ctx->activate();

    m_lock.unlock();
    acquire_cond.wait();
    m_lock.lock();

    std::vector<std::string> instance_ids;
    m_leader_watcher->list_instances(&instance_ids);

    for (auto &name : mirroring_namespaces) {
      auto it = m_namespace_replayers.find(name);
      if (it == m_namespace_replayers.end()) {
        // acquire leader for this namespace replayer failed
        continue;
      }
      it->second->handle_instances_added(instance_ids);
    }
  } else {
    std::string leader_instance_id;
    if (m_leader_watcher->get_leader_instance_id(&leader_instance_id)) {
      for (auto &name : mirroring_namespaces) {
        m_namespace_replayers[name]->handle_update_leader(leader_instance_id);
      }
    }
  }
}

template <typename I>
int PoolReplayer<I>::list_mirroring_namespaces(
    std::set<std::string> *namespaces) {
  dout(20) << dendl;
  ceph_assert(ceph_mutex_is_locked(m_lock));

  std::vector<std::string> names;

  int r = librbd::api::Namespace<I>::list(m_local_io_ctx, &names);
  if (r < 0) {
    derr << "failed to list namespaces: " << cpp_strerror(r) << dendl;
    return r;
  }

  for (auto &name : names) {
    cls::rbd::MirrorMode mirror_mode = cls::rbd::MIRROR_MODE_DISABLED;
    int r = librbd::cls_client::mirror_mode_get(&m_local_io_ctx, &mirror_mode);
    if (r < 0 && r != -ENOENT) {
      derr << "failed to get namespace mirror mode: " << cpp_strerror(r)
           << dendl;
      if (m_namespace_replayers.count(name) == 0) {
        continue;
      }
    } else if (mirror_mode == cls::rbd::MIRROR_MODE_DISABLED) {
      dout(10) << "mirroring is disabled for namespace " << name << dendl;
      continue;
    }

    namespaces->insert(name);
  }

  return 0;
}

template <typename I>
void PoolReplayer<I>::reopen_logs()
{
  dout(20) << dendl;
  std::lock_guard locker{m_lock};

  if (m_local_rados) {
    reinterpret_cast<CephContext *>(m_local_rados->cct())->reopen_logs();
  }
  if (m_remote_rados) {
    reinterpret_cast<CephContext *>(m_remote_rados->cct())->reopen_logs();
  }
}

template <typename I>
void PoolReplayer<I>::namespace_replayer_acquire_leader(const std::string &name,
                                                        Context *on_finish) {
  dout(20) << dendl;
  ceph_assert(ceph_mutex_is_locked(m_lock));

  auto it = m_namespace_replayers.find(name);
  ceph_assert(it != m_namespace_replayers.end());

  on_finish = new LambdaContext(
      [this, name, on_finish](int r) {
        if (r < 0) {
          derr << "failed to handle acquire leader for namespace: "
               << name << ": " << cpp_strerror(r) << dendl;

          // remove the namespace replayer -- update_namespace_replayers will
          // retry to create it and acquire leader.

          std::lock_guard locker{m_lock};

          auto namespace_replayer = m_namespace_replayers[name];
          m_namespace_replayers.erase(name);
          auto on_shut_down = new LambdaContext(
              [namespace_replayer, on_finish](int r) {
                delete namespace_replayer;
                on_finish->complete(r);
              });
          m_service_daemon->remove_namespace(m_local_pool_id, name);
          namespace_replayer->shut_down(on_shut_down);
          return;
        }
        on_finish->complete(0);
      });

  it->second->handle_acquire_leader(on_finish);
}

template <typename I>
void PoolReplayer<I>::print_status(Formatter *f) {
  dout(20) << dendl;

  assert(f);

  std::lock_guard l{m_lock};

  f->open_object_section("pool_replayer_status");
  f->dump_stream("peer") << m_peer;
  if (m_local_io_ctx.is_valid()) {
    f->dump_string("pool", m_local_io_ctx.get_pool_name());
    f->dump_stream("instance_id") << m_local_io_ctx.get_instance_id();
  }

  std::string state("running");
  if (m_manual_stop) {
    state = "stopped (manual)";
  } else if (m_stopping) {
    state = "stopped";
  } else if (!is_running()) {
    state = "error";
  }
  f->dump_string("state", state);

  if (m_leader_watcher) {
    std::string leader_instance_id;
    m_leader_watcher->get_leader_instance_id(&leader_instance_id);
    f->dump_string("leader_instance_id", leader_instance_id);

    bool leader = m_leader_watcher->is_leader();
    f->dump_bool("leader", leader);
    if (leader) {
      std::vector<std::string> instance_ids;
      m_leader_watcher->list_instances(&instance_ids);
      f->open_array_section("instances");
      for (auto instance_id : instance_ids) {
        f->dump_string("instance_id", instance_id);
      }
      f->close_section(); // instances
    }
  }

  if (m_local_rados) {
    auto cct = reinterpret_cast<CephContext *>(m_local_rados->cct());
    f->dump_string("local_cluster_admin_socket",
                   cct->_conf.get_val<std::string>("admin_socket"));
  }
  if (m_remote_rados) {
    auto cct = reinterpret_cast<CephContext *>(m_remote_rados->cct());
    f->dump_string("remote_cluster_admin_socket",
                   cct->_conf.get_val<std::string>("admin_socket"));
  }

  if (m_image_sync_throttler) {
    f->open_object_section("sync_throttler");
    m_image_sync_throttler->print_status(f);
    f->close_section(); // sync_throttler
  }

  if (m_image_deletion_throttler) {
    f->open_object_section("deletion_throttler");
    m_image_deletion_throttler->print_status(f);
    f->close_section(); // deletion_throttler
  }

  if (m_default_namespace_replayer) {
    m_default_namespace_replayer->print_status(f);
  }

  f->open_array_section("namespaces");
  for (auto &it : m_namespace_replayers) {
    f->open_object_section("namespace");
    f->dump_string("name", it.first);
    it.second->print_status(f);
    f->close_section(); // namespace
  }
  f->close_section(); // namespaces

  f->close_section(); // pool_replayer_status
}

template <typename I>
void PoolReplayer<I>::start() {
  dout(20) << dendl;

  std::lock_guard l{m_lock};

  if (m_stopping) {
    return;
  }

  m_manual_stop = false;

  if (m_default_namespace_replayer) {
    m_default_namespace_replayer->start();
  }
  for (auto &it : m_namespace_replayers) {
    it.second->start();
  }
}

template <typename I>
void PoolReplayer<I>::stop(bool manual) {
  dout(20) << "enter: manual=" << manual << dendl;

  std::lock_guard l{m_lock};
  if (!manual) {
    m_stopping = true;
    m_cond.notify_all();
    return;
  } else if (m_stopping) {
    return;
  }

  m_manual_stop = true;

  if (m_default_namespace_replayer) {
    m_default_namespace_replayer->stop();
  }
  for (auto &it : m_namespace_replayers) {
    it.second->stop();
  }
}

template <typename I>
void PoolReplayer<I>::restart() {
  dout(20) << dendl;

  std::lock_guard l{m_lock};

  if (m_stopping) {
    return;
  }

  if (m_default_namespace_replayer) {
    m_default_namespace_replayer->restart();
  }
  for (auto &it : m_namespace_replayers) {
    it.second->restart();
  }
}

template <typename I>
void PoolReplayer<I>::flush() {
  dout(20) << dendl;

  std::lock_guard l{m_lock};

  if (m_stopping || m_manual_stop) {
    return;
  }

  if (m_default_namespace_replayer) {
    m_default_namespace_replayer->flush();
  }
  for (auto &it : m_namespace_replayers) {
    it.second->flush();
  }
}

template <typename I>
void PoolReplayer<I>::release_leader() {
  dout(20) << dendl;

  std::lock_guard l{m_lock};

  if (m_stopping || !m_leader_watcher) {
    return;
  }

  m_leader_watcher->release_leader();
}

template <typename I>
void PoolReplayer<I>::handle_post_acquire_leader(Context *on_finish) {
  dout(20) << dendl;

  with_namespace_replayers(
      [this](Context *on_finish) {
        dout(10) << "handle_post_acquire_leader" << dendl;

        ceph_assert(ceph_mutex_is_locked(m_lock));

        m_service_daemon->add_or_update_attribute(m_local_pool_id,
                                                  SERVICE_DAEMON_LEADER_KEY,
                                                  true);
        auto ctx = new LambdaContext(
            [this, on_finish](int r) {
              if (r == 0) {
                std::lock_guard locker{m_lock};
                m_leader = true;
              }
              on_finish->complete(r);
            });

        auto cct = reinterpret_cast<CephContext *>(m_local_io_ctx.cct());
        auto gather_ctx = new C_Gather(cct, ctx);

        m_default_namespace_replayer->handle_acquire_leader(
            gather_ctx->new_sub());

        for (auto &it : m_namespace_replayers) {
          namespace_replayer_acquire_leader(it.first, gather_ctx->new_sub());
        }

        gather_ctx->activate();
      }, on_finish);
}

template <typename I>
void PoolReplayer<I>::handle_pre_release_leader(Context *on_finish) {
  dout(20) << dendl;

  with_namespace_replayers(
      [this](Context *on_finish) {
        dout(10) << "handle_pre_release_leader" << dendl;

        ceph_assert(ceph_mutex_is_locked(m_lock));

        m_leader = false;
        m_service_daemon->remove_attribute(m_local_pool_id,
                                           SERVICE_DAEMON_LEADER_KEY);

        auto cct = reinterpret_cast<CephContext *>(m_local_io_ctx.cct());
        auto gather_ctx = new C_Gather(cct, on_finish);

        m_default_namespace_replayer->handle_release_leader(
            gather_ctx->new_sub());

        for (auto &it : m_namespace_replayers) {
          it.second->handle_release_leader(gather_ctx->new_sub());
        }

        gather_ctx->activate();
      }, on_finish);
}

template <typename I>
void PoolReplayer<I>::handle_update_leader(
    const std::string &leader_instance_id) {
  dout(10) << "leader_instance_id=" << leader_instance_id << dendl;

  std::lock_guard locker{m_lock};

  m_default_namespace_replayer->handle_update_leader(leader_instance_id);

  for (auto &it : m_namespace_replayers) {
    it.second->handle_update_leader(leader_instance_id);
  }
}

template <typename I>
void PoolReplayer<I>::handle_instances_added(
    const std::vector<std::string> &instance_ids) {
  dout(5) << "instance_ids=" << instance_ids << dendl;

  std::lock_guard locker{m_lock};
  if (!m_leader_watcher->is_leader()) {
    return;
  }

  m_default_namespace_replayer->handle_instances_added(instance_ids);

  for (auto &it : m_namespace_replayers) {
    it.second->handle_instances_added(instance_ids);
  }
}

template <typename I>
void PoolReplayer<I>::handle_instances_removed(
    const std::vector<std::string> &instance_ids) {
  dout(5) << "instance_ids=" << instance_ids << dendl;

  std::lock_guard locker{m_lock};
  if (!m_leader_watcher->is_leader()) {
    return;
  }

  m_default_namespace_replayer->handle_instances_removed(instance_ids);

  for (auto &it : m_namespace_replayers) {
    it.second->handle_instances_removed(instance_ids);
  }
}

template <typename I>
void PoolReplayer<I>::handle_remote_pool_meta_updated(
    const RemotePoolMeta& remote_pool_meta) {
  dout(5) << "remote_pool_meta=" << remote_pool_meta << dendl;

  if (!m_default_namespace_replayer) {
    m_remote_pool_meta = remote_pool_meta;
    return;
  }

  derr << "remote pool metadata updated unexpectedly" << dendl;
  std::unique_lock locker{m_lock};
  m_stopping = true;
  m_cond.notify_all();
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::PoolReplayer<librbd::ImageCtx>;
