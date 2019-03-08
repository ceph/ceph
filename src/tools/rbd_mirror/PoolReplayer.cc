// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "PoolReplayer.h"
#include <boost/bind.hpp>
#include "common/Formatter.h"
#include "common/admin_socket.h"
#include "common/ceph_argparse.h"
#include "common/code_environment.h"
#include "common/common_init.h"
#include "common/debug.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "cls/rbd/cls_rbd_client.h"
#include "global/global_context.h"
#include "librbd/internal.h"
#include "librbd/Utils.h"
#include "librbd/Watcher.h"
#include "librbd/api/Config.h"
#include "librbd/api/Mirror.h"
#include "ImageMap.h"
#include "InstanceReplayer.h"
#include "InstanceWatcher.h"
#include "LeaderWatcher.h"
#include "ServiceDaemon.h"
#include "Threads.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::PoolReplayer: " \
                           << this << " " << __func__ << ": "

using std::chrono::seconds;
using std::map;
using std::string;
using std::unique_ptr;
using std::vector;

using librbd::cls_client::dir_get_name;
using librbd::util::create_async_context_callback;

namespace rbd {
namespace mirror {

using ::operator<<;

namespace {

const std::string SERVICE_DAEMON_INSTANCE_ID_KEY("instance_id");
const std::string SERVICE_DAEMON_LEADER_KEY("leader");
const std::string SERVICE_DAEMON_LOCAL_COUNT_KEY("image_local_count");
const std::string SERVICE_DAEMON_REMOTE_COUNT_KEY("image_remote_count");

const std::vector<std::string> UNIQUE_PEER_CONFIG_KEYS {
  {"monmap", "mon_host", "mon_dns_srv_name", "key", "keyfile", "keyring"}};

template <typename I>
class PoolReplayerAdminSocketCommand {
public:
  PoolReplayerAdminSocketCommand(PoolReplayer<I> *pool_replayer)
    : pool_replayer(pool_replayer) {
  }
  virtual ~PoolReplayerAdminSocketCommand() {}
  virtual bool call(Formatter *f, stringstream *ss) = 0;
protected:
  PoolReplayer<I> *pool_replayer;
};

template <typename I>
class StatusCommand : public PoolReplayerAdminSocketCommand<I> {
public:
  explicit StatusCommand(PoolReplayer<I> *pool_replayer)
    : PoolReplayerAdminSocketCommand<I>(pool_replayer) {
  }

  bool call(Formatter *f, stringstream *ss) override {
    this->pool_replayer->print_status(f, ss);
    return true;
  }
};

template <typename I>
class StartCommand : public PoolReplayerAdminSocketCommand<I> {
public:
  explicit StartCommand(PoolReplayer<I> *pool_replayer)
    : PoolReplayerAdminSocketCommand<I>(pool_replayer) {
  }

  bool call(Formatter *f, stringstream *ss) override {
    this->pool_replayer->start();
    return true;
  }
};

template <typename I>
class StopCommand : public PoolReplayerAdminSocketCommand<I> {
public:
  explicit StopCommand(PoolReplayer<I> *pool_replayer)
    : PoolReplayerAdminSocketCommand<I>(pool_replayer) {
  }

  bool call(Formatter *f, stringstream *ss) override {
    this->pool_replayer->stop(true);
    return true;
  }
};

template <typename I>
class RestartCommand : public PoolReplayerAdminSocketCommand<I> {
public:
  explicit RestartCommand(PoolReplayer<I> *pool_replayer)
    : PoolReplayerAdminSocketCommand<I>(pool_replayer) {
  }

  bool call(Formatter *f, stringstream *ss) override {
    this->pool_replayer->restart();
    return true;
  }
};

template <typename I>
class FlushCommand : public PoolReplayerAdminSocketCommand<I> {
public:
  explicit FlushCommand(PoolReplayer<I> *pool_replayer)
    : PoolReplayerAdminSocketCommand<I>(pool_replayer) {
  }

  bool call(Formatter *f, stringstream *ss) override {
    this->pool_replayer->flush();
    return true;
  }
};

template <typename I>
class LeaderReleaseCommand : public PoolReplayerAdminSocketCommand<I> {
public:
  explicit LeaderReleaseCommand(PoolReplayer<I> *pool_replayer)
    : PoolReplayerAdminSocketCommand<I>(pool_replayer) {
  }

  bool call(Formatter *f, stringstream *ss) override {
    this->pool_replayer->release_leader();
    return true;
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
    r = admin_socket->register_command(command, command, this,
				       "get status for rbd mirror " + name);
    if (r == 0) {
      commands[command] = new StatusCommand<I>(pool_replayer);
    }

    command = "rbd mirror start " + name;
    r = admin_socket->register_command(command, command, this,
				       "start rbd mirror " + name);
    if (r == 0) {
      commands[command] = new StartCommand<I>(pool_replayer);
    }

    command = "rbd mirror stop " + name;
    r = admin_socket->register_command(command, command, this,
				       "stop rbd mirror " + name);
    if (r == 0) {
      commands[command] = new StopCommand<I>(pool_replayer);
    }

    command = "rbd mirror restart " + name;
    r = admin_socket->register_command(command, command, this,
				       "restart rbd mirror " + name);
    if (r == 0) {
      commands[command] = new RestartCommand<I>(pool_replayer);
    }

    command = "rbd mirror flush " + name;
    r = admin_socket->register_command(command, command, this,
				       "flush rbd mirror " + name);
    if (r == 0) {
      commands[command] = new FlushCommand<I>(pool_replayer);
    }

    command = "rbd mirror leader release " + name;
    r = admin_socket->register_command(command, command, this,
                                       "release rbd mirror leader " + name);
    if (r == 0) {
      commands[command] = new LeaderReleaseCommand<I>(pool_replayer);
    }
  }

  ~PoolReplayerAdminSocketHook() override {
    for (auto i = commands.begin(); i != commands.end(); ++i) {
      (void)admin_socket->unregister_command(i->first);
      delete i->second;
    }
  }

  bool call(std::string_view command, const cmdmap_t& cmdmap,
	    std::string_view format, bufferlist& out) override {
    auto i = commands.find(command);
    ceph_assert(i != commands.end());
    Formatter *f = Formatter::create(format);
    stringstream ss;
    bool r = i->second->call(f, &ss);
    delete f;
    out.append(ss);
    return r;
  }

private:
  typedef std::map<std::string, PoolReplayerAdminSocketCommand<I>*,
		   std::less<>> Commands;

  AdminSocket *admin_socket;
  Commands commands;
};

} // anonymous namespace

template <typename I>
PoolReplayer<I>::PoolReplayer(Threads<I> *threads,
                              ServiceDaemon<I>* service_daemon,
			      int64_t local_pool_id, const PeerSpec &peer,
			      const std::vector<const char*> &args) :
  m_threads(threads),
  m_service_daemon(service_daemon),
  m_local_pool_id(local_pool_id),
  m_peer(peer),
  m_args(args),
  m_lock(stringify("rbd::mirror::PoolReplayer ") + stringify(peer)),
  m_local_pool_watcher_listener(this, true),
  m_remote_pool_watcher_listener(this, false),
  m_image_map_listener(this),
  m_pool_replayer_thread(this),
  m_leader_listener(this)
{
}

template <typename I>
PoolReplayer<I>::~PoolReplayer()
{
  delete m_asok_hook;
  shut_down();
}

template <typename I>
bool PoolReplayer<I>::is_blacklisted() const {
  Mutex::Locker locker(m_lock);
  return m_blacklisted;
}

template <typename I>
bool PoolReplayer<I>::is_leader() const {
  Mutex::Locker locker(m_lock);
  return m_leader_watcher && m_leader_watcher->is_leader();
}

template <typename I>
bool PoolReplayer<I>::is_running() const {
  return m_pool_replayer_thread.is_started();
}

template <typename I>
void PoolReplayer<I>::init()
{
  ceph_assert(!m_pool_replayer_thread.is_started());

  // reset state
  m_stopping = false;
  m_blacklisted = false;

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

  std::string local_mirror_uuid;
  r = librbd::cls_client::mirror_uuid_get(&m_local_io_ctx,
                                          &local_mirror_uuid);
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

  m_instance_replayer.reset(InstanceReplayer<I>::create(
    m_threads, m_service_daemon, m_local_rados, local_mirror_uuid,
    m_local_pool_id));
  m_instance_replayer->init();
  m_instance_replayer->add_peer(m_peer.uuid, m_remote_io_ctx);

  m_instance_watcher.reset(InstanceWatcher<I>::create(
    m_local_io_ctx, m_threads->work_queue, m_instance_replayer.get()));
  r = m_instance_watcher->init();
  if (r < 0) {
    derr << "error initializing instance watcher: " << cpp_strerror(r) << dendl;
    m_callout_id = m_service_daemon->add_or_update_callout(
      m_local_pool_id, m_callout_id, service_daemon::CALLOUT_LEVEL_ERROR,
      "unable to initialize instance messenger object");
    return;
  }
  m_service_daemon->add_or_update_attribute(
      m_local_pool_id, SERVICE_DAEMON_INSTANCE_ID_KEY,
      m_instance_watcher->get_instance_id());

  m_leader_watcher.reset(LeaderWatcher<I>::create(m_threads, m_local_io_ctx,
                                                  &m_leader_listener));
  r = m_leader_watcher->init();
  if (r < 0) {
    derr << "error initializing leader watcher: " << cpp_strerror(r) << dendl;
    m_callout_id = m_service_daemon->add_or_update_callout(
      m_local_pool_id, m_callout_id, service_daemon::CALLOUT_LEVEL_ERROR,
      "unable to initialize leader messenger object");
    return;
  }

  if (m_callout_id != service_daemon::CALLOUT_ID_NONE) {
    m_service_daemon->remove_callout(m_local_pool_id, m_callout_id);
    m_callout_id = service_daemon::CALLOUT_ID_NONE;
  }

  m_pool_replayer_thread.create("pool replayer");
}

template <typename I>
void PoolReplayer<I>::shut_down() {
  m_stopping = true;
  {
    Mutex::Locker l(m_lock);
    m_cond.Signal();
  }
  if (m_pool_replayer_thread.is_started()) {
    m_pool_replayer_thread.join();
  }
  if (m_leader_watcher) {
    m_leader_watcher->shut_down();
  }
  if (m_instance_watcher) {
    m_instance_watcher->shut_down();
  }
  if (m_instance_replayer) {
    m_instance_replayer->shut_down();
  }

  m_leader_watcher.reset();
  m_instance_watcher.reset();
  m_instance_replayer.reset();

  ceph_assert(!m_image_map);
  ceph_assert(!m_image_deleter);
  ceph_assert(!m_local_pool_watcher);
  ceph_assert(!m_remote_pool_watcher);
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
  rados_ref->reset(new librados::Rados());

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
    derr << "could not read ceph conf for " << description << ": "
	 << cpp_strerror(r) << dendl;
    cct->put();
    return r;
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
  cct->_conf.complain_about_parse_errors(cct);

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
void PoolReplayer<I>::run()
{
  dout(20) << "enter" << dendl;

  while (!m_stopping) {
    std::string asok_hook_name = m_local_io_ctx.get_pool_name() + " " +
                                 m_peer.cluster_name;
    if (m_asok_hook_name != asok_hook_name || m_asok_hook == nullptr) {
      m_asok_hook_name = asok_hook_name;
      delete m_asok_hook;

      m_asok_hook = new PoolReplayerAdminSocketHook<I>(g_ceph_context,
						       m_asok_hook_name, this);
    }

    Mutex::Locker locker(m_lock);
    if ((m_local_pool_watcher && m_local_pool_watcher->is_blacklisted()) ||
	(m_remote_pool_watcher && m_remote_pool_watcher->is_blacklisted())) {
      m_blacklisted = true;
      m_stopping = true;
      break;
    }

    if (!m_stopping) {
      m_cond.WaitInterval(m_lock, utime_t(1, 0));
    }
  }

  m_instance_replayer->stop();
}

template <typename I>
void PoolReplayer<I>::print_status(Formatter *f, stringstream *ss)
{
  dout(20) << "enter" << dendl;

  if (!f) {
    return;
  }

  Mutex::Locker l(m_lock);

  f->open_object_section("pool_replayer_status");
  f->dump_string("pool", m_local_io_ctx.get_pool_name());
  f->dump_stream("peer") << m_peer;
  f->dump_string("instance_id", m_instance_watcher->get_instance_id());

  std::string state("running");
  if (m_manual_stop) {
    state = "stopped (manual)";
  } else if (m_stopping) {
    state = "stopped";
  }
  f->dump_string("state", state);

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
    f->close_section();
  }

  f->dump_string("local_cluster_admin_socket",
                 reinterpret_cast<CephContext *>(m_local_io_ctx.cct())->_conf.
                     get_val<std::string>("admin_socket"));
  f->dump_string("remote_cluster_admin_socket",
                 reinterpret_cast<CephContext *>(m_remote_io_ctx.cct())->_conf.
                     get_val<std::string>("admin_socket"));

  f->open_object_section("sync_throttler");
  m_instance_watcher->print_sync_status(f, ss);
  f->close_section();

  m_instance_replayer->print_status(f, ss);

  if (m_image_deleter) {
    f->open_object_section("image_deleter");
    m_image_deleter->print_status(f, ss);
    f->close_section();
  }

  f->close_section();
  f->flush(*ss);
}

template <typename I>
void PoolReplayer<I>::start()
{
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_lock);

  if (m_stopping) {
    return;
  }

  m_manual_stop = false;
  m_instance_replayer->start();
}

template <typename I>
void PoolReplayer<I>::stop(bool manual)
{
  dout(20) << "enter: manual=" << manual << dendl;

  Mutex::Locker l(m_lock);
  if (!manual) {
    m_stopping = true;
    m_cond.Signal();
    return;
  } else if (m_stopping) {
    return;
  }

  m_manual_stop = true;
  m_instance_replayer->stop();
}

template <typename I>
void PoolReplayer<I>::restart()
{
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_lock);

  if (m_stopping) {
    return;
  }

  m_instance_replayer->restart();
}

template <typename I>
void PoolReplayer<I>::flush()
{
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_lock);

  if (m_stopping || m_manual_stop) {
    return;
  }

  m_instance_replayer->flush();
}

template <typename I>
void PoolReplayer<I>::release_leader()
{
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_lock);

  if (m_stopping || !m_leader_watcher) {
    return;
  }

  m_leader_watcher->release_leader();
}

template <typename I>
void PoolReplayer<I>::handle_update(const std::string &mirror_uuid,
				    ImageIds &&added_image_ids,
				    ImageIds &&removed_image_ids) {
  if (m_stopping) {
    return;
  }

  dout(10) << "mirror_uuid=" << mirror_uuid << ", "
           << "added_count=" << added_image_ids.size() << ", "
           << "removed_count=" << removed_image_ids.size() << dendl;
  Mutex::Locker locker(m_lock);
  if (!m_leader_watcher->is_leader()) {
    return;
  }

  m_service_daemon->add_or_update_attribute(
    m_local_pool_id, SERVICE_DAEMON_LOCAL_COUNT_KEY,
    m_local_pool_watcher->get_image_count());
  if (m_remote_pool_watcher) {
    m_service_daemon->add_or_update_attribute(
      m_local_pool_id, SERVICE_DAEMON_REMOTE_COUNT_KEY,
      m_remote_pool_watcher->get_image_count());
  }

  std::set<std::string> added_global_image_ids;
  for (auto& image_id : added_image_ids) {
    added_global_image_ids.insert(image_id.global_id);
  }

  std::set<std::string> removed_global_image_ids;
  for (auto& image_id : removed_image_ids) {
    removed_global_image_ids.insert(image_id.global_id);
  }

  m_image_map->update_images(mirror_uuid,
                             std::move(added_global_image_ids),
                             std::move(removed_global_image_ids));
}

template <typename I>
void PoolReplayer<I>::handle_post_acquire_leader(Context *on_finish) {
  dout(10) << dendl;

  m_service_daemon->add_or_update_attribute(m_local_pool_id,
                                            SERVICE_DAEMON_LEADER_KEY, true);
  m_instance_watcher->handle_acquire_leader();
  init_image_map(on_finish);
}

template <typename I>
void PoolReplayer<I>::handle_pre_release_leader(Context *on_finish) {
  dout(10) << dendl;

  m_service_daemon->remove_attribute(m_local_pool_id,
                                     SERVICE_DAEMON_LEADER_KEY);
  m_instance_watcher->handle_release_leader();
  shut_down_image_deleter(on_finish);
}

template <typename I>
void PoolReplayer<I>::init_image_map(Context *on_finish) {
  dout(5) << dendl;

  Mutex::Locker locker(m_lock);
  ceph_assert(!m_image_map);
  m_image_map.reset(ImageMap<I>::create(m_local_io_ctx, m_threads,
                                        m_instance_watcher->get_instance_id(),
                                        m_image_map_listener));

  auto ctx = new FunctionContext([this, on_finish](int r) {
      handle_init_image_map(r, on_finish);
    });
  m_image_map->init(create_async_context_callback(
    m_threads->work_queue, ctx));
}

template <typename I>
void PoolReplayer<I>::handle_init_image_map(int r, Context *on_finish) {
  dout(5) << "r=" << r << dendl;
  if (r < 0) {
    derr << "failed to init image map: " << cpp_strerror(r) << dendl;
    on_finish = new FunctionContext([on_finish, r](int) {
        on_finish->complete(r);
      });
    shut_down_image_map(on_finish);
    return;
  }

  init_local_pool_watcher(on_finish);
}

template <typename I>
void PoolReplayer<I>::init_local_pool_watcher(Context *on_finish) {
  dout(10) << dendl;

  Mutex::Locker locker(m_lock);
  ceph_assert(!m_local_pool_watcher);
  m_local_pool_watcher.reset(PoolWatcher<I>::create(
    m_threads, m_local_io_ctx, m_local_pool_watcher_listener));

  // ensure the initial set of local images is up-to-date
  // after acquiring the leader role
  auto ctx = new FunctionContext([this, on_finish](int r) {
      handle_init_local_pool_watcher(r, on_finish);
    });
  m_local_pool_watcher->init(create_async_context_callback(
    m_threads->work_queue, ctx));
}

template <typename I>
void PoolReplayer<I>::handle_init_local_pool_watcher(
    int r, Context *on_finish) {
  dout(10) << "r=" << r << dendl;
  if (r < 0) {
    derr << "failed to retrieve local images: " << cpp_strerror(r) << dendl;
    on_finish = new FunctionContext([on_finish, r](int) {
        on_finish->complete(r);
      });
    shut_down_pool_watchers(on_finish);
    return;
  }

  init_remote_pool_watcher(on_finish);
}

template <typename I>
void PoolReplayer<I>::init_remote_pool_watcher(Context *on_finish) {
  dout(10) << dendl;

  Mutex::Locker locker(m_lock);
  ceph_assert(!m_remote_pool_watcher);
  m_remote_pool_watcher.reset(PoolWatcher<I>::create(
    m_threads, m_remote_io_ctx, m_remote_pool_watcher_listener));

  auto ctx = new FunctionContext([this, on_finish](int r) {
      handle_init_remote_pool_watcher(r, on_finish);
    });
  m_remote_pool_watcher->init(create_async_context_callback(
    m_threads->work_queue, ctx));
}

template <typename I>
void PoolReplayer<I>::handle_init_remote_pool_watcher(
    int r, Context *on_finish) {
  dout(10) << "r=" << r << dendl;
  if (r == -ENOENT) {
    // Technically nothing to do since the other side doesn't
    // have mirroring enabled. Eventually the remote pool watcher will
    // detect images (if mirroring is enabled), so no point propagating
    // an error which would just busy-spin the state machines.
    dout(0) << "remote peer does not have mirroring configured" << dendl;
  } else if (r < 0) {
    derr << "failed to retrieve remote images: " << cpp_strerror(r) << dendl;
    on_finish = new FunctionContext([on_finish, r](int) {
        on_finish->complete(r);
      });
    shut_down_pool_watchers(on_finish);
    return;
  }

  init_image_deleter(on_finish);
}

template <typename I>
void PoolReplayer<I>::init_image_deleter(Context *on_finish) {
  dout(10) << dendl;

  Mutex::Locker locker(m_lock);
  ceph_assert(!m_image_deleter);

  on_finish = new FunctionContext([this, on_finish](int r) {
      handle_init_image_deleter(r, on_finish);
    });
  m_image_deleter.reset(ImageDeleter<I>::create(m_local_io_ctx, m_threads,
                                                m_service_daemon));
  m_image_deleter->init(create_async_context_callback(
    m_threads->work_queue, on_finish));
}

template <typename I>
void PoolReplayer<I>::handle_init_image_deleter(int r, Context *on_finish) {
  dout(10) << "r=" << r << dendl;
  if (r < 0) {
    derr << "failed to init image deleter: " << cpp_strerror(r) << dendl;
    on_finish = new FunctionContext([on_finish, r](int) {
        on_finish->complete(r);
      });
    shut_down_image_deleter(on_finish);
    return;
  }

  on_finish->complete(0);

  Mutex::Locker locker(m_lock);
  m_cond.Signal();
}

template <typename I>
void PoolReplayer<I>::shut_down_image_deleter(Context* on_finish) {
  dout(10) << dendl;
  {
    Mutex::Locker locker(m_lock);
    if (m_image_deleter) {
      Context *ctx = new FunctionContext([this, on_finish](int r) {
          handle_shut_down_image_deleter(r, on_finish);
	});
      ctx = create_async_context_callback(m_threads->work_queue, ctx);

      m_image_deleter->shut_down(ctx);
      return;
    }
  }
  shut_down_pool_watchers(on_finish);
}

template <typename I>
void PoolReplayer<I>::handle_shut_down_image_deleter(
    int r, Context* on_finish) {
  dout(10) << "r=" << r << dendl;

  {
    Mutex::Locker locker(m_lock);
    ceph_assert(m_image_deleter);
    m_image_deleter.reset();
  }

  shut_down_pool_watchers(on_finish);
}

template <typename I>
void PoolReplayer<I>::shut_down_pool_watchers(Context *on_finish) {
  dout(10) << dendl;

  {
    Mutex::Locker locker(m_lock);
    if (m_local_pool_watcher) {
      Context *ctx = new FunctionContext([this, on_finish](int r) {
          handle_shut_down_pool_watchers(r, on_finish);
	});
      ctx = create_async_context_callback(m_threads->work_queue, ctx);

      auto gather_ctx = new C_Gather(g_ceph_context, ctx);
      m_local_pool_watcher->shut_down(gather_ctx->new_sub());
      if (m_remote_pool_watcher) {
	m_remote_pool_watcher->shut_down(gather_ctx->new_sub());
      }
      gather_ctx->activate();
      return;
    }
  }

  on_finish->complete(0);
}

template <typename I>
void PoolReplayer<I>::handle_shut_down_pool_watchers(
    int r, Context *on_finish) {
  dout(10) << "r=" << r << dendl;

  {
    Mutex::Locker locker(m_lock);
    ceph_assert(m_local_pool_watcher);
    m_local_pool_watcher.reset();

    if (m_remote_pool_watcher) {
      m_remote_pool_watcher.reset();
    }
  }
  wait_for_update_ops(on_finish);
}

template <typename I>
void PoolReplayer<I>::wait_for_update_ops(Context *on_finish) {
  dout(10) << dendl;

  Mutex::Locker locker(m_lock);

  Context *ctx = new FunctionContext([this, on_finish](int r) {
      handle_wait_for_update_ops(r, on_finish);
    });
  ctx = create_async_context_callback(m_threads->work_queue, ctx);

  m_update_op_tracker.wait_for_ops(ctx);
}

template <typename I>
void PoolReplayer<I>::handle_wait_for_update_ops(int r, Context *on_finish) {
  dout(10) << "r=" << r << dendl;
  ceph_assert(r == 0);

  shut_down_image_map(on_finish);
}

template <typename I>
void PoolReplayer<I>::shut_down_image_map(Context *on_finish) {
  dout(5) << dendl;

  {
    Mutex::Locker locker(m_lock);
    if (m_image_map) {
      on_finish = new FunctionContext([this, on_finish](int r) {
          handle_shut_down_image_map(r, on_finish);
        });
      m_image_map->shut_down(create_async_context_callback(
        m_threads->work_queue, on_finish));
      return;
    }
  }

  on_finish->complete(0);
}

template <typename I>
void PoolReplayer<I>::handle_shut_down_image_map(int r, Context *on_finish) {
  dout(5) << "r=" << r << dendl;
  if (r < 0 && r != -EBLACKLISTED) {
    derr << "failed to shut down image map: " << cpp_strerror(r) << dendl;
  }

  Mutex::Locker locker(m_lock);
  ceph_assert(m_image_map);
  m_image_map.reset();

  m_instance_replayer->release_all(on_finish);
}

template <typename I>
void PoolReplayer<I>::handle_update_leader(
    const std::string &leader_instance_id) {
  dout(10) << "leader_instance_id=" << leader_instance_id << dendl;

  m_instance_watcher->handle_update_leader(leader_instance_id);
}

template <typename I>
void PoolReplayer<I>::handle_acquire_image(const std::string &global_image_id,
                                           const std::string &instance_id,
                                           Context* on_finish) {
  dout(5) << "global_image_id=" << global_image_id << ", "
          << "instance_id=" << instance_id << dendl;

  m_instance_watcher->notify_image_acquire(instance_id, global_image_id,
                                           on_finish);
}

template <typename I>
void PoolReplayer<I>::handle_release_image(const std::string &global_image_id,
                                           const std::string &instance_id,
                                           Context* on_finish) {
  dout(5) << "global_image_id=" << global_image_id << ", "
          << "instance_id=" << instance_id << dendl;

  m_instance_watcher->notify_image_release(instance_id, global_image_id,
                                           on_finish);
}

template <typename I>
void PoolReplayer<I>::handle_remove_image(const std::string &mirror_uuid,
                                          const std::string &global_image_id,
                                          const std::string &instance_id,
                                          Context* on_finish) {
  ceph_assert(!mirror_uuid.empty());
  dout(5) << "mirror_uuid=" << mirror_uuid << ", "
          << "global_image_id=" << global_image_id << ", "
          << "instance_id=" << instance_id << dendl;

  m_instance_watcher->notify_peer_image_removed(instance_id, global_image_id,
                                                mirror_uuid, on_finish);
}

template <typename I>
void PoolReplayer<I>::handle_instances_added(const InstanceIds &instance_ids) {
  dout(5) << "instance_ids=" << instance_ids << dendl;
  Mutex::Locker locker(m_lock);
  if (!m_leader_watcher->is_leader()) {
    return;
  }

  ceph_assert(m_image_map);
  m_image_map->update_instances_added(instance_ids);
}

template <typename I>
void PoolReplayer<I>::handle_instances_removed(
    const InstanceIds &instance_ids) {
  dout(5) << "instance_ids=" << instance_ids << dendl;
  Mutex::Locker locker(m_lock);
  if (!m_leader_watcher->is_leader()) {
    return;
  }

  ceph_assert(m_image_map);
  m_image_map->update_instances_removed(instance_ids);
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::PoolReplayer<librbd::ImageCtx>;
