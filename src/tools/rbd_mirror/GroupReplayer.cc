// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/stringify.h"
#include "common/Formatter.h"
#include "common/admin_socket.h"
#include "common/Cond.h"
#include "common/debug.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/group/ListSnapshotsRequest.h"
#include "tools/rbd_mirror/ImageReplayer.h"
#include "tools/rbd_mirror/MirrorStatusUpdater.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/group_replayer/BootstrapRequest.h"
#include "tools/rbd_mirror/image_replayer/Utils.h"
#include "GroupReplayer.h"

#include <algorithm>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::" << *this << " " \
                           << __func__ << ": "

extern PerfCounters *g_perf_counters;

namespace rbd {
namespace mirror {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;
using librbd::util::unique_lock_name;

namespace {

template <typename I>
class GroupReplayerAdminSocketCommand {
public:
  GroupReplayerAdminSocketCommand(const std::string &desc,
                                  GroupReplayer<I> *replayer)
    : desc(desc), replayer(replayer) {
  }
  virtual ~GroupReplayerAdminSocketCommand() {}
  virtual int call(Formatter *f) = 0;

  std::string desc;
  GroupReplayer<I> *replayer;
  bool registered = false;
};

template <typename I>
class StatusCommand : public GroupReplayerAdminSocketCommand<I> {
public:
  explicit StatusCommand(const std::string &desc, GroupReplayer<I> *replayer)
    : GroupReplayerAdminSocketCommand<I>(desc, replayer) {
  }

  int call(Formatter *f) override {
    this->replayer->print_status(f);
    return 0;
  }
};

template <typename I>
class StartCommand : public GroupReplayerAdminSocketCommand<I> {
public:
  explicit StartCommand(const std::string &desc, GroupReplayer<I> *replayer)
    : GroupReplayerAdminSocketCommand<I>(desc, replayer) {
  }

  int call(Formatter *f) override {
    this->replayer->start(nullptr, true);
    return 0;
  }
};

template <typename I>
class StopCommand : public GroupReplayerAdminSocketCommand<I> {
public:
  explicit StopCommand(const std::string &desc, GroupReplayer<I> *replayer)
    : GroupReplayerAdminSocketCommand<I>(desc, replayer) {
  }

  int call(Formatter *f) override {
    this->replayer->stop(nullptr, true);
    return 0;
  }
};

template <typename I>
class RestartCommand : public GroupReplayerAdminSocketCommand<I> {
public:
  explicit RestartCommand(const std::string &desc, GroupReplayer<I> *replayer)
    : GroupReplayerAdminSocketCommand<I>(desc, replayer) {
  }

  int call(Formatter *f) override {
    this->replayer->restart();
    return 0;
  }
};

template <typename I>
class FlushCommand : public GroupReplayerAdminSocketCommand<I> {
public:
  explicit FlushCommand(const std::string &desc, GroupReplayer<I> *replayer)
    : GroupReplayerAdminSocketCommand<I>(desc, replayer) {
  }

  int call(Formatter *f) override {
    this->replayer->flush();
    return 0;
  }
};

template <typename I>
class GroupReplayerAdminSocketHook : public AdminSocketHook {
public:
  GroupReplayerAdminSocketHook(CephContext *cct, const std::string &name,
			       GroupReplayer<I> *replayer)
    : admin_socket(cct->get_admin_socket()),
      commands{{"rbd mirror group flush " + name,
                new FlushCommand<I>("flush rbd mirror group " + name, replayer)},
               {"rbd mirror group restart " + name,
                new RestartCommand<I>("restart rbd mirror group " + name, replayer)},
               {"rbd mirror group start " + name,
                new StartCommand<I>("start rbd mirror group " + name, replayer)},
               {"rbd mirror group status " + name,
                new StatusCommand<I>("get status for rbd mirror group " + name, replayer)},
               {"rbd mirror group stop " + name,
                new StopCommand<I>("stop rbd mirror group " + name, replayer)}} {
  }

  int register_commands() {
    for (auto &it : commands) {
      int r = admin_socket->register_command(it.first, this,
                                             it.second->desc);
      if (r < 0) {
        return r;
      }
      it.second->registered = true;
    }
    return 0;
  }

  ~GroupReplayerAdminSocketHook() override {
    admin_socket->unregister_commands(this);
    for (auto &it : commands) {
      delete it.second;
    }
    commands.clear();
  }

  int call(std::string_view command, const cmdmap_t& cmdmap,
	   const bufferlist&,
	   Formatter *f,
	   std::ostream& errss,
	   bufferlist& out) override {
    auto i = commands.find(command);
    ceph_assert(i != commands.end());
    return i->second->call(f);
  }

private:
  typedef std::map<std::string, GroupReplayerAdminSocketCommand<I>*,
		   std::less<>> Commands;

  AdminSocket *admin_socket;
  Commands commands;
};

} // anonymous namespace

template <typename I>
std::ostream &operator<<(std::ostream &os, const GroupReplayer<I> &replayer) {
  std::string nspace = replayer.get_namespace();
  if (!nspace.empty()) {
    nspace += "/";
  }

  os << "GroupReplayer: " << &replayer << " [" << replayer.get_local_pool_id() << "/"
     << nspace << replayer.get_global_group_id() << "]";
  return os;
}

template <typename I>
GroupReplayer<I>::GroupReplayer(
    librados::IoCtx &local_io_ctx, const std::string &local_mirror_uuid,
    const std::string &global_group_id, Threads<I> *threads,
    InstanceWatcher<I> *instance_watcher,
    MirrorStatusUpdater<I>* local_status_updater,
    journal::CacheManagerHandler *cache_manager_handler,
    PoolMetaCache* pool_meta_cache) :
  m_local_io_ctx(local_io_ctx), m_local_mirror_uuid(local_mirror_uuid),
  m_global_group_id(global_group_id), m_threads(threads),
  m_instance_watcher(instance_watcher),
  m_local_status_updater(local_status_updater),
  m_cache_manager_handler(cache_manager_handler),
  m_pool_meta_cache(pool_meta_cache),
  m_local_group_name(global_group_id),
  m_lock(ceph::make_mutex(unique_lock_name("GroupReplayer::m_lock", this))) {
  // Register asok commands using a temporary "remote_pool_name/global_group_id"
  // name.  When the group name becomes known on start the asok commands will be
  // re-registered using "remote_pool_name/remote_group_name" name.

  m_group_spec = image_replayer::util::compute_image_spec(
      local_io_ctx, global_group_id);
  register_admin_socket_hook();
}

template <typename I>
GroupReplayer<I>::~GroupReplayer() {
  unregister_admin_socket_hook();
  ceph_assert(m_on_start_finish == nullptr);
  ceph_assert(m_on_stop_finish == nullptr);
  ceph_assert(m_bootstrap_request == nullptr);
}

template <typename I>
bool GroupReplayer<I>::needs_restart() const {
  dout(10) << dendl;

  std::lock_guard locker{m_lock};
  if (m_state != STATE_REPLAYING) {
    return false;
  }

  for (auto &[_, image_replayer] : m_image_replayers) {
    if (image_replayer->is_stopped()) {
      dout(10) << "image replayer is in stopped state, needs restart" << dendl;
      return true;
    }
  }

  return false;
}

template <typename I>
void GroupReplayer<I>::sync_group_names() {
  dout(10) << dendl;

  std::string local_group_name;
  std::string remote_group_name;
  int r = librbd::cls_client::dir_get_name(&m_local_io_ctx,
                                           RBD_GROUP_DIRECTORY,
                                           m_local_group_id,
                                           &local_group_name);
  if (r < 0) {
    derr << "failed to retrieve local group name: "
         << cpp_strerror(r) << dendl;
    return;
  }

  r = librbd::cls_client::dir_get_name(&m_remote_group_peer.io_ctx,
                                       RBD_GROUP_DIRECTORY,
                                       m_remote_group_id,
                                       &remote_group_name);
  if (r < 0) {
    derr << "failed to retrieve remote group name: "
         << cpp_strerror(r) << dendl;
    return;
  }

  if (local_group_name != remote_group_name) {
    dout(5) << "group renamed. local group name: " << local_group_name
            << ", remote group name: " << remote_group_name << dendl;

    r = librbd::cls_client::group_dir_rename(&m_local_io_ctx,
                                             RBD_GROUP_DIRECTORY,
                                             local_group_name,
                                             remote_group_name,
                                             m_local_group_id);
    if (r < 0) {
      derr << "error renaming group from directory"
           << cpp_strerror(r) << dendl;
      return;
    }

    m_local_group_name = remote_group_name;
    reregister_admin_socket_hook();
  }
}

template <typename I>
image_replayer::HealthState GroupReplayer<I>::get_health_state() const {
  // TODO: Implement something like m_mirror_image_status_state for group
  return image_replayer::HEALTH_STATE_OK;
}

template <typename I>
void GroupReplayer<I>::add_peer(const Peer<I>& peer) {
  dout(10) << "peer=" << peer << dendl;

  std::lock_guard locker{m_lock};
  auto it = m_peers.find(peer);
  if (it == m_peers.end()) {
    m_peers.insert(peer);
  }
}

template <typename I>
void GroupReplayer<I>::set_state_description(int r, const std::string &desc) {
  dout(10) << "r=" << r << ", desc=" << desc << dendl;

  std::lock_guard l{m_lock};
  m_last_r = r;
  m_state_desc = desc;
}

template <typename I>
void GroupReplayer<I>::start(Context *on_finish, bool manual, bool restart) {
  dout(10) << "on_finish=" << on_finish << ", manual=" << manual
           << ", restart=" << restart << dendl;

  int r = 0;
  {
    std::lock_guard locker{m_lock};
    if (!is_stopped_()) {
      derr << "already running: " << m_state << dendl;
      r = -EINVAL;
    } else if (m_manual_stop && !manual) {
      dout(5) << "stopped manually, ignoring start without manual flag"
	      << dendl;
      r = -EPERM;
    } else if (restart && !m_restart_requested) {
      dout(10) << "canceled restart" << dendl;
      r = -ECANCELED;
    } else {
      m_state = STATE_STARTING;
      m_last_r = 0;
      m_state_desc.clear();
      m_local_group_snaps.clear();
      m_image_replayers.clear();
      m_image_replayer_index.clear();
      m_get_remote_group_snap_ret_vals.clear();
      m_manual_stop = false;
      ceph_assert(m_on_start_finish == nullptr);
      std::swap(m_on_start_finish, on_finish);
    }
  }

  if (r < 0) {
    if (on_finish) {
      on_finish->complete(r);
    }
    return;
  }

  bootstrap_group();
}

template <typename I>
void GroupReplayer<I>::stop(Context *on_finish, bool manual, bool restart) {
  dout(10) << "on_finish=" << on_finish << ", manual=" << manual
           << ", restart=" << restart << dendl;

  group_replayer::BootstrapRequest<I> *bootstrap_request = nullptr;
  std::map<std::string, std::map<ImageReplayer<I> *, Context *>> create_snap_requests;
  bool shut_down_replay = false;
  bool running = true;
  {
    std::lock_guard locker{m_lock};

    if (restart) {
      m_restart_requested = true;
    }

    dout(10) << "state: " << m_state << ", m_stop_requested: " << m_stop_requested << dendl;
    if (!is_running_()) {
      dout(10) << "replayers not running" << dendl;
      running = false;
      if (!restart && m_restart_requested) {
        dout(10) << "canceling restart" << dendl;
        m_restart_requested = false;
      }
    } else {
      dout(10) << "replayers still running" << dendl;
      if (!is_stopped_() || m_state == STATE_STOPPING) {
	if (m_state == STATE_STARTING) {
	  dout(10) << "canceling start" << dendl;
	  if (m_bootstrap_request != nullptr) {
            bootstrap_request = m_bootstrap_request;
            bootstrap_request->get();
	  }
	  shut_down_replay = true;
	} else {
          dout(10) << "interrupting replay" << dendl;
          shut_down_replay = true;
          for (auto it = m_create_snap_requests.begin();
               it != m_create_snap_requests.end(); ) {
            auto &remote_group_snap_id = it->first;
            auto &requests = it->second;
            if (m_get_remote_group_snap_ret_vals.count(remote_group_snap_id) == 0) {
              dout(20) << "getting remote group snap for "
                       << remote_group_snap_id << " is still in-progress"
                       << dendl;
              shut_down_replay = false;
            } else if (m_pending_snap_create.count(remote_group_snap_id) > 0) {
              dout(20) << "group snap create for " << remote_group_snap_id
                       << " is still in-progress" << dendl;
              shut_down_replay = false;
            } else {
              create_snap_requests[remote_group_snap_id] = requests;
              it = m_create_snap_requests.erase(it);
              continue;
            }
            it++;
          }
	}
        m_state = STATE_STOPPING;

        ceph_assert(m_on_stop_finish == nullptr);
        std::swap(m_on_stop_finish, on_finish);
        m_stop_requested = true;
        m_manual_stop = manual;
      }
    }
  }

  if (bootstrap_request != nullptr) {
    dout(10) << "canceling bootstrap" << dendl;
    bootstrap_request->cancel();
    bootstrap_request->put();
  }

  for (auto &[_, requests] : create_snap_requests) {
    for (auto &[_, on_finish] : requests) {
      on_finish->complete(-ESTALE);
    }
  }

  if (shut_down_replay) {
    stop_image_replayers();
  } else if (on_finish != nullptr) {
    // XXXMG: clean up
    {
      std::lock_guard locker{m_lock};
      m_stop_requested = false;
    }
    on_finish->complete(0);
  }

  if (!running && shut_down_replay) {
    dout(20) << "not running" << dendl;
    if (on_finish) {
      on_finish->complete(-EINVAL);
    }
  }
}

template <typename I>
void GroupReplayer<I>::restart(Context *on_finish) {
  dout(10) << dendl;
  {
    std::lock_guard locker{m_lock};
    m_restart_requested = true;
    m_on_start_finish = nullptr;
  }

  auto ctx = new LambdaContext(
    [this, on_finish](int r) {
      if (r < 0) {
	// Try start anyway.
      }
      start(on_finish, true, true);
    });
  stop(ctx, false, true);
}

template <typename I>
void GroupReplayer<I>::flush() {
  std::unique_lock locker{m_lock};
  if (m_state != STATE_REPLAYING) {
    return;
  }

  dout(10) << dendl;

  for (auto &[_, image_replayer] : m_image_replayers) {
    image_replayer->flush();
  }
}

template <typename I>
void GroupReplayer<I>::print_status(Formatter *f) {
  dout(10) << dendl;

  std::lock_guard l{m_lock};

  f->open_object_section("group_replayer");
  f->dump_string("name", m_group_spec);
  auto state = m_state;
  if (m_local_group_ctx.primary && state == STATE_REPLAYING) { // XXXMG
    state = STATE_STOPPED;
  }
  f->dump_string("state", state_to_string(state));
  f->open_array_section("image_replayers");
  for (auto &[_, image_replayer] : m_image_replayers) {
    image_replayer->print_status(f);
  }
  f->close_section(); // image_replayers
  f->close_section(); // group_replayer
}

template <typename I>
void GroupReplayer<I>::bootstrap_group() {
  dout(10) << dendl;

  std::unique_lock locker{m_lock};
  if (m_peers.empty()) {
    locker.unlock();

    dout(5) << "no peer clusters" << dendl;
    finish_start(-ENOENT, "no peer clusters");
    return;
  }

  // TODO need to support multiple remote groups
  ceph_assert(!m_peers.empty());
  m_remote_group_peer = *m_peers.begin();

  if (finish_start_if_interrupted(m_lock)) {
    return;
  }

  ceph_assert(m_image_replayers.empty());

  auto ctx = create_context_callback<
      GroupReplayer,
      &GroupReplayer<I>::handle_bootstrap_group>(this);
  auto request = group_replayer::BootstrapRequest<I>::create(
    m_threads, m_local_io_ctx, m_remote_group_peer.io_ctx, m_global_group_id,
    m_local_mirror_uuid, m_instance_watcher, m_local_status_updater,
    m_remote_group_peer.mirror_status_updater, m_cache_manager_handler,
    m_pool_meta_cache, &m_local_group_id, &m_remote_group_id,
    &m_local_group_snaps, &m_local_group_ctx, &m_image_replayers,
    &m_image_replayer_index, ctx);

  request->get();
  m_bootstrap_request = request;
  locker.unlock();

  set_mirror_group_status_update(
    cls::rbd::MIRROR_GROUP_STATUS_STATE_STARTING_REPLAY, "bootstrapping");
  request->send();
}

template <typename I>
void GroupReplayer<I>::handle_bootstrap_group(int r) {
  dout(10) << "r=" << r << dendl;
  {
    std::lock_guard locker{m_lock};
    if (m_state == STATE_STOPPING || m_state == STATE_STOPPED) {
      dout(10) << "stop prevailed" <<dendl;
      return;
    }
    if (m_bootstrap_request != nullptr) {
      m_bootstrap_request->put();
      m_bootstrap_request = nullptr;
    }
    m_local_group_ctx.listener = &m_listener;
    if (!m_local_group_ctx.name.empty()) {
      m_local_group_name = m_local_group_ctx.name;
    }
  }

  reregister_admin_socket_hook();

  if (finish_start_if_interrupted()) {
    return;
  } else if (r == -ENOENT) {
    finish_start(r, "group removed");
    return;
  } else if (r == -EREMOTEIO) {
    finish_start(r, "remote group is non-primary");
    return;
  } else if (r == -EEXIST) {
    finish_start(r, "split-brain detected");
    return;
  } else if (r < 0) {
    finish_start(r, "bootstrap failed");
    return;
  }

  start_image_replayers();
}

template <typename I>
void GroupReplayer<I>::start_image_replayers() {
  dout(10) << m_image_replayers.size() << dendl;

  set_mirror_group_status_update(
    cls::rbd::MIRROR_GROUP_STATUS_STATE_STARTING_REPLAY, "starting replay");

  auto ctx = create_context_callback<
      GroupReplayer, &GroupReplayer<I>::handle_start_image_replayers>(this);
  C_Gather *gather_ctx = new C_Gather(g_ceph_context, ctx);
  {
    std::lock_guard locker{m_lock};
    for (auto &[_, image_replayer] : m_image_replayers) {
      image_replayer->start(gather_ctx->new_sub(), false);
    }
  }
  gather_ctx->activate();
}

template <typename I>
void GroupReplayer<I>::handle_start_image_replayers(int r) {
  dout(10) << "r=" << r << dendl;
  {
    std::lock_guard locker{m_lock};
    if (m_state == STATE_STOPPING || m_state == STATE_STOPPED) {
      dout(10) << "stop prevailed" <<dendl;
      return;
    }
  }

  if (finish_start_if_interrupted()) {
    return;
  } else if (r < 0) {
    finish_start(r, "");
    return;
  }

  finish_start(0, "");
}

template <typename I>
void GroupReplayer<I>::stop_image_replayers() {
  dout(10) << m_image_replayers.size() << dendl;

  set_mirror_group_status_update(
      cls::rbd::MIRROR_GROUP_STATUS_STATE_STOPPING_REPLAY, "stopping");

  auto ctx = create_context_callback<
      GroupReplayer, &GroupReplayer<I>::handle_stop_image_replayers>(this);
  C_Gather *gather_ctx = new C_Gather(g_ceph_context, ctx);
  {
    std::lock_guard locker{m_lock};
    for (auto &[_, image_replayer] : m_image_replayers) {
      image_replayer->stop(gather_ctx->new_sub(), false);
    }
  }
  gather_ctx->activate();
}

template <typename I>
void GroupReplayer<I>::handle_stop_image_replayers(int r) {
  dout(10) << "r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    std::lock_guard locker{m_lock};
    ceph_assert(m_state == STATE_STOPPING);
    m_stop_requested = false;
    m_state = STATE_STOPPED;
    std::swap(on_finish, m_on_stop_finish);

    m_image_replayer_index.clear();

    for (auto &[_, image_replayer] : m_image_replayers) {
      delete image_replayer;
    }
    m_image_replayers.clear();
  }

  set_mirror_group_status_update(
      cls::rbd::MIRROR_GROUP_STATUS_STATE_STOPPED, "stopped");

  if (on_finish != nullptr) {
    on_finish->complete(0);
  }
}

template <typename I>
bool GroupReplayer<I>::finish_start_if_interrupted() {
  std::lock_guard locker{m_lock};

  return finish_start_if_interrupted(m_lock);
}

template <typename I>
bool GroupReplayer<I>::finish_start_if_interrupted(ceph::mutex &lock) {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  ceph_assert(m_state == STATE_STARTING);
  if (!m_stop_requested) {
    return false;
  }

  finish_start(-ECANCELED, "");
  return true;
}

template <typename I>
void GroupReplayer<I>::finish_start(int r, const std::string &desc) {
  dout(10) << "r=" << r << ", desc=" << desc << dendl;
  Context *ctx = new LambdaContext(
    [this, r, desc](int _r) {
      Context *on_finish = nullptr;
      {
	std::lock_guard locker{m_lock};
        ceph_assert(m_state == STATE_STARTING);
        m_state = STATE_REPLAYING;
        std::swap(m_on_start_finish, on_finish);
        m_state_desc = desc;
        if (r < 0) {
          auto state = cls::rbd::MIRROR_GROUP_STATUS_STATE_STOPPED;
          if (r == -ECANCELED) {
            dout(10) << "start canceled" << dendl;
          } else if (r == -ENOENT) {
            dout(10) << "mirroring group removed" << dendl;
          } else if (r == -EREMOTEIO) {
            dout(10) << "mirroring group demoted" << dendl;
          } else {
            derr << "start failed: " << cpp_strerror(r) << dendl;
            state = cls::rbd::MIRROR_GROUP_STATUS_STATE_ERROR;
          }
          on_finish = new LambdaContext(
              [this, r, state, desc, on_finish](int) {
                set_mirror_group_status_update(state, desc);

                if (r == -ENOENT) {
                  set_finished(true);
                }
                if (on_finish != nullptr) {
                  on_finish->complete(r);
                }
              });
        }
      }

      if (r < 0) {
        stop(on_finish, false, false);
        return;
      }

      if (m_local_group_ctx.primary) { // XXXMG
        set_mirror_group_status_update(
            cls::rbd::MIRROR_GROUP_STATUS_STATE_STOPPED,
            "local group is primary");
      } else {
        set_mirror_group_status_update(
            cls::rbd::MIRROR_GROUP_STATUS_STATE_REPLAYING, "replaying");
      }

      if (on_finish != nullptr) {
        on_finish->complete(0);
      }
    });
  m_threads->work_queue->queue(ctx, 0);
}


template <typename I>
void GroupReplayer<I>::register_admin_socket_hook() {
  GroupReplayerAdminSocketHook<I> *asok_hook;
  {
    std::lock_guard locker{m_lock};
    if (m_asok_hook != nullptr) {
      return;
    }

    dout(15) << "registered asok hook: " << m_group_spec << dendl;
    asok_hook = new GroupReplayerAdminSocketHook<I>(
      g_ceph_context, m_group_spec, this);
    int r = asok_hook->register_commands();
    if (r == 0) {
      m_asok_hook = asok_hook;
      return;
    }
    derr << "error registering admin socket commands" << dendl;
  }
  delete asok_hook;
}

template <typename I>
void GroupReplayer<I>::unregister_admin_socket_hook() {
  dout(15) << dendl;

  AdminSocketHook *asok_hook = nullptr;
  {
    std::lock_guard locker{m_lock};
    std::swap(asok_hook, m_asok_hook);
  }
  delete asok_hook;
}

template <typename I>
void GroupReplayer<I>::reregister_admin_socket_hook() {
  std::unique_lock locker{m_lock};

  auto group_spec = image_replayer::util::compute_image_spec(
    m_local_io_ctx, m_local_group_name);
  if (m_asok_hook != nullptr && m_group_spec == group_spec) {
    return;
  }

  dout(15) << "old_group_spec=" << m_group_spec << ", "
           << "new_group_spec=" << group_spec << dendl;
  m_group_spec = group_spec;

  if (m_state == STATE_STOPPING || m_state == STATE_STOPPED) {
    // no need to re-register if stopping
    return;
  }
  locker.unlock();

  unregister_admin_socket_hook();
  register_admin_socket_hook();
}

template <typename I>
void GroupReplayer<I>::set_mirror_group_status_update(
    cls::rbd::MirrorGroupStatusState state, const std::string &desc) {
  dout(20) << "state=" << state << ", description=" << desc << dendl;

  cls::rbd::MirrorGroupSiteStatus local_status;
  local_status.state = state;
  local_status.description = desc;
  local_status.up = true;

  auto remote_status = local_status;

  {
    std::unique_lock locker{m_lock};
    for (auto &[_, ir] : m_image_replayers) {
      cls::rbd::MirrorImageSiteStatus mirror_image;
      if (ir->is_running()) {
        if (ir->is_replaying()) {
          mirror_image.state = cls::rbd::MIRROR_IMAGE_STATUS_STATE_REPLAYING;
        } else {
          mirror_image.state = cls::rbd::MIRROR_IMAGE_STATUS_STATE_STARTING_REPLAY;
        }
      } else if (ir->is_stopped()) {
        mirror_image.state = cls::rbd::MIRROR_IMAGE_STATUS_STATE_STOPPED;
      } else {
        mirror_image.state = cls::rbd::MIRROR_IMAGE_STATUS_STATE_STOPPING_REPLAY;
      }
      mirror_image.description = ir->get_state_description();

      local_status.mirror_images[{ir->get_local_pool_id(),
                                  ir->get_global_image_id()}] = mirror_image;
      auto remote_pool_id = ir->get_remote_pool_id();
      if (remote_pool_id >= 0) {
        remote_status.mirror_images[{remote_pool_id,
                                     ir->get_global_image_id()}] = mirror_image;
      }
    }
  }

  m_local_status_updater->set_mirror_group_status(m_global_group_id,
                                                  local_status, true);
  if (m_remote_group_peer.mirror_status_updater != nullptr) {
    m_remote_group_peer.mirror_status_updater->set_mirror_group_status(
        m_global_group_id, remote_status, true);
  }
}

template <typename I>
void GroupReplayer<I>::create_regular_group_snapshot(
    const std::string &remote_group_snap_name,
    const std::string &remote_group_snap_id,
    std::vector<cls::rbd::GroupImageStatus> *local_images,
    Context *on_finish) {
  // each image will have one snapshot specific to group snap, and so for each
  // image get a ImageSnapshotSpec and prepare a vector
  // for image :: <images in that group> {
  //   * get snap whos name has group snap_id for that we can list snaps and
  //     filter with remote_group_snap_id
  //   * get its { pool_id, snap_id, image_id }
  // }
  // finally write to the object
  dout(10) << dendl;
  librados::ObjectWriteOperation op;
  cls::rbd::GroupSnapshot group_snap{
    remote_group_snap_id, // keeping it same as remote group snap id
    cls::rbd::UserGroupSnapshotNamespace{},
      remote_group_snap_name,
      cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE};
  librbd::cls_client::group_snap_set(&op, group_snap);
  if (m_local_group_snaps.find(group_snap.id) == m_local_group_snaps.end()) {
    m_local_group_snaps.insert(make_pair(group_snap.id, group_snap));
  }

  std::vector<cls::rbd::ImageSnapshotSpec> local_image_snap_specs;
  local_image_snap_specs = std::vector<cls::rbd::ImageSnapshotSpec>(
      local_images->size(), cls::rbd::ImageSnapshotSpec());
  for (auto& image : *local_images) {
    std::string image_header_oid = librbd::util::header_name(
        image.spec.image_id);
    ::SnapContext snapc;
    int r = librbd::cls_client::get_snapcontext(&m_local_io_ctx,
                                                image_header_oid, &snapc);
    if (r < 0) {
      derr << "get snap context failed: " << cpp_strerror(r) << dendl;
      on_finish->complete(r);
      return;
    }

    auto image_snap_name = ".group." + std::to_string(image.spec.pool_id) +
                           "_" + m_remote_group_id + "_" + remote_group_snap_id;
    // stored in reverse order
    for (auto snap_id : snapc.snaps) {
      cls::rbd::SnapshotInfo snap_info;
      r = librbd::cls_client::snapshot_get(&m_local_io_ctx, image_header_oid,
                                           snap_id, &snap_info);
      if (r < 0) {
        derr << "failed getting snap info for snap id: " << snap_id
          << ", : " << cpp_strerror(r) << dendl;
        on_finish->complete(r);
        return;
      }

      // extract { pool_id, snap_id, image_id }
      if (snap_info.name == image_snap_name) {
        cls::rbd::ImageSnapshotSpec snap_spec;
        snap_spec.pool = image.spec.pool_id;
        snap_spec.image_id = image.spec.image_id;
        snap_spec.snap_id = snap_info.id;

        local_image_snap_specs.push_back(snap_spec);
      }
    }
  }

  group_snap.snaps = local_image_snap_specs;
  group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE;
  librbd::cls_client::group_snap_set(&op, group_snap);
  m_local_group_snaps[group_snap.id] = group_snap;

  auto comp = create_rados_callback(
      new LambdaContext([this, on_finish](int r) {
        handle_create_regular_group_snapshot(r, on_finish);
      }));
  int r = m_local_io_ctx.aio_operate(
      librbd::util::group_header_name(m_local_group_ctx.group_id), comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupReplayer<I>::handle_create_regular_group_snapshot(
    int r, Context *on_finish) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error creating local non-primary group snapshot: "
         << cpp_strerror(r) << dendl;
  }

  on_finish->complete(0);
}

template <typename I>
void GroupReplayer<I>::list_remote_group_snapshots(Context *on_finish) {
  dout(10) << dendl;

  remote_group_snaps.clear();
  auto ctx = new LambdaContext(
    [this, on_finish] (int r)  {
        handle_list_remote_group_snapshots(r, on_finish);
  });

  auto req = librbd::group::ListSnapshotsRequest<I>::create(
      m_remote_group_peer.io_ctx, m_remote_group_id, &remote_group_snaps, ctx);
  req->send();
}

template <typename I>
void GroupReplayer<I>::handle_list_remote_group_snapshots(int r,
                                                          Context *on_finish) {
  dout(10) << "r=" << r << dendl;
  std::unique_lock locker{m_lock};

  if (r < 0) {
    derr << "error listing remote mirror group snapshots: " << cpp_strerror(r)
         << dendl;
    on_finish->complete(r);
    return;
  }

  m_remote_group_snaps.clear();
  for (auto it : remote_group_snaps) {
    dout(10) << "found remote group snap id: " << it.id << dendl;
    m_remote_group_snaps.insert(make_pair(it.id, it));
  }

  std::vector<cls::rbd::GroupImageStatus> local_images;
  std::vector<C_SaferCond*> on_finishes;
  for (auto it = m_remote_group_snaps.begin(); it != m_remote_group_snaps.end(); ++it) {
    auto snap_type = cls::rbd::get_group_snap_namespace_type(
        it->second.snapshot_namespace);
    if (snap_type == cls::rbd::GROUP_SNAPSHOT_NAMESPACE_TYPE_USER) {
      dout(10) << "found user snap, snap name: " << it->second.name
               << ", remote group snap id: " << it->second.id << dendl;
      if (local_images.empty()) {
        r = local_group_image_list_by_id(&local_images);
        if (r < 0) {
          locker.unlock();
          on_finish->complete(r);
          return;
        }
      }
      if (m_local_group_snaps.find(it->second.id) == m_local_group_snaps.end()) {
        C_SaferCond* ctx = new C_SaferCond;
        create_regular_group_snapshot(it->second.name,
                                      it->second.id, &local_images, ctx);
        on_finishes.push_back(ctx);
      }
    }
  }

  for (auto &finish : on_finishes) {
    finish->wait();
  }

  locker.unlock();
  on_finish->complete(0);
}

template <typename I>
int GroupReplayer<I>::local_group_image_list_by_id(
    std::vector<cls::rbd::GroupImageStatus> *image_ids) {
  std::string group_header_oid = librbd::util::group_header_name(
      m_local_group_ctx.group_id);

  dout(10) << "listing images in local group id " << group_header_oid << dendl;
  image_ids->clear();

  int r = 0;
  const int max_read = 1024;
  cls::rbd::GroupImageSpec start_last;
  do {
    std::vector<cls::rbd::GroupImageStatus> image_ids_page;

    r = librbd::cls_client::group_image_list(&m_local_io_ctx, group_header_oid,
                                             start_last, max_read,
                                             &image_ids_page);

    if (r < 0) {
      derr << "error reading image list from local group: "
           << cpp_strerror(-r) << dendl;
      return r;
    }
    image_ids->insert(image_ids->end(), image_ids_page.begin(),
                      image_ids_page.end());

    if (image_ids_page.size() > 0)
      start_last = image_ids_page.rbegin()->spec;

    r = image_ids_page.size();
  } while (r == max_read);

  return 0;
}

template <typename I>
void GroupReplayer<I>::create_mirror_snapshot_start(
    const cls::rbd::MirrorSnapshotNamespace &remote_group_snap_ns,
    ImageReplayer<I> *image_replayer, int64_t *local_group_pool_id,
    std::string *local_group_id, std::string *local_group_snap_id,
    Context *on_finish) {
  dout(20) << remote_group_snap_ns << " " << image_replayer << dendl;

  ceph_assert(remote_group_snap_ns.is_primary());

  int r = 0;
  std::unique_lock locker{m_lock};
  auto &remote_group_snap_id = remote_group_snap_ns.group_snap_id;
  if (m_local_group_snaps.find(remote_group_snap_id) != m_local_group_snaps.end() &&
      m_local_group_snaps[remote_group_snap_id].state == cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE) {
    dout(20) << "group snapshot: " << remote_group_snap_id << " already exists"
             << dendl;
    r = -EEXIST;
  } else if (m_state == STATE_STARTING) {
    derr << "group replayer is not ready yet, m_state: " << m_state << dendl;
    r = -EAGAIN;
  } else if (m_state != STATE_REPLAYING) {
    derr << "group replayer is not in replaying state, m_state: "
         << m_state << dendl;
    r = -ESTALE;
  }

  if (r != 0) {
    locker.unlock();
    on_finish->complete(r);
    return;
  }

  auto requests_it = m_create_snap_requests.find(remote_group_snap_id);

  if (requests_it == m_create_snap_requests.end()) {
    ceph_assert(m_local_group_snaps.count(remote_group_snap_id) == 0);

    requests_it = m_create_snap_requests.insert(
        {remote_group_snap_id, {}}).first;

    auto snap_state =
        remote_group_snap_ns.state == cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY ?
          cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY :
          cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY_DEMOTED;

    m_local_group_snaps[remote_group_snap_id] =
        {remote_group_snap_id,
         cls::rbd::MirrorGroupSnapshotNamespace{
             snap_state, {}, m_remote_group_peer.uuid, remote_group_snap_id},
         {}, cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE};

    librados::ObjectReadOperation op;
    librbd::cls_client::group_snap_get_by_id_start(&op, remote_group_snap_id);
    auto ctx = new C_GetRemoteGroupSnap(this, remote_group_snap_id);
    auto comp = create_rados_callback(ctx);

    r = m_remote_group_peer.io_ctx.aio_operate(
        librbd::util::group_header_name(m_remote_group_id), comp, &op,
        &ctx->bl);
    ceph_assert(r == 0);
    comp->release();
  }

  ceph_assert(requests_it->second.count(image_replayer) == 0);
  requests_it->second[image_replayer] = on_finish;

  ceph_assert(m_local_group_snaps.count(remote_group_snap_id) > 0);
  auto &local_group_snap = m_local_group_snaps[remote_group_snap_id];

  local_group_snap.snaps.emplace_back(image_replayer->get_local_pool_id(),
                                      image_replayer->get_local_image_id(),
                                      CEPH_NOSNAP);
  ceph_assert(!local_group_snap.snaps.back().image_id.empty());

  *local_group_pool_id = m_local_io_ctx.get_id();
  *local_group_id = m_local_group_ctx.group_id;
  *local_group_snap_id = m_local_group_snaps[remote_group_snap_id].id;

  if (m_get_remote_group_snap_ret_vals.count(remote_group_snap_id) == 0) {
    dout(20) << "getting remote group snap is still in-progress" << dendl;
    locker.unlock();
    return;
  }

  maybe_create_mirror_snapshot(locker, remote_group_snap_id);
}

template <typename I>
void GroupReplayer<I>::maybe_create_mirror_snapshot(
    std::unique_lock<ceph::mutex>& locker,
    const std::string &remote_group_snap_id) {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  dout(20) << remote_group_snap_id << dendl;

  auto &remote_group_snap = m_remote_group_snaps[remote_group_snap_id];
  ceph_assert(!remote_group_snap.id.empty());

  ceph_assert(m_create_snap_requests.count(remote_group_snap_id) > 0);
  auto &create_snap_requests = m_create_snap_requests[remote_group_snap_id];

  for (auto &s : remote_group_snap.snaps) {
    auto it = m_image_replayer_index.find({s.pool, s.image_id});
    if (it == m_image_replayer_index.end()) {
      continue;
    }

    if (create_snap_requests.count(it->second) == 0) {
      dout(20) << "waiting for create_mirror_snapshot_start "
               << remote_group_snap_id << " from " << it->second << dendl;
      locker.unlock();
      return;
    }
  }

  ceph_assert(m_local_group_snaps.count(remote_group_snap_id) > 0);
  auto &local_group_snap = m_local_group_snaps[remote_group_snap_id];

  dout(20) << local_group_snap.id << " " << local_group_snap.name << dendl;

  bool inserted = m_pending_snap_create.insert(remote_group_snap_id).second;
  ceph_assert(inserted);

  ceph_assert(m_get_remote_group_snap_ret_vals.count(remote_group_snap_id) > 0);
  int r = m_get_remote_group_snap_ret_vals[remote_group_snap_id];
  m_get_remote_group_snap_ret_vals.erase(remote_group_snap_id);
  if (r < 0) {
    locker.unlock();
    handle_create_mirror_snapshot_start(remote_group_snap_id, r);
    return;
  }

  librados::ObjectWriteOperation op;
  librbd::cls_client::group_snap_set(&op, local_group_snap);
  auto comp = create_rados_callback(
      new LambdaContext([this, remote_group_snap_id](int r) {
        handle_create_mirror_snapshot_start(remote_group_snap_id, r);
      }));

  r = m_local_io_ctx.aio_operate(
      librbd::util::group_header_name(m_local_group_ctx.group_id), comp, &op);
  locker.unlock();
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupReplayer<I>::handle_get_remote_group_snapshot(
    const std::string &remote_group_snap_id, bufferlist &out_bl, int r) {
  dout(20) << remote_group_snap_id << " r=" << r << dendl;

  auto &remote_group_snap = m_remote_group_snaps[remote_group_snap_id];

  std::unique_lock locker{m_lock};
  if (r == 0) {
    auto iter = out_bl.cbegin();
    r = librbd::cls_client::group_snap_get_by_id_finish(
        &iter, &remote_group_snap);
  }

  bool complete = (remote_group_snap.state == cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE);
  if (r < 0) {
    derr << "failed to get remote group snapshot: " << cpp_strerror(r) << dendl;
  } else if (!complete) {
    derr << "incomplete remote group snapshot: " << remote_group_snap_id
         << dendl;
    r = -EAGAIN;
  } else {
    m_local_group_snaps[remote_group_snap_id].name = m_bootstrap_request->prepare_non_primary_mirror_snap_name(
        m_global_group_id,
        m_local_group_snaps[remote_group_snap_id].id);
  }

  if (m_state == STATE_STOPPING) {
    dout(20) << "interrupted" << dendl;
    m_local_group_snaps.erase(remote_group_snap_id);
    auto create_snap_requests = m_create_snap_requests[remote_group_snap_id];
    m_create_snap_requests.erase(remote_group_snap_id);
    bool shut_down_replay = m_pending_snap_create.empty() &&
                            m_create_snap_requests.empty();
    locker.unlock();
    for (auto &[_, on_finish] : create_snap_requests) {
      on_finish->complete(r);
    }
    if (shut_down_replay) {
      stop_image_replayers();
    }
    return;
  }

  m_get_remote_group_snap_ret_vals[remote_group_snap_id] = r;

  maybe_create_mirror_snapshot(locker, remote_group_snap_id);
}

template <typename I>
void GroupReplayer<I>::handle_create_mirror_snapshot_start(
    const std::string &remote_group_snap_id, int r) {
  dout(20) << remote_group_snap_id << " r=" << r << dendl;

  std::unique_lock locker{m_lock};
  ceph_assert(m_pending_snap_create.count(remote_group_snap_id) > 0);

  ceph_assert(m_create_snap_requests.count(remote_group_snap_id) > 0);
  auto create_snap_requests = m_create_snap_requests[remote_group_snap_id];
  m_create_snap_requests.erase(remote_group_snap_id);

  bool shut_down_replay = false;
  if (r == -EEXIST) {
    dout(20) << "group snapshot: " << remote_group_snap_id << " already exists"
             << dendl;
    r = 0;
  } else if (r < 0) {
    m_pending_snap_create.erase(remote_group_snap_id);
    m_local_group_snaps.erase(remote_group_snap_id);
    shut_down_replay = m_state == STATE_STOPPING && !m_restart_requested &&
                       m_pending_snap_create.empty() &&
                       m_create_snap_requests.empty();
  }
  locker.unlock();

  for (auto &[_, on_finish] : create_snap_requests) {
    on_finish->complete(r);
  }
  if (shut_down_replay) {
    stop_image_replayers();
  }
}

template <typename I>
void GroupReplayer<I>::create_mirror_snapshot_finish(
    const std::string &remote_group_snap_id, ImageReplayer<I> *image_replayer,
    uint64_t snap_id, Context *on_finish) {
  dout(20) << remote_group_snap_id << " " << image_replayer << " snap_id="
           << snap_id << dendl;

  std::lock_guard locker{m_lock};
  if (m_local_group_snaps.find(remote_group_snap_id) != m_local_group_snaps.end() &&
      m_local_group_snaps[remote_group_snap_id].state == cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE) {
    on_finish->complete(-EEXIST);
    return;
  }

  ceph_assert(m_pending_snap_create.count(remote_group_snap_id) > 0);

  auto &create_snap_requests = m_create_snap_requests[remote_group_snap_id];

  ceph_assert(create_snap_requests.count(image_replayer) == 0);
  create_snap_requests[image_replayer] = on_finish;

  auto pool = image_replayer->get_local_pool_id();
  auto image_id = image_replayer->get_local_image_id();
  auto &local_group_snap = m_local_group_snaps[remote_group_snap_id];
  auto it = std::find_if(
      local_group_snap.snaps.begin(), local_group_snap.snaps.end(),
      [&pool, &image_id](const cls::rbd::ImageSnapshotSpec &s) {
        return pool == s.pool && image_id == s.image_id;
      });
  ceph_assert(it != local_group_snap.snaps.end());
  it->snap_id = snap_id;

  if (create_snap_requests.size() < local_group_snap.snaps.size()) {
    return;
  }

  local_group_snap.state = cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE;

  dout(20) << local_group_snap.id << " " << local_group_snap.name << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::group_snap_set(&op, local_group_snap);
  auto comp = create_rados_callback(
      new LambdaContext([this, remote_group_snap_id](int r) {
        handle_create_mirror_snapshot_finish(remote_group_snap_id, r);
      }));

  int r = m_local_io_ctx.aio_operate(
      librbd::util::group_header_name(m_local_group_ctx.group_id), comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupReplayer<I>::handle_create_mirror_snapshot_finish(
    const std::string &remote_group_snap_id, int r) {
  dout(20) << remote_group_snap_id << " r=" << r << dendl;

  std::unique_lock locker{m_lock};
  auto count = m_pending_snap_create.erase(remote_group_snap_id);
  ceph_assert(count > 0);

  auto create_snap_requests = m_create_snap_requests[remote_group_snap_id];
  m_create_snap_requests.erase(remote_group_snap_id);
  bool shut_down_replay = m_state == STATE_STOPPING && !m_restart_requested &&
                          m_pending_snap_create.empty() &&
                          m_create_snap_requests.empty();
  locker.unlock();

  for (auto &[_, on_finish] : create_snap_requests) {
    on_finish->complete(r);
  }

  if (shut_down_replay) {
    stop_image_replayers();
  }
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::GroupReplayer<librbd::ImageCtx>;
