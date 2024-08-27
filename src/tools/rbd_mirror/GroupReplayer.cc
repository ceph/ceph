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
#include "tools/rbd_mirror/group_replayer/Replayer.h"
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
  //ceph_assert(m_on_start_finish == nullptr);
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
      m_finished = false;
      //ceph_assert(m_on_start_finish == nullptr);
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
    m_pool_meta_cache, &m_resync_requested, &m_local_group_id,
    &m_remote_group_id, &m_local_group_snaps, &m_local_group_ctx,
    &m_image_replayers, &m_image_replayer_index, ctx);

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

  C_SaferCond ctx;
  create_group_replayer(&ctx);
  ctx.wait();
}

template <typename I>
void GroupReplayer<I>::create_group_replayer(Context *on_finish) {
  dout(10) << dendl;

  auto ctx = new LambdaContext(
    [this, on_finish](int r) {
      handle_create_group_replayer(r, on_finish);
    });

  m_replayer = group_replayer::Replayer<I>::create(
    m_threads, m_local_io_ctx, m_remote_group_peer.io_ctx, m_global_group_id,
    m_local_mirror_uuid, m_remote_group_peer.uuid, m_pool_meta_cache,
    m_local_group_id, m_remote_group_id, &m_local_group_ctx, &m_image_replayers);

  m_replayer->init(ctx);
}

template <typename I>
void GroupReplayer<I>::handle_create_group_replayer(int r, Context *on_finish) {
  dout(10) << "r=" << r << dendl;

  if (m_state == STATE_STOPPING || m_state == STATE_STOPPED) {
    dout(10) << "stop prevailed" <<dendl;
    on_finish->complete(r);
    return;
  }
  on_finish->complete(0);
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
void GroupReplayer<I>::stop_group_replayer(Context *on_finish) {
  dout(10) << dendl;

  Context *ctx = new LambdaContext(
      [this, on_finish](int r) {
      handle_stop_group_replayer(r, on_finish);
      });

  if (m_replayer != nullptr) {
    m_replayer->shut_down(ctx);
    return;
  }
  on_finish->complete(0);
}

template <typename I>
void GroupReplayer<I>::handle_stop_group_replayer(int r, Context *on_finish) {
  dout(10) << "r=" << r << dendl;

  if (on_finish != nullptr) {
    on_finish->complete(0);
  }
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

  stop_group_replayer(on_finish);
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

                if (r == -ENOENT && !m_resync_requested) {
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

} // namespace mirror
} // namespace rbd

template class rbd::mirror::GroupReplayer<librbd::ImageCtx>;
