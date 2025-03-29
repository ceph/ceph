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

using librbd::util::create_async_context_callback;
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
  ceph_assert(m_bootstrap_request == nullptr);
  ceph_assert(m_replayer == nullptr);
  ceph_assert(m_image_replayers.empty());
  ceph_assert(m_on_stop_contexts.empty());
  ceph_assert(m_replayer_check_task == nullptr);
}

template <typename I>
void GroupReplayer<I>::destroy() {
  {
    std::lock_guard locker{m_lock};

    for (auto &it : m_image_replayers) {
      ceph_assert(it.second->is_stopped());
      it.second->destroy();
    }
    m_image_replayers.clear();
  }
  delete this;
}

template <typename I>
void GroupReplayer<I>::sync_group_names() {
  if (m_local_group_ctx.primary) {
    reregister_admin_socket_hook();
    return;
  }

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
  }

  reregister_admin_socket_hook();
}

template <typename I>
group_replayer::HealthState GroupReplayer<I>::get_health_state() const {
  std::lock_guard locker{m_lock};

  if (!m_status_state) {
    return group_replayer::HEALTH_STATE_OK;
  } else if (*m_status_state ==
               cls::rbd::MIRROR_GROUP_STATUS_STATE_UNKNOWN) {
    return group_replayer::HEALTH_STATE_WARNING;
  }
  return group_replayer::HEALTH_STATE_ERROR;
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
      ceph_assert(m_replayer_check_task == nullptr);
      ceph_assert(m_replayer == nullptr);
      if (m_destroy_replayers) {
        ceph_assert(m_image_replayers.empty());
      }
      m_destroy_replayers = false;
  //  FIXME: replayer index is not used
      m_image_replayer_index.clear();
      m_manual_stop = false;
      m_finished = false;
      m_delete_requested = false;
      m_status_removed = false;
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
  bool shut_down_replay = false;
  bool is_stopped = false;
  {
    std::lock_guard locker{m_lock};

    dout(10) << "state: " << m_state << ", m_stop_requested: "
             << m_stop_requested << dendl;

    if (!is_running_()) {
      dout(10) << "replayers not running" << dendl;
      if (manual && !m_manual_stop) {
        dout(10) << "marking manual" << dendl;
        m_manual_stop = true;
      }
      if (!restart && m_restart_requested) {
        dout(10) << "canceling restart" << dendl;
        m_restart_requested = false;
      }
      if (is_stopped_()) {
        dout(10) << "already stopped" << dendl;
        is_stopped = true;
      } else {
        dout(10) << "joining in-flight stop" << dendl;
        if (on_finish != nullptr) {
          m_on_stop_contexts.push_back(on_finish);
        }
      }
    } else {
      dout(10) << "replayers still running" << dendl;
      if (m_state == STATE_STARTING) {
	dout(10) << "canceling start" << dendl;
	if (m_bootstrap_request != nullptr) {
          bootstrap_request = m_bootstrap_request;
          bootstrap_request->get();
	}
      } else {
        dout(10) << "interrupting replay" << dendl;
        shut_down_replay = true;
      }

      m_stop_requested = true;
      m_manual_stop = manual;
      if (on_finish != nullptr) {
	m_on_stop_contexts.push_back(on_finish);
      }
    }
  }

  if (is_stopped) {
    if (on_finish) {
      on_finish->complete(-EINVAL);
    }
    return;
  }

  if (bootstrap_request != nullptr) {
    dout(10) << "canceling bootstrap" << dendl;
    bootstrap_request->cancel();
    bootstrap_request->put();
  }

  if (shut_down_replay) {
    on_stop_replay(0);
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
  dout(10) <<  m_state << dendl;

  std::lock_guard l{m_lock};

  f->open_object_section("group_replayer");
  f->dump_string("name", m_group_spec);
  auto state = m_state;
  if (m_local_group_ctx.primary && state == STATE_REPLAYING) { // XXXMG
    dout(10) <<  "setting state to stopped" << dendl;
    state = STATE_STOPPED;
  }
  f->dump_string("state", state_to_string(state));
/*
  f->open_array_section("image_replayers");
  for (auto &[_, image_replayer] : m_image_replayers) {
    image_replayer->print_status(f);
  }
  f->close_section(); // image_replayers
*/
  f->close_section(); // group_replayer
}

template <typename I>
void GroupReplayer<I>::on_stop_replay(int r, const std::string &desc)
{
  dout(10) << "r=" << r << ", desc=" << desc << dendl;
  {
    std::lock_guard locker{m_lock};
    if (m_state != STATE_REPLAYING) {
      // might be invoked multiple times while stopping
      return;
    }

    m_stop_requested = true;
    m_state = STATE_STOPPING;
    m_status_state = cls::rbd::MIRROR_GROUP_STATUS_STATE_STOPPED;
    m_state_desc = "";
  }

  cancel_image_replayers_check();
  shut_down(r);
}

template <typename I>
void GroupReplayer<I>::bootstrap_group() {
  dout(10) << dendl;

  std::unique_lock locker{m_lock};
  if (m_peers.empty()) {
    locker.unlock();

    dout(5) << "no peer clusters" << dendl;
    finish_start_fail(-ENOENT, "no peer clusters");
    return;
  }

  // TODO need to support multiple remote groups
  ceph_assert(!m_peers.empty());
  m_remote_group_peer = *m_peers.begin();

  if (finish_start_if_interrupted(m_lock)) {
    return;
  }

  ceph_assert(m_replayer == nullptr);

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
// Should never happen
    if (m_state == STATE_STOPPING || m_state == STATE_STOPPED) {
      dout(10) << "stop prevailed" <<dendl;
      return;
    }
    if (m_bootstrap_request != nullptr) {
      m_bootstrap_request->put();
      m_bootstrap_request = nullptr;
    }
  }

  if (!m_local_group_ctx.name.empty()) {
    m_local_group_name = m_local_group_ctx.name;
  }
  if (r == -EINVAL) {
    sync_group_names();
  } else {
    reregister_admin_socket_hook();
  }

//FIXME : Relook at this once the Bootstrap no longer reuses values.
// Since a rerun may end up with variables populated by a previous run,
// it is safer to destroy the image_replayers now.
  m_destroy_replayers = true;
  if (finish_start_if_interrupted()) {
    return;
  } else if (r == -ENOENT) {
    finish_start_fail(r, "group removed");
    return;
  } else if (r == -EREMOTEIO) {
    m_destroy_replayers = true;
    finish_start_fail(r, "remote group is non-primary");
    return;
  } else if (r == -EEXIST) {
    finish_start_fail(r, "split-brain detected");
    return;
  } else if (r < 0) {
    finish_start_fail(r, "bootstrap failed");
    return;
  }

  // Start the image replayers in case the group is primary
  // in order to have the mirror pool health status set to ok.
  m_destroy_replayers = false;

/*
  if (m_local_group_ctx.primary) { // XXXMG
    set_mirror_group_status_update(
       cls::rbd::MIRROR_GROUP_STATUS_STATE_STOPPED,
       "local group is primary");
    finish_start_fail(0, "local group is primary");
    return;
  }
*/
  m_local_group_ctx.listener = &m_listener;

  // Start the image replayers first
  start_image_replayers();
}

template <typename I>
void GroupReplayer<I>::create_group_replayer() {
  dout(10) << dendl;

  ceph_assert(m_replayer == nullptr);

  auto ctx = create_context_callback<
    GroupReplayer, &GroupReplayer<I>::handle_create_group_replayer>(this);

  m_replayer = group_replayer::Replayer<I>::create(
    m_threads, m_local_io_ctx, m_remote_group_peer.io_ctx, m_global_group_id,
    m_local_mirror_uuid, m_pool_meta_cache, m_local_group_id, m_remote_group_id,
    &m_local_group_ctx, &m_image_replayers);

  m_replayer->init(ctx);
}

template <typename I>
void GroupReplayer<I>::handle_create_group_replayer(int r) {
  dout(10) << "r=" << r << dendl;

  if (finish_start_if_interrupted()) {
    return;
  } else if (r < 0) {

    finish_start_fail(r, "failed to create group replayer");
    return;
  }

  Context *on_finish = nullptr;
  {
    std::unique_lock locker{m_lock};
    ceph_assert(m_state == STATE_STARTING);
    m_state = STATE_REPLAYING;
    std::swap(m_on_start_finish, on_finish);

    std::unique_lock timer_locker{m_threads->timer_lock};
    schedule_image_replayers_check();
  }

  set_mirror_group_status_update(
     cls::rbd::MIRROR_GROUP_STATUS_STATE_REPLAYING, "replaying");

  if (on_finish) {
    on_finish->complete(0);
  }
}

// TODO: Move this to group_replayer::Replayer?
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
    finish_start_fail(r, "failed to start image replayers");
    return;
  }

  create_group_replayer();
}

template <typename I>
void GroupReplayer<I>::check_image_replayers_running() {
  dout(10) << dendl;
  bool stopped = false;
  {
    std::lock_guard locker{m_lock};
    if (m_state == STATE_STOPPING || m_state == STATE_STOPPED) {
      dout(10) << "stopping" <<dendl;
      return;
    }

    for (auto &it : m_image_replayers) {
      if (it.second->is_stopped()) {
	dout(10) << "image replayer stopped for global_id : "
                 << it.second->get_global_image_id() <<  dendl;
        stopped = true;
        break;
      }
    }
  }

  if (stopped) {
    // shut down
    dout(10) << " stopping group replayer" << dendl;
    //TODO: determine the error
    on_stop_replay();
    return;
  }
}

template <typename I>
void GroupReplayer<I>::schedule_image_replayers_check() {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  ceph_assert(ceph_mutex_is_locked_by_me(m_threads->timer_lock));

  if (m_state != STATE_REPLAYING) {
    return;
  }

  dout(10) << dendl;

  ceph_assert(m_replayer_check_task == nullptr);
  m_replayer_check_task = create_context_callback<
    GroupReplayer<I>,
    &GroupReplayer<I>::handle_image_replayers_check>(this);
  m_threads->timer->add_event_after(10, m_replayer_check_task);
}

template <typename I>
void GroupReplayer<I>::handle_image_replayers_check(int r) {
  dout(10) << dendl;

  ceph_assert(ceph_mutex_is_locked_by_me(m_threads->timer_lock));

  ceph_assert(m_replayer_check_task != nullptr);
  m_replayer_check_task = nullptr;

  auto ctx = new LambdaContext([this](int) {
    check_image_replayers_running();
    {
      std::unique_lock locker{m_lock};
      std::unique_lock timer_locker{m_threads->timer_lock};

      schedule_image_replayers_check();
    }
    m_in_flight_op_tracker.finish_op();
  });

  m_in_flight_op_tracker.start_op();
  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void GroupReplayer<I>::cancel_image_replayers_check() {
  std::unique_lock timer_locker{m_threads->timer_lock};
  if (m_replayer_check_task != nullptr) {
    dout(10) << dendl;

    if (m_threads->timer->cancel_event(m_replayer_check_task)) {
      m_replayer_check_task = nullptr;
    }
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

  finish_start_fail(-ECANCELED, "");
  return true;
}

template <typename I>
void GroupReplayer<I>::finish_start_fail(int r, const std::string &desc) {
  dout(10) << "r=" << r << ", desc=" << desc << dendl;
  Context *ctx = new LambdaContext([this, r, desc](int _r) {
    m_status_state = cls::rbd::MIRROR_GROUP_STATUS_STATE_STOPPED;
    m_state_desc = desc;
    {
      std::lock_guard locker{m_lock};
      ceph_assert(m_state == STATE_STARTING);
      m_state = STATE_STOPPING;
      if (r < 0) {
	if (r == -ECANCELED) {
	  dout(10) << "start canceled" << dendl;
	} else if (r == -ENOENT) {
          m_delete_requested = true;
	  dout(10) << "mirroring group removed" << dendl;
	} else if (r == -EREMOTEIO) {
	  dout(10) << "mirroring group demoted" << dendl;
	  m_status_state = cls::rbd::MIRROR_GROUP_STATUS_STATE_UNKNOWN;
	} else {
	  derr << "start failed: " << cpp_strerror(r) << dendl;
	  m_status_state = cls::rbd::MIRROR_GROUP_STATUS_STATE_ERROR;
	}
      }
    }
    shut_down(r);
  });

  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void GroupReplayer<I>::shut_down(int r) {
  dout(10) << "r=" << r << ", state=" << m_state << dendl;

  {
    std::lock_guard locker{m_lock};
    ceph_assert(m_state == STATE_STOPPING);
  }

  if (!m_in_flight_op_tracker.empty()) {
    dout(15) << "waiting for in-flight operations to complete" << dendl;
    m_in_flight_op_tracker.wait_for_ops(new LambdaContext([this, r](int) {
        shut_down(r);
      }));
    return;
  }

  // chain the shut down sequence (reverse order)
  Context *ctx = new LambdaContext(
    [this, r](int _r) {
      set_mirror_group_status_update(*m_status_state, m_state_desc);
      handle_shut_down(r);
    });

  // stop and destroy the replayers
  if (m_destroy_replayers) {
    ctx = new LambdaContext([this, ctx](int r) {
      {
	std::lock_guard locker{m_lock};

	for (auto &it : m_image_replayers) {
	  ceph_assert(it.second->is_stopped());
	  it.second->destroy();
	}
	m_image_replayers.clear();
      }
      ctx->complete(0);
    });
  }
  ctx = new LambdaContext([this, ctx](int r) {
    C_Gather *gather_ctx = new C_Gather(g_ceph_context, ctx);
    {
      std::lock_guard locker{m_lock};
      for (auto &it : m_image_replayers) {
        it.second->stop(gather_ctx->new_sub());
      }
    }
    gather_ctx->activate();
  });

  if (m_replayer != nullptr) {
    ctx = new LambdaContext([this, ctx](int r) {
      m_replayer->destroy();
      m_replayer = nullptr;
      ctx->complete(0);
    });

    ctx = new LambdaContext([this, ctx](int r) {
      m_replayer->shut_down(ctx);
    });
  }

  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void GroupReplayer<I>::handle_shut_down(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == -ENOENT) { // group removed
    if (!m_resync_requested) {
      set_finished(true);
    }
    unregister_admin_socket_hook();
  }

  if (!m_status_removed) {
    auto ctx = new LambdaContext([this, r](int) {
      m_status_removed = true;
      handle_shut_down(r);
    });
    remove_group_status(m_delete_requested, ctx);
    return;
  }

  dout(10) << "stop complete" << dendl;
  Context *on_start = nullptr;
  std::list<Context *> on_stop_contexts;
  {
    std::lock_guard locker{m_lock};
    std::swap(on_start, m_on_start_finish);
    on_stop_contexts = std::move(m_on_stop_contexts);
    m_stop_requested = false;
    ceph_assert(m_state == STATE_STOPPING);
    m_state = STATE_STOPPED;
  }

  if (on_start != nullptr) {
    dout(10) << "on start finish complete, r=" << r << dendl;
    on_start->complete(r);
    r = 0;
  }
  for (auto ctx : on_stop_contexts) {
    dout(10) << "on stop finish " << ctx << " complete, r=" << r << dendl;
    ctx->complete(r);
  }
}

template <typename I>
void GroupReplayer<I>::register_admin_socket_hook() {
  GroupReplayerAdminSocketHook<I> *asok_hook;
  {
    std::lock_guard locker{m_lock};
    if (m_asok_hook != nullptr) {
      return;
    }

    asok_hook = new GroupReplayerAdminSocketHook<I>(
      g_ceph_context, m_group_spec, this);
    int r = asok_hook->register_commands();
    if (r == 0) {
      m_asok_hook = asok_hook;
      dout(15) << "registered asok hook: " << m_group_spec << dendl;
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
void GroupReplayer<I>::remove_group_status(bool force, Context *on_finish)
{
  auto ctx = new LambdaContext([this, force, on_finish](int) {
    remove_group_status_remote(force, on_finish);
  });

  if (m_local_status_updater->mirror_group_exists(m_global_group_id)) {
    dout(15) << "removing local mirror group status: "
             << m_global_group_id << dendl;
    if (force) {
      m_local_status_updater->remove_mirror_group_status(
          m_global_group_id, true, ctx);
    } else {
      m_local_status_updater->remove_refresh_mirror_group_status(
          m_global_group_id, ctx);
    }
    return;
  }

  ctx->complete(0);
}

template <typename I>
void GroupReplayer<I>::remove_group_status_remote(bool force, Context *on_finish)
{
  if (m_remote_group_peer.mirror_status_updater != nullptr &&
      m_remote_group_peer.mirror_status_updater->mirror_group_exists(m_global_group_id)) {
    dout(15) << "removing remote mirror group status: "
             << m_global_group_id << dendl;
    if (force) {
      m_remote_group_peer.mirror_status_updater->remove_mirror_group_status(
          m_global_group_id, true, on_finish);
    } else {
      m_remote_group_peer.mirror_status_updater->remove_refresh_mirror_group_status(
          m_global_group_id, on_finish);
    }
    return;
  }
  if (on_finish) {
    on_finish->complete(0);
  }
}

template <typename I>
void GroupReplayer<I>::set_mirror_group_status_update(
    cls::rbd::MirrorGroupStatusState state, const std::string &desc) {
  dout(20) << "state=" << state << ", description=" << desc << dendl;

  reregister_admin_socket_hook();

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

  auto images_status = local_status.mirror_images;
  // catch and bubble-up all the image level errors to group status
  if (!images_status.empty() &&
      ((m_local_group_ctx.primary && state == cls::rbd::MIRROR_GROUP_STATUS_STATE_STOPPED) ||
      (!m_local_group_ctx.primary && state == cls::rbd::MIRROR_GROUP_STATUS_STATE_REPLAYING))) {
    for (auto &image_site_status : images_status) {
      if (image_site_status.second.state == cls::rbd::MIRROR_IMAGE_STATUS_STATE_ERROR) {
        dout(10) << "ImageReplayer with global image id: " << image_site_status.first.global_image_id
            << " in error state, with description: " << image_site_status.second.description
            << " marking group replayer to error state with global group id: " << m_global_group_id
            << dendl;
        local_status.state = cls::rbd::MIRROR_GROUP_STATUS_STATE_ERROR;
        local_status.description = "image in error state";
        break;
      }
    }
  }

  m_local_status_updater->set_mirror_group_status(m_global_group_id,
                                                  local_status, true);
  if (m_remote_group_peer.mirror_status_updater != nullptr) {
    m_remote_group_peer.mirror_status_updater->set_mirror_group_status(
        m_global_group_id, remote_status, true);
  }
  m_status_state = local_status.state;
  m_state_desc = local_status.description;
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::GroupReplayer<librbd::ImageCtx>;
