// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/stringify.h"
#include "common/Formatter.h"
#include "common/admin_socket.h"
#include "common/debug.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "tools/rbd_mirror/image_replayer/Utils.h"
#include "GroupReplayer.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::" << *this << " " \
                           << __func__ << ": "

extern PerfCounters *g_perf_counters;

namespace rbd {
namespace mirror {

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
std::ostream &operator<<(std::ostream &os,
                         const typename GroupReplayer<I>::State &state) {
  switch (state) {
  case GroupReplayer<I>::STATE_STARTING:
    os << "Starting";
    break;
  case GroupReplayer<I>::STATE_REPLAYING:
    os << "Replaying";
    break;
  case GroupReplayer<I>::STATE_STOPPING:
    os << "Stopping";
    break;
  case GroupReplayer<I>::STATE_STOPPED:
    os << "Stopped";
    break;
  default:
    os << "Unknown (" << static_cast<uint32_t>(state) << ")";
    break;
  }
  return os;
}

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
  m_lock(ceph::make_mutex("rbd::mirror::GroupReplayer " +
      stringify(local_io_ctx.get_id()) + " " + global_group_id)) {
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
      derr << "already running" << dendl;
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
      m_manual_stop = false;
    }
  }

  if (r < 0) {
    if (on_finish) {
      on_finish->complete(r);
    }
    return;
  }

  // TODO
  on_finish->complete(0);
}

template <typename I>
void GroupReplayer<I>::stop(Context *on_finish, bool manual, bool restart) {
  dout(10) << "on_finish=" << on_finish << ", manual=" << manual
           << ", restart=" << restart << dendl;

  bool running = true;
  {
    std::lock_guard locker{m_lock};

    if (restart) {
      m_restart_requested = true;
    }

    if (!is_running_()) {
      running = false;
      if (!restart && m_restart_requested) {
        dout(10) << "canceling restart" << dendl;
        m_restart_requested = false;
      }
    } else {
      if (!is_stopped_()) {
	if (m_state == STATE_STARTING) {
	  dout(10) << "canceling start" << dendl;
	} else {
	  dout(10) << "interrupting replay" << dendl;
	}
      }
    }
  }

  if (!running) {
    dout(20) << "not running" << dendl;
    if (on_finish) {
      on_finish->complete(-EINVAL);
    }
    return;
  }

  // TODO
  on_finish->complete(0);
}

template <typename I>
void GroupReplayer<I>::restart(Context *on_finish) {
  {
    std::lock_guard locker{m_lock};
    m_restart_requested = true;
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
  // TODO
}

template <typename I>
void GroupReplayer<I>::print_status(Formatter *f) {
  dout(10) << dendl;

  std::lock_guard l{m_lock};

  f->open_object_section("group_replayer");
  f->dump_string("name", m_group_spec);
  f->dump_stream("state") << m_state;
  f->close_section();
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

} // namespace mirror
} // namespace rbd

template class rbd::mirror::GroupReplayer<librbd::ImageCtx>;
