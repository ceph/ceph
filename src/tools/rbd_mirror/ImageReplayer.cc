// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/compat.h"
#include "common/Formatter.h"
#include "common/admin_socket.h"
#include "common/debug.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "cls/rbd/cls_rbd_client.h"
#include "common/Timer.h"
#include "global/global_context.h"
#include "journal/Journaler.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Journal.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "ImageDeleter.h"
#include "ImageReplayer.h"
#include "MirrorStatusUpdater.h"
#include "Threads.h"
#include "tools/rbd_mirror/image_replayer/BootstrapRequest.h"
#include "tools/rbd_mirror/image_replayer/ReplayerListener.h"
#include "tools/rbd_mirror/image_replayer/StateBuilder.h"
#include "tools/rbd_mirror/image_replayer/Utils.h"
#include "tools/rbd_mirror/image_replayer/journal/Replayer.h"
#include "tools/rbd_mirror/image_replayer/journal/StateBuilder.h"
#include <map>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::" << *this << " " \
                           << __func__ << ": "

extern PerfCounters *g_perf_counters;

namespace rbd {
namespace mirror {

using librbd::util::create_context_callback;

template <typename I>
std::ostream &operator<<(std::ostream &os,
                         const typename ImageReplayer<I>::State &state);

namespace {

template <typename I>
class ImageReplayerAdminSocketCommand {
public:
  ImageReplayerAdminSocketCommand(const std::string &desc,
                                  ImageReplayer<I> *replayer)
    : desc(desc), replayer(replayer) {
  }
  virtual ~ImageReplayerAdminSocketCommand() {}
  virtual int call(Formatter *f) = 0;

  std::string desc;
  ImageReplayer<I> *replayer;
  bool registered = false;
};

template <typename I>
class StatusCommand : public ImageReplayerAdminSocketCommand<I> {
public:
  explicit StatusCommand(const std::string &desc, ImageReplayer<I> *replayer)
    : ImageReplayerAdminSocketCommand<I>(desc, replayer) {
  }

  int call(Formatter *f) override {
    this->replayer->print_status(f);
    return 0;
  }
};

template <typename I>
class StartCommand : public ImageReplayerAdminSocketCommand<I> {
public:
  explicit StartCommand(const std::string &desc, ImageReplayer<I> *replayer)
    : ImageReplayerAdminSocketCommand<I>(desc, replayer) {
  }

  int call(Formatter *f) override {
    this->replayer->start(nullptr, true);
    return 0;
  }
};

template <typename I>
class StopCommand : public ImageReplayerAdminSocketCommand<I> {
public:
  explicit StopCommand(const std::string &desc, ImageReplayer<I> *replayer)
    : ImageReplayerAdminSocketCommand<I>(desc, replayer) {
  }

  int call(Formatter *f) override {
    this->replayer->stop(nullptr, true);
    return 0;
  }
};

template <typename I>
class RestartCommand : public ImageReplayerAdminSocketCommand<I> {
public:
  explicit RestartCommand(const std::string &desc, ImageReplayer<I> *replayer)
    : ImageReplayerAdminSocketCommand<I>(desc, replayer) {
  }

  int call(Formatter *f) override {
    this->replayer->restart();
    return 0;
  }
};

template <typename I>
class FlushCommand : public ImageReplayerAdminSocketCommand<I> {
public:
  explicit FlushCommand(const std::string &desc, ImageReplayer<I> *replayer)
    : ImageReplayerAdminSocketCommand<I>(desc, replayer) {
  }

  int call(Formatter *f) override {
    this->replayer->flush();
    return 0;
  }
};

template <typename I>
class ImageReplayerAdminSocketHook : public AdminSocketHook {
public:
  ImageReplayerAdminSocketHook(CephContext *cct, const std::string &name,
			       ImageReplayer<I> *replayer)
    : admin_socket(cct->get_admin_socket()),
      commands{{"rbd mirror flush " + name,
                new FlushCommand<I>("flush rbd mirror " + name, replayer)},
               {"rbd mirror restart " + name,
                new RestartCommand<I>("restart rbd mirror " + name, replayer)},
               {"rbd mirror start " + name,
                new StartCommand<I>("start rbd mirror " + name, replayer)},
               {"rbd mirror status " + name,
                new StatusCommand<I>("get status for rbd mirror " + name, replayer)},
               {"rbd mirror stop " + name,
                new StopCommand<I>("stop rbd mirror " + name, replayer)}} {
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

  ~ImageReplayerAdminSocketHook() override {
    admin_socket->unregister_commands(this);
    for (auto &it : commands) {
      delete it.second;
    }
    commands.clear();
  }

  int call(std::string_view command, const cmdmap_t& cmdmap,
	   Formatter *f,
	   std::ostream& errss,
	   bufferlist& out) override {
    auto i = commands.find(command);
    ceph_assert(i != commands.end());
    return i->second->call(f);
  }

private:
  typedef std::map<std::string, ImageReplayerAdminSocketCommand<I>*,
		   std::less<>> Commands;

  AdminSocket *admin_socket;
  Commands commands;
};

} // anonymous namespace

template <typename I>
void ImageReplayer<I>::BootstrapProgressContext::update_progress(
  const std::string &description, bool flush)
{
  const std::string desc = "bootstrapping, " + description;
  replayer->set_state_description(0, desc);
  if (flush) {
    replayer->update_mirror_image_status(false, boost::none);
  }
}

template <typename I>
struct ImageReplayer<I>::ReplayerListener
  : public image_replayer::ReplayerListener {
  ImageReplayer<I>* image_replayer;

  ReplayerListener(ImageReplayer<I>* image_replayer)
    : image_replayer(image_replayer) {
  }

  void handle_notification() override {
    image_replayer->handle_replayer_notification();
  }
};

template <typename I>
ImageReplayer<I>::ImageReplayer(
    librados::IoCtx &local_io_ctx, const std::string &local_mirror_uuid,
    const std::string &global_image_id, Threads<I> *threads,
    InstanceWatcher<I> *instance_watcher,
    MirrorStatusUpdater<I>* local_status_updater,
    journal::CacheManagerHandler *cache_manager_handler,
    PoolMetaCache* pool_meta_cache) :
  m_local_io_ctx(local_io_ctx), m_local_mirror_uuid(local_mirror_uuid),
  m_global_image_id(global_image_id), m_threads(threads),
  m_instance_watcher(instance_watcher),
  m_local_status_updater(local_status_updater),
  m_cache_manager_handler(cache_manager_handler),
  m_pool_meta_cache(pool_meta_cache),
  m_local_image_name(global_image_id),
  m_lock(ceph::make_mutex("rbd::mirror::ImageReplayer " +
      stringify(local_io_ctx.get_id()) + " " + global_image_id)),
  m_progress_cxt(this),
  m_replayer_listener(new ReplayerListener(this))
{
  // Register asok commands using a temporary "remote_pool_name/global_image_id"
  // name.  When the image name becomes known on start the asok commands will be
  // re-registered using "remote_pool_name/remote_image_name" name.

  m_image_spec = image_replayer::util::compute_image_spec(
    local_io_ctx, global_image_id);
  register_admin_socket_hook();
}

template <typename I>
ImageReplayer<I>::~ImageReplayer()
{
  unregister_admin_socket_hook();
  ceph_assert(m_state_builder == nullptr);
  ceph_assert(m_on_start_finish == nullptr);
  ceph_assert(m_on_stop_finish == nullptr);
  ceph_assert(m_bootstrap_request == nullptr);
  ceph_assert(m_update_status_task == nullptr);
  delete m_replayer_listener;
}

template <typename I>
image_replayer::HealthState ImageReplayer<I>::get_health_state() const {
  std::lock_guard locker{m_lock};

  if (!m_mirror_image_status_state) {
    return image_replayer::HEALTH_STATE_OK;
  } else if (*m_mirror_image_status_state ==
               cls::rbd::MIRROR_IMAGE_STATUS_STATE_SYNCING ||
             *m_mirror_image_status_state ==
               cls::rbd::MIRROR_IMAGE_STATUS_STATE_UNKNOWN) {
    return image_replayer::HEALTH_STATE_WARNING;
  }
  return image_replayer::HEALTH_STATE_ERROR;
}

template <typename I>
void ImageReplayer<I>::add_peer(const Peer<I>& peer) {
  dout(10) << "peer=" << peer << dendl;

  std::lock_guard locker{m_lock};
  auto it = m_peers.find(peer);
  if (it == m_peers.end()) {
    m_peers.insert(peer);
  }
}

template <typename I>
void ImageReplayer<I>::set_state_description(int r, const std::string &desc) {
  dout(10) << "r=" << r << ", desc=" << desc << dendl;

  std::lock_guard l{m_lock};
  m_last_r = r;
  m_state_desc = desc;
}

template <typename I>
void ImageReplayer<I>::start(Context *on_finish, bool manual, bool restart)
{
  dout(10) << "on_finish=" << on_finish << dendl;

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
      m_delete_requested = false;
      m_restart_requested = false;

      if (on_finish != nullptr) {
        ceph_assert(m_on_start_finish == nullptr);
        m_on_start_finish = on_finish;
      }
      ceph_assert(m_on_stop_finish == nullptr);
    }
  }

  if (r < 0) {
    if (on_finish) {
      on_finish->complete(r);
    }
    return;
  }

  bootstrap();
}

template <typename I>
void ImageReplayer<I>::bootstrap() {
  dout(10) << dendl;

  std::unique_lock locker{m_lock};
  if (m_peers.empty()) {
    locker.unlock();

    dout(5) << "no peer clusters" << dendl;
    on_start_fail(-ENOENT, "no peer clusters");
    return;
  }

  // TODO need to support multiple remote images
  ceph_assert(!m_peers.empty());
  m_remote_image_peer = *m_peers.begin();

  if (on_start_interrupted(m_lock)) {
    return;
  }

  ceph_assert(m_state_builder == nullptr);
  auto ctx = create_context_callback<
      ImageReplayer, &ImageReplayer<I>::handle_bootstrap>(this);
  auto request = image_replayer::BootstrapRequest<I>::create(
      m_threads, m_local_io_ctx, m_remote_image_peer.io_ctx, m_instance_watcher,
      m_global_image_id, m_local_mirror_uuid,
      m_remote_image_peer.remote_pool_meta, m_cache_manager_handler,
      m_pool_meta_cache, &m_progress_cxt, &m_state_builder, &m_resync_requested,
      ctx);

  request->get();
  m_bootstrap_request = request;
  locker.unlock();

  update_mirror_image_status(false, boost::none);
  request->send();
}

template <typename I>
void ImageReplayer<I>::handle_bootstrap(int r) {
  dout(10) << "r=" << r << dendl;
  {
    std::lock_guard locker{m_lock};
    m_bootstrap_request->put();
    m_bootstrap_request = nullptr;
  }

  if (on_start_interrupted()) {
    return;
  } else if (r == -ENOMSG) {
    dout(5) << "local image is primary" << dendl;
    on_start_fail(0, "local image is primary");
    return;
  } else if (r == -EREMOTEIO) {
    dout(5) << "remote image is non-primary" << dendl;
    on_start_fail(-EREMOTEIO, "remote image is non-primary");
    return;
  } else if (r == -EEXIST) {
    on_start_fail(r, "split-brain detected");
    return;
  } else if (r == -ENOLINK) {
    m_delete_requested = true;
    on_start_fail(0, "remote image no longer exists");
    return;
  } else if (r < 0) {
    on_start_fail(r, "error bootstrapping replay");
    return;
  } else if (m_resync_requested) {
    on_start_fail(0, "resync requested");
    return;
  }

  start_replay();
}

template <typename I>
void ImageReplayer<I>::start_replay() {
  dout(10) << dendl;

  std::unique_lock locker{m_lock};
  ceph_assert(m_replayer == nullptr);
  m_replayer = m_state_builder->create_replayer(m_threads, m_instance_watcher,
                                                m_local_mirror_uuid,
                                                m_pool_meta_cache,
                                                m_replayer_listener);

  auto ctx = create_context_callback<
    ImageReplayer<I>, &ImageReplayer<I>::handle_start_replay>(this);
  m_replayer->init(ctx);
}

template <typename I>
void ImageReplayer<I>::handle_start_replay(int r) {
  dout(10) << "r=" << r << dendl;

  if (on_start_interrupted()) {
    return;
  } else if (r < 0) {
    std::string error_description = m_replayer->get_error_description();
    if (r == -ENOTCONN && m_replayer->is_resync_requested()) {
      std::unique_lock locker{m_lock};
      m_resync_requested = true;
    }

    // shut down not required if init failed
    m_replayer->destroy();
    m_replayer = nullptr;

    derr << "error starting replay: " << cpp_strerror(r) << dendl;
    on_start_fail(r, error_description);
    return;
  }

  Context *on_finish = nullptr;
  {
    std::unique_lock locker{m_lock};
    ceph_assert(m_state == STATE_STARTING);
    m_state = STATE_REPLAYING;
    std::swap(m_on_start_finish, on_finish);

    std::unique_lock timer_locker{m_threads->timer_lock};
    schedule_update_mirror_image_replay_status();
  }

  update_mirror_image_status(true, boost::none);
  if (on_replay_interrupted()) {
    if (on_finish != nullptr) {
      on_finish->complete(r);
    }
    return;
  }

  dout(10) << "start succeeded" << dendl;
  if (on_finish != nullptr) {
    dout(10) << "on finish complete, r=" << r << dendl;
    on_finish->complete(r);
  }
}

template <typename I>
void ImageReplayer<I>::on_start_fail(int r, const std::string &desc)
{
  dout(10) << "r=" << r << ", desc=" << desc << dendl;
  Context *ctx = new LambdaContext([this, r, desc](int _r) {
      {
	std::lock_guard locker{m_lock};
        ceph_assert(m_state == STATE_STARTING);
        m_state = STATE_STOPPING;
        if (r < 0 && r != -ECANCELED && r != -EREMOTEIO && r != -ENOENT) {
          derr << "start failed: " << cpp_strerror(r) << dendl;
        } else {
          dout(10) << "start canceled" << dendl;
        }
      }

      set_state_description(r, desc);
      update_mirror_image_status(false, boost::none);
      shut_down(r);
    });
  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
bool ImageReplayer<I>::on_start_interrupted() {
  std::lock_guard locker{m_lock};
  return on_start_interrupted(m_lock);
}

template <typename I>
bool ImageReplayer<I>::on_start_interrupted(ceph::mutex& lock) {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  ceph_assert(m_state == STATE_STARTING);
  if (!m_stop_requested) {
    return false;
  }

  on_start_fail(-ECANCELED, "");
  return true;
}

template <typename I>
void ImageReplayer<I>::stop(Context *on_finish, bool manual, bool restart)
{
  dout(10) << "on_finish=" << on_finish << ", manual=" << manual
           << ", restart=" << restart << dendl;

  image_replayer::BootstrapRequest<I> *bootstrap_request = nullptr;
  bool shut_down_replay = false;
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
	  if (m_bootstrap_request != nullptr) {
            bootstrap_request = m_bootstrap_request;
            bootstrap_request->get();
	  }
	} else {
	  dout(10) << "interrupting replay" << dendl;
	  shut_down_replay = true;
	}

        ceph_assert(m_on_stop_finish == nullptr);
        std::swap(m_on_stop_finish, on_finish);
        m_stop_requested = true;
        m_manual_stop = manual;
      }
    }
  }

  // avoid holding lock since bootstrap request will update status
  if (bootstrap_request != nullptr) {
    dout(10) << "canceling bootstrap" << dendl;
    bootstrap_request->cancel();
    bootstrap_request->put();
  }

  if (!running) {
    dout(20) << "not running" << dendl;
    if (on_finish) {
      on_finish->complete(-EINVAL);
    }
    return;
  }

  if (shut_down_replay) {
    on_stop_journal_replay();
  } else if (on_finish != nullptr) {
    on_finish->complete(0);
  }
}

template <typename I>
void ImageReplayer<I>::on_stop_journal_replay(int r, const std::string &desc)
{
  dout(10) << dendl;

  {
    std::lock_guard locker{m_lock};
    if (m_state != STATE_REPLAYING) {
      // might be invoked multiple times while stopping
      return;
    }

    m_stop_requested = true;
    m_state = STATE_STOPPING;
  }

  cancel_update_mirror_image_replay_status();
  set_state_description(r, desc);
  update_mirror_image_status(true, boost::none);
  shut_down(0);
}

template <typename I>
void ImageReplayer<I>::restart(Context *on_finish)
{
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
void ImageReplayer<I>::flush()
{
  C_SaferCond ctx;

  {
    std::unique_lock locker{m_lock};
    if (m_state != STATE_REPLAYING) {
      return;
    }

    dout(10) << dendl;
    ceph_assert(m_replayer != nullptr);
    m_replayer->flush(&ctx);
  }

  int r = ctx.wait();
  if (r >= 0) {
    update_mirror_image_status(false, boost::none);
  }
}

template <typename I>
bool ImageReplayer<I>::on_replay_interrupted()
{
  bool shut_down;
  {
    std::lock_guard locker{m_lock};
    shut_down = m_stop_requested;
  }

  if (shut_down) {
    on_stop_journal_replay();
  }
  return shut_down;
}

template <typename I>
void ImageReplayer<I>::print_status(Formatter *f)
{
  dout(10) << dendl;

  std::lock_guard l{m_lock};

  f->open_object_section("image_replayer");
  f->dump_string("name", m_image_spec);
  f->dump_string("state", to_string(m_state));
  f->close_section();
}

template <typename I>
void ImageReplayer<I>::schedule_update_mirror_image_replay_status() {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  ceph_assert(ceph_mutex_is_locked_by_me(m_threads->timer_lock));
  if (m_state != STATE_REPLAYING) {
    return;
  }

  dout(10) << dendl;

  // periodically update the replaying status even if nothing changes
  // so that we can adjust our performance stats
  ceph_assert(m_update_status_task == nullptr);
  m_update_status_task = create_context_callback<
    ImageReplayer<I>,
    &ImageReplayer<I>::handle_update_mirror_image_replay_status>(this);
  m_threads->timer->add_event_after(10, m_update_status_task);
}

template <typename I>
void ImageReplayer<I>::handle_update_mirror_image_replay_status(int r) {
  dout(10) << dendl;

  auto ctx = new LambdaContext([this](int) {
      update_mirror_image_status(false, boost::none);

      {
        std::unique_lock locker{m_lock};
        std::unique_lock timer_locker{m_threads->timer_lock};
        ceph_assert(m_update_status_task != nullptr);
        m_update_status_task = nullptr;

        schedule_update_mirror_image_replay_status();
      }

      m_in_flight_op_tracker.finish_op();
    });

  m_in_flight_op_tracker.start_op();
  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void ImageReplayer<I>::cancel_update_mirror_image_replay_status() {
  std::unique_lock timer_locker{m_threads->timer_lock};
  if (m_update_status_task != nullptr) {
    dout(10) << dendl;

    if (m_threads->timer->cancel_event(m_update_status_task)) {
      m_update_status_task = nullptr;
    }
  }
}

template <typename I>
void ImageReplayer<I>::update_mirror_image_status(
    bool force, const OptionalState &opt_state) {
  dout(15) << "force=" << force << ", "
           << "state=" << opt_state << dendl;

  {
    std::lock_guard locker{m_lock};
    if (!force && !is_stopped_() && !is_running_()) {
      dout(15) << "shut down in-progress: ignoring update" << dendl;
      return;
    }
  }

  m_in_flight_op_tracker.start_op();
  auto ctx = new LambdaContext(
    [this, force, opt_state](int r) {
      set_mirror_image_status_update(force, opt_state);
    });
  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void ImageReplayer<I>::set_mirror_image_status_update(
    bool force, const OptionalState &opt_state) {
  dout(15) << "force=" << force << ", "
           << "state=" << opt_state << dendl;

  reregister_admin_socket_hook();

  State state;
  std::string state_desc;
  int last_r;
  bool stopping_replay;

  auto mirror_image_status_state = boost::make_optional(
    false, cls::rbd::MIRROR_IMAGE_STATUS_STATE_UNKNOWN);
  image_replayer::BootstrapRequest<I>* bootstrap_request = nullptr;
  {
    std::lock_guard locker{m_lock};
    state = m_state;
    state_desc = m_state_desc;
    mirror_image_status_state = m_mirror_image_status_state;
    last_r = m_last_r;
    stopping_replay = (m_replayer != nullptr);

    if (m_bootstrap_request != nullptr) {
      bootstrap_request = m_bootstrap_request;
      bootstrap_request->get();
    }
  }

  bool syncing = false;
  if (bootstrap_request != nullptr) {
    syncing = bootstrap_request->is_syncing();
    bootstrap_request->put();
    bootstrap_request = nullptr;
  }

  if (opt_state) {
    state = *opt_state;
  }

  cls::rbd::MirrorImageSiteStatus status;
  status.up = true;
  switch (state) {
  case STATE_STARTING:
    if (syncing) {
      status.state = cls::rbd::MIRROR_IMAGE_STATUS_STATE_SYNCING;
      status.description = state_desc.empty() ? "syncing" : state_desc;
      mirror_image_status_state = status.state;
    } else {
      status.state = cls::rbd::MIRROR_IMAGE_STATUS_STATE_STARTING_REPLAY;
      status.description = "starting replay";
    }
    break;
  case STATE_REPLAYING:
    status.state = cls::rbd::MIRROR_IMAGE_STATUS_STATE_REPLAYING;
    {
      std::string desc;
      auto on_req_finish = new LambdaContext(
        [this, force](int r) {
          dout(15) << "replay status ready: r=" << r << dendl;
          if (r >= 0) {
            set_mirror_image_status_update(force, boost::none);
          } else if (r == -EAGAIN) {
            m_in_flight_op_tracker.finish_op();
          }
        });

      ceph_assert(m_replayer != nullptr);
      if (!m_replayer->get_replay_status(&desc, on_req_finish)) {
        dout(15) << "waiting for replay status" << dendl;
        return;
      }

      status.description = "replaying, " + desc;
      mirror_image_status_state = boost::make_optional(
        false, cls::rbd::MIRROR_IMAGE_STATUS_STATE_UNKNOWN);
    }
    break;
  case STATE_STOPPING:
    if (stopping_replay) {
      status.state = cls::rbd::MIRROR_IMAGE_STATUS_STATE_STOPPING_REPLAY;
      status.description = state_desc.empty() ? "stopping replay" : state_desc;
      break;
    }
    // FALLTHROUGH
  case STATE_STOPPED:
    if (last_r == -EREMOTEIO) {
      status.state = cls::rbd::MIRROR_IMAGE_STATUS_STATE_UNKNOWN;
      status.description = state_desc;
      mirror_image_status_state = status.state;
    } else if (last_r < 0 && last_r != -ECANCELED) {
      status.state = cls::rbd::MIRROR_IMAGE_STATUS_STATE_ERROR;
      status.description = state_desc;
      mirror_image_status_state = status.state;
    } else {
      status.state = cls::rbd::MIRROR_IMAGE_STATUS_STATE_STOPPED;
      status.description = state_desc.empty() ? "stopped" : state_desc;
      mirror_image_status_state = boost::none;
    }
    break;
  default:
    ceph_assert(!"invalid state");
  }

  {
    std::lock_guard locker{m_lock};
    m_mirror_image_status_state = mirror_image_status_state;
  }

  // prevent the status from ping-ponging when failed replays are restarted
  if (mirror_image_status_state &&
      *mirror_image_status_state == cls::rbd::MIRROR_IMAGE_STATUS_STATE_ERROR) {
    status.state = *mirror_image_status_state;
  }

  dout(15) << "status=" << status << dendl;
  m_local_status_updater->set_mirror_image_status(m_global_image_id, status,
                                                  force);
  if (m_remote_image_peer.mirror_status_updater != nullptr) {
    m_remote_image_peer.mirror_status_updater->set_mirror_image_status(
      m_global_image_id, status, force);
  }

  m_in_flight_op_tracker.finish_op();
}

template <typename I>
void ImageReplayer<I>::shut_down(int r) {
  dout(10) << "r=" << r << dendl;

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
      update_mirror_image_status(true, STATE_STOPPED);
      handle_shut_down(r);
    });

  // destruct the state builder
  if (m_state_builder != nullptr) {
    ctx = new LambdaContext([this, ctx](int r) {
      m_state_builder->close(ctx);
    });
  }

  // close the replayer
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
void ImageReplayer<I>::handle_shut_down(int r) {
  bool resync_requested = false;
  bool delete_requested = false;
  bool unregister_asok_hook = false;
  {
    std::lock_guard locker{m_lock};

    if (m_delete_requested && m_state_builder != nullptr &&
        !m_state_builder->local_image_id.empty()) {
      ceph_assert(m_state_builder->remote_image_id.empty());
      dout(0) << "remote image no longer exists: scheduling deletion" << dendl;
      unregister_asok_hook = true;
      std::swap(delete_requested, m_delete_requested);
    }

    std::swap(resync_requested, m_resync_requested);
    if (!delete_requested && !resync_requested && m_last_r == -ENOENT &&
        ((m_state_builder == nullptr) ||
         (m_state_builder->local_image_id.empty() &&
          m_state_builder->remote_image_id.empty()))) {
      dout(0) << "mirror image no longer exists" << dendl;
      unregister_asok_hook = true;
      m_finished = true;
    }
  }

  if (unregister_asok_hook) {
    unregister_admin_socket_hook();
  }

  if (delete_requested || resync_requested) {
    dout(5) << "moving image to trash" << dendl;
    auto ctx = new LambdaContext([this, r](int) {
      handle_shut_down(r);
    });
    ImageDeleter<I>::trash_move(m_local_io_ctx, m_global_image_id,
                                resync_requested, m_threads->work_queue, ctx);
    return;
  }

  if (!m_in_flight_op_tracker.empty()) {
    dout(15) << "waiting for in-flight operations to complete" << dendl;
    m_in_flight_op_tracker.wait_for_ops(new LambdaContext([this, r](int) {
        handle_shut_down(r);
      }));
    return;
  }

  if (m_local_status_updater->exists(m_global_image_id)) {
    dout(15) << "removing local mirror image status" << dendl;
    auto ctx = new LambdaContext([this, r](int) {
        handle_shut_down(r);
      });
    m_local_status_updater->remove_mirror_image_status(m_global_image_id, ctx);
    return;
  }

  if (m_remote_image_peer.mirror_status_updater != nullptr &&
      m_remote_image_peer.mirror_status_updater->exists(m_global_image_id)) {
    dout(15) << "removing remote mirror image status" << dendl;
    auto ctx = new LambdaContext([this, r](int) {
        handle_shut_down(r);
      });
    m_remote_image_peer.mirror_status_updater->remove_mirror_image_status(
      m_global_image_id, ctx);
    return;
  }

  if (m_state_builder != nullptr) {
    m_state_builder->destroy();
    m_state_builder = nullptr;
  }

  dout(10) << "stop complete" << dendl;
  Context *on_start = nullptr;
  Context *on_stop = nullptr;
  {
    std::lock_guard locker{m_lock};
    std::swap(on_start, m_on_start_finish);
    std::swap(on_stop, m_on_stop_finish);
    m_stop_requested = false;
    ceph_assert(m_state == STATE_STOPPING);
    m_state = STATE_STOPPED;
  }

  if (on_start != nullptr) {
    dout(10) << "on start finish complete, r=" << r << dendl;
    on_start->complete(r);
    r = 0;
  }
  if (on_stop != nullptr) {
    dout(10) << "on stop finish complete, r=" << r << dendl;
    on_stop->complete(r);
  }
}

template <typename I>
void ImageReplayer<I>::handle_replayer_notification() {
  dout(10) << dendl;

  std::unique_lock locker{m_lock};
  if (m_state != STATE_REPLAYING) {
    // might be attempting to shut down
    return;
  }

  {
    // detect a rename of the local image
    ceph_assert(m_state_builder != nullptr &&
                m_state_builder->local_image_ctx != nullptr);
    std::shared_lock image_locker{m_state_builder->local_image_ctx->image_lock};
    if (m_local_image_name != m_state_builder->local_image_ctx->name) {
      // will re-register with new name after next status update
      dout(10) << "image renamed" << dendl;
      m_local_image_name = m_state_builder->local_image_ctx->name;
    }
  }

  // replayer cannot be shut down while notification is in-flight
  ceph_assert(m_replayer != nullptr);
  locker.unlock();

  if (m_replayer->is_resync_requested()) {
    dout(10) << "resync requested" << dendl;
    m_resync_requested = true;
    on_stop_journal_replay(0, "resync requested");
    return;
  }

  if (!m_replayer->is_replaying()) {
    auto error_code = m_replayer->get_error_code();
    auto error_description = m_replayer->get_error_description();
    dout(10) << "replay interrupted: "
             << "r=" << error_code << ", "
             << "error=" << error_description << dendl;
    on_stop_journal_replay(error_code, error_description);
    return;
  }

  update_mirror_image_status(false, {});
}

template <typename I>
std::string ImageReplayer<I>::to_string(const State state) {
  switch (state) {
  case ImageReplayer<I>::STATE_STARTING:
    return "Starting";
  case ImageReplayer<I>::STATE_REPLAYING:
    return "Replaying";
  case ImageReplayer<I>::STATE_STOPPING:
    return "Stopping";
  case ImageReplayer<I>::STATE_STOPPED:
    return "Stopped";
  default:
    break;
  }
  return "Unknown(" + stringify(state) + ")";
}

template <typename I>
void ImageReplayer<I>::register_admin_socket_hook() {
  ImageReplayerAdminSocketHook<I> *asok_hook;
  {
    std::lock_guard locker{m_lock};
    if (m_asok_hook != nullptr) {
      return;
    }

    dout(15) << "registered asok hook: " << m_image_spec << dendl;
    asok_hook = new ImageReplayerAdminSocketHook<I>(
      g_ceph_context, m_image_spec, this);
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
void ImageReplayer<I>::unregister_admin_socket_hook() {
  dout(15) << dendl;

  AdminSocketHook *asok_hook = nullptr;
  {
    std::lock_guard locker{m_lock};
    std::swap(asok_hook, m_asok_hook);
  }
  delete asok_hook;
}

template <typename I>
void ImageReplayer<I>::reregister_admin_socket_hook() {
  std::unique_lock locker{m_lock};
  if (m_state == STATE_STARTING && m_bootstrap_request != nullptr) {
    m_local_image_name = m_bootstrap_request->get_local_image_name();
  }

  auto image_spec = image_replayer::util::compute_image_spec(
    m_local_io_ctx, m_local_image_name);
  if (m_asok_hook != nullptr && m_image_spec == image_spec) {
    return;
  }

  dout(15) << "old_image_spec=" << m_image_spec << ", "
           << "new_image_spec=" << image_spec << dendl;
  m_image_spec = image_spec;

  if (m_state == STATE_STOPPING || m_state == STATE_STOPPED) {
    // no need to re-register if stopping
    return;
  }
  locker.unlock();

  unregister_admin_socket_hook();
  register_admin_socket_hook();
}

template <typename I>
std::ostream &operator<<(std::ostream &os, const ImageReplayer<I> &replayer)
{
  os << "ImageReplayer: " << &replayer << " [" << replayer.get_local_pool_id()
     << "/" << replayer.get_global_image_id() << "]";
  return os;
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::ImageReplayer<librbd::ImageCtx>;
