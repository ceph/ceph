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
#include "common/WorkQueue.h"
#include "global/global_context.h"
#include "journal/Journaler.h"
#include "journal/ReplayHandler.h"
#include "journal/Settings.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Journal.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/journal/Replay.h"
#include "ImageDeleter.h"
#include "ImageReplayer.h"
#include "Threads.h"
#include "tools/rbd_mirror/image_replayer/BootstrapRequest.h"
#include "tools/rbd_mirror/image_replayer/CloseImageRequest.h"
#include "tools/rbd_mirror/image_replayer/EventPreprocessor.h"
#include "tools/rbd_mirror/image_replayer/PrepareLocalImageRequest.h"
#include "tools/rbd_mirror/image_replayer/PrepareRemoteImageRequest.h"
#include "tools/rbd_mirror/image_replayer/ReplayStatusFormatter.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::" << *this << " " \
                           << __func__ << ": "

using std::map;
using std::string;
using std::unique_ptr;
using std::shared_ptr;
using std::vector;

extern PerfCounters *g_perf_counters;

namespace rbd {
namespace mirror {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;
using namespace rbd::mirror::image_replayer;

template <typename I>
std::ostream &operator<<(std::ostream &os,
                         const typename ImageReplayer<I>::State &state);

namespace {

template <typename I>
struct ReplayHandler : public ::journal::ReplayHandler {
  ImageReplayer<I> *replayer;
  ReplayHandler(ImageReplayer<I> *replayer) : replayer(replayer) {}
  void get() override {}
  void put() override {}

  void handle_entries_available() override {
    replayer->handle_replay_ready();
  }
  void handle_complete(int r) override {
    std::stringstream ss;
    if (r < 0) {
      ss << "replay completed with error: " << cpp_strerror(r);
    }
    replayer->handle_replay_complete(r, ss.str());
  }
};

template <typename I>
class ImageReplayerAdminSocketCommand {
public:
  ImageReplayerAdminSocketCommand(const std::string &desc,
                                  ImageReplayer<I> *replayer)
    : desc(desc), replayer(replayer) {
  }
  virtual ~ImageReplayerAdminSocketCommand() {}
  virtual bool call(Formatter *f, stringstream *ss) = 0;

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

  bool call(Formatter *f, stringstream *ss) override {
    this->replayer->print_status(f, ss);
    return true;
  }
};

template <typename I>
class StartCommand : public ImageReplayerAdminSocketCommand<I> {
public:
  explicit StartCommand(const std::string &desc, ImageReplayer<I> *replayer)
    : ImageReplayerAdminSocketCommand<I>(desc, replayer) {
  }

  bool call(Formatter *f, stringstream *ss) override {
    this->replayer->start(nullptr, true);
    return true;
  }
};

template <typename I>
class StopCommand : public ImageReplayerAdminSocketCommand<I> {
public:
  explicit StopCommand(const std::string &desc, ImageReplayer<I> *replayer)
    : ImageReplayerAdminSocketCommand<I>(desc, replayer) {
  }

  bool call(Formatter *f, stringstream *ss) override {
    this->replayer->stop(nullptr, true);
    return true;
  }
};

template <typename I>
class RestartCommand : public ImageReplayerAdminSocketCommand<I> {
public:
  explicit RestartCommand(const std::string &desc, ImageReplayer<I> *replayer)
    : ImageReplayerAdminSocketCommand<I>(desc, replayer) {
  }

  bool call(Formatter *f, stringstream *ss) override {
    this->replayer->restart();
    return true;
  }
};

template <typename I>
class FlushCommand : public ImageReplayerAdminSocketCommand<I> {
public:
  explicit FlushCommand(const std::string &desc, ImageReplayer<I> *replayer)
    : ImageReplayerAdminSocketCommand<I>(desc, replayer) {
  }

  bool call(Formatter *f, stringstream *ss) override {
    this->replayer->flush();
    return true;
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
      int r = admin_socket->register_command(it.first, it.first, this,
                                             it.second->desc);
      if (r < 0) {
        return r;
      }
      it.second->registered = true;
    }
    return 0;
  }

  ~ImageReplayerAdminSocketHook() override {
    for (auto &it : commands) {
      if (it.second->registered) {
        admin_socket->unregister_command(it.first);
      }
      delete it.second;
    }
    commands.clear();
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
  typedef std::map<std::string, ImageReplayerAdminSocketCommand<I>*,
		   std::less<>> Commands;

  AdminSocket *admin_socket;
  Commands commands;
};

uint32_t calculate_replay_delay(const utime_t &event_time,
                                int mirroring_replay_delay) {
    if (mirroring_replay_delay <= 0) {
      return 0;
    }

    utime_t now = ceph_clock_now();
    if (event_time + mirroring_replay_delay <= now) {
      return 0;
    }

    // ensure it is rounded up when converting to integer
    return (event_time + mirroring_replay_delay - now) + 1;
}

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
void ImageReplayer<I>::RemoteJournalerListener::handle_update(
  ::journal::JournalMetadata *) {
  FunctionContext *ctx = new FunctionContext([this](int r) {
      replayer->handle_remote_journal_metadata_updated();
    });
  replayer->m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
ImageReplayer<I>::ImageReplayer(Threads<I> *threads,
                                InstanceWatcher<I> *instance_watcher,
                                RadosRef local,
                                const std::string &local_mirror_uuid,
                                int64_t local_pool_id,
                                const std::string &global_image_id) :
  m_threads(threads),
  m_instance_watcher(instance_watcher),
  m_local(local),
  m_local_mirror_uuid(local_mirror_uuid),
  m_local_pool_id(local_pool_id),
  m_global_image_id(global_image_id), m_local_image_name(global_image_id),
  m_lock("rbd::mirror::ImageReplayer " + stringify(local_pool_id) + " " +
	 global_image_id),
  m_progress_cxt(this),
  m_journal_listener(new JournalListener(this)),
  m_remote_listener(this)
{
  // Register asok commands using a temporary "remote_pool_name/global_image_id"
  // name.  When the image name becomes known on start the asok commands will be
  // re-registered using "remote_pool_name/remote_image_name" name.

  std::string pool_name;
  int r = m_local->pool_reverse_lookup(m_local_pool_id, &pool_name);
  if (r < 0) {
    derr << "error resolving local pool " << m_local_pool_id
	 << ": " << cpp_strerror(r) << dendl;
    pool_name = stringify(m_local_pool_id);
  }

  m_name = pool_name + "/" + m_global_image_id;
  register_admin_socket_hook();
}

template <typename I>
ImageReplayer<I>::~ImageReplayer()
{
  unregister_admin_socket_hook();
  ceph_assert(m_event_preprocessor == nullptr);
  ceph_assert(m_replay_status_formatter == nullptr);
  ceph_assert(m_local_image_ctx == nullptr);
  ceph_assert(m_local_replay == nullptr);
  ceph_assert(m_remote_journaler == nullptr);
  ceph_assert(m_replay_handler == nullptr);
  ceph_assert(m_on_start_finish == nullptr);
  ceph_assert(m_on_stop_finish == nullptr);
  ceph_assert(m_bootstrap_request == nullptr);
  ceph_assert(m_in_flight_status_updates == 0);

  delete m_journal_listener;
}

template <typename I>
image_replayer::HealthState ImageReplayer<I>::get_health_state() const {
  Mutex::Locker locker(m_lock);

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
void ImageReplayer<I>::add_peer(const std::string &peer_uuid,
                                librados::IoCtx &io_ctx) {
  Mutex::Locker locker(m_lock);
  auto it = m_peers.find({peer_uuid});
  if (it == m_peers.end()) {
    m_peers.insert({peer_uuid, io_ctx});
  }
}

template <typename I>
void ImageReplayer<I>::set_state_description(int r, const std::string &desc) {
  dout(10) << r << " " << desc << dendl;

  Mutex::Locker l(m_lock);
  m_last_r = r;
  m_state_desc = desc;
}

template <typename I>
void ImageReplayer<I>::start(Context *on_finish, bool manual)
{
  dout(10) << "on_finish=" << on_finish << dendl;

  int r = 0;
  {
    Mutex::Locker locker(m_lock);
    if (!is_stopped_()) {
      derr << "already running" << dendl;
      r = -EINVAL;
    } else if (m_manual_stop && !manual) {
      dout(5) << "stopped manually, ignoring start without manual flag"
	      << dendl;
      r = -EPERM;
    } else {
      m_state = STATE_STARTING;
      m_last_r = 0;
      m_state_desc.clear();
      m_manual_stop = false;
      m_delete_requested = false;

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

  m_local_ioctx.reset(new librados::IoCtx{});
  r = m_local->ioctx_create2(m_local_pool_id, *m_local_ioctx);
  if (r < 0) {
    m_local_ioctx.reset();

    derr << "error opening ioctx for local pool " << m_local_pool_id
         << ": " << cpp_strerror(r) << dendl;
    on_start_fail(r, "error opening local pool");
    return;
  }

  prepare_local_image();
}

template <typename I>
void ImageReplayer<I>::prepare_local_image() {
  dout(10) << dendl;

  m_local_image_id = "";
  Context *ctx = create_context_callback<
    ImageReplayer, &ImageReplayer<I>::handle_prepare_local_image>(this);
  auto req = PrepareLocalImageRequest<I>::create(
    *m_local_ioctx, m_global_image_id, &m_local_image_id, &m_local_image_name,
    &m_local_image_tag_owner, m_threads->work_queue, ctx);
  req->send();
}

template <typename I>
void ImageReplayer<I>::handle_prepare_local_image(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == -ENOENT) {
    dout(10) << "local image does not exist" << dendl;
  } else if (r < 0) {
    on_start_fail(r, "error preparing local image for replay");
    return;
  } else {
    reregister_admin_socket_hook();
  }

  // local image doesn't exist or is non-primary
  prepare_remote_image();
}

template <typename I>
void ImageReplayer<I>::prepare_remote_image() {
  dout(10) << dendl;
  if (m_peers.empty()) {
    // technically nothing to bootstrap, but it handles the status update
    bootstrap();
    return;
  }

  // TODO need to support multiple remote images
  ceph_assert(!m_peers.empty());
  m_remote_image = {*m_peers.begin()};

  auto cct = static_cast<CephContext *>(m_local->cct());
  journal::Settings journal_settings;
  journal_settings.commit_interval = cct->_conf.get_val<double>(
    "rbd_mirror_journal_commit_age");
  journal_settings.max_fetch_bytes = cct->_conf.get_val<Option::size_t>(
    "rbd_mirror_journal_max_fetch_bytes");

  Context *ctx = create_context_callback<
    ImageReplayer, &ImageReplayer<I>::handle_prepare_remote_image>(this);
  auto req = PrepareRemoteImageRequest<I>::create(
    m_threads, m_remote_image.io_ctx, m_global_image_id, m_local_mirror_uuid,
    m_local_image_id, journal_settings, &m_remote_image.mirror_uuid,
    &m_remote_image.image_id, &m_remote_journaler, &m_client_state,
    &m_client_meta, ctx);
  req->send();
}

template <typename I>
void ImageReplayer<I>::handle_prepare_remote_image(int r) {
  dout(10) << "r=" << r << dendl;

  ceph_assert(r < 0 ? m_remote_journaler == nullptr : m_remote_journaler != nullptr);
  if (r < 0 && !m_local_image_id.empty() &&
      m_local_image_tag_owner == librbd::Journal<>::LOCAL_MIRROR_UUID) {
    // local image is primary -- fall-through
  } else if (r == -ENOENT) {
    dout(10) << "remote image does not exist" << dendl;

    // TODO need to support multiple remote images
    if (m_remote_image.image_id.empty() && !m_local_image_id.empty() &&
        m_local_image_tag_owner == m_remote_image.mirror_uuid) {
      // local image exists and is non-primary and linked to the missing
      // remote image

      m_delete_requested = true;
      on_start_fail(0, "remote image no longer exists");
    } else {
      on_start_fail(-ENOENT, "remote image does not exist");
    }
    return;
  } else if (r < 0) {
    on_start_fail(r, "error retrieving remote image id");
    return;
  }

  bootstrap();
}

template <typename I>
void ImageReplayer<I>::bootstrap() {
  dout(10) << dendl;

  if (!m_local_image_id.empty() &&
      m_local_image_tag_owner == librbd::Journal<>::LOCAL_MIRROR_UUID) {
    dout(5) << "local image is primary" << dendl;
    on_start_fail(0, "local image is primary");
    return;
  } else if (m_peers.empty()) {
    dout(5) << "no peer clusters" << dendl;
    on_start_fail(-ENOENT, "no peer clusters");
    return;
  }

  BootstrapRequest<I> *request = nullptr;
  {
    Mutex::Locker locker(m_lock);
    if (on_start_interrupted(m_lock)) {
      return;
    }

    auto ctx = create_context_callback<
      ImageReplayer, &ImageReplayer<I>::handle_bootstrap>(this);
    request = BootstrapRequest<I>::create(
      m_threads, *m_local_ioctx, m_remote_image.io_ctx, m_instance_watcher,
      &m_local_image_ctx, m_local_image_id, m_remote_image.image_id,
      m_global_image_id, m_local_mirror_uuid, m_remote_image.mirror_uuid,
      m_remote_journaler, &m_client_state, &m_client_meta, ctx,
      &m_resync_requested, &m_progress_cxt);
    request->get();
    m_bootstrap_request = request;
  }

  update_mirror_image_status(false, boost::none);
  reschedule_update_status_task(10);

  request->send();
}

template <typename I>
void ImageReplayer<I>::handle_bootstrap(int r) {
  dout(10) << "r=" << r << dendl;
  {
    Mutex::Locker locker(m_lock);
    m_bootstrap_request->put();
    m_bootstrap_request = nullptr;
    if (m_local_image_ctx) {
      m_local_image_id = m_local_image_ctx->id;
    }
  }

  if (on_start_interrupted()) {
    return;
  } else if (r == -EREMOTEIO) {
    m_local_image_tag_owner = "";
    dout(5) << "remote image is non-primary" << dendl;
    on_start_fail(-EREMOTEIO, "remote image is non-primary");
    return;
  } else if (r == -EEXIST) {
    m_local_image_tag_owner = "";
    on_start_fail(r, "split-brain detected");
    return;
  } else if (r < 0) {
    on_start_fail(r, "error bootstrapping replay");
    return;
  } else if (m_resync_requested) {
    on_start_fail(0, "resync requested");
    return;
  }

  ceph_assert(m_local_journal == nullptr);
  {
    RWLock::RLocker snap_locker(m_local_image_ctx->snap_lock);
    if (m_local_image_ctx->journal != nullptr) {
      m_local_journal = m_local_image_ctx->journal;
      m_local_journal->add_listener(m_journal_listener);
    }
  }

  if (m_local_journal == nullptr) {
    on_start_fail(-EINVAL, "error accessing local journal");
    return;
  }

  update_mirror_image_status(false, boost::none);
  init_remote_journaler();
}

template <typename I>
void ImageReplayer<I>::init_remote_journaler() {
  dout(10) << dendl;

  Context *ctx = create_context_callback<
    ImageReplayer, &ImageReplayer<I>::handle_init_remote_journaler>(this);
  m_remote_journaler->init(ctx);
}

template <typename I>
void ImageReplayer<I>::handle_init_remote_journaler(int r) {
  dout(10) << "r=" << r << dendl;

  if (on_start_interrupted()) {
    return;
  } else if (r < 0) {
    derr << "failed to initialize remote journal: " << cpp_strerror(r) << dendl;
    on_start_fail(r, "error initializing remote journal");
    return;
  }

  m_remote_journaler->add_listener(&m_remote_listener);

  cls::journal::Client client;
  r = m_remote_journaler->get_cached_client(m_local_mirror_uuid, &client);
  if (r < 0) {
    derr << "error retrieving remote journal client: " << cpp_strerror(r)
	 << dendl;
    on_start_fail(r, "error retrieving remote journal client");
    return;
  }

  dout(5) << "image_id=" << m_local_image_id << ", "
          << "client_meta.image_id=" << m_client_meta.image_id << ", "
          << "client.state=" << client.state << dendl;
  if (m_client_meta.image_id == m_local_image_id &&
      client.state != cls::journal::CLIENT_STATE_CONNECTED) {
    dout(5) << "client flagged disconnected, stopping image replay" << dendl;
    if (m_local_image_ctx->config.template get_val<bool>("rbd_mirroring_resync_after_disconnect")) {
      m_resync_requested = true;
      on_start_fail(-ENOTCONN, "disconnected: automatic resync");
    } else {
      on_start_fail(-ENOTCONN, "disconnected");
    }
    return;
  }

  start_replay();
}

template <typename I>
void ImageReplayer<I>::start_replay() {
  dout(10) << dendl;

  Context *start_ctx = create_context_callback<
    ImageReplayer, &ImageReplayer<I>::handle_start_replay>(this);
  m_local_journal->start_external_replay(&m_local_replay, start_ctx);
}

template <typename I>
void ImageReplayer<I>::handle_start_replay(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    ceph_assert(m_local_replay == nullptr);
    derr << "error starting external replay on local image "
	 <<  m_local_image_id << ": " << cpp_strerror(r) << dendl;
    on_start_fail(r, "error starting replay on local image");
    return;
  }

  m_replay_status_formatter =
    ReplayStatusFormatter<I>::create(m_remote_journaler, m_local_mirror_uuid);

  Context *on_finish(nullptr);
  {
    Mutex::Locker locker(m_lock);
    ceph_assert(m_state == STATE_STARTING);
    m_state = STATE_REPLAYING;
    std::swap(m_on_start_finish, on_finish);
  }

  m_event_preprocessor = EventPreprocessor<I>::create(
    *m_local_image_ctx, *m_remote_journaler, m_local_mirror_uuid,
    &m_client_meta, m_threads->work_queue);

  update_mirror_image_status(true, boost::none);
  reschedule_update_status_task(30);

  if (on_replay_interrupted()) {
    return;
  }

  {
    CephContext *cct = static_cast<CephContext *>(m_local->cct());
    double poll_seconds = cct->_conf.get_val<double>(
      "rbd_mirror_journal_poll_age");

    Mutex::Locker locker(m_lock);
    m_replay_handler = new ReplayHandler<I>(this);
    m_remote_journaler->start_live_replay(m_replay_handler, poll_seconds);

    dout(10) << "m_remote_journaler=" << *m_remote_journaler << dendl;
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
  dout(10) << "r=" << r << dendl;
  Context *ctx = new FunctionContext([this, r, desc](int _r) {
      {
        Mutex::Locker locker(m_lock);
        ceph_assert(m_state == STATE_STARTING);
        m_state = STATE_STOPPING;
        if (r < 0 && r != -ECANCELED && r != -EREMOTEIO && r != -ENOENT) {
          derr << "start failed: " << cpp_strerror(r) << dendl;
        } else {
          dout(10) << "start canceled" << dendl;
        }
      }

      set_state_description(r, desc);
      if (m_local_ioctx) {
        update_mirror_image_status(false, boost::none);
      }
      reschedule_update_status_task(-1);
      shut_down(r);
    });
  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
bool ImageReplayer<I>::on_start_interrupted() {
  Mutex::Locker locker(m_lock);
  return on_start_interrupted(m_lock);
}

template <typename I>
bool ImageReplayer<I>::on_start_interrupted(Mutex& lock) {
  ceph_assert(m_lock.is_locked());
  ceph_assert(m_state == STATE_STARTING);
  if (!m_stop_requested) {
    return false;
  }

  on_start_fail(-ECANCELED, "");
  return true;
}

template <typename I>
void ImageReplayer<I>::stop(Context *on_finish, bool manual, int r,
			    const std::string& desc)
{
  dout(10) << "on_finish=" << on_finish << ", manual=" << manual
	   << ", desc=" << desc << dendl;

  image_replayer::BootstrapRequest<I> *bootstrap_request = nullptr;
  bool shut_down_replay = false;
  bool running = true;
  {
    Mutex::Locker locker(m_lock);

    if (!is_running_()) {
      running = false;
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
    on_stop_journal_replay(r, desc);
  } else if (on_finish != nullptr) {
    on_finish->complete(0);
  }
}

template <typename I>
void ImageReplayer<I>::on_stop_journal_replay(int r, const std::string &desc)
{
  dout(10) << dendl;

  {
    Mutex::Locker locker(m_lock);
    if (m_state != STATE_REPLAYING) {
      // might be invoked multiple times while stopping
      return;
    }
    m_stop_requested = true;
    m_state = STATE_STOPPING;
  }

  set_state_description(r, desc);
  update_mirror_image_status(true, boost::none);
  reschedule_update_status_task(-1);
  shut_down(0);
}

template <typename I>
void ImageReplayer<I>::handle_replay_ready()
{
  dout(20) << dendl;
  if (on_replay_interrupted()) {
    return;
  }

  if (!m_remote_journaler->try_pop_front(&m_replay_entry, &m_replay_tag_tid)) {
    return;
  }

  m_event_replay_tracker.start_op();

  m_lock.Lock();
  bool stopping = (m_state == STATE_STOPPING);
  m_lock.Unlock();

  if (stopping) {
    dout(10) << "stopping event replay" << dendl;
    m_event_replay_tracker.finish_op();
    return;
  }

  if (m_replay_tag_valid && m_replay_tag.tid == m_replay_tag_tid) {
    preprocess_entry();
    return;
  }

  replay_flush();
}

template <typename I>
void ImageReplayer<I>::restart(Context *on_finish)
{
  FunctionContext *ctx = new FunctionContext(
    [this, on_finish](int r) {
      if (r < 0) {
	// Try start anyway.
      }
      start(on_finish, true);
    });
  stop(ctx);
}

template <typename I>
void ImageReplayer<I>::flush(Context *on_finish)
{
  dout(10) << dendl;

  {
    Mutex::Locker locker(m_lock);
    if (m_state == STATE_REPLAYING) {
      Context *ctx = new FunctionContext(
        [on_finish](int r) {
          if (on_finish != nullptr) {
            on_finish->complete(r);
          }
        });
      on_flush_local_replay_flush_start(ctx);
      return;
    }
  }

  if (on_finish) {
    on_finish->complete(0);
  }
}

template <typename I>
void ImageReplayer<I>::on_flush_local_replay_flush_start(Context *on_flush)
{
  dout(10) << dendl;
  FunctionContext *ctx = new FunctionContext(
    [this, on_flush](int r) {
      on_flush_local_replay_flush_finish(on_flush, r);
    });

  ceph_assert(m_lock.is_locked());
  ceph_assert(m_state == STATE_REPLAYING);
  m_local_replay->flush(ctx);
}

template <typename I>
void ImageReplayer<I>::on_flush_local_replay_flush_finish(Context *on_flush,
                                                          int r)
{
  dout(10) << "r=" << r << dendl;
  if (r < 0) {
    derr << "error flushing local replay: " << cpp_strerror(r) << dendl;
    on_flush->complete(r);
    return;
  }

  on_flush_flush_commit_position_start(on_flush);
}

template <typename I>
void ImageReplayer<I>::on_flush_flush_commit_position_start(Context *on_flush)
{
  FunctionContext *ctx = new FunctionContext(
    [this, on_flush](int r) {
      on_flush_flush_commit_position_finish(on_flush, r);
    });

  m_remote_journaler->flush_commit_position(ctx);
}

template <typename I>
void ImageReplayer<I>::on_flush_flush_commit_position_finish(Context *on_flush,
                                                             int r)
{
  if (r < 0) {
    derr << "error flushing remote journal commit position: "
	 << cpp_strerror(r) << dendl;
  }

  update_mirror_image_status(false, boost::none);

  dout(20) << "flush complete, r=" << r << dendl;
  on_flush->complete(r);
}

template <typename I>
bool ImageReplayer<I>::on_replay_interrupted()
{
  bool shut_down;
  {
    Mutex::Locker locker(m_lock);
    shut_down = m_stop_requested;
  }

  if (shut_down) {
    on_stop_journal_replay();
  }
  return shut_down;
}

template <typename I>
void ImageReplayer<I>::print_status(Formatter *f, stringstream *ss)
{
  dout(10) << dendl;

  Mutex::Locker l(m_lock);

  if (f) {
    f->open_object_section("image_replayer");
    f->dump_string("name", m_name);
    f->dump_string("state", to_string(m_state));
    f->close_section();
    f->flush(*ss);
  } else {
    *ss << m_name << ": state: " << to_string(m_state);
  }
}

template <typename I>
void ImageReplayer<I>::handle_replay_complete(int r, const std::string &error_desc)
{
  dout(10) << "r=" << r << dendl;
  if (r < 0) {
    derr << "replay encountered an error: " << cpp_strerror(r) << dendl;
    set_state_description(r, error_desc);
  }

  {
    Mutex::Locker locker(m_lock);
    m_stop_requested = true;
  }
  on_replay_interrupted();
}

template <typename I>
void ImageReplayer<I>::replay_flush() {
  dout(10) << dendl;

  bool interrupted = false;
  {
    Mutex::Locker locker(m_lock);
    if (m_state != STATE_REPLAYING) {
      dout(10) << "replay interrupted" << dendl;
      interrupted = true;
    } else {
      m_state = STATE_REPLAY_FLUSHING;
    }
  }

  if (interrupted) {
    m_event_replay_tracker.finish_op();
    return;
  }

  // shut down the replay to flush all IO and ops and create a new
  // replayer to handle the new tag epoch
  Context *ctx = create_context_callback<
    ImageReplayer<I>, &ImageReplayer<I>::handle_replay_flush>(this);
  ctx = new FunctionContext([this, ctx](int r) {
      m_local_image_ctx->journal->stop_external_replay();
      m_local_replay = nullptr;

      if (r < 0) {
        ctx->complete(r);
        return;
      }

      m_local_journal->start_external_replay(&m_local_replay, ctx);
    });
  m_local_replay->shut_down(false, ctx);
}

template <typename I>
void ImageReplayer<I>::handle_replay_flush(int r) {
  dout(10) << "r=" << r << dendl;

  {
    Mutex::Locker locker(m_lock);
    ceph_assert(m_state == STATE_REPLAY_FLUSHING);
    m_state = STATE_REPLAYING;
  }

  if (r < 0) {
    derr << "replay flush encountered an error: " << cpp_strerror(r) << dendl;
    m_event_replay_tracker.finish_op();
    handle_replay_complete(r, "replay flush encountered an error");
    return;
  } else if (on_replay_interrupted()) {
    m_event_replay_tracker.finish_op();
    return;
  }

  get_remote_tag();
}

template <typename I>
void ImageReplayer<I>::get_remote_tag() {
  dout(15) << "tag_tid: " << m_replay_tag_tid << dendl;

  Context *ctx = create_context_callback<
    ImageReplayer, &ImageReplayer<I>::handle_get_remote_tag>(this);
  m_remote_journaler->get_tag(m_replay_tag_tid, &m_replay_tag, ctx);
}

template <typename I>
void ImageReplayer<I>::handle_get_remote_tag(int r) {
  dout(15) << "r=" << r << dendl;

  if (r == 0) {
    try {
      auto it = m_replay_tag.data.cbegin();
      decode(m_replay_tag_data, it);
    } catch (const buffer::error &err) {
      r = -EBADMSG;
    }
  }

  if (r < 0) {
    derr << "failed to retrieve remote tag " << m_replay_tag_tid << ": "
         << cpp_strerror(r) << dendl;
    m_event_replay_tracker.finish_op();
    handle_replay_complete(r, "failed to retrieve remote tag");
    return;
  }

  m_replay_tag_valid = true;
  dout(15) << "decoded remote tag " << m_replay_tag_tid << ": "
           << m_replay_tag_data << dendl;

  allocate_local_tag();
}

template <typename I>
void ImageReplayer<I>::allocate_local_tag() {
  dout(15) << dendl;

  std::string mirror_uuid = m_replay_tag_data.mirror_uuid;
  if (mirror_uuid == librbd::Journal<>::LOCAL_MIRROR_UUID) {
    mirror_uuid = m_remote_image.mirror_uuid;
  } else if (mirror_uuid == m_local_mirror_uuid) {
    mirror_uuid = librbd::Journal<>::LOCAL_MIRROR_UUID;
  } else if (mirror_uuid == librbd::Journal<>::ORPHAN_MIRROR_UUID) {
    // handle possible edge condition where daemon can failover and
    // the local image has already been promoted/demoted
    auto local_tag_data = m_local_journal->get_tag_data();
    if (local_tag_data.mirror_uuid == librbd::Journal<>::ORPHAN_MIRROR_UUID &&
        (local_tag_data.predecessor.commit_valid &&
         local_tag_data.predecessor.mirror_uuid ==
           librbd::Journal<>::LOCAL_MIRROR_UUID)) {
      dout(15) << "skipping stale demotion event" << dendl;
      handle_process_entry_safe(m_replay_entry, m_replay_start_time, 0);
      handle_replay_ready();
      return;
    } else {
      dout(5) << "encountered image demotion: stopping" << dendl;
      Mutex::Locker locker(m_lock);
      m_stop_requested = true;
    }
  }

  librbd::journal::TagPredecessor predecessor(m_replay_tag_data.predecessor);
  if (predecessor.mirror_uuid == librbd::Journal<>::LOCAL_MIRROR_UUID) {
    predecessor.mirror_uuid = m_remote_image.mirror_uuid;
  } else if (predecessor.mirror_uuid == m_local_mirror_uuid) {
    predecessor.mirror_uuid = librbd::Journal<>::LOCAL_MIRROR_UUID;
  }

  dout(15) << "mirror_uuid=" << mirror_uuid << ", "
           << "predecessor=" << predecessor << ", "
           << "replay_tag_tid=" << m_replay_tag_tid << dendl;
  Context *ctx = create_context_callback<
    ImageReplayer, &ImageReplayer<I>::handle_allocate_local_tag>(this);
  m_local_journal->allocate_tag(mirror_uuid, predecessor, ctx);
}

template <typename I>
void ImageReplayer<I>::handle_allocate_local_tag(int r) {
  dout(15) << "r=" << r << ", "
           << "tag_tid=" << m_local_journal->get_tag_tid() << dendl;

  if (r < 0) {
    derr << "failed to allocate journal tag: " << cpp_strerror(r) << dendl;
    m_event_replay_tracker.finish_op();
    handle_replay_complete(r, "failed to allocate journal tag");
    return;
  }

  preprocess_entry();
}

template <typename I>
void ImageReplayer<I>::preprocess_entry() {
  dout(20) << "preprocessing entry tid=" << m_replay_entry.get_commit_tid()
           << dendl;

  bufferlist data = m_replay_entry.get_data();
  auto it = data.cbegin();
  int r = m_local_replay->decode(&it, &m_event_entry);
  if (r < 0) {
    derr << "failed to decode journal event" << dendl;
    m_event_replay_tracker.finish_op();
    handle_replay_complete(r, "failed to decode journal event");
    return;
  }

  uint32_t delay = calculate_replay_delay(
    m_event_entry.timestamp, m_local_image_ctx->mirroring_replay_delay);
  if (delay == 0) {
    handle_preprocess_entry_ready(0);
    return;
  }

  dout(20) << "delaying replay by " << delay << " sec" << dendl;

  Mutex::Locker timer_locker(m_threads->timer_lock);
  ceph_assert(m_delayed_preprocess_task == nullptr);
  m_delayed_preprocess_task = new FunctionContext(
    [this](int r) {
      ceph_assert(m_threads->timer_lock.is_locked());
      m_delayed_preprocess_task = nullptr;
      m_threads->work_queue->queue(
        create_context_callback<ImageReplayer,
        &ImageReplayer<I>::handle_preprocess_entry_ready>(this), 0);
    });
  m_threads->timer->add_event_after(delay, m_delayed_preprocess_task);
}

template <typename I>
void ImageReplayer<I>::handle_preprocess_entry_ready(int r) {
  dout(20) << "r=" << r << dendl;
  ceph_assert(r == 0);

  m_replay_start_time = ceph_clock_now();
  if (!m_event_preprocessor->is_required(m_event_entry)) {
    process_entry();
    return;
  }

  Context *ctx = create_context_callback<
    ImageReplayer, &ImageReplayer<I>::handle_preprocess_entry_safe>(this);
  m_event_preprocessor->preprocess(&m_event_entry, ctx);
}

template <typename I>
void ImageReplayer<I>::handle_preprocess_entry_safe(int r) {
  dout(20) << "r=" << r << dendl;

  if (r < 0) {
    m_event_replay_tracker.finish_op();

    if (r == -ECANCELED) {
      handle_replay_complete(0, "lost exclusive lock");
    } else {
      derr << "failed to preprocess journal event" << dendl;
      handle_replay_complete(r, "failed to preprocess journal event");
    }
    return;
  }

  process_entry();
}

template <typename I>
void ImageReplayer<I>::process_entry() {
  dout(20) << "processing entry tid=" << m_replay_entry.get_commit_tid()
           << dendl;

  // stop replaying events if stop has been requested
  if (on_replay_interrupted()) {
    m_event_replay_tracker.finish_op();
    return;
  }

  Context *on_ready = create_context_callback<
    ImageReplayer, &ImageReplayer<I>::handle_process_entry_ready>(this);
  Context *on_commit = new C_ReplayCommitted(this, std::move(m_replay_entry),
                                             m_replay_start_time);

  m_local_replay->process(m_event_entry, on_ready, on_commit);
}

template <typename I>
void ImageReplayer<I>::handle_process_entry_ready(int r) {
  dout(20) << dendl;
  ceph_assert(r == 0);

  bool update_status = false;
  {
    RWLock::RLocker snap_locker(m_local_image_ctx->snap_lock);
    if (m_local_image_name != m_local_image_ctx->name) {
      m_local_image_name = m_local_image_ctx->name;
      update_status = true;
    }
  }

  if (update_status) {
    reschedule_update_status_task(0);
  }

  // attempt to process the next event
  handle_replay_ready();
}

template <typename I>
void ImageReplayer<I>::handle_process_entry_safe(const ReplayEntry &replay_entry,
                                                 const utime_t &replay_start_time,
                                                 int r) {
  dout(20) << "commit_tid=" << replay_entry.get_commit_tid() << ", r=" << r
	   << dendl;

  if (r < 0) {
    derr << "failed to commit journal event: " << cpp_strerror(r) << dendl;
    handle_replay_complete(r, "failed to commit journal event");
  } else {
    ceph_assert(m_remote_journaler != nullptr);
    m_remote_journaler->committed(replay_entry);
  }

  auto bytes = replay_entry.get_data().length();
  auto latency = ceph_clock_now() - replay_start_time;

  if (g_perf_counters) {
    g_perf_counters->inc(l_rbd_mirror_replay);
    g_perf_counters->inc(l_rbd_mirror_replay_bytes, bytes);
    g_perf_counters->tinc(l_rbd_mirror_replay_latency, latency);
  }

  auto ctx = new FunctionContext(
    [this, bytes, latency](int r) {
      Mutex::Locker locker(m_lock);
      if (m_perf_counters) {
        m_perf_counters->inc(l_rbd_mirror_replay);
        m_perf_counters->inc(l_rbd_mirror_replay_bytes, bytes);
        m_perf_counters->tinc(l_rbd_mirror_replay_latency, latency);
      }
      m_event_replay_tracker.finish_op();
    });
  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
bool ImageReplayer<I>::update_mirror_image_status(bool force,
                                                  const OptionalState &state) {
  dout(15) << dendl;
  {
    Mutex::Locker locker(m_lock);
    if (!start_mirror_image_status_update(force, false)) {
      return false;
    }
  }

  queue_mirror_image_status_update(state);
  return true;
}

template <typename I>
bool ImageReplayer<I>::start_mirror_image_status_update(bool force,
                                                        bool restarting) {
  ceph_assert(m_lock.is_locked());

  if (!force && !is_stopped_()) {
    if (!is_running_()) {
      dout(15) << "shut down in-progress: ignoring update" << dendl;
      return false;
    } else if (m_in_flight_status_updates > (restarting ? 1 : 0)) {
      dout(15) << "already sending update" << dendl;
      m_update_status_requested = true;
      return false;
    }
  }

  ++m_in_flight_status_updates;
  dout(15) << "in-flight updates=" << m_in_flight_status_updates << dendl;
  return true;
}

template <typename I>
void ImageReplayer<I>::finish_mirror_image_status_update() {
  reregister_admin_socket_hook();

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);
    ceph_assert(m_in_flight_status_updates > 0);
    if (--m_in_flight_status_updates > 0) {
      dout(15) << "waiting on " << m_in_flight_status_updates << " in-flight "
               << "updates" << dendl;
      return;
    }

    std::swap(on_finish, m_on_update_status_finish);
  }

  dout(15) << dendl;
  if (on_finish != nullptr) {
    on_finish->complete(0);
  }
}

template <typename I>
void ImageReplayer<I>::queue_mirror_image_status_update(const OptionalState &state) {
  dout(15) << dendl;
  FunctionContext *ctx = new FunctionContext(
    [this, state](int r) {
      send_mirror_status_update(state);
    });
  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void ImageReplayer<I>::send_mirror_status_update(const OptionalState &opt_state) {
  State state;
  std::string state_desc;
  int last_r;
  bool stopping_replay;

  OptionalMirrorImageStatusState mirror_image_status_state =
    boost::make_optional(false, cls::rbd::MIRROR_IMAGE_STATUS_STATE_UNKNOWN);
  image_replayer::BootstrapRequest<I>* bootstrap_request = nullptr;
  {
    Mutex::Locker locker(m_lock);
    state = m_state;
    state_desc = m_state_desc;
    mirror_image_status_state = m_mirror_image_status_state;
    last_r = m_last_r;
    stopping_replay = (m_local_image_ctx != nullptr);

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

  cls::rbd::MirrorImageStatus status;
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
  case STATE_REPLAY_FLUSHING:
    status.state = cls::rbd::MIRROR_IMAGE_STATUS_STATE_REPLAYING;
    {
      Context *on_req_finish = new FunctionContext(
        [this](int r) {
          dout(15) << "replay status ready: r=" << r << dendl;
          if (r >= 0) {
            send_mirror_status_update(boost::none);
          } else if (r == -EAGAIN) {
            // decrement in-flight status update counter
            handle_mirror_status_update(r);
          }
        });

      std::string desc;
      ceph_assert(m_replay_status_formatter != nullptr);
      if (!m_replay_status_formatter->get_or_send_update(&desc,
                                                         on_req_finish)) {
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
    } else if (last_r < 0) {
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
    Mutex::Locker locker(m_lock);
    m_mirror_image_status_state = mirror_image_status_state;
  }

  // prevent the status from ping-ponging when failed replays are restarted
  if (mirror_image_status_state &&
      *mirror_image_status_state == cls::rbd::MIRROR_IMAGE_STATUS_STATE_ERROR) {
    status.state = *mirror_image_status_state;
  }

  dout(15) << "status=" << status << dendl;
  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_image_status_set(&op, m_global_image_id, status);

  ceph_assert(m_local_ioctx);
  librados::AioCompletion *aio_comp = create_rados_callback<
    ImageReplayer<I>, &ImageReplayer<I>::handle_mirror_status_update>(this);
  int r = m_local_ioctx->aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void ImageReplayer<I>::handle_mirror_status_update(int r) {
  dout(15) << "r=" << r << dendl;

  bool running = false;
  bool started = false;
  {
    Mutex::Locker locker(m_lock);
    bool update_status_requested = false;
    std::swap(update_status_requested, m_update_status_requested);

    running = is_running_();
    if (running && update_status_requested) {
      started = start_mirror_image_status_update(false, true);
    }
  }

  // if a deferred update is available, send it -- otherwise reschedule
  // the timer task
  if (started) {
    queue_mirror_image_status_update(boost::none);
  } else if (running) {
    reschedule_update_status_task(0);
  }

  // mark committed status update as no longer in-flight
  finish_mirror_image_status_update();
}

template <typename I>
void ImageReplayer<I>::reschedule_update_status_task(int new_interval) {
  bool canceled_task = false;
  {
    Mutex::Locker locker(m_lock);
    Mutex::Locker timer_locker(m_threads->timer_lock);

    if (m_update_status_task) {
      dout(15) << "canceling existing status update task" << dendl;

      canceled_task = m_threads->timer->cancel_event(m_update_status_task);
      m_update_status_task = nullptr;
    }

    if (new_interval > 0) {
      m_update_status_interval = new_interval;
    }

    if (new_interval >= 0 && is_running_() &&
        start_mirror_image_status_update(true, false)) {
      m_update_status_task = new FunctionContext(
        [this](int r) {
          ceph_assert(m_threads->timer_lock.is_locked());
          m_update_status_task = nullptr;

          queue_mirror_image_status_update(boost::none);
        });
      dout(15) << "scheduling status update task after "
               << m_update_status_interval << " seconds" << dendl;
      m_threads->timer->add_event_after(m_update_status_interval,
                                        m_update_status_task);
    }
  }

  if (canceled_task) {
    // decrement in-flight status update counter for canceled task
    finish_mirror_image_status_update();
  }
}

template <typename I>
void ImageReplayer<I>::shut_down(int r) {
  dout(10) << "r=" << r << dendl;

  bool canceled_delayed_preprocess_task = false;
  {
    Mutex::Locker timer_locker(m_threads->timer_lock);
    if (m_delayed_preprocess_task != nullptr) {
      canceled_delayed_preprocess_task = m_threads->timer->cancel_event(
        m_delayed_preprocess_task);
      ceph_assert(canceled_delayed_preprocess_task);
      m_delayed_preprocess_task = nullptr;
    }
  }
  if (canceled_delayed_preprocess_task) {
    // wake up sleeping replay
    m_event_replay_tracker.finish_op();
  }

  reschedule_update_status_task(-1);

  {
    Mutex::Locker locker(m_lock);
    ceph_assert(m_state == STATE_STOPPING);

    // if status updates are in-flight, wait for them to complete
    // before proceeding
    if (m_in_flight_status_updates > 0) {
      if (m_on_update_status_finish == nullptr) {
        dout(15) << "waiting for in-flight status update" << dendl;
        m_on_update_status_finish = new FunctionContext(
          [this, r](int _r) {
            shut_down(r);
          });
      }
      return;
    }
  }

  // NOTE: it's important to ensure that the local image is fully
  // closed before attempting to close the remote journal in
  // case the remote cluster is unreachable

  // chain the shut down sequence (reverse order)
  Context *ctx = new FunctionContext(
    [this, r](int _r) {
      if (m_local_ioctx) {
        update_mirror_image_status(true, STATE_STOPPED);
      }
      handle_shut_down(r);
    });

  // close the remote journal
  if (m_remote_journaler != nullptr) {
    ctx = new FunctionContext([this, ctx](int r) {
        delete m_remote_journaler;
        m_remote_journaler = nullptr;
        ctx->complete(0);
      });
    ctx = new FunctionContext([this, ctx](int r) {
	m_remote_journaler->remove_listener(&m_remote_listener);
        m_remote_journaler->shut_down(ctx);
      });
  }

  // stop the replay of remote journal events
  if (m_replay_handler != nullptr) {
    ctx = new FunctionContext([this, ctx](int r) {
        delete m_replay_handler;
        m_replay_handler = nullptr;

        m_event_replay_tracker.wait_for_ops(ctx);
      });
    ctx = new FunctionContext([this, ctx](int r) {
        m_remote_journaler->stop_replay(ctx);
      });
  }

  // close the local image (release exclusive lock)
  if (m_local_image_ctx) {
    ctx = new FunctionContext([this, ctx](int r) {
      CloseImageRequest<I> *request = CloseImageRequest<I>::create(
        &m_local_image_ctx, ctx);
      request->send();
    });
  }

  // shut down event replay into the local image
  if (m_local_journal != nullptr) {
    ctx = new FunctionContext([this, ctx](int r) {
        m_local_journal = nullptr;
        ctx->complete(0);
      });
    if (m_local_replay != nullptr) {
      ctx = new FunctionContext([this, ctx](int r) {
          m_local_journal->stop_external_replay();
          m_local_replay = nullptr;

          EventPreprocessor<I>::destroy(m_event_preprocessor);
          m_event_preprocessor = nullptr;
          ctx->complete(0);
        });
    }
    ctx = new FunctionContext([this, ctx](int r) {
        // blocks if listener notification is in-progress
        m_local_journal->remove_listener(m_journal_listener);
        ctx->complete(0);
      });
  }

  // wait for all local in-flight replay events to complete
  ctx = new FunctionContext([this, ctx](int r) {
      if (r < 0) {
        derr << "error shutting down journal replay: " << cpp_strerror(r)
             << dendl;
      }

      m_event_replay_tracker.wait_for_ops(ctx);
    });

  // flush any local in-flight replay events
  if (m_local_replay != nullptr) {
    ctx = new FunctionContext([this, ctx](int r) {
        m_local_replay->shut_down(true, ctx);
      });
  }

  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void ImageReplayer<I>::handle_shut_down(int r) {
  reschedule_update_status_task(-1);

  bool resync_requested = false;
  bool delete_requested = false;
  bool unregister_asok_hook = false;
  {
    Mutex::Locker locker(m_lock);

    // if status updates are in-flight, wait for them to complete
    // before proceeding
    if (m_in_flight_status_updates > 0) {
      if (m_on_update_status_finish == nullptr) {
        dout(15) << "waiting for in-flight status update" << dendl;
        m_on_update_status_finish = new FunctionContext(
          [this, r](int _r) {
            handle_shut_down(r);
          });
      }
      return;
    }

    if (m_delete_requested && !m_local_image_id.empty()) {
      ceph_assert(m_remote_image.image_id.empty());
      dout(0) << "remote image no longer exists: scheduling deletion" << dendl;
      unregister_asok_hook = true;
      std::swap(delete_requested, m_delete_requested);
    }

    std::swap(resync_requested, m_resync_requested);
    if (delete_requested || resync_requested) {
      m_local_image_id = "";
    } else if (m_last_r == -ENOENT &&
               m_local_image_id.empty() && m_remote_image.image_id.empty()) {
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
    auto ctx = new FunctionContext([this, r](int) {
      handle_shut_down(r);
    });
    ImageDeleter<I>::trash_move(*m_local_ioctx, m_global_image_id,
                                resync_requested, m_threads->work_queue, ctx);
    return;
  }

  dout(10) << "stop complete" << dendl;
  ReplayStatusFormatter<I>::destroy(m_replay_status_formatter);
  m_replay_status_formatter = nullptr;

  Context *on_start = nullptr;
  Context *on_stop = nullptr;
  {
    Mutex::Locker locker(m_lock);
    std::swap(on_start, m_on_start_finish);
    std::swap(on_stop, m_on_stop_finish);
    m_stop_requested = false;
    ceph_assert(m_delayed_preprocess_task == nullptr);
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
void ImageReplayer<I>::handle_remote_journal_metadata_updated() {
  dout(20) << dendl;

  cls::journal::Client client;
  {
    Mutex::Locker locker(m_lock);
    if (!is_running_()) {
      return;
    }

    int r = m_remote_journaler->get_cached_client(m_local_mirror_uuid, &client);
    if (r < 0) {
      derr << "failed to retrieve client: " << cpp_strerror(r) << dendl;
      return;
    }
  }

  if (client.state != cls::journal::CLIENT_STATE_CONNECTED) {
    dout(0) << "client flagged disconnected, stopping image replay" << dendl;
    stop(nullptr, false, -ENOTCONN, "disconnected");
  }
}

template <typename I>
std::string ImageReplayer<I>::to_string(const State state) {
  switch (state) {
  case ImageReplayer<I>::STATE_STARTING:
    return "Starting";
  case ImageReplayer<I>::STATE_REPLAYING:
    return "Replaying";
  case ImageReplayer<I>::STATE_REPLAY_FLUSHING:
    return "ReplayFlushing";
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
void ImageReplayer<I>::resync_image(Context *on_finish) {
  dout(10) << dendl;

  m_resync_requested = true;
  stop(on_finish);
}

template <typename I>
void ImageReplayer<I>::register_admin_socket_hook() {
  ImageReplayerAdminSocketHook<I> *asok_hook;
  {
    Mutex::Locker locker(m_lock);
    if (m_asok_hook != nullptr) {
      return;
    }

    ceph_assert(m_perf_counters == nullptr);

    dout(15) << "registered asok hook: " << m_name << dendl;
    asok_hook = new ImageReplayerAdminSocketHook<I>(g_ceph_context, m_name,
                                                    this);
    int r = asok_hook->register_commands();
    if (r == 0) {
      m_asok_hook = asok_hook;

      CephContext *cct = static_cast<CephContext *>(m_local->cct());
      auto prio = cct->_conf.get_val<int64_t>("rbd_mirror_perf_stats_prio");
      PerfCountersBuilder plb(g_ceph_context, "rbd_mirror_" + m_name,
                              l_rbd_mirror_first, l_rbd_mirror_last);
      plb.add_u64_counter(l_rbd_mirror_replay, "replay", "Replays", "r", prio);
      plb.add_u64_counter(l_rbd_mirror_replay_bytes, "replay_bytes",
                          "Replayed data", "rb", prio, unit_t(UNIT_BYTES));
      plb.add_time_avg(l_rbd_mirror_replay_latency, "replay_latency",
                       "Replay latency", "rl", prio);
      m_perf_counters = plb.create_perf_counters();
      g_ceph_context->get_perfcounters_collection()->add(m_perf_counters);

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
  PerfCounters *perf_counters = nullptr;
  {
    Mutex::Locker locker(m_lock);
    std::swap(asok_hook, m_asok_hook);
    std::swap(perf_counters, m_perf_counters);
  }
  delete asok_hook;
  if (perf_counters != nullptr) {
    g_ceph_context->get_perfcounters_collection()->remove(perf_counters);
    delete perf_counters;
  }
}

template <typename I>
void ImageReplayer<I>::reregister_admin_socket_hook() {
  {
    Mutex::Locker locker(m_lock);
    auto name = m_local_ioctx->get_pool_name() + "/" + m_local_image_name;
    if (m_asok_hook != nullptr && m_name == name) {
      return;
    }
    m_name = name;
  }
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
