// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Replayer.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/journal/Replay.h"
#include "journal/Journaler.h"
#include "journal/JournalMetadataListener.h"
#include "journal/ReplayHandler.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/Types.h"
#include "tools/rbd_mirror/image_replayer/CloseImageRequest.h"
#include "tools/rbd_mirror/image_replayer/ReplayerListener.h"
#include "tools/rbd_mirror/image_replayer/Utils.h"
#include "tools/rbd_mirror/image_replayer/journal/EventPreprocessor.h"
#include "tools/rbd_mirror/image_replayer/journal/ReplayStatusFormatter.h"
#include "tools/rbd_mirror/image_replayer/journal/StateBuilder.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::journal::" \
                           << "Replayer: " << this << " " << __func__ << ": "

extern PerfCounters *g_perf_counters;

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace journal {

namespace {

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

using librbd::util::create_async_context_callback;
using librbd::util::create_context_callback;

template <typename I>
struct Replayer<I>::C_ReplayCommitted : public Context {
  Replayer* replayer;
  ReplayEntry replay_entry;
  uint64_t replay_bytes;
  utime_t replay_start_time;

  C_ReplayCommitted(Replayer* replayer, ReplayEntry &&replay_entry,
                    uint64_t replay_bytes, const utime_t &replay_start_time)
    : replayer(replayer), replay_entry(std::move(replay_entry)),
      replay_bytes(replay_bytes), replay_start_time(replay_start_time) {
  }

  void finish(int r) override {
    replayer->handle_process_entry_safe(replay_entry, replay_bytes,
                                        replay_start_time, r);
  }
};

template <typename I>
struct Replayer<I>::RemoteJournalerListener
  : public ::journal::JournalMetadataListener {
  Replayer* replayer;

  RemoteJournalerListener(Replayer* replayer) : replayer(replayer) {}

  void handle_update(::journal::JournalMetadata*) override {
    auto ctx = new C_TrackedOp(
      replayer->m_in_flight_op_tracker,
      new LambdaContext([this](int r) {
        replayer->handle_remote_journal_metadata_updated();
      }));
    replayer->m_threads->work_queue->queue(ctx, 0);
  }
};

template <typename I>
struct Replayer<I>::RemoteReplayHandler : public ::journal::ReplayHandler {
  Replayer* replayer;

  RemoteReplayHandler(Replayer* replayer) : replayer(replayer) {}
  ~RemoteReplayHandler() override {};

  void handle_entries_available() override {
    replayer->handle_replay_ready();
  }

  void handle_complete(int r) override {
    std::string error;
    if (r == -ENOMEM) {
      error = "not enough memory in autotune cache";
    } else if (r < 0) {
      error = "replay completed with error: " + cpp_strerror(r);
    }
    replayer->handle_replay_complete(r, error);
  }
};

template <typename I>
struct Replayer<I>::LocalJournalListener
  : public librbd::journal::Listener {
  Replayer* replayer;

  LocalJournalListener(Replayer* replayer) : replayer(replayer) {
  }

  void handle_close() override {
    replayer->handle_replay_complete(0, "");
  }

  void handle_promoted() override {
    replayer->handle_replay_complete(0, "force promoted");
  }

  void handle_resync() override {
    replayer->handle_resync_image();
  }
};

template <typename I>
Replayer<I>::Replayer(
    Threads<I>* threads,
    const std::string& local_mirror_uuid,
    StateBuilder<I>* state_builder,
    ReplayerListener* replayer_listener)
  : m_threads(threads),
    m_local_mirror_uuid(local_mirror_uuid),
    m_state_builder(state_builder),
    m_replayer_listener(replayer_listener),
    m_lock(ceph::make_mutex(librbd::util::unique_lock_name(
      "rbd::mirror::image_replayer::journal::Replayer", this))) {
  dout(10) << dendl;

  {
    std::unique_lock locker{m_lock};
    register_perf_counters();
  }
}

template <typename I>
Replayer<I>::~Replayer() {
  dout(10) << dendl;

  {
    std::unique_lock locker{m_lock};
    unregister_perf_counters();
  }

  ceph_assert(m_remote_listener == nullptr);
  ceph_assert(m_local_journal_listener == nullptr);
  ceph_assert(m_local_journal_replay == nullptr);
  ceph_assert(m_remote_replay_handler == nullptr);
  ceph_assert(m_event_preprocessor == nullptr);
  ceph_assert(m_replay_status_formatter == nullptr);
  ceph_assert(m_delayed_preprocess_task == nullptr);
  ceph_assert(m_flush_local_replay_task == nullptr);
  ceph_assert(m_state_builder->local_image_ctx == nullptr);
}

template <typename I>
void Replayer<I>::init(Context* on_finish) {
  dout(10) << dendl;

  {
    auto local_image_ctx = m_state_builder->local_image_ctx;
    std::shared_lock image_locker{local_image_ctx->image_lock};
    m_image_spec = util::compute_image_spec(local_image_ctx->md_ctx,
                                            local_image_ctx->name);
  }

  ceph_assert(m_on_init_shutdown == nullptr);
  m_on_init_shutdown = on_finish;

  init_remote_journaler();
}

template <typename I>
void Replayer<I>::shut_down(Context* on_finish) {
  dout(10) << dendl;

  std::unique_lock locker{m_lock};
  ceph_assert(m_on_init_shutdown == nullptr);
  m_on_init_shutdown = on_finish;

  if (m_state == STATE_INIT) {
    // raced with the last piece of the init state machine
    return;
  } else if (m_state == STATE_REPLAYING) {
    m_state = STATE_COMPLETE;
  }

  // if shutting down due to an error notification, we don't
  // need to propagate the same error again
  m_error_code = 0;
  m_error_description = "";

  cancel_delayed_preprocess_task();
  cancel_flush_local_replay_task();
  shut_down_local_journal_replay();
}

template <typename I>
void Replayer<I>::flush(Context* on_finish) {
  dout(10) << dendl;

  flush_local_replay(new C_TrackedOp(m_in_flight_op_tracker, on_finish));
}

template <typename I>
bool Replayer<I>::get_replay_status(std::string* description,
                                    Context* on_finish) {
  dout(10) << dendl;

  std::unique_lock locker{m_lock};
  if (m_replay_status_formatter == nullptr) {
    derr << "replay not running" << dendl;
    locker.unlock();

    on_finish->complete(-EAGAIN);
    return false;
  }

  on_finish = new C_TrackedOp(m_in_flight_op_tracker, on_finish);
  return m_replay_status_formatter->get_or_send_update(description,
                                                       on_finish);
}

template <typename I>
void Replayer<I>::init_remote_journaler() {
  dout(10) << dendl;

  Context *ctx = create_context_callback<
    Replayer, &Replayer<I>::handle_init_remote_journaler>(this);
  m_state_builder->remote_journaler->init(ctx);
}

template <typename I>
void Replayer<I>::handle_init_remote_journaler(int r) {
  dout(10) << "r=" << r << dendl;

  std::unique_lock locker{m_lock};
  if (r < 0) {
    derr << "failed to initialize remote journal: " << cpp_strerror(r) << dendl;
    handle_replay_complete(locker, r, "error initializing remote journal");
    close_local_image();
    return;
  }

  // listen for metadata updates to check for disconnect events
  ceph_assert(m_remote_listener == nullptr);
  m_remote_listener = new RemoteJournalerListener(this);
  m_state_builder->remote_journaler->add_listener(m_remote_listener);

  cls::journal::Client remote_client;
  r = m_state_builder->remote_journaler->get_cached_client(m_local_mirror_uuid,
                                                           &remote_client);
  if (r < 0) {
    derr << "error retrieving remote journal client: " << cpp_strerror(r)
         << dendl;
    handle_replay_complete(locker, r, "error retrieving remote journal client");
    close_local_image();
    return;
  }

  std::string error;
  r = validate_remote_client_state(remote_client,
                                   &m_state_builder->remote_client_meta,
                                   &m_resync_requested, &error);
  if (r < 0) {
    handle_replay_complete(locker, r, error);
    close_local_image();
    return;
  }

  start_external_replay(locker);
}

template <typename I>
void Replayer<I>::start_external_replay(std::unique_lock<ceph::mutex>& locker) {
  dout(10) << dendl;

  auto local_image_ctx = m_state_builder->local_image_ctx;
  std::shared_lock local_image_locker{local_image_ctx->image_lock};

  ceph_assert(m_local_journal == nullptr);
  m_local_journal = local_image_ctx->journal;
  if (m_local_journal == nullptr) {
    local_image_locker.unlock();

    derr << "local image journal closed" << dendl;
    handle_replay_complete(locker, -EINVAL, "error accessing local journal");
    close_local_image();
    return;
  }

  // safe to hold pointer to journal after external playback starts
  Context *start_ctx = create_context_callback<
    Replayer, &Replayer<I>::handle_start_external_replay>(this);
  m_local_journal->start_external_replay(&m_local_journal_replay, start_ctx);
}

template <typename I>
void Replayer<I>::handle_start_external_replay(int r) {
  dout(10) << "r=" << r << dendl;

  std::unique_lock locker{m_lock};
  if (r < 0) {
    ceph_assert(m_local_journal_replay == nullptr);
    derr << "error starting external replay on local image "
         << m_state_builder->local_image_ctx->id << ": "
         << cpp_strerror(r) << dendl;

    handle_replay_complete(locker, r, "error starting replay on local image");
    close_local_image();
    return;
  }

  if (!notify_init_complete(locker)) {
    return;
  }

  m_state = STATE_REPLAYING;

  // check for resync/promotion state after adding listener
  if (!add_local_journal_listener(locker)) {
    return;
  }

  // start remote journal replay
  m_event_preprocessor = EventPreprocessor<I>::create(
    *m_state_builder->local_image_ctx, *m_state_builder->remote_journaler,
    m_local_mirror_uuid, &m_state_builder->remote_client_meta,
    m_threads->work_queue);
  m_replay_status_formatter = ReplayStatusFormatter<I>::create(
    m_state_builder->remote_journaler, m_local_mirror_uuid);

  auto cct = static_cast<CephContext *>(m_state_builder->local_image_ctx->cct);
  double poll_seconds = cct->_conf.get_val<double>(
    "rbd_mirror_journal_poll_age");
  m_remote_replay_handler = new RemoteReplayHandler(this);
  m_state_builder->remote_journaler->start_live_replay(m_remote_replay_handler,
                                                       poll_seconds);

  notify_status_updated();
}

template <typename I>
bool Replayer<I>::add_local_journal_listener(
    std::unique_lock<ceph::mutex>& locker) {
  dout(10) << dendl;

  // listen for promotion and resync requests against local journal
  ceph_assert(m_local_journal_listener == nullptr);
  m_local_journal_listener = new LocalJournalListener(this);
  m_local_journal->add_listener(m_local_journal_listener);

  // verify that the local image wasn't force-promoted and that a resync hasn't
  // been requested now that we are listening for events
  if (m_local_journal->is_tag_owner()) {
    dout(10) << "local image force-promoted" << dendl;
    handle_replay_complete(locker, 0, "force promoted");
    return false;
  }

  bool resync_requested = false;
  int r = m_local_journal->is_resync_requested(&resync_requested);
  if (r < 0) {
    dout(10) << "failed to determine resync state: " << cpp_strerror(r)
             << dendl;
    handle_replay_complete(locker, r, "error parsing resync state");
    return false;
  } else if (resync_requested) {
    dout(10) << "local image resync requested" << dendl;
    handle_replay_complete(locker, 0, "resync requested");
    return false;
  }

  return true;
}

template <typename I>
bool Replayer<I>::notify_init_complete(std::unique_lock<ceph::mutex>& locker) {
  dout(10) << dendl;

  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  ceph_assert(m_state == STATE_INIT);

  // notify that init has completed
  Context *on_finish = nullptr;
  std::swap(m_on_init_shutdown, on_finish);

  locker.unlock();
  on_finish->complete(0);
  locker.lock();

  if (m_on_init_shutdown != nullptr) {
    // shut down requested after we notified init complete but before we
    // grabbed the lock
    close_local_image();
    return false;
  }

  return true;
}

template <typename I>
void Replayer<I>::shut_down_local_journal_replay() {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  if (m_local_journal_replay == nullptr) {
    wait_for_event_replay();
    return;
  }

  dout(10) << dendl;
  auto ctx = create_context_callback<
    Replayer<I>, &Replayer<I>::handle_shut_down_local_journal_replay>(this);
  m_local_journal_replay->shut_down(true, ctx);
}

template <typename I>
void Replayer<I>::handle_shut_down_local_journal_replay(int r) {
  dout(10) << "r=" << r << dendl;

  std::unique_lock locker{m_lock};
  if (r < 0) {
    derr << "error shutting down journal replay: " << cpp_strerror(r) << dendl;
    handle_replay_error(r, "failed to shut down local journal replay");
  }

  wait_for_event_replay();
}

template <typename I>
void Replayer<I>::wait_for_event_replay() {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  dout(10) << dendl;
  auto ctx = create_async_context_callback(
    m_threads->work_queue, create_context_callback<
      Replayer<I>, &Replayer<I>::handle_wait_for_event_replay>(this));
  m_event_replay_tracker.wait_for_ops(ctx);
}

template <typename I>
void Replayer<I>::handle_wait_for_event_replay(int r) {
  dout(10) << "r=" << r << dendl;

  std::unique_lock locker{m_lock};
  close_local_image();
}

template <typename I>
void Replayer<I>::close_local_image() {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  if (m_state_builder->local_image_ctx == nullptr) {
    stop_remote_journaler_replay();
    return;
  }

  dout(10) << dendl;
  if (m_local_journal_listener != nullptr) {
    // blocks if listener notification is in-progress
    m_local_journal->remove_listener(m_local_journal_listener);
    delete m_local_journal_listener;
    m_local_journal_listener = nullptr;
  }

  if (m_local_journal_replay != nullptr) {
    m_local_journal->stop_external_replay();
    m_local_journal_replay = nullptr;
  }

  if (m_event_preprocessor != nullptr) {
    image_replayer::journal::EventPreprocessor<I>::destroy(
      m_event_preprocessor);
    m_event_preprocessor = nullptr;
  }

  m_local_journal.reset();

  // NOTE: it's important to ensure that the local image is fully
  // closed before attempting to close the remote journal in
  // case the remote cluster is unreachable
  ceph_assert(m_state_builder->local_image_ctx != nullptr);
  auto ctx = create_context_callback<
    Replayer<I>, &Replayer<I>::handle_close_local_image>(this);
  auto request = image_replayer::CloseImageRequest<I>::create(
    &m_state_builder->local_image_ctx, ctx);
  request->send();
}


template <typename I>
void Replayer<I>::handle_close_local_image(int r) {
  dout(10) << "r=" << r << dendl;

  std::unique_lock locker{m_lock};
  if (r < 0) {
    derr << "error closing local iamge: " << cpp_strerror(r) << dendl;
    handle_replay_error(r, "failed to close local image");
  }

  ceph_assert(m_state_builder->local_image_ctx == nullptr);
  stop_remote_journaler_replay();
}

template <typename I>
void Replayer<I>::stop_remote_journaler_replay() {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  if (m_state_builder->remote_journaler == nullptr) {
    wait_for_in_flight_ops();
    return;
  } else if (m_remote_replay_handler == nullptr) {
    wait_for_in_flight_ops();
    return;
  }

  dout(10) << dendl;
  auto ctx = create_async_context_callback(
    m_threads->work_queue, create_context_callback<
      Replayer<I>, &Replayer<I>::handle_stop_remote_journaler_replay>(this));
  m_state_builder->remote_journaler->stop_replay(ctx);
}

template <typename I>
void Replayer<I>::handle_stop_remote_journaler_replay(int r) {
  dout(10) << "r=" << r << dendl;

  std::unique_lock locker{m_lock};
  if (r < 0) {
    derr << "failed to stop remote journaler replay : " << cpp_strerror(r)
         << dendl;
    handle_replay_error(r, "failed to stop remote journaler replay");
  }

  delete m_remote_replay_handler;
  m_remote_replay_handler = nullptr;

  wait_for_in_flight_ops();
}

template <typename I>
void Replayer<I>::wait_for_in_flight_ops() {
  dout(10) << dendl;
  if (m_remote_listener != nullptr) {
    m_state_builder->remote_journaler->remove_listener(m_remote_listener);
    delete m_remote_listener;
    m_remote_listener = nullptr;
  }

  auto ctx = create_async_context_callback(
    m_threads->work_queue, create_context_callback<
      Replayer<I>, &Replayer<I>::handle_wait_for_in_flight_ops>(this));
  m_in_flight_op_tracker.wait_for_ops(ctx);
}

template <typename I>
void Replayer<I>::handle_wait_for_in_flight_ops(int r) {
  dout(10) << "r=" << r << dendl;

  ReplayStatusFormatter<I>::destroy(m_replay_status_formatter);
  m_replay_status_formatter = nullptr;

  Context* on_init_shutdown = nullptr;
  {
    std::unique_lock locker{m_lock};
    ceph_assert(m_on_init_shutdown != nullptr);
    std::swap(m_on_init_shutdown, on_init_shutdown);
    m_state = STATE_COMPLETE;
  }
  on_init_shutdown->complete(m_error_code);
}

template <typename I>
void Replayer<I>::handle_remote_journal_metadata_updated() {
  dout(20) << dendl;

  std::unique_lock locker{m_lock};
  if (m_state != STATE_REPLAYING) {
    return;
  }

  cls::journal::Client remote_client;
  int r = m_state_builder->remote_journaler->get_cached_client(
    m_local_mirror_uuid, &remote_client);
  if (r < 0) {
    derr << "failed to retrieve client: " << cpp_strerror(r) << dendl;
    return;
  }

  librbd::journal::MirrorPeerClientMeta remote_client_meta;
  std::string error;
  r = validate_remote_client_state(remote_client, &remote_client_meta,
                                   &m_resync_requested, &error);
  if (r < 0) {
    dout(0) << "client flagged disconnected, stopping image replay" << dendl;
    handle_replay_complete(locker, r, error);
  }
}

template <typename I>
void Replayer<I>::schedule_flush_local_replay_task() {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  std::unique_lock timer_locker{m_threads->timer_lock};
  if (m_state != STATE_REPLAYING || m_flush_local_replay_task != nullptr) {
    return;
  }

  dout(15) << dendl;
  m_flush_local_replay_task = create_async_context_callback(
    m_threads->work_queue, create_context_callback<
      Replayer<I>, &Replayer<I>::handle_flush_local_replay_task>(this));
  m_threads->timer->add_event_after(30, m_flush_local_replay_task);
}

template <typename I>
void Replayer<I>::cancel_flush_local_replay_task() {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  std::unique_lock timer_locker{m_threads->timer_lock};
  if (m_flush_local_replay_task != nullptr) {
    dout(10) << dendl;
    m_threads->timer->cancel_event(m_flush_local_replay_task);
    m_flush_local_replay_task = nullptr;
  }
}

template <typename I>
void Replayer<I>::handle_flush_local_replay_task(int) {
  dout(15) << dendl;

  m_in_flight_op_tracker.start_op();
  auto on_finish = new LambdaContext([this](int) {
      std::unique_lock locker{m_lock};

      {
        std::unique_lock timer_locker{m_threads->timer_lock};
        m_flush_local_replay_task = nullptr;
      }

      notify_status_updated();
      m_in_flight_op_tracker.finish_op();
    });
  flush_local_replay(on_finish);
}

template <typename I>
void Replayer<I>::flush_local_replay(Context* on_flush) {
  std::unique_lock locker{m_lock};
  if (m_state != STATE_REPLAYING) {
    locker.unlock();
    on_flush->complete(0);
    return;
  } else if (m_local_journal_replay == nullptr) {
    // raced w/ a tag creation stop/start, which implies that
    // the replay is flushed
    locker.unlock();
    flush_commit_position(on_flush);
    return;
  }

  dout(15) << dendl;
  auto ctx = new LambdaContext(
    [this, on_flush](int r) {
      handle_flush_local_replay(on_flush, r);
    });
  m_local_journal_replay->flush(ctx);
}

template <typename I>
void Replayer<I>::handle_flush_local_replay(Context* on_flush, int r) {
  dout(15) << "r=" << r << dendl;
  if (r < 0) {
    derr << "error flushing local replay: " << cpp_strerror(r) << dendl;
    on_flush->complete(r);
    return;
  }

  flush_commit_position(on_flush);
}

template <typename I>
void Replayer<I>::flush_commit_position(Context* on_flush) {
  std::unique_lock locker{m_lock};
  if (m_state != STATE_REPLAYING) {
    locker.unlock();
    on_flush->complete(0);
    return;
  }

  dout(15) << dendl;
  auto ctx = new LambdaContext(
    [this, on_flush](int r) {
      handle_flush_commit_position(on_flush, r);
    });
  m_state_builder->remote_journaler->flush_commit_position(ctx);
}

template <typename I>
void Replayer<I>::handle_flush_commit_position(Context* on_flush, int r) {
  dout(15) << "r=" << r << dendl;
  if (r < 0) {
    derr << "error flushing remote journal commit position: "
         << cpp_strerror(r) << dendl;
  }

  on_flush->complete(r);
}

template <typename I>
void Replayer<I>::handle_replay_error(int r, const std::string &error) {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  if (m_error_code == 0) {
    m_error_code = r;
    m_error_description = error;
  }
}

template <typename I>
bool Replayer<I>::is_replay_complete() const {
  std::unique_lock locker{m_lock};
  return is_replay_complete(locker);
}

template <typename I>
bool Replayer<I>::is_replay_complete(
    const std::unique_lock<ceph::mutex>&) const {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  return (m_state == STATE_COMPLETE);
}

template <typename I>
void Replayer<I>::handle_replay_complete(int r, const std::string &error) {
  std::unique_lock locker{m_lock};
  handle_replay_complete(locker, r, error);
}

template <typename I>
void Replayer<I>::handle_replay_complete(
    const std::unique_lock<ceph::mutex>&, int r, const std::string &error) {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  dout(10) << "r=" << r << ", error=" << error << dendl;
  if (r < 0) {
    derr << "replay encountered an error: " << cpp_strerror(r) << dendl;
    handle_replay_error(r, error);
  }

  if (m_state != STATE_REPLAYING) {
    return;
  }

  m_state = STATE_COMPLETE;
  notify_status_updated();
}

template <typename I>
void Replayer<I>::handle_replay_ready() {
  std::unique_lock locker{m_lock};
  handle_replay_ready(locker);
}

template <typename I>
void Replayer<I>::handle_replay_ready(
    std::unique_lock<ceph::mutex>& locker) {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  dout(20) << dendl;
  if (is_replay_complete(locker)) {
    return;
  }

  if (!m_state_builder->remote_journaler->try_pop_front(&m_replay_entry,
                                                        &m_replay_tag_tid)) {
    dout(20) << "no entries ready for replay" << dendl;
    return;
  }

  // can safely drop lock once the entry is tracked
  m_event_replay_tracker.start_op();
  locker.unlock();

  dout(20) << "entry tid=" << m_replay_entry.get_commit_tid()
           << "tag_tid=" << m_replay_tag_tid << dendl;
  if (!m_replay_tag_valid || m_replay_tag.tid != m_replay_tag_tid) {
    // must allocate a new local journal tag prior to processing
    replay_flush();
    return;
  }

  preprocess_entry();
}

template <typename I>
void Replayer<I>::replay_flush() {
  dout(10) << dendl;

  // shut down the replay to flush all IO and ops and create a new
  // replayer to handle the new tag epoch
  auto ctx = create_context_callback<
    Replayer<I>, &Replayer<I>::handle_replay_flush_shut_down>(this);
  ceph_assert(m_local_journal_replay != nullptr);
  m_local_journal_replay->shut_down(false, ctx);
}

template <typename I>
void Replayer<I>::handle_replay_flush_shut_down(int r) {
  std::unique_lock locker{m_lock};
  dout(10) << "r=" << r << dendl;

  ceph_assert(m_local_journal != nullptr);
  ceph_assert(m_local_journal_listener != nullptr);

  // blocks if listener notification is in-progress
  m_local_journal->remove_listener(m_local_journal_listener);
  delete m_local_journal_listener;
  m_local_journal_listener = nullptr;

  m_local_journal->stop_external_replay();
  m_local_journal_replay = nullptr;
  m_local_journal.reset();

  if (r < 0) {
    locker.unlock();

    handle_replay_flush(r);
    return;
  }

  // journal might have been closed now that we stopped external replay
  auto local_image_ctx = m_state_builder->local_image_ctx;
  std::shared_lock local_image_locker{local_image_ctx->image_lock};
  m_local_journal = local_image_ctx->journal;
  if (m_local_journal == nullptr) {
    local_image_locker.unlock();
    locker.unlock();

    derr << "local image journal closed" << dendl;
    handle_replay_flush(-EINVAL);
    return;
  }

  auto ctx = create_context_callback<
    Replayer<I>, &Replayer<I>::handle_replay_flush>(this);
  m_local_journal->start_external_replay(&m_local_journal_replay, ctx);
}

template <typename I>
void Replayer<I>::handle_replay_flush(int r) {
  std::unique_lock locker{m_lock};
  dout(10) << "r=" << r << dendl;
  if (r < 0) {
    derr << "replay flush encountered an error: " << cpp_strerror(r) << dendl;
    handle_replay_complete(locker, r, "replay flush encountered an error");
    m_event_replay_tracker.finish_op();
    return;
  } else if (is_replay_complete(locker)) {
    m_event_replay_tracker.finish_op();
    return;
  }

  // check for resync/promotion state after adding listener
  if (!add_local_journal_listener(locker)) {
    m_event_replay_tracker.finish_op();
    return;
  }
  locker.unlock();

  get_remote_tag();
}

template <typename I>
void Replayer<I>::get_remote_tag() {
  dout(15) << "tag_tid: " << m_replay_tag_tid << dendl;

  Context *ctx = create_context_callback<
    Replayer, &Replayer<I>::handle_get_remote_tag>(this);
  m_state_builder->remote_journaler->get_tag(m_replay_tag_tid, &m_replay_tag,
                                             ctx);
}

template <typename I>
void Replayer<I>::handle_get_remote_tag(int r) {
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
    handle_replay_complete(r, "failed to retrieve remote tag");
    m_event_replay_tracker.finish_op();
    return;
  }

  m_replay_tag_valid = true;
  dout(15) << "decoded remote tag " << m_replay_tag_tid << ": "
           << m_replay_tag_data << dendl;

  allocate_local_tag();
}

template <typename I>
void Replayer<I>::allocate_local_tag() {
  dout(15) << dendl;

  std::string mirror_uuid = m_replay_tag_data.mirror_uuid;
  if (mirror_uuid == librbd::Journal<>::LOCAL_MIRROR_UUID) {
    mirror_uuid = m_state_builder->remote_mirror_uuid;
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
      handle_process_entry_safe(m_replay_entry, m_replay_bytes,
                                m_replay_start_time, 0);
      handle_replay_ready();
      return;
    } else {
      dout(5) << "encountered image demotion: stopping" << dendl;
      handle_replay_complete(0, "");
    }
  }

  librbd::journal::TagPredecessor predecessor(m_replay_tag_data.predecessor);
  if (predecessor.mirror_uuid == librbd::Journal<>::LOCAL_MIRROR_UUID) {
    predecessor.mirror_uuid = m_state_builder->remote_mirror_uuid;
  } else if (predecessor.mirror_uuid == m_local_mirror_uuid) {
    predecessor.mirror_uuid = librbd::Journal<>::LOCAL_MIRROR_UUID;
  }

  dout(15) << "mirror_uuid=" << mirror_uuid << ", "
           << "predecessor=" << predecessor << ", "
           << "replay_tag_tid=" << m_replay_tag_tid << dendl;
  Context *ctx = create_context_callback<
    Replayer, &Replayer<I>::handle_allocate_local_tag>(this);
  m_local_journal->allocate_tag(mirror_uuid, predecessor, ctx);
}

template <typename I>
void Replayer<I>::handle_allocate_local_tag(int r) {
  dout(15) << "r=" << r << ", "
           << "tag_tid=" << m_local_journal->get_tag_tid() << dendl;
  if (r < 0) {
    derr << "failed to allocate journal tag: " << cpp_strerror(r) << dendl;
    handle_replay_complete(r, "failed to allocate journal tag");
    m_event_replay_tracker.finish_op();
    return;
  }

  preprocess_entry();
}

template <typename I>
void Replayer<I>::preprocess_entry() {
  dout(20) << "preprocessing entry tid=" << m_replay_entry.get_commit_tid()
           << dendl;

  bufferlist data = m_replay_entry.get_data();
  auto it = data.cbegin();
  int r = m_local_journal_replay->decode(&it, &m_event_entry);
  if (r < 0) {
    derr << "failed to decode journal event" << dendl;
    handle_replay_complete(r, "failed to decode journal event");
    m_event_replay_tracker.finish_op();
    return;
  }

  m_replay_bytes = data.length();
  uint32_t delay = calculate_replay_delay(
    m_event_entry.timestamp,
    m_state_builder->local_image_ctx->mirroring_replay_delay);
  if (delay == 0) {
    handle_preprocess_entry_ready(0);
    return;
  }

  std::unique_lock locker{m_lock};
  if (is_replay_complete(locker)) {
    // don't schedule a delayed replay task if a shut-down is in-progress
    m_event_replay_tracker.finish_op();
    return;
  }

  dout(20) << "delaying replay by " << delay << " sec" << dendl;
  std::unique_lock timer_locker{m_threads->timer_lock};
  ceph_assert(m_delayed_preprocess_task == nullptr);
  m_delayed_preprocess_task = create_context_callback<
    Replayer<I>, &Replayer<I>::handle_delayed_preprocess_task>(this);
  m_threads->timer->add_event_after(delay, m_delayed_preprocess_task);
}

template <typename I>
void Replayer<I>::handle_delayed_preprocess_task(int r) {
  dout(20) << "r=" << r << dendl;

  ceph_assert(ceph_mutex_is_locked_by_me(m_threads->timer_lock));
  m_delayed_preprocess_task = nullptr;

  m_threads->work_queue->queue(create_context_callback<
    Replayer, &Replayer<I>::handle_preprocess_entry_ready>(this), 0);
}

template <typename I>
void Replayer<I>::handle_preprocess_entry_ready(int r) {
  dout(20) << "r=" << r << dendl;
  ceph_assert(r == 0);

  m_replay_start_time = ceph_clock_now();
  if (!m_event_preprocessor->is_required(m_event_entry)) {
    process_entry();
    return;
  }

  Context *ctx = create_context_callback<
    Replayer, &Replayer<I>::handle_preprocess_entry_safe>(this);
  m_event_preprocessor->preprocess(&m_event_entry, ctx);
}

template <typename I>
void Replayer<I>::handle_preprocess_entry_safe(int r) {
  dout(20) << "r=" << r << dendl;

  if (r < 0) {
    if (r == -ECANCELED) {
      handle_replay_complete(0, "lost exclusive lock");
    } else {
      derr << "failed to preprocess journal event" << dendl;
      handle_replay_complete(r, "failed to preprocess journal event");
    }

    m_event_replay_tracker.finish_op();
    return;
  }

  process_entry();
}

template <typename I>
void Replayer<I>::process_entry() {
  dout(20) << "processing entry tid=" << m_replay_entry.get_commit_tid()
           << dendl;

  Context *on_ready = create_context_callback<
    Replayer, &Replayer<I>::handle_process_entry_ready>(this);
  Context *on_commit = new C_ReplayCommitted(this, std::move(m_replay_entry),
                                             m_replay_bytes,
                                             m_replay_start_time);

  m_local_journal_replay->process(m_event_entry, on_ready, on_commit);
}

template <typename I>
void Replayer<I>::handle_process_entry_ready(int r) {
  std::unique_lock locker{m_lock};

  dout(20) << dendl;
  ceph_assert(r == 0);

  bool update_status = false;
  {
    auto local_image_ctx = m_state_builder->local_image_ctx;
    std::shared_lock image_locker{local_image_ctx->image_lock};
    auto image_spec = util::compute_image_spec(local_image_ctx->md_ctx,
                                               local_image_ctx->name);
    if (m_image_spec != image_spec) {
      m_image_spec = image_spec;
      update_status = true;
    }
  }

  m_replay_status_formatter->handle_entry_processed(m_replay_bytes);

  if (update_status) {
    unregister_perf_counters();
    register_perf_counters();
    notify_status_updated();
  }

  // attempt to process the next event
  handle_replay_ready(locker);
}

template <typename I>
void Replayer<I>::handle_process_entry_safe(
    const ReplayEntry &replay_entry, uint64_t replay_bytes,
    const utime_t &replay_start_time, int r) {
  dout(20) << "commit_tid=" << replay_entry.get_commit_tid() << ", r=" << r
           << dendl;

  if (r < 0) {
    derr << "failed to commit journal event: " << cpp_strerror(r) << dendl;
    handle_replay_complete(r, "failed to commit journal event");
  } else {
    ceph_assert(m_state_builder->remote_journaler != nullptr);
    m_state_builder->remote_journaler->committed(replay_entry);
  }

  auto latency = ceph_clock_now() - replay_start_time;
  if (g_perf_counters) {
    g_perf_counters->inc(l_rbd_mirror_replay);
    g_perf_counters->inc(l_rbd_mirror_replay_bytes, replay_bytes);
    g_perf_counters->tinc(l_rbd_mirror_replay_latency, latency);
  }

  auto ctx = new LambdaContext(
    [this, replay_bytes, latency](int r) {
      std::unique_lock locker{m_lock};
      schedule_flush_local_replay_task();

      if (m_perf_counters) {
        m_perf_counters->inc(l_rbd_mirror_replay);
        m_perf_counters->inc(l_rbd_mirror_replay_bytes, replay_bytes);
        m_perf_counters->tinc(l_rbd_mirror_replay_latency, latency);
      }

      m_event_replay_tracker.finish_op();
    });
  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void Replayer<I>::handle_resync_image() {
  dout(10) << dendl;

  std::unique_lock locker{m_lock};
  m_resync_requested = true;
  handle_replay_complete(locker, 0, "resync requested");
}

template <typename I>
void Replayer<I>::notify_status_updated() {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  dout(10) << dendl;

  auto ctx = new C_TrackedOp(m_in_flight_op_tracker, new LambdaContext(
    [this](int) {
      m_replayer_listener->handle_notification();
    }));
  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void Replayer<I>::cancel_delayed_preprocess_task() {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  bool canceled_delayed_preprocess_task = false;
  {
    std::unique_lock timer_locker{m_threads->timer_lock};
    if (m_delayed_preprocess_task != nullptr) {
      dout(10) << dendl;
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
}

template <typename I>
int Replayer<I>::validate_remote_client_state(
    const cls::journal::Client& remote_client,
    librbd::journal::MirrorPeerClientMeta* remote_client_meta,
    bool* resync_requested, std::string* error) {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  if (!util::decode_client_meta(remote_client, remote_client_meta)) {
    // require operator intervention since the data is corrupt
    *error = "error retrieving remote journal client";
    return -EBADMSG;
  }

  auto local_image_ctx = m_state_builder->local_image_ctx;
  dout(5) << "image_id=" << local_image_ctx->id << ", "
          << "remote_client_meta.image_id="
          << remote_client_meta->image_id << ", "
          << "remote_client.state=" << remote_client.state << dendl;
  if (remote_client_meta->image_id == local_image_ctx->id &&
      remote_client.state != cls::journal::CLIENT_STATE_CONNECTED) {
    dout(5) << "client flagged disconnected, stopping image replay" << dendl;
    if (local_image_ctx->config.template get_val<bool>(
          "rbd_mirroring_resync_after_disconnect")) {
      dout(10) << "disconnected: automatic resync" << dendl;
      *resync_requested = true;
      *error = "disconnected: automatic resync";
      return -ENOTCONN;
    } else {
      dout(10) << "disconnected" << dendl;
      *error = "disconnected";
      return -ENOTCONN;
    }
  }

  return 0;
}

template <typename I>
void Replayer<I>::register_perf_counters() {
  dout(5) << dendl;

  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  ceph_assert(m_perf_counters == nullptr);

  auto cct = static_cast<CephContext *>(m_state_builder->local_image_ctx->cct);
  auto prio = cct->_conf.get_val<int64_t>("rbd_mirror_image_perf_stats_prio");
  PerfCountersBuilder plb(g_ceph_context, "rbd_mirror_image_" + m_image_spec,
                          l_rbd_mirror_first, l_rbd_mirror_last);
  plb.add_u64_counter(l_rbd_mirror_replay, "replay", "Replays", "r", prio);
  plb.add_u64_counter(l_rbd_mirror_replay_bytes, "replay_bytes",
                      "Replayed data", "rb", prio, unit_t(UNIT_BYTES));
  plb.add_time_avg(l_rbd_mirror_replay_latency, "replay_latency",
                   "Replay latency", "rl", prio);
  m_perf_counters = plb.create_perf_counters();
  g_ceph_context->get_perfcounters_collection()->add(m_perf_counters);
}

template <typename I>
void Replayer<I>::unregister_perf_counters() {
  dout(5) << dendl;
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  PerfCounters *perf_counters = nullptr;
  std::swap(perf_counters, m_perf_counters);

  if (perf_counters != nullptr) {
    g_ceph_context->get_perfcounters_collection()->remove(perf_counters);
    delete perf_counters;
  }
}

} // namespace journal
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_replayer::journal::Replayer<librbd::ImageCtx>;
