// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_JOURNAL_REPLAYER_H
#define RBD_MIRROR_IMAGE_REPLAYER_JOURNAL_REPLAYER_H

#include "tools/rbd_mirror/image_replayer/Replayer.h"
#include "include/utime.h"
#include "common/AsyncOpTracker.h"
#include "common/ceph_mutex.h"
#include "common/RefCountedObj.h"
#include "cls/journal/cls_journal_types.h"
#include "journal/ReplayEntry.h"
#include "librbd/ImageCtx.h"
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"
#include <string>
#include <type_traits>

namespace journal { class Journaler; }
namespace librbd {

struct ImageCtx;
namespace journal { template <typename I> class Replay; }

} // namespace librbd

namespace rbd {
namespace mirror {

template <typename> struct Threads;

namespace image_replayer {

struct ReplayerListener;

namespace journal {

template <typename> class EventPreprocessor;
template <typename> class ReplayStatusFormatter;
template <typename> class StateBuilder;

template <typename ImageCtxT>
class Replayer : public image_replayer::Replayer {
public:
  typedef typename librbd::journal::TypeTraits<ImageCtxT>::Journaler Journaler;

  static Replayer* create(
      Threads<ImageCtxT>* threads,
      const std::string& local_mirror_uuid,
      StateBuilder<ImageCtxT>* state_builder,
      ReplayerListener* replayer_listener) {
    return new Replayer(threads, local_mirror_uuid, state_builder,
                        replayer_listener);
  }

  Replayer(
      Threads<ImageCtxT>* threads,
      const std::string& local_mirror_uuid,
      StateBuilder<ImageCtxT>* state_builder,
      ReplayerListener* replayer_listener);
  ~Replayer();

  void destroy() override {
    delete this;
  }

  void init(Context* on_finish) override;
  void shut_down(Context* on_finish) override;

  void flush(Context* on_finish) override;

  bool get_replay_status(std::string* description, Context* on_finish) override;

  bool is_replaying() const override {
    std::unique_lock locker{m_lock};
    return (m_state == STATE_REPLAYING);
  }

  bool is_resync_requested() const override {
    std::unique_lock locker(m_lock);
    return m_resync_requested;
  }

  int get_error_code() const override {
    std::unique_lock locker(m_lock);
    return m_error_code;
  }

  std::string get_error_description() const override {
    std::unique_lock locker(m_lock);
    return m_error_description;
  }

  std::string get_image_spec() const {
    std::unique_lock locker(m_lock);
    return m_image_spec;
  }

private:
  /**
   * @verbatim
   *
   *  <init>
   *    |
   *    v                     (error)
   * INIT_REMOTE_JOURNALER  * * * * * * * * * * * * * * * * * * *
   *    |                                                       *
   *    v                     (error)                           *
   * START_EXTERNAL_REPLAY  * * * * * * * * * * * * * * * * * * *
   *    |                                                       *
   *    |  /--------------------------------------------\       *
   *    |  |                                            |       *
   *    v  v   (asok flush)                             |       *
   * REPLAYING -------------> LOCAL_REPLAY_FLUSH        |       *
   *    |       \                 |                     |       *
   *    |       |                 v                     |       *
   *    |       |             FLUSH_COMMIT_POSITION     |       *
   *    |       |                 |                     |       *
   *    |       |                 \--------------------/|       *
   *    |       |                                       |       *
   *    |       | (entries available)                   |       *
   *    |       \-----------> REPLAY_READY              |       *
   *    |                         |                     |       *
   *    |                         | (skip if not        |       *
   *    |                         v  needed)        (error)     *
   *    |                     REPLAY_FLUSH  * * * * * * * * *   *
   *    |                         |                     |   *   *
   *    |                         | (skip if not        |   *   *
   *    |                         v  needed)        (error) *   *
   *    |                     GET_REMOTE_TAG  * * * * * * * *   *
   *    |                         |                     |   *   *
   *    |                         | (skip if not        |   *   *
   *    |                         v  needed)        (error) *   *
   *    |                     ALLOCATE_LOCAL_TAG  * * * * * *   *
   *    |                         |                     |   *   *
   *    |                         v                 (error) *   *
   *    |                     PREPROCESS_ENTRY  * * * * * * *   *
   *    |                         |                     |   *   *
   *    |                         v                 (error) *   *
   *    |                     PROCESS_ENTRY * * * * * * * * *   *
   *    |                         |                     |   *   *
   *    |                         \---------------------/   *   *
   *    v (shutdown)                                        *   *
   * REPLAY_COMPLETE  < * * * * * * * * * * * * * * * * * * *   *
   *    |                                                       *
   *    v                                                       *
   * SHUT_DOWN_LOCAL_JOURNAL_REPLAY                             *
   *    |                                                       *
   *    v                                                       *
   * WAIT_FOR_REPLAY                                            *
   *    |                                                       *
   *    v                                                       *
   * CLOSE_LOCAL_IMAGE  < * * * * * * * * * * * * * * * * * * * *
   *    |
   *    v (skip if not started)
   * STOP_REMOTE_JOURNALER_REPLAY
   *    |
   *    v
   * WAIT_FOR_IN_FLIGHT_OPS
   *    |
   *    v
   * <shutdown>
   *
   * @endverbatim
   */

  typedef typename librbd::journal::TypeTraits<ImageCtxT>::ReplayEntry ReplayEntry;

  enum State {
    STATE_INIT,
    STATE_REPLAYING,
    STATE_COMPLETE
  };

  struct C_ReplayCommitted;
  struct RemoteJournalerListener;
  struct RemoteReplayHandler;
  struct LocalJournalListener;

  Threads<ImageCtxT>* m_threads;
  std::string m_local_mirror_uuid;
  StateBuilder<ImageCtxT>* m_state_builder;
  ReplayerListener* m_replayer_listener;

  mutable ceph::mutex m_lock;

  std::string m_image_spec;
  Context* m_on_init_shutdown = nullptr;

  State m_state = STATE_INIT;
  int m_error_code = 0;
  std::string m_error_description;
  bool m_resync_requested = false;

  ceph::ref_t<typename std::remove_pointer<decltype(ImageCtxT::journal)>::type>
    m_local_journal;
  RemoteJournalerListener* m_remote_listener = nullptr;

  librbd::journal::Replay<ImageCtxT>* m_local_journal_replay = nullptr;
  EventPreprocessor<ImageCtxT>* m_event_preprocessor = nullptr;
  ReplayStatusFormatter<ImageCtxT>* m_replay_status_formatter = nullptr;
  RemoteReplayHandler* m_remote_replay_handler = nullptr;
  LocalJournalListener* m_local_journal_listener = nullptr;

  PerfCounters *m_perf_counters = nullptr;

  ReplayEntry m_replay_entry;
  uint64_t m_replay_bytes = 0;
  utime_t m_replay_start_time;
  bool m_replay_tag_valid = false;
  uint64_t m_replay_tag_tid = 0;
  cls::journal::Tag m_replay_tag;
  librbd::journal::TagData m_replay_tag_data;
  librbd::journal::EventEntry m_event_entry;

  AsyncOpTracker m_event_replay_tracker;
  Context *m_delayed_preprocess_task = nullptr;

  AsyncOpTracker m_in_flight_op_tracker;
  Context *m_flush_local_replay_task = nullptr;

  void handle_remote_journal_metadata_updated();

  void schedule_flush_local_replay_task();
  void cancel_flush_local_replay_task();
  void handle_flush_local_replay_task(int r);

  void flush_local_replay(Context* on_flush);
  void handle_flush_local_replay(Context* on_flush, int r);

  void flush_commit_position(Context* on_flush);
  void handle_flush_commit_position(Context* on_flush, int r);

  void init_remote_journaler();
  void handle_init_remote_journaler(int r);

  void start_external_replay(std::unique_lock<ceph::mutex>& locker);
  void handle_start_external_replay(int r);

  bool add_local_journal_listener(std::unique_lock<ceph::mutex>& locker);

  bool notify_init_complete(std::unique_lock<ceph::mutex>& locker);

  void shut_down_local_journal_replay();
  void handle_shut_down_local_journal_replay(int r);

  void wait_for_event_replay();
  void handle_wait_for_event_replay(int r);

  void close_local_image();
  void handle_close_local_image(int r);

  void stop_remote_journaler_replay();
  void handle_stop_remote_journaler_replay(int r);

  void wait_for_in_flight_ops();
  void handle_wait_for_in_flight_ops(int r);

  void replay_flush();
  void handle_replay_flush_shut_down(int r);
  void handle_replay_flush(int r);

  void get_remote_tag();
  void handle_get_remote_tag(int r);

  void allocate_local_tag();
  void handle_allocate_local_tag(int r);

  void handle_replay_error(int r, const std::string &error);

  bool is_replay_complete() const;
  bool is_replay_complete(const std::unique_lock<ceph::mutex>& locker) const;

  void handle_replay_complete(int r, const std::string &error_desc);
  void handle_replay_complete(const std::unique_lock<ceph::mutex>&,
                              int r, const std::string &error_desc);
  void handle_replay_ready();
  void handle_replay_ready(std::unique_lock<ceph::mutex>& locker);

  void preprocess_entry();
  void handle_delayed_preprocess_task(int r);
  void handle_preprocess_entry_ready(int r);
  void handle_preprocess_entry_safe(int r);

  void process_entry();
  void handle_process_entry_ready(int r);
  void handle_process_entry_safe(const ReplayEntry& replay_entry,
                                 uint64_t relay_bytes,
                                 const utime_t &replay_start_time, int r);

  void handle_resync_image();

  void notify_status_updated();

  void cancel_delayed_preprocess_task();

  int validate_remote_client_state(
      const cls::journal::Client& remote_client,
      librbd::journal::MirrorPeerClientMeta* remote_client_meta,
      bool* resync_requested, std::string* error);

  void register_perf_counters();
  void unregister_perf_counters();

};

} // namespace journal
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::journal::Replayer<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_JOURNAL_REPLAYER_H
