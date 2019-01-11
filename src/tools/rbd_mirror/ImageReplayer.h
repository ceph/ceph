// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_REPLAYER_H
#define CEPH_RBD_MIRROR_IMAGE_REPLAYER_H

#include "common/AsyncOpTracker.h"
#include "common/Mutex.h"
#include "common/WorkQueue.h"
#include "include/rados/librados.hpp"
#include "cls/journal/cls_journal_types.h"
#include "cls/rbd/cls_rbd_types.h"
#include "journal/JournalMetadataListener.h"
#include "journal/ReplayEntry.h"
#include "librbd/ImageCtx.h"
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"
#include "ProgressContext.h"
#include "tools/rbd_mirror/Types.h"
#include "tools/rbd_mirror/image_replayer/Types.h"

#include <boost/noncopyable.hpp>
#include <boost/optional.hpp>

#include <set>
#include <map>
#include <atomic>
#include <string>
#include <vector>

class AdminSocketHook;
class PerfCounters;

namespace journal {

class Journaler;
class ReplayHandler;

}

namespace librbd {

class ImageCtx;
namespace journal { template <typename> class Replay; }

}

namespace rbd {
namespace mirror {

template <typename> struct InstanceWatcher;
template <typename> struct Threads;

namespace image_replayer { template <typename> class BootstrapRequest; }
namespace image_replayer { template <typename> class EventPreprocessor; }
namespace image_replayer { template <typename> class ReplayStatusFormatter; }

/**
 * Replays changes from a remote cluster for a single image.
 */
template <typename ImageCtxT = librbd::ImageCtx>
class ImageReplayer {
public:
  static ImageReplayer *create(
    Threads<ImageCtxT> *threads, InstanceWatcher<ImageCtxT> *instance_watcher,
    RadosRef local, const std::string &local_mirror_uuid, int64_t local_pool_id,
    const std::string &global_image_id) {
    return new ImageReplayer(threads, instance_watcher, local,
                             local_mirror_uuid, local_pool_id, global_image_id);
  }
  void destroy() {
    delete this;
  }

  ImageReplayer(Threads<ImageCtxT> *threads,
                InstanceWatcher<ImageCtxT> *instance_watcher,
                RadosRef local, const std::string &local_mirror_uuid,
                int64_t local_pool_id, const std::string &global_image_id);
  virtual ~ImageReplayer();
  ImageReplayer(const ImageReplayer&) = delete;
  ImageReplayer& operator=(const ImageReplayer&) = delete;

  bool is_stopped() { Mutex::Locker l(m_lock); return is_stopped_(); }
  bool is_running() { Mutex::Locker l(m_lock); return is_running_(); }
  bool is_replaying() { Mutex::Locker l(m_lock); return is_replaying_(); }

  std::string get_name() { Mutex::Locker l(m_lock); return m_name; };
  void set_state_description(int r, const std::string &desc);

  // TODO temporary until policy handles release of image replayers
  inline bool is_finished() const {
    Mutex::Locker locker(m_lock);
    return m_finished;
  }
  inline void set_finished(bool finished) {
    Mutex::Locker locker(m_lock);
    m_finished = finished;
  }

  inline bool is_blacklisted() const {
    Mutex::Locker locker(m_lock);
    return (m_last_r == -EBLACKLISTED);
  }

  image_replayer::HealthState get_health_state() const;

  void add_peer(const std::string &peer_uuid, librados::IoCtx &remote_io_ctx);

  inline int64_t get_local_pool_id() const {
    return m_local_pool_id;
  }
  inline const std::string& get_global_image_id() const {
    return m_global_image_id;
  }

  void start(Context *on_finish = nullptr, bool manual = false);
  void stop(Context *on_finish = nullptr, bool manual = false,
	    int r = 0, const std::string& desc = "");
  void restart(Context *on_finish = nullptr);
  void flush(Context *on_finish = nullptr);

  void resync_image(Context *on_finish=nullptr);

  void print_status(Formatter *f, stringstream *ss);

  virtual void handle_replay_ready();
  virtual void handle_replay_complete(int r, const std::string &error_desc);

protected:
  /**
   * @verbatim
   *                   (error)
   * <uninitialized> <------------------------------------ FAIL
   *    |                                                   ^
   *    v                                                   *
   * <starting>                                             *
   *    |                                                   *
   *    v                                           (error) *
   * PREPARE_LOCAL_IMAGE  * * * * * * * * * * * * * * * * * *
   *    |                                                   *
   *    v                                           (error) *
   * PREPARE_REMOTE_IMAGE * * * * * * * * * * * * * * * * * *
   *    |                                                   *
   *    v                                           (error) *
   * BOOTSTRAP_IMAGE  * * * * * * * * * * * * * * * * * * * *
   *    |                                                   *
   *    v                                           (error) *
   * INIT_REMOTE_JOURNALER  * * * * * * * * * * * * * * * * *
   *    |                                                   *
   *    v                                           (error) *
   * START_REPLAY * * * * * * * * * * * * * * * * * * * * * *
   *    |
   *    |  /--------------------------------------------\
   *    |  |                                            |
   *    v  v   (asok flush)                             |
   * REPLAYING -------------> LOCAL_REPLAY_FLUSH        |
   *    |       \                 |                     |
   *    |       |                 v                     |
   *    |       |             FLUSH_COMMIT_POSITION     |
   *    |       |                 |                     |
   *    |       |                 \--------------------/|
   *    |       |                                       |
   *    |       | (entries available)                   |
   *    |       \-----------> REPLAY_READY              |
   *    |                         |                     |
   *    |                         | (skip if not        |
   *    |                         v  needed)        (error)
   *    |                     REPLAY_FLUSH  * * * * * * * * *
   *    |                         |                     |   *
   *    |                         | (skip if not        |   *
   *    |                         v  needed)        (error) *
   *    |                     GET_REMOTE_TAG  * * * * * * * *
   *    |                         |                     |   *
   *    |                         | (skip if not        |   *
   *    |                         v  needed)        (error) *
   *    |                     ALLOCATE_LOCAL_TAG  * * * * * *
   *    |                         |                     |   *
   *    |                         v                 (error) *
   *    |                     PREPROCESS_ENTRY  * * * * * * *
   *    |                         |                     |   *
   *    |                         v                 (error) *
   *    |                     PROCESS_ENTRY * * * * * * * * *
   *    |                         |                     |   *
   *    |                         \---------------------/   *
   *    v                                                   *
   * REPLAY_COMPLETE  < * * * * * * * * * * * * * * * * * * *
   *    |
   *    v
   * JOURNAL_REPLAY_SHUT_DOWN
   *    |
   *    v
   * LOCAL_IMAGE_CLOSE
   *    |
   *    v
   * <stopped>
   *
   * @endverbatim
   */

  virtual void on_start_fail(int r, const std::string &desc);
  virtual bool on_start_interrupted();

  virtual void on_stop_journal_replay(int r = 0, const std::string &desc = "");

  virtual void on_flush_local_replay_flush_start(Context *on_flush);
  virtual void on_flush_local_replay_flush_finish(Context *on_flush, int r);
  virtual void on_flush_flush_commit_position_start(Context *on_flush);
  virtual void on_flush_flush_commit_position_finish(Context *on_flush, int r);

  bool on_replay_interrupted();

private:
  typedef typename librbd::journal::TypeTraits<ImageCtxT>::ReplayEntry ReplayEntry;

  enum State {
    STATE_UNKNOWN,
    STATE_STARTING,
    STATE_REPLAYING,
    STATE_REPLAY_FLUSHING,
    STATE_STOPPING,
    STATE_STOPPED,
  };

  struct RemoteImage {
    std::string mirror_uuid;
    std::string image_id;
    librados::IoCtx io_ctx;

    RemoteImage() {
    }
    RemoteImage(const Peer& peer) : io_ctx(peer.io_ctx) {
    }
  };

  typedef typename librbd::journal::TypeTraits<ImageCtxT>::Journaler Journaler;
  typedef boost::optional<State> OptionalState;
  typedef boost::optional<cls::rbd::MirrorImageStatusState>
      OptionalMirrorImageStatusState;

  struct JournalListener : public librbd::journal::Listener {
    ImageReplayer *img_replayer;

    JournalListener(ImageReplayer *img_replayer)
      : img_replayer(img_replayer) {
    }

    void handle_close() override {
      img_replayer->on_stop_journal_replay();
    }

    void handle_promoted() override {
      img_replayer->on_stop_journal_replay(0, "force promoted");
    }

    void handle_resync() override {
      img_replayer->resync_image();
    }
  };

  class BootstrapProgressContext : public ProgressContext {
  public:
    BootstrapProgressContext(ImageReplayer<ImageCtxT> *replayer) :
      replayer(replayer) {
    }

    void update_progress(const std::string &description,
				 bool flush = true) override;
  private:
    ImageReplayer<ImageCtxT> *replayer;
  };

  Threads<ImageCtxT> *m_threads;
  InstanceWatcher<ImageCtxT> *m_instance_watcher;

  Peers m_peers;
  RemoteImage m_remote_image;

  RadosRef m_local;
  std::string m_local_mirror_uuid;
  int64_t m_local_pool_id;
  std::string m_local_image_id;
  std::string m_global_image_id;
  std::string m_local_image_name;
  std::string m_name;

  mutable Mutex m_lock;
  State m_state = STATE_STOPPED;
  std::string m_state_desc;

  OptionalMirrorImageStatusState m_mirror_image_status_state =
    boost::make_optional(false, cls::rbd::MIRROR_IMAGE_STATUS_STATE_UNKNOWN);
  int m_last_r = 0;

  BootstrapProgressContext m_progress_cxt;

  bool m_finished = false;
  bool m_delete_requested = false;
  bool m_resync_requested = false;

  image_replayer::EventPreprocessor<ImageCtxT> *m_event_preprocessor = nullptr;
  image_replayer::ReplayStatusFormatter<ImageCtxT> *m_replay_status_formatter =
    nullptr;
  IoCtxRef m_local_ioctx;
  ImageCtxT *m_local_image_ctx = nullptr;
  std::string m_local_image_tag_owner;

  decltype(ImageCtxT::journal) m_local_journal = nullptr;
  librbd::journal::Replay<ImageCtxT> *m_local_replay = nullptr;
  Journaler* m_remote_journaler = nullptr;
  ::journal::ReplayHandler *m_replay_handler = nullptr;
  librbd::journal::Listener *m_journal_listener;

  Context *m_on_start_finish = nullptr;
  Context *m_on_stop_finish = nullptr;
  Context *m_update_status_task = nullptr;
  int m_update_status_interval = 0;
  librados::AioCompletion *m_update_status_comp = nullptr;
  bool m_stop_requested = false;
  bool m_manual_stop = false;

  AdminSocketHook *m_asok_hook = nullptr;
  PerfCounters *m_perf_counters = nullptr;

  image_replayer::BootstrapRequest<ImageCtxT> *m_bootstrap_request = nullptr;

  uint32_t m_in_flight_status_updates = 0;
  bool m_update_status_requested = false;
  Context *m_on_update_status_finish = nullptr;

  cls::journal::ClientState m_client_state =
    cls::journal::CLIENT_STATE_DISCONNECTED;
  librbd::journal::MirrorPeerClientMeta m_client_meta;

  ReplayEntry m_replay_entry;
  utime_t m_replay_start_time;
  bool m_replay_tag_valid = false;
  uint64_t m_replay_tag_tid = 0;
  cls::journal::Tag m_replay_tag;
  librbd::journal::TagData m_replay_tag_data;
  librbd::journal::EventEntry m_event_entry;
  AsyncOpTracker m_event_replay_tracker;
  Context *m_delayed_preprocess_task = nullptr;

  struct RemoteJournalerListener : public ::journal::JournalMetadataListener {
    ImageReplayer *replayer;

    RemoteJournalerListener(ImageReplayer *replayer) : replayer(replayer) { }

    void handle_update(::journal::JournalMetadata *) override;
  } m_remote_listener;

  struct C_ReplayCommitted : public Context {
    ImageReplayer *replayer;
    ReplayEntry replay_entry;
    utime_t replay_start_time;

    C_ReplayCommitted(ImageReplayer *replayer,
                      ReplayEntry &&replay_entry,
                      const utime_t &replay_start_time)
      : replayer(replayer), replay_entry(std::move(replay_entry)),
        replay_start_time(replay_start_time) {
    }
    void finish(int r) override {
      replayer->handle_process_entry_safe(replay_entry, replay_start_time, r);
    }
  };

  static std::string to_string(const State state);

  bool is_stopped_() const {
    return m_state == STATE_STOPPED;
  }
  bool is_running_() const {
    return !is_stopped_() && m_state != STATE_STOPPING && !m_stop_requested;
  }
  bool is_replaying_() const {
    return (m_state == STATE_REPLAYING ||
            m_state == STATE_REPLAY_FLUSHING);
  }

  bool update_mirror_image_status(bool force, const OptionalState &state);
  bool start_mirror_image_status_update(bool force, bool restarting);
  void finish_mirror_image_status_update();
  void queue_mirror_image_status_update(const OptionalState &state);
  void send_mirror_status_update(const OptionalState &state);
  void handle_mirror_status_update(int r);
  void reschedule_update_status_task(int new_interval);

  void shut_down(int r);
  void handle_shut_down(int r);
  void handle_remote_journal_metadata_updated();

  void prepare_local_image();
  void handle_prepare_local_image(int r);

  void prepare_remote_image();
  void handle_prepare_remote_image(int r);

  void bootstrap();
  void handle_bootstrap(int r);

  void init_remote_journaler();
  void handle_init_remote_journaler(int r);

  void start_replay();
  void handle_start_replay(int r);

  void replay_flush();
  void handle_replay_flush(int r);

  void get_remote_tag();
  void handle_get_remote_tag(int r);

  void allocate_local_tag();
  void handle_allocate_local_tag(int r);

  void preprocess_entry();
  void handle_preprocess_entry_ready(int r);
  void handle_preprocess_entry_safe(int r);

  void process_entry();
  void handle_process_entry_ready(int r);
  void handle_process_entry_safe(const ReplayEntry& replay_entry,
                                 const utime_t &m_replay_start_time, int r);

  void register_admin_socket_hook();
  void unregister_admin_socket_hook();
  void reregister_admin_socket_hook();
};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::ImageReplayer<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_IMAGE_REPLAYER_H
