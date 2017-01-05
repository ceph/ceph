// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_REPLAYER_H
#define CEPH_RBD_MIRROR_IMAGE_REPLAYER_H

#include <map>
#include <string>
#include <vector>

#include "include/atomic.h"
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
#include "ImageDeleter.h"
#include "ProgressContext.h"
#include "types.h"
#include <boost/optional.hpp>

class AdminSocketHook;

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

struct Threads;

namespace image_replayer { template <typename> class BootstrapRequest; }
namespace image_replayer { template <typename> class EventPreprocessor; }
namespace image_replayer { template <typename> class ReplayStatusFormatter; }

/**
 * Replays changes from a remote cluster for a single image.
 */
template <typename ImageCtxT = librbd::ImageCtx>
class ImageReplayer {
public:
  typedef typename librbd::journal::TypeTraits<ImageCtxT>::ReplayEntry ReplayEntry;

  enum State {
    STATE_UNKNOWN,
    STATE_STARTING,
    STATE_REPLAYING,
    STATE_REPLAY_FLUSHING,
    STATE_STOPPING,
    STATE_STOPPED,
  };

  ImageReplayer(Threads *threads, std::shared_ptr<ImageDeleter> image_deleter,
                ImageSyncThrottlerRef<ImageCtxT> image_sync_throttler,
                RadosRef local, RadosRef remote,
                const std::string &local_mirror_uuid,
                const std::string &remote_mirror_uuid, int64_t local_pool_id,
		int64_t remote_pool_id, const std::string &remote_image_id,
                const std::string &global_image_id);
  virtual ~ImageReplayer();
  ImageReplayer(const ImageReplayer&) = delete;
  ImageReplayer& operator=(const ImageReplayer&) = delete;

  State get_state() { Mutex::Locker l(m_lock); return get_state_(); }
  bool is_stopped() { Mutex::Locker l(m_lock); return is_stopped_(); }
  bool is_running() { Mutex::Locker l(m_lock); return is_running_(); }
  bool is_replaying() { Mutex::Locker l(m_lock); return is_replaying_(); }

  std::string get_name() { Mutex::Locker l(m_lock); return m_name; };
  void set_state_description(int r, const std::string &desc);

  inline bool is_blacklisted() const {
    Mutex::Locker locker(m_lock);
    return (m_last_r == -EBLACKLISTED);
  }

  inline int64_t get_local_pool_id() const {
    return m_local_pool_id;
  }
  inline int64_t get_remote_pool_id() const {
    return m_remote_pool_id;
  }
  inline const std::string& get_global_image_id() const {
    return m_global_image_id;
  }
  inline const std::string& get_remote_image_id() const {
    return m_remote_image_id;
  }
  inline std::string get_local_image_id() {
    Mutex::Locker locker(m_lock);
    return m_local_image_id;
  }
  inline std::string get_local_image_name() {
    Mutex::Locker locker(m_lock);
    return m_local_image_name;
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

  virtual void on_start_fail(int r, const std::string &desc = "");
  virtual bool on_start_interrupted();

  virtual void on_stop_journal_replay(int r = 0, const std::string &desc = "");

  virtual void on_flush_local_replay_flush_start(Context *on_flush);
  virtual void on_flush_local_replay_flush_finish(Context *on_flush, int r);
  virtual void on_flush_flush_commit_position_start(Context *on_flush);
  virtual void on_flush_flush_commit_position_finish(Context *on_flush, int r);

  bool on_replay_interrupted();

private:
  typedef typename librbd::journal::TypeTraits<ImageCtxT>::Journaler Journaler;
  typedef boost::optional<State> OptionalState;

  struct JournalListener : public librbd::journal::Listener {
    ImageReplayer *img_replayer;

    JournalListener(ImageReplayer *img_replayer)
      : img_replayer(img_replayer) {
    }

    virtual void handle_close() {
      img_replayer->on_stop_journal_replay();
    }

    virtual void handle_promoted() {
      img_replayer->on_stop_journal_replay(0, "force promoted");
    }

    virtual void handle_resync() {
      img_replayer->resync_image();
    }
  };

  class BootstrapProgressContext : public ProgressContext {
  public:
    BootstrapProgressContext(ImageReplayer<ImageCtxT> *replayer) :
      replayer(replayer) {
    }

    virtual void update_progress(const std::string &description,
				 bool flush = true);
  private:
    ImageReplayer<ImageCtxT> *replayer;
  };

  Threads *m_threads;
  std::shared_ptr<ImageDeleter> m_image_deleter;
  ImageSyncThrottlerRef<ImageCtxT> m_image_sync_throttler;
  RadosRef m_local, m_remote;
  std::string m_local_mirror_uuid;
  std::string m_remote_mirror_uuid;
  int64_t m_remote_pool_id, m_local_pool_id;
  std::string m_remote_image_id, m_local_image_id, m_global_image_id;
  std::string m_local_image_name;
  std::string m_name;
  mutable Mutex m_lock;
  State m_state = STATE_STOPPED;
  int m_last_r = 0;
  std::string m_state_desc;
  BootstrapProgressContext m_progress_cxt;
  bool m_do_resync;
  image_replayer::EventPreprocessor<ImageCtxT> *m_event_preprocessor = nullptr;
  image_replayer::ReplayStatusFormatter<ImageCtxT> *m_replay_status_formatter =
    nullptr;
  librados::IoCtx m_local_ioctx, m_remote_ioctx;
  ImageCtxT *m_local_image_ctx = nullptr;

  decltype(ImageCtxT::journal) m_local_journal = nullptr;
  librbd::journal::Replay<ImageCtxT> *m_local_replay = nullptr;
  Journaler* m_remote_journaler = nullptr;
  ::journal::ReplayHandler *m_replay_handler = nullptr;
  librbd::journal::Listener *m_journal_listener;
  bool m_stopping_for_resync = false;

  Context *m_on_start_finish = nullptr;
  Context *m_on_stop_finish = nullptr;
  Context *m_update_status_task = nullptr;
  int m_update_status_interval = 0;
  librados::AioCompletion *m_update_status_comp = nullptr;
  bool m_stop_requested = false;
  bool m_manual_stop = false;

  AdminSocketHook *m_asok_hook = nullptr;

  image_replayer::BootstrapRequest<ImageCtxT> *m_bootstrap_request = nullptr;

  uint32_t m_in_flight_status_updates = 0;
  bool m_update_status_requested = false;
  Context *m_on_update_status_finish = nullptr;

  librbd::journal::MirrorPeerClientMeta m_client_meta;

  ReplayEntry m_replay_entry;
  bool m_replay_tag_valid = false;
  uint64_t m_replay_tag_tid = 0;
  cls::journal::Tag m_replay_tag;
  librbd::journal::TagData m_replay_tag_data;
  librbd::journal::EventEntry m_event_entry;
  AsyncOpTracker m_event_replay_tracker;

  struct RemoteJournalerListener : public ::journal::JournalMetadataListener {
    ImageReplayer *replayer;

    RemoteJournalerListener(ImageReplayer *replayer) : replayer(replayer) { }

    void handle_update(::journal::JournalMetadata *);
  } m_remote_listener;

  struct C_ReplayCommitted : public Context {
    ImageReplayer *replayer;
    ReplayEntry replay_entry;

    C_ReplayCommitted(ImageReplayer *replayer,
                      ReplayEntry &&replay_entry)
      : replayer(replayer), replay_entry(std::move(replay_entry)) {
    }
    virtual void finish(int r) {
      replayer->handle_process_entry_safe(replay_entry, r);
    }
  };

  static std::string to_string(const State state);

  State get_state_() const {
    return m_state;
  }
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
  void reschedule_update_status_task(int new_interval = 0);

  void shut_down(int r);
  void handle_shut_down(int r);
  void handle_remote_journal_metadata_updated();

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
  void handle_preprocess_entry(int r);

  void process_entry();
  void handle_process_entry_ready(int r);
  void handle_process_entry_safe(const ReplayEntry& replay_entry, int r);

};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::ImageReplayer<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_IMAGE_REPLAYER_H
