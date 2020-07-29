// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_REPLAYER_H
#define CEPH_RBD_MIRROR_IMAGE_REPLAYER_H

#include "common/AsyncOpTracker.h"
#include "common/ceph_mutex.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include "ProgressContext.h"
#include "tools/rbd_mirror/Types.h"
#include "tools/rbd_mirror/image_replayer/Types.h"
#include <boost/optional.hpp>
#include <string>

class AdminSocketHook;

namespace journal { struct CacheManagerHandler; }
namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> struct InstanceWatcher;
template <typename> struct MirrorStatusUpdater;
struct PoolMetaCache;
template <typename> struct Threads;

namespace image_replayer {

class Replayer;
template <typename> class BootstrapRequest;
template <typename> class StateBuilder;

} // namespace image_replayer

/**
 * Replays changes from a remote cluster for a single image.
 */
template <typename ImageCtxT = librbd::ImageCtx>
class ImageReplayer {
public:
  static ImageReplayer *create(
      librados::IoCtx &local_io_ctx, const std::string &local_mirror_uuid,
      const std::string &global_image_id, Threads<ImageCtxT> *threads,
      InstanceWatcher<ImageCtxT> *instance_watcher,
      MirrorStatusUpdater<ImageCtxT>* local_status_updater,
      journal::CacheManagerHandler *cache_manager_handler,
      PoolMetaCache* pool_meta_cache) {
    return new ImageReplayer(local_io_ctx, local_mirror_uuid, global_image_id,
                             threads, instance_watcher, local_status_updater,
                             cache_manager_handler, pool_meta_cache);
  }
  void destroy() {
    delete this;
  }

  ImageReplayer(librados::IoCtx &local_io_ctx,
                const std::string &local_mirror_uuid,
                const std::string &global_image_id,
                Threads<ImageCtxT> *threads,
                InstanceWatcher<ImageCtxT> *instance_watcher,
                MirrorStatusUpdater<ImageCtxT>* local_status_updater,
                journal::CacheManagerHandler *cache_manager_handler,
                PoolMetaCache* pool_meta_cache);
  virtual ~ImageReplayer();
  ImageReplayer(const ImageReplayer&) = delete;
  ImageReplayer& operator=(const ImageReplayer&) = delete;

  bool is_stopped() { std::lock_guard l{m_lock}; return is_stopped_(); }
  bool is_running() { std::lock_guard l{m_lock}; return is_running_(); }
  bool is_replaying() { std::lock_guard l{m_lock}; return is_replaying_(); }

  std::string get_name() { std::lock_guard l{m_lock}; return m_image_spec; };
  void set_state_description(int r, const std::string &desc);

  // TODO temporary until policy handles release of image replayers
  inline bool is_finished() const {
    std::lock_guard locker{m_lock};
    return m_finished;
  }
  inline void set_finished(bool finished) {
    std::lock_guard locker{m_lock};
    m_finished = finished;
  }

  inline bool is_blacklisted() const {
    std::lock_guard locker{m_lock};
    return (m_last_r == -EBLACKLISTED);
  }

  image_replayer::HealthState get_health_state() const;

  void add_peer(const Peer<ImageCtxT>& peer);

  inline int64_t get_local_pool_id() const {
    return m_local_io_ctx.get_id();
  }
  inline const std::string& get_global_image_id() const {
    return m_global_image_id;
  }

  void start(Context *on_finish = nullptr, bool manual = false,
             bool restart = false);
  void stop(Context *on_finish = nullptr, bool manual = false,
            bool restart = false);
  void restart(Context *on_finish = nullptr);
  void flush();

  void print_status(Formatter *f);

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
   * START_REPLAY * * * * * * * * * * * * * * * * * * * * * *
   *    |
   *    v
   * REPLAYING
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

  void on_start_fail(int r, const std::string &desc);
  bool on_start_interrupted();
  bool on_start_interrupted(ceph::mutex& lock);

  void on_stop_journal_replay(int r = 0, const std::string &desc = "");

  bool on_replay_interrupted();

private:
  typedef std::set<Peer<ImageCtxT>> Peers;

  enum State {
    STATE_UNKNOWN,
    STATE_STARTING,
    STATE_REPLAYING,
    STATE_STOPPING,
    STATE_STOPPED,
  };

  struct ReplayerListener;

  typedef boost::optional<State> OptionalState;
  typedef boost::optional<cls::rbd::MirrorImageStatusState>
      OptionalMirrorImageStatusState;

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

  librados::IoCtx &m_local_io_ctx;
  std::string m_local_mirror_uuid;
  std::string m_global_image_id;
  Threads<ImageCtxT> *m_threads;
  InstanceWatcher<ImageCtxT> *m_instance_watcher;
  MirrorStatusUpdater<ImageCtxT>* m_local_status_updater;
  journal::CacheManagerHandler *m_cache_manager_handler;
  PoolMetaCache* m_pool_meta_cache;

  Peers m_peers;
  Peer<ImageCtxT> m_remote_image_peer;

  std::string m_local_image_name;
  std::string m_image_spec;

  mutable ceph::mutex m_lock;
  State m_state = STATE_STOPPED;
  std::string m_state_desc;

  OptionalMirrorImageStatusState m_mirror_image_status_state =
    boost::make_optional(false, cls::rbd::MIRROR_IMAGE_STATUS_STATE_UNKNOWN);
  int m_last_r = 0;

  BootstrapProgressContext m_progress_cxt;

  bool m_finished = false;
  bool m_delete_requested = false;
  bool m_resync_requested = false;
  bool m_restart_requested = false;

  image_replayer::StateBuilder<ImageCtxT>* m_state_builder = nullptr;
  image_replayer::Replayer* m_replayer = nullptr;
  ReplayerListener* m_replayer_listener = nullptr;

  Context *m_on_start_finish = nullptr;
  Context *m_on_stop_finish = nullptr;
  bool m_stop_requested = false;
  bool m_manual_stop = false;

  AdminSocketHook *m_asok_hook = nullptr;

  image_replayer::BootstrapRequest<ImageCtxT> *m_bootstrap_request = nullptr;

  AsyncOpTracker m_in_flight_op_tracker;

  Context* m_update_status_task = nullptr;

  static std::string to_string(const State state);

  bool is_stopped_() const {
    return m_state == STATE_STOPPED;
  }
  bool is_running_() const {
    return !is_stopped_() && m_state != STATE_STOPPING && !m_stop_requested;
  }
  bool is_replaying_() const {
    return (m_state == STATE_REPLAYING);
  }

  void schedule_update_mirror_image_replay_status();
  void handle_update_mirror_image_replay_status(int r);
  void cancel_update_mirror_image_replay_status();

  void update_mirror_image_status(bool force, const OptionalState &state);
  void set_mirror_image_status_update(bool force, const OptionalState &state);

  void shut_down(int r);
  void handle_shut_down(int r);

  void bootstrap();
  void handle_bootstrap(int r);

  void start_replay();
  void handle_start_replay(int r);

  void handle_replayer_notification();

  void register_admin_socket_hook();
  void unregister_admin_socket_hook();
  void reregister_admin_socket_hook();

};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::ImageReplayer<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_IMAGE_REPLAYER_H
