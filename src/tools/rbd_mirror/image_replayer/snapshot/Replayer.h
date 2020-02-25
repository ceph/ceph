// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_SNAPSHOT_REPLAYER_H
#define RBD_MIRROR_IMAGE_REPLAYER_SNAPSHOT_REPLAYER_H

#include "tools/rbd_mirror/image_replayer/Replayer.h"
#include "common/ceph_mutex.h"
#include "common/AsyncOpTracker.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/mirror/snapshot/Types.h"
#include <string>
#include <type_traits>

namespace librbd {

struct ImageCtx;
namespace snapshot { template <typename I> class Replay; }

} // namespace librbd

namespace rbd {
namespace mirror {

class PoolMetaCache;
template <typename> struct Threads;

namespace image_replayer {

struct ReplayerListener;

namespace snapshot {

template <typename> class EventPreprocessor;
template <typename> class ReplayStatusFormatter;
template <typename> class StateBuilder;

template <typename ImageCtxT>
class Replayer : public image_replayer::Replayer {
public:
  static Replayer* create(
      Threads<ImageCtxT>* threads,
      const std::string& local_mirror_uuid,
      PoolMetaCache* pool_meta_cache,
      StateBuilder<ImageCtxT>* state_builder,
      ReplayerListener* replayer_listener) {
    return new Replayer(threads, local_mirror_uuid, pool_meta_cache,
                        state_builder, replayer_listener);
  }

  Replayer(
      Threads<ImageCtxT>* threads,
      const std::string& local_mirror_uuid,
      PoolMetaCache* pool_meta_cache,
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
    return (m_state == STATE_REPLAYING || m_state == STATE_IDLE);
  }

  bool is_resync_requested() const override {
    std::unique_lock locker(m_lock);
    // TODO
    return false;
  }

  int get_error_code() const override {
    std::unique_lock locker(m_lock);
    return m_error_code;
  }

  std::string get_error_description() const override {
    std::unique_lock locker(m_lock);
    return m_error_description;
  }

private:
  /**
   * @verbatim
   *
   * <init>
   *    |
   *    v
   * REGISTER_UPDATE_WATCHER
   *    |
   *    v (skip if not needed)
   * REFRESH_LOCAL_IMAGE <------------------------------\
   *    |                                               |
   *    v (skip if not needed)                          |
   * REFRESH_REMOTE_IMAGE                               |
   *    |                                               |
   *    | (interrupted sync)                            |
   *    |\--------------> GET_LOCAL_IMAGE_STATE ------\ |
   *    |                                             | |
   *    | (new snapshot)                              | |
   *    |\--------------> COPY_SNAPSHOTS              | |
   *    |                     |                       | |
   *    |                     v                       | |
   *    |                 GET_REMOTE_IMAGE_STATE      | |
   *    |                     |                       | |
   *    |                     v                       | |
   *    |                 CREATE_NON_PRIMARY_SNAPSHOT | |
   *    |                     |                       | |
   *    |                     |/----------------------/ |
   *    |                     |                         |
   *    |                     v                         |
   *    |                 COPY_IMAGE                    |
   *    |                     |                         |
   *    |                     v                         |
   *    |                 APPLY_IMAGE_STATE             |
   *    |                     |                         |
   *    |                     v                         |
   *    |                 UPDATE_NON_PRIMARY_SNAPSHOT   |
   *    |                     |                         |
   *    |                     v                         |
   *    |                 NOTIFY_IMAGE_UPDATE           |
   *    |                     |                         |
   *    |                     v                         |
   *    |                 UNLINK_PEER                   |
   *    |                     |                         |
   *    |                     v                         |
   *    |                 NOTIFY_LISTENER               |
   *    |                     |                         |
   *    |                     \------------------------/|
   *    |                                               |
   *    | (remote demoted)                              |
   *    \---------------> NOTIFY_LISTENER               |
   *    |                     |                         |
   *    |/--------------------/                         |
   *    |                                               |
   *    |   (update notification)                       |
   * <idle> --------------------------------------------/
   *    |
   *    v
   * <shut down>
   *    |
   *    v
   * UNREGISTER_UPDATE_WATCHER
   *    |
   *    v
   * WAIT_FOR_IN_FLIGHT_OPS
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  enum State {
    STATE_INIT,
    STATE_REPLAYING,
    STATE_IDLE,
    STATE_COMPLETE
  };

  struct C_UpdateWatchCtx;
  struct C_TrackedOp;
  struct ProgressContext;

  Threads<ImageCtxT>* m_threads;
  std::string m_local_mirror_uuid;
  PoolMetaCache* m_pool_meta_cache;
  StateBuilder<ImageCtxT>* m_state_builder;
  ReplayerListener* m_replayer_listener;

  mutable ceph::mutex m_lock;

  State m_state = STATE_INIT;

  Context* m_on_init_shutdown = nullptr;

  int m_error_code = 0;
  std::string m_error_description;

  C_UpdateWatchCtx* m_update_watch_ctx;
  uint64_t m_update_watcher_handle = 0;

  AsyncOpTracker m_in_flight_op_tracker;

  uint64_t m_local_snap_id_start = 0;
  uint64_t m_local_snap_id_end = CEPH_NOSNAP;
  cls::rbd::MirrorSnapshotNamespace m_local_mirror_snap_ns;
  uint64_t m_local_object_count = 0;

  std::string m_remote_mirror_peer_uuid;
  uint64_t m_remote_snap_id_start = 0;
  uint64_t m_remote_snap_id_end = CEPH_NOSNAP;
  cls::rbd::MirrorSnapshotNamespace m_remote_mirror_snap_ns;

  librbd::mirror::snapshot::ImageState m_image_state;
  ProgressContext* m_progress_ctx = nullptr;

  bool m_remote_image_updated = false;
  bool m_updating_sync_point = false;

  void refresh_local_image();
  void handle_refresh_local_image(int r);

  void refresh_remote_image();
  void handle_refresh_remote_image(int r);

  void scan_local_mirror_snapshots(std::unique_lock<ceph::mutex>* locker);
  void scan_remote_mirror_snapshots(std::unique_lock<ceph::mutex>* locker);

  void copy_snapshots();
  void handle_copy_snapshots(int r);

  void get_remote_image_state();
  void handle_get_remote_image_state(int r);

  void get_local_image_state();
  void handle_get_local_image_state(int r);

  void create_non_primary_snapshot();
  void handle_create_non_primary_snapshot(int r);

  void copy_image();
  void handle_copy_image(int r);
  void handle_copy_image_progress(uint64_t object_number,
                                  uint64_t object_count);

  void apply_image_state();
  void handle_apply_image_state(int r);

  void update_non_primary_snapshot(bool complete);
  void handle_update_non_primary_snapshot(bool complete, int r);

  void notify_image_update();
  void handle_notify_image_update(int r);

  void unlink_peer();
  void handle_unlink_peer(int r);

  void register_update_watcher();
  void handle_register_update_watcher(int r);

  void unregister_update_watcher();
  void handle_unregister_update_watcher(int r);

  void wait_for_in_flight_ops();
  void handle_wait_for_in_flight_ops(int r);

  void handle_remote_image_update_notify();

  void handle_replay_complete(int r, const std::string& description);
  void handle_replay_complete(std::unique_lock<ceph::mutex>* locker,
                              int r, const std::string& description);
  void notify_status_updated();

  bool is_replay_interrupted();
  bool is_replay_interrupted(std::unique_lock<ceph::mutex>* lock);

};

} // namespace snapshot
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::snapshot::Replayer<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_SNAPSHOT_REPLAYER_H
