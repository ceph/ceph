// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_GROUP_REPLAYER_REPLAYER_H
#define RBD_MIRROR_GROUP_REPLAYER_REPLAYER_H

#include "common/AsyncOpTracker.h"
#include "common/ceph_mutex.h"
#include "cls/rbd/cls_rbd_types.h"
#include "include/rados/librados.hpp"
#include "librbd/mirror/snapshot/Types.h"
#include "tools/rbd_mirror/Types.h"
#include "tools/rbd_mirror/group_replayer/Types.h"
#include <string>

class Context;
namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> class ImageReplayer;
class PoolMetaCache;
template <typename> struct Threads;

namespace group_replayer {

template <typename ImageCtxT = librbd::ImageCtx>
class Replayer {
public:
  static Replayer* create(
      Threads<ImageCtxT>* threads,
      librados::IoCtx &local_io_ctx,
      librados::IoCtx &remote_io_ctx,
      const std::string &global_group_id,
      const std::string& local_mirror_uuid,
      PoolMetaCache* pool_meta_cache,
      std::string local_group_id,
      std::string remote_group_id,
      GroupCtx *local_group_ctx,
      std::list<std::pair<librados::IoCtx, ImageReplayer<ImageCtxT> *>> *image_replayers) {
    return new Replayer(threads, local_io_ctx, remote_io_ctx, global_group_id,
        local_mirror_uuid, pool_meta_cache, local_group_id, remote_group_id,
        local_group_ctx, image_replayers);
  }

  Replayer(
      Threads<ImageCtxT>* threads,
      librados::IoCtx &local_io_ctx,
      librados::IoCtx &remote_io_ctx,
      const std::string &global_group_id,
      const std::string& local_mirror_uuid,
      PoolMetaCache* pool_meta_cache,
      std::string local_group_id,
      std::string remote_group_id,
      GroupCtx *local_group_ctx,
      std::list<std::pair<librados::IoCtx, ImageReplayer<ImageCtxT> *>> *image_replayers);
  ~Replayer();

  void destroy() {
    delete this;
  }
  void init(Context* on_finish);
  void shut_down(Context* on_finish);
  void finish_shut_down();

  bool is_replaying() const {
    std::unique_lock locker{m_lock};
    return (m_state == STATE_REPLAYING || m_state == STATE_IDLE);
  }

  bool get_replay_status(std::string* description);

  int get_error_code() const {
    std::unique_lock locker(m_lock);
    return m_error_code;
  }

  std::string get_error_description() const {
    std::unique_lock locker(m_lock);
    return m_error_description;
  }


private:
  enum State {
    STATE_INIT,
    STATE_REPLAYING,
    STATE_IDLE,
    STATE_COMPLETE
  };

  Threads<ImageCtxT> *m_threads;
  librados::IoCtx &m_local_io_ctx;
  librados::IoCtx &m_remote_io_ctx;
  std::string m_global_group_id;
  std::string m_local_mirror_uuid;
  std::string m_remote_mirror_uuid;
  PoolMetaCache* m_pool_meta_cache;
  std::string m_local_group_id;
  std::string m_remote_group_id;
  GroupCtx *m_local_group_ctx;
  std::list<std::pair<librados::IoCtx, ImageReplayer<ImageCtxT> *>> *m_image_replayers;

  mutable ceph::mutex m_lock;

  State m_state = STATE_INIT;
  std::string m_remote_mirror_peer_uuid;

  std::vector<cls::rbd::GroupSnapshot> m_local_group_snaps;
  std::vector<cls::rbd::GroupSnapshot> m_remote_group_snaps;

  bool m_update_group_state = true;

  Context* m_load_snapshots_task = nullptr;
  Context* m_on_shutdown = nullptr;

  AsyncOpTracker m_in_flight_op_tracker;
  bufferlist m_out_bl;

  int m_error_code = 0;
  std::string m_error_description;

  bool m_stop_requested = false;
  bool m_retry_validate_snap = false;

  utime_t m_snapshot_start;
  uint64_t m_last_snapshot_complete_seconds = 0;

  uint64_t m_last_snapshot_bytes = 0;

  bool is_replay_interrupted(std::unique_lock<ceph::mutex>* locker);

  void schedule_load_group_snapshots();
  void handle_schedule_load_group_snapshots(int r);
  void cancel_load_group_snapshots();

  void handle_replay_complete(std::unique_lock<ceph::mutex>* locker,
                              int r, const std::string& desc);
  void notify_group_listener();

  void local_group_image_list_by_id(
    bufferlist* out_bl_ptr,
    std::vector<cls::rbd::GroupImageStatus>* local_images_ptr,
    Context* on_finish);
  void handle_local_group_image_list_by_id(int r,
    bufferlist* out_bl_ptr,
    std::vector<cls::rbd::GroupImageStatus>* local_images_ptr,
    Context* on_finish);

  void validate_image_snaps_sync_complete(
    const cls::rbd::GroupSnapshot &local_snap, Context *on_finish);

  void validate_local_group_snapshots();
  void load_local_group_snapshots(std::unique_lock<ceph::mutex>* locker);
  void handle_load_local_group_snapshots(int r);

  void load_remote_group_snapshots();
  void handle_load_remote_group_snapshots(int r);
  void is_resync_requested();
  void handle_is_resync_requested(int r);
  void is_rename_requested();
  void handle_is_rename_requested(int r);
  void check_local_group_snapshots(std::unique_lock<ceph::mutex>* locker);

  void scan_for_unsynced_group_snapshots(std::unique_lock<ceph::mutex>* locker);

  void try_create_group_snapshot(std::string prev_snap_id,
                                 std::unique_lock<ceph::mutex>* locker);
  void create_group_snapshot(cls::rbd::GroupSnapshot snap,
                             std::unique_lock<ceph::mutex>* locker);

  void create_mirror_snapshot(
    cls::rbd::GroupSnapshot *snap,
    const cls::rbd::MirrorSnapshotState &snap_state,
    Context *on_finish);
  void handle_create_mirror_snapshot(
    int r, const std::string &group_snap_id, Context *on_finish);

  void update_local_group_state(cls::rbd::GroupSnapshot snap);
  void handle_update_local_group_state(int r, cls::rbd::GroupSnapshot snap);

  void mirror_snapshot_complete(
    const std::string &group_snap_id, Context *on_finish);
  void handle_mirror_snapshot_image_list(
    const std::string &group_snap_id,
    const cls::rbd::GroupSnapshot &local_snap,
    const cls::rbd::GroupSnapshot &remote_snap,
    const std::vector<cls::rbd::GroupImageStatus>& local_images,
    Context *on_finish);
  void post_mirror_snapshot_complete(
    const std::string &group_snap_id,
    const cls::rbd::GroupSnapshot &local_snap,
    const cls::rbd::GroupSnapshot &remote_snap,
    const std::vector<cls::rbd::GroupImageStatus>& local_images,
    Context *on_finish);
  void handle_post_mirror_snapshot_complete(
    int r, const std::string &group_snap_id, Context *on_finish);

  void create_regular_snapshot(
    cls::rbd::GroupSnapshot *snap,
    Context *on_finish);
  void handle_create_regular_snapshot(
      int r, const std::string &group_snap_id, Context *on_finish);

  void regular_snapshot_complete(
    const std::string &group_snap_id,
    Context *on_finish);
  void handle_regular_snapshot_image_list(
    const std::string &group_snap_id,
    const cls::rbd::GroupSnapshot &local_snap,
    const cls::rbd::GroupSnapshot &remote_snap,
    const std::vector<cls::rbd::GroupImageStatus>& local_images,
    Context *on_finish);
  void post_regular_snapshot_complete(
    const std::string &group_snap_id,
    const cls::rbd::GroupSnapshot &local_snap,
    const cls::rbd::GroupSnapshot &remote_snap,
    const std::vector<cls::rbd::GroupImageStatus>& local_images,
    Context *on_finish);
  void handle_post_regular_snapshot_complete(
    int r, const std::string &group_snap_id, Context *on_finish);

  void mirror_group_snapshot_unlink_peer(const std::string &snap_id);
  void handle_mirror_group_snapshot_unlink_peer(
      int r, const std::string &snap_id);

  bool prune_all_image_snapshots(
      cls::rbd::GroupSnapshot *local_snap,
      std::unique_lock<ceph::mutex>* locker);
  void prune_user_group_snapshots(std::unique_lock<ceph::mutex>* locker);
  void prune_mirror_group_snapshots(std::unique_lock<ceph::mutex>* locker);
  void prune_group_snapshots(std::unique_lock<ceph::mutex>* locker);

  void set_image_replayer_limits(const std::string &image_id,
                                 const cls::rbd::GroupSnapshot *remote_snap,
                                 std::unique_lock<ceph::mutex>* locker);
};

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::group_replayer::Replayer<librbd::ImageCtx>;

#endif // RBD_MIRROR_GROUP_REPLAYER_REPLAYER_H
