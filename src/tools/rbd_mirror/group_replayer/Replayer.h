// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef RBD_MIRROR_GROUP_REPLAYER_REPLAYER_H
#define RBD_MIRROR_GROUP_REPLAYER_REPLAYER_H

#include "common/AsyncOpTracker.h"
#include "common/ceph_mutex.h"
#include "cls/rbd/cls_rbd_types.h"
#include "include/rados/librados.hpp"
#include "librbd/mirror/snapshot/Types.h"
#include "tools/rbd_mirror/Types.h"
#include "tools/rbd_mirror/image_replayer/Replayer.h"
#include "tools/rbd_mirror/image_replayer/Types.h"
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
      const std::string& remote_mirror_uuid,
      PoolMetaCache* pool_meta_cache,
      std::string local_group_id,
      std::string remote_group_id,
      GroupCtx *local_group_ctx,
      std::list<std::pair<librados::IoCtx, ImageReplayer<ImageCtxT> *>> *image_replayers) {
    return new Replayer(threads, local_io_ctx, remote_io_ctx, global_group_id,
        local_mirror_uuid, remote_mirror_uuid, pool_meta_cache, local_group_id,
        remote_group_id, local_group_ctx, image_replayers);
  }

  Replayer(
      Threads<ImageCtxT>* threads,
      librados::IoCtx &local_io_ctx,
      librados::IoCtx &remote_io_ctx,
      const std::string &global_group_id,
      const std::string& local_mirror_uuid,
      const std::string& remote_mirror_uuid,
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

  Context* m_load_snapshots_task = nullptr;
  Context* m_on_shutdown = nullptr;

  AsyncOpTracker m_in_flight_op_tracker;

  bool m_stop_requested = false;

  // map of <group_snap_id, pair<GroupSnapshot, on_finish>>
  std::map<std::string, std::pair<cls::rbd::GroupSnapshot, Context *>> m_create_snap_requests;

  // map of <group_snap_id, vec<pair<cls::rbd::ImageSnapshotSpec, bool>>>
  std::map<std::string, std::vector<std::pair<cls::rbd::ImageSnapshotSpec, bool>>> m_pending_group_snaps;

  bool is_replay_interrupted();
  bool is_replay_interrupted(std::unique_lock<ceph::mutex>* locker);
  int local_group_image_list_by_id(
      std::vector<cls::rbd::GroupImageStatus> *image_ids);

  void schedule_load_group_snapshots();
  void handle_schedule_load_group_snapshots(int r);
  void cancel_load_group_snapshots();

  void notify_group_listener_stop();
  bool is_resync_requested();
  bool is_rename_requested();

  void load_local_group_snapshots();
  void handle_load_local_group_snapshots(int r);

  void load_remote_group_snapshots();
  void handle_load_remote_group_snapshots(int r);

  void validate_image_snaps_sync_complete(const std::string &remote_group_snap_id);
  void scan_for_unsynced_group_snapshots();

  void try_create_group_snapshot(cls::rbd::GroupSnapshot snap,
                                 std::unique_lock<ceph::mutex> &locker);

  void create_mirror_snapshot(
    cls::rbd::GroupSnapshot *snap,
    const cls::rbd::MirrorSnapshotState &snap_state,
    std::unique_lock<ceph::mutex> &locker,
    Context *on_finish);
  void handle_create_mirror_snapshot(
    int r, const std::string &remote_group_snap_id, Context *on_finish);

  std::string prepare_non_primary_mirror_snap_name(
    const std::string &global_group_id, const std::string &snap_id);

  void mirror_snapshot_complete(
    const std::string &remote_group_snap_id,
    cls::rbd::ImageSnapshotSpec *spec,
    Context *on_finish);
  void handle_mirror_snapshot_complete(
    int r, const std::string &remote_group_snap_id, Context *on_finish);

  void remove_mirror_peer_uuid(cls::rbd::GroupSnapshot *remote_snap);
  bool prune_all_image_snapshots(cls::rbd::GroupSnapshot *local_snap);
  void unlink_group_snapshots(const std::string &remote_group_snap_id);

  void create_regular_snapshot(
    const std::string &remote_group_snap_name,
    const std::string &remote_group_snap_id,
    Context *on_finish);
  void handle_create_regular_snapshot(int r, Context *on_finish);
  void set_image_replayer_limits(const std::string &image_id,
                                 cls::rbd::GroupSnapshot *remote_snap);

  void regular_snapshot_complete(
    const std::string &remote_group_snap_id,
    Context *on_finish);
  void handle_regular_snapshot_complete(int r, Context *on_finish);
};

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::group_replayer::Replayer<librbd::ImageCtx>;

#endif // RBD_MIRROR_GROUP_REPLAYER_REPLAYER_H
