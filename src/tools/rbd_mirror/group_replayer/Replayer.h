// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_GROUP_REPLAYER_REPLAYER_H
#define RBD_MIRROR_GROUP_REPLAYER_REPLAYER_H

#include "tools/rbd_mirror/image_replayer/Replayer.h"
#include "common/ceph_mutex.h"
#include "cls/rbd/cls_rbd_types.h"
#include "include/rados/librados.hpp"
#include "librbd/mirror/snapshot/Types.h"
#include "tools/rbd_mirror/Types.h"
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

  bool is_replaying() const {
    std::unique_lock locker{m_lock};
    return (m_state == STATE_REPLAYING || m_state == STATE_IDLE);
  }

  void notify_group_snap_image_complete(
    int64_t local_pool_id,
    const std::string &local_image_id,
    const std::string &remote_group_snap_id,
    uint64_t local_snap_id);

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
  Context* m_on_shutdown = nullptr;
  std::string m_remote_mirror_peer_uuid;

  std::vector<cls::rbd::GroupSnapshot> m_local_group_snaps;
  std::vector<cls::rbd::GroupSnapshot> m_remote_group_snaps;

  bool m_remote_demoted = false;
  bool m_resync_requested = false;
  bool m_rename_requested = false;

  // map of <group_snap_id, pair<GroupSnapshot, on_finish>>
  std::map<std::string, std::pair<cls::rbd::GroupSnapshot, Context *>> m_create_snap_requests;

  // map of <group_snap_id, vec<pair<cls::rbd::ImageSnapshotSpec, bool>>>
  std::map<std::string, std::vector<std::pair<cls::rbd::ImageSnapshotSpec, bool>>> m_pending_group_snaps;

  int local_group_image_list_by_id(
      std::vector<cls::rbd::GroupImageStatus> *image_ids);

  void schedule_load_group_snapshots();
  void notify_group_listener_stop();
  bool is_resync_requested();
  bool is_rename_requested();

  void load_local_group_snapshots();
  void handle_load_local_group_snapshots(int r);

  void load_remote_group_snapshots();
  void handle_load_remote_group_snapshots(int r);

  void scan_for_unsynced_group_snapshots(std::unique_lock<ceph::mutex>& locker);

  void try_create_group_snapshot(cls::rbd::GroupSnapshot snap);

  void create_mirror_snapshot(
    const std::string &remote_group_snap_id,
    const cls::rbd::MirrorSnapshotState &snap_state);
  void handle_create_mirror_snapshot(
    const std::string &remote_group_snap_id, int r);

  std::string prepare_non_primary_mirror_snap_name(
    const std::string &global_group_id, const std::string &snap_id);

  void mirror_snapshot_complete(
    const std::string &remote_group_snap_id,
    cls::rbd::ImageSnapshotSpec *spec,
    Context *on_finish);
  void handle_mirror_snapshot_complete(
    int r, const std::string &remote_group_snap_id, Context *on_finish);

  void unlink_group_snapshots(const std::string &remote_group_snap_id);

  void update_image_snapshot(
    const std::string &remote_group_snap_id,
    cls::rbd::ImageSnapshotSpec spec,
    Context *on_finish);
  void handle_update_image_snapshot(
    int r, uint64_t local_snap_id, Context *on_finish);

  void mirror_regular_snapshot(
    const std::string &remote_group_snap_name,
    const std::string &remote_group_snap_id,
    std::vector<cls::rbd::GroupImageStatus> *local_images,
    Context *on_finish);
  void handle_mirror_regular_snapshot(int r, Context *on_finish);
};

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::group_replayer::Replayer<librbd::ImageCtx>;

#endif // RBD_MIRROR_GROUP_REPLAYER_REPLAYER_H
