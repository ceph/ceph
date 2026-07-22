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
/**
 * GROUP REPLAYER:
 * ==============
 *
 * At a high level, the Replayer keeps the local mirror group on the secondary
 * cluster synchronized with the remote primary group. It loads local and
 * remote group snapshots, validates snapshot synchronization, creates new
 * group snapshots, waits for image replay and marks group snaps to complete,
 * updates group state, and prunes snapshots that are no longer needed.
 *
 * STATES:
 * ======
 *
 * STATE_INIT: Initial state after construction. No work has been started.
 * STATE_REPLAYING: Actively loading snapshots, creating new snapshots and wait
 *             for replays to complete. And prune snapshots no longer needed.
 * STATE_IDLE: Up to date with the snapshots sync. No work pending. Waiting for
 *             the scheduler to trigger the next scan.
 * STATE_COMPLETE: Shutdown has completed. No further work will be scheduled.
 *
 *
 * STATE MACHINE:
 * =============
 *
 * @verbatim
 *                                             CONSTRUCTOR
 *                                                 | m_state = STATE_INIT
 *                                                 v
 *                                               INIT
 *                                                 | m_state = STATE_REPLAYING
 *                                                 v
 *                                   LOAD_LOCAL_GROUP_SNAPSHOTS <----------------------------------------------------------------------------- +
 *                                                 |  if an incomplete mirror snapshot is found, set m_retry_validate_snap = true              ^
 *                                                 |                                                                                           |
 *                                                 v determine local group role: primary?                                                      |
 *                                                 + -------------------------------------> HANDLE_REPLAY_COMPLETE                             |
 *                                                 | non-primary                                                                               |
 *                                                 v                                          snaps in creating phase exists                   |
 *                 + <---------------------------- + -----------------------------------------------------------------> +                      |
 *                 |                                                                                                    |                      |
 *                 v                                                                                                    v                      |
 *      LOAD_REMOTE_GROUP_SNAPSHOTS --------------------------------------------> +                     PRUNE_CREATING_GROUP_SNAPSHOTS       ^ |
 *                 |                                                              |                                     |                    | |
 *                 + <-------------------------------------- +                    v                                     v            error --+ |
 *                 v                   demoted/split-brain   ^          PRUNE_GROUP_SNAPSHOTS           LOCAL_GROUP_IMAGE_LIST_BY_ID --------> +
 *      CHECK_LOCAL_GROUP_SNAPSHOTS ----------> +            |                    |                                     |                      ^
 *                 |                            |            |                    v                                     v                      |
 *                 v          yes               v            |         PRUNE_USER_GROUP_SNAPSHOTS       PRUNE_ALL_IMAGE_SNAPSHOTS_BY_GSID      |
 *   IS_RESYNC_REQUESTED ----------> HANDLE_REPLAY_COMPLETE  |                    |                                     |                      |
 *                 |                            ^            |                    v                                     v                      |
 *                 v          yes               |            |        PRUNE_MIRROR_GROUP_SNAPSHOT             PRUNE_IMAGE_SNAPSHOT             |
 *   IS_RENAME_REQUESTED ---------------------> +            |                    |                                     |                      |
 *                 |                                         |                    v                                     v                   ^  |
 *                 v   m_retry_validate_snap == true         |          PRUNE_GROUP_SNAPSHOT                   GROUP_SNAP_REMOVE            |  |
 *                 + --------------------------------> +     |                    |                                     |                 --+  |
 *                 |                                   |     |                    |              if all_pruned == true, + -------------------> +
 *                 v                                   |     |                    v                            m_check_creating_snaps = false  ^
 *  SCAN_FOR_UNSYNCED_GROUP_SNAPSHOTS                  |     |        PRUNE_ALL_IMAGE_SNAPSHOTS                                                |
 *                 |                                   |     |                    |                                                            |
 *                 |    if all snaps synced            v    ╭─╮  -EAGAIN          v                                                            |
 *                 + --------> m_state = STATE_IDLE -> + <--╯|╰----------- PRUNE_IMAGE_SNAPSHOT                                              ^ |
 *                 |                                   |     |                    |                                                          | |
 *                 v                                   |     |    error           v              (all image snapshots removed)             --+ |
 *       CREATE_USER_SNAPSHOT (if required)            |     + <---------- GROUP_SNAP_REMOVE ------------------------------------------------> +
 *                 |                                   |                                          m_refresh_snaps = true                       |
 *                 v                                   |                                                                                       |
 *       CREATE_MIRROR_SNAPSHOT                        |                                                                                       |
 *                 |                                   |                                                                                       |
 *                 v                                   |                                                                                       |
 *        UPDATE_LOCAL_GROUP_STATE                     |                                                                                       |
 *                 |                                   |                                                                                       |
 *                 \ ------->------------------------> +                                                                                     ^ |
 *                                                     |                                                                                     | |
 *                                                     v                             m_retry_validate_snap == false                        --+ |
 *                                         SCHEDULE_LOAD_GROUP_SNAPSHOTS --------------------------------------------------------------------> +
 *                                                     |                                                                                       ^
 *                                                     v  m_state = STATE_REPLAYING                                                            |
 *                                       VALIDATE_LOCAL_GROUP_SNAPSHOTS                                                                        |
 *                                                     |                                                                                       |
 *                                                     v iterate m_local_group_snaps                                                           |
 *                                 + ----------------- + ---------------- +                                                                    |
 *                snapshot already |                                      | snapshot requires                                                  |
 *                   complete      |                                      v     validation                                                     |
 *                                 |                       VALIDATE_IMAGE_SNAPS_SYNC_COMPLETE                                                  |
 *                                 |                                      |                                                                    |
 *                                 |                                      v                                                                    |
 *                                 |                         + ---------- +----------- +                                                       |
 *                                 |     is mirror snapshot  |                         | is user snapshot                                      |
 *                                 |                         v                         v                                                       |
 *                                 |             MIRROR_SNAPSHOT_COMPLETE      USER_SNAPSHOT_COMPLETE                                          |
 *                                 |                         |                         |                                                       |
 *                                 |                         v                         v                                                       |
 *                                 |         LOCAL_GROUP_IMAGE_LIST_BY_ID      LOCAL_GROUP_IMAGE_LIST_BY_ID                                    |
 *                                 |                         |                         |                                                       |
 *                                 |                         v                         v                                                       |
 *                                 |    HANDLE_MIRROR_SNAPSHOT_IMAGE_LIST      HANDLE_USER_SNAPSHOT_IMAGE_LIST                                 |
 *                                 |                         |                         |                                                       |
 *                                 |                         v                         v                                                       |
 *                                 |         POST_MIRROR_SNAPSHOT_CREATED      POST_USER_SNAPSHOT_CREATED                                      |
 *                                 |                         |                         |                                                       |
 *                                 |                         v                         |                                                       |
 *                                 |      CHECK_MIRROR_SNAPSHOT_SYNC_COMPLETE          |                                                       |
 *                                 |                         |                         |                                                       |
 *                                 |                         v                         |                                                       |
 *                                 |               + ------- + ------- +               |                                                       |
 *                                 |        images |                   | all image     |                                                       |
 *                                 |       syncing |                   | snapshots     |                                                       |
 *                                 |  (retry next  |                   |  sycned       |                                                       |
 *                                 |     cycle)    |                   v               |                                                       |
 *                                 |               |    SET_MIRROR_SNAPSHOT_COMPLETE   |                                                       |
 *                                 |               |                   |               |                                                       |
 *                                 |               |                   v               |                                                       |
 *                                 |               | MIRROR_GROUP_SNAPSHOT_UNLINK_PEER |                                                       |
 *                                 |               |                   |               |                                                       |
 *                                 v               v                   v               v                                                       |
 *                                 + ------------- + ----------------- + ------------- +                                                       |
 *                                                           |                                                                                 |
 *                                                           v c_gather waits for all callbacks                                                |
 *                                                           + ------------------------------------------------------------------------------> +
 *
 *
 *
 *
 * SHUTDOWN PATH
 * =============
 *
 * SHUT_DOWN
 *      |
 *      + -> CANCEL_LOAD_GROUP_SNAPSHOTS
 *      |
 *      + -> WAIT_FOR_IN_FLIGHT_OPS
 *                |
 *                v
 *           STATE_COMPLETE
 *
 * @endverbatim
 *
 * HELPER OPERATIONS (NOT REPRESENTED ABOVE)
 * =================
 * GET_REPLAYERS_BY_IMAGE_ID
 * GET_GLOBAL_IMAGE_ID
 * SET_IMAGE_REPLAYER_LIMITS
 * SET_IMAGE_REPLAYER_END_LIMITS
 * NOTIFY_GROUP_LISTENER
 * IS_REPLAY_INTERRUPTED
 * GET_REPLAY_STATUS
 *
 */

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
  std::vector<std::pair<std::string, ImageReplayer<ImageCtxT> *>> m_replayers_by_image_id;
  const cls::rbd::GroupSnapshot* m_last_local_snap = nullptr;
  const cls::rbd::GroupSnapshot* m_prune_group_snap = nullptr;
  bool m_update_group_state = true;

  Context* m_load_snapshots_task = nullptr;
  Context* m_on_shutdown = nullptr;

  AsyncOpTracker m_in_flight_op_tracker;
  bufferlist m_out_bl;

  int m_error_code = 0;
  std::string m_error_description;

  bool m_retry_validate_snap = false;
  std::string m_remote_snap_id_start = "";
  bool m_peer_unlink = false;

  utime_t m_snapshot_start;
  uint64_t m_last_snapshot_complete_seconds = 0;

  uint64_t m_last_snapshot_bytes = 0;

  bool m_check_creating_snaps = true; // check and identify creating snaps just after restart
  bool m_resync_requested = false;
  bool m_refresh_snaps = false;

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

  void is_resync_requested();
  void handle_is_resync_requested(int r);
  void is_rename_requested();
  void handle_is_rename_requested(int r);
  void validate_local_group_snapshots(std::unique_lock<ceph::mutex>* locker);
  void load_local_group_snapshots(std::unique_lock<ceph::mutex>* locker);
  void handle_load_local_group_snapshots(int r);

  void load_remote_group_snapshots();
  void handle_load_remote_group_snapshots(int r);
  void check_local_group_snapshots(std::unique_lock<ceph::mutex>* locker);

  void scan_for_unsynced_group_snapshots(std::unique_lock<ceph::mutex>* locker);

  void create_mirror_snapshot(
    cls::rbd::GroupSnapshot *snap, Context *on_finish);
  void handle_create_mirror_snapshot(
    int r, cls::rbd::GroupSnapshot *snap, Context *on_finish);

  void update_local_group_state(cls::rbd::GroupSnapshot snap,
                                Context *on_finish);
  void handle_update_local_group_state(int r, cls::rbd::GroupSnapshot snap,
                                       Context *on_finish);

  void mirror_snapshot_complete(
    const std::string &group_snap_id, Context *on_finish);
  void handle_mirror_snapshot_image_list(
    const std::string &group_snap_id,
    const cls::rbd::GroupSnapshot &local_snap,
    const cls::rbd::GroupSnapshot &remote_snap,
    const std::vector<cls::rbd::GroupImageStatus>& local_images,
    Context *on_finish);
  void post_mirror_snapshot_created(
    const std::string &group_snap_id,
    const cls::rbd::GroupSnapshot &local_snap,
    const cls::rbd::GroupSnapshot &remote_snap,
    const std::vector<cls::rbd::GroupImageStatus>& local_images,
    Context *on_finish);
  void handle_post_mirror_snapshot_created(
    int r, const std::string &group_snap_id,
    const cls::rbd::GroupSnapshot &local_snap,
    Context *on_finish);
  void check_mirror_snapshot_sync_complete(
    const std::string &group_snap_id,
    const cls::rbd::GroupSnapshot &local_snap,
    Context *on_finish);
  void set_mirror_snapshot_complete(
    const std::string &group_snap_id,
    const cls::rbd::GroupSnapshot &local_snap,
    Context *on_finish);
  void handle_set_mirror_snapshot_complete(
    int r, const std::string &group_snap_id, Context *on_finish);

  void create_user_snapshot(
    cls::rbd::GroupSnapshot *snap,
    Context *on_finish);
  void handle_create_user_snapshot(
      int r, const std::string &group_snap_id, Context *on_finish);

  void user_snapshot_complete(
    const std::string &group_snap_id,
    Context *on_finish);
  void handle_user_snapshot_image_list(
    const std::string &group_snap_id,
    const cls::rbd::GroupSnapshot &local_snap,
    const cls::rbd::GroupSnapshot &remote_snap,
    const std::vector<cls::rbd::GroupImageStatus>& local_images,
    Context *on_finish);
  void post_user_snapshot_created(
    const std::string &group_snap_id,
    const cls::rbd::GroupSnapshot &local_snap,
    const cls::rbd::GroupSnapshot &remote_snap,
    const std::vector<cls::rbd::GroupImageStatus>& local_images,
    Context *on_finish);
  void handle_post_user_snapshot_created(
    int r, const std::string &group_snap_id, Context *on_finish);

  void mirror_group_snapshot_unlink_peer(const std::string &snap_id);
  void handle_mirror_group_snapshot_unlink_peer(
      int r, const std::string &snap_id);

  void prune_image_snapshot(ImageReplayer<ImageCtxT>* image_replayer,
      uint64_t snap_id,
      std::unique_lock<ceph::mutex>& locker);
  bool prune_all_image_snapshots_by_gsid(
      const std::string &group_snap_id,
      const std::vector<cls::rbd::GroupImageStatus>& local_images,
      std::unique_lock<ceph::mutex>* locker);
  bool prune_all_image_snapshots(
      const cls::rbd::GroupSnapshot *local_snap,
      std::unique_lock<ceph::mutex>* locker);

  void prune_creating_group_snapshots(
      std::shared_ptr<std::vector<cls::rbd::GroupSnapshot>> prune_creating_snaps);
  void handle_prune_creating_group_snapshots(
      const std::vector<cls::rbd::GroupImageStatus>& local_images,
      const std::vector<cls::rbd::GroupSnapshot>& prune_creating_snaps);

  int prune_group_snapshots(std::unique_lock<ceph::mutex>* locker);
  int prune_user_group_snapshots(std::unique_lock<ceph::mutex>* locker);
  int prune_mirror_group_snapshot(std::unique_lock<ceph::mutex>* locker);
  int prune_group_snapshot(const cls::rbd::GroupSnapshot* snap,
                           std::unique_lock<ceph::mutex>* locker);

  void get_replayers_by_image_id(std::unique_lock<ceph::mutex>* locker);
  std::string get_global_image_id(ImageReplayer<ImageCtxT>* image_replayer,
      std::unique_lock<ceph::mutex>& locker);
  void set_image_replayer_end_limits(ImageReplayer<ImageCtxT>* image_replayer,
      uint64_t snap_id, std::unique_lock<ceph::mutex>& locker);
  void set_image_replayer_limits(const std::string &image_id,
                                 const cls::rbd::GroupSnapshot *remote_snap,
                                 std::unique_lock<ceph::mutex>* locker);
  void wait_for_in_flight_ops();
  void handle_wait_for_in_flight_ops(int r);
};

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::group_replayer::Replayer<librbd::ImageCtx>;

#endif // RBD_MIRROR_GROUP_REPLAYER_REPLAYER_H
