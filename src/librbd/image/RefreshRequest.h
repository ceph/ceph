// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_REFRESH_REQUEST_H
#define CEPH_LIBRBD_IMAGE_REFRESH_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/utime.h"
#include "common/snap_types.h"
#include "cls/lock/cls_lock_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/Types.h"
#include <string>
#include <vector>

class Context;

namespace librbd {

class ImageCtx;

namespace image {

template<typename> class RefreshParentRequest;

template<typename ImageCtxT = ImageCtx>
class RefreshRequest {
public:
  static constexpr int MAX_ENOENT_RETRIES = 10;

  static RefreshRequest *create(ImageCtxT &image_ctx, bool acquiring_lock,
                                bool skip_open_parent, Context *on_finish) {
    return new RefreshRequest(image_ctx, acquiring_lock, skip_open_parent,
                              on_finish);
  }

  RefreshRequest(ImageCtxT &image_ctx, bool acquiring_lock,
                 bool skip_open_parent, Context *on_finish);
  ~RefreshRequest();

  void send();

private:
  /**
   * @verbatim
   *
   * <start> < * * * * * * * * * * * * * * * * * * * * * * * * * * (ENOENT)
   *  ^ |                                                        *
   *  * | (v1)                                                   *
   *  * |-----> V1_READ_HEADER -------------> GET_MIGRATION_HEADER (skip if not
   *  * |                                                     |     migrating)
   *  * | (v2)                                                v
   *  * \-----> V2_GET_MUTABLE_METADATA                   V1_GET_SNAPSHOTS
   *  *    *        |                                         |
   *  *    *        |     -EOPNOTSUPP                         v
   *  *    *        |  * * *                              V1_GET_LOCKS
   *  *    *        |  *   *                                  |
   *  *    *        v  v   *                                  v
   *  *    *    V2_GET_PARENT                              <apply>
   *  *    *        |                                         |
   *  *             v                                         |
   *  * * * * * GET_MIGRATION_HEADER (skip if not             |
   *  (ENOENT)      |                 migrating)              |
   *                v                                         |
   *       *    V2_GET_METADATA                               |
   *       *        |                                         |
   *       *        v                                         |
   *       *    V2_GET_POOL_METADATA                          |
   *       *        |                                         |
   *       *        v (skip if not enabled)                   |
   *       *    V2_GET_OP_FEATURES                            |
   *       *        |                                         |
   *       *        v                                         |
   *       *    V2_GET_GROUP                                  |
   *       *        |                                         |
   *       *        |        -EOPNOTSUPP                      |
   *       *        |     * * *                               |
   *       *        |     *   *                               |
   *       *        v     v   *                               |
   *        * * V2_GET_SNAPSHOTS (skip if no snaps)           |
   *     (ENOENT)   |                                         |
   *       *        v                                         |
   *        * * V2_REFRESH_PARENT (skip if no parent or       |
   *     (ENOENT)   |              refresh not needed)        |
   *                v                                         |
   *            V2_INIT_EXCLUSIVE_LOCK (skip if lock          |
   *                |                   active or disabled)   |
   *                v                                         |
   *            V2_OPEN_OBJECT_MAP (skip if map               |
   *                |               active or disabled)       |
   *                v                                         |
   *            V2_OPEN_JOURNAL (skip if journal              |
   *                |            active or disabled)          |
   *                v                                         |
   *            V2_BLOCK_WRITES (skip if journal not          |
   *                |            disabled)                    |
   *                v                                         |
   *             <apply>                                      |
   *                |                                         |
   *                v                                         |
   *            V2_FINALIZE_REFRESH_PARENT (skip if refresh   |
   *                |                       not needed)       |
   *  (error)       v                                         |
   *  * * * * > V2_SHUT_DOWN_EXCLUSIVE_LOCK (skip if lock     |
   *                |                      active or enabled) |
   *                v                                         |
   *            V2_CLOSE_JOURNAL (skip if journal inactive    |
   *                |             or enabled)                 |
   *                v                                         |
   *            V2_CLOSE_OBJECT_MAP (skip if map inactive     |
   *                |                or enabled)              |
   *                |                                         |
   *                \-------------------\/--------------------/
   *                                    |
   *                                    v
   *                                  FLUSH (skip if no new
   *                                    |    snapshots)
   *                                    v
   *                                 <finish>
   *
   * @endverbatim
   */

  enum LegacySnapshot {
    LEGACY_SNAPSHOT_DISABLED,
    LEGACY_SNAPSHOT_ENABLED,
    LEGACY_SNAPSHOT_ENABLED_NO_TIMESTAMP
  };

  ImageCtxT &m_image_ctx;
  bool m_acquiring_lock;
  bool m_skip_open_parent_image;
  Context *m_on_finish;

  cls::rbd::MigrationSpec m_migration_spec;
  int m_error_result;
  bool m_flush_aio;
  decltype(m_image_ctx.exclusive_lock) m_exclusive_lock;
  decltype(m_image_ctx.object_map) m_object_map;
  decltype(m_image_ctx.journal) m_journal;
  RefreshParentRequest<ImageCtxT> *m_refresh_parent;

  bufferlist m_out_bl;

  bool m_legacy_parent = false;
  LegacySnapshot m_legacy_snapshot = LEGACY_SNAPSHOT_DISABLED;

  int m_enoent_retries = 0;

  uint8_t m_order = 0;
  uint64_t m_size = 0;
  uint64_t m_features = 0;
  uint64_t m_incompatible_features = 0;
  uint64_t m_flags = 0;
  uint64_t m_op_features = 0;
  uint32_t m_read_only_flags = 0U;
  bool m_read_only = false;

  librados::IoCtx m_pool_metadata_io_ctx;
  std::map<std::string, bufferlist> m_metadata;

  std::string m_object_prefix;
  ParentImageInfo m_parent_md;
  bool m_head_parent_overlap = false;
  cls::rbd::GroupSpec m_group_spec;

  ::SnapContext m_snapc;
  std::vector<cls::rbd::SnapshotInfo> m_snap_infos;
  std::vector<ParentImageInfo> m_snap_parents;
  std::vector<uint8_t> m_snap_protection;
  std::vector<uint64_t> m_snap_flags;

  std::map<rados::cls::lock::locker_id_t,
           rados::cls::lock::locker_info_t> m_lockers;
  std::string m_lock_tag;
  bool m_exclusive_locked = false;

  bool m_blocked_writes = false;
  bool m_incomplete_update = false;

  void send_get_migration_header();
  Context *handle_get_migration_header(int *result);

  void send_v1_read_header();
  Context *handle_v1_read_header(int *result);

  void send_v1_get_snapshots();
  Context *handle_v1_get_snapshots(int *result);

  void send_v1_get_locks();
  Context *handle_v1_get_locks(int *result);

  void send_v1_apply();
  Context *handle_v1_apply(int *result);

  void send_v2_get_mutable_metadata();
  Context *handle_v2_get_mutable_metadata(int *result);

  void send_v2_get_parent();
  Context *handle_v2_get_parent(int *result);

  void send_v2_get_metadata();
  Context *handle_v2_get_metadata(int *result);

  void send_v2_get_pool_metadata();
  Context *handle_v2_get_pool_metadata(int *result);

  void send_v2_get_op_features();
  Context *handle_v2_get_op_features(int *result);

  void send_v2_get_group();
  Context *handle_v2_get_group(int *result);

  void send_v2_get_snapshots();
  Context *handle_v2_get_snapshots(int *result);

  void send_v2_get_snapshots_legacy();
  Context *handle_v2_get_snapshots_legacy(int *result);

  void send_v2_refresh_parent();
  Context *handle_v2_refresh_parent(int *result);

  void send_v2_init_exclusive_lock();
  Context *handle_v2_init_exclusive_lock(int *result);

  void send_v2_open_journal();
  Context *handle_v2_open_journal(int *result);

  void send_v2_block_writes();
  Context *handle_v2_block_writes(int *result);

  void send_v2_open_object_map();
  Context *handle_v2_open_object_map(int *result);

  void send_v2_apply();
  Context *handle_v2_apply(int *result);

  Context *send_v2_finalize_refresh_parent();
  Context *handle_v2_finalize_refresh_parent(int *result);

  Context *send_v2_shut_down_exclusive_lock();
  Context *handle_v2_shut_down_exclusive_lock(int *result);

  Context *send_v2_close_journal();
  Context *handle_v2_close_journal(int *result);

  Context *send_v2_close_object_map();
  Context *handle_v2_close_object_map(int *result);

  Context *send_flush_aio();
  Context *handle_flush_aio(int *result);

  Context *handle_error(int *result);

  void save_result(int *result) {
    if (m_error_result == 0 && *result < 0) {
      m_error_result = *result;
    }
  }

  void apply();
  int get_parent_info(uint64_t snap_id, ParentImageInfo *parent_md,
                      MigrationInfo *migration_info);
  int get_migration_info(ParentImageInfo *parent_md,
                         MigrationInfo *migration_info,
                         bool* migration_info_valid);
};

} // namespace image
} // namespace librbd

extern template class librbd::image::RefreshRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_REFRESH_REQUEST_H
