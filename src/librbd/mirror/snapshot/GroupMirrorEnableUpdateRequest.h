// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_GROUP_MIRROR_ENABLE_UPDATE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_GROUP_MIRROR_ENABLE_UPDATE_REQUEST_H

#include "include/rados/librados_fwd.hpp"
#include "include/buffer_fwd.h"
#include "cls/rbd/cls_rbd_types.h"

#include <unordered_map>
#include <set>
#include <string>
#include <vector>

struct Context;
struct CephContext;

namespace librbd {

class ImageCtx;

namespace mirror {
namespace snapshot {

/**
 * @brief Create primary mirror group snapshot and enable mirroring.
 *
 * Coordinates:
 *   - Primary group snapshot creation
 *   - Per-image primary snapshot creation
 *   - Mirror image enablement
 *
 * On failure, performs ordered rollback depending on Type.
 */
template <typename I>
class GroupMirrorEnableUpdateRequest {
public:
  enum class Type {
    ENABLE,
    ADD_IMAGE
  };

  static GroupMirrorEnableUpdateRequest* create(librados::IoCtx& group_ioctx,
    const std::string& group_id, cls::rbd::MirrorGroup& mirror_group,
    std::vector<I*>& image_ctxs, std::vector<cls::rbd::GroupImageStatus>& images,
    const std::set<std::string>& mirror_peer_uuids,
    cls::rbd::MirrorImageMode mode,
    uint64_t group_snap_create_flags, const std::string& image_id,
    Type type, const std::unordered_map<std::string,
    std::string> &image_to_global_id, Context* on_finish);

  GroupMirrorEnableUpdateRequest(librados::IoCtx& group_ioctx,
    const std::string& group_id, cls::rbd::MirrorGroup& mirror_group,
    std::vector<I*>& image_ctxs, std::vector<cls::rbd::GroupImageStatus>& images,
    const std::set<std::string>& mirror_peer_uuids,
    cls::rbd::MirrorImageMode mode, uint64_t group_snap_create_flags,
    const std::string& image_id, Type type, const std::unordered_map<std::string,
    std::string> &image_to_global_id, Context* on_finish);

  void send();

private:

/**
 * @verbatim
 *
 * <start>
 *    |
 *    v                                      (on error)
 * VALIDATE_IMAGES   * * * * * * * * * * * * * * * * * * *
 *    |                                                  *
 *    v                                                  *
 * SET_MIRROR_GROUP_ENABLING                             *
 *    |                                                  *
 *    v                                                  *
 * CREATE_PRIMARY_GROUP_SNAPSHOT                         *
 *    |                                                  *
 *    v                                                  *
 * CREATE_PRIMARY_IMAGE_SNAPSHOTS                        *
 *    |                                                  *
 *    v                                                  *
 * UPDATE_PRIMARY_GROUP_SNAPSHOT                         *
 *    |                                                  *
 *    v                                                  *
 * SET_MIRROR_IMAGES_ENABLED                             *
 *    |                                                  *
 *    v                                                  *
 * SET_MIRROR_GROUP_ENABLED                              *
 *    |                                                  *
 *    v                                                  *
 * NOTIFY_MIRRORING_WATCHER                              *
 *    |                                                  *
 *    v                                                  *
 * CLOSE_IMAGES  < * * * * * * * * * * * * * * * * * * * *
 *    |
 *    v
 * <finish>
 *
 *
 * Error:
 *
 * DISABLE_MIRROR_GROUP  * * * * * * * * * * * * * * * *
 *    |                                                  *
 *    v                                                  *
 * GET_MIRROR_IMAGES_FOR_CLEANUP                         *
 *    |                                                  *
 *    v                                                  *
 * DISABLE_MIRROR_IMAGES                                 *
 *    |                                                  *
 *    v                                                  *
 * REMOVE_PRIMARY_GROUP_SNAPSHOT                         *
 *    |                                                  *
 *    v                                                  *
 * REMOVE_MIRROR_IMAGES                                  *
 *    |                                                  *
 *    v                                                  *
 * REMOVE_MIRROR_GROUP                                   *
 *    |                                                  *
 *    v                                                  *
 * CLOSE_IMAGES  < * * * * * * * * * * * * * * * * * * * *
 *
 * @endverbatim
 */

  std::unordered_map<std::string, I*> m_image_ctx_map;

  librados::IoCtx& m_group_ioctx;
  std::string m_group_id;
  cls::rbd::MirrorGroup& m_mirror_group;
  std::vector<I*>& m_image_ctxs;
  std::vector<cls::rbd::GroupImageStatus> m_images;
  std::set<std::string> m_mirror_peer_uuids;
  cls::rbd::MirrorImageMode m_mode;
  uint64_t m_group_snap_create_flags;
  std::string m_image_id;
  Type m_type;
  std::unordered_map<std::string, std::string> m_image_to_global_id;
  Context* m_on_finish;

  CephContext* m_cct = nullptr;
  int m_ret_val = 0;

  cls::rbd::GroupSnapshot m_group_snap;

  std::vector<std::string> m_global_image_ids;
  std::vector<uint64_t> m_snap_ids;
  std::vector<std::pair<std::string, cls::rbd::MirrorImage>> m_mirror_images;

  std::vector<ceph::bufferlist> m_out_bls;

  bool m_need_to_cleanup_mirror_images = false;
  bool m_need_to_cleanup_group_snapshot = false;

  // Primary flow
  void validate_images();

  void set_mirror_group_enabling();
  void handle_set_mirror_group_enabling(int r);

  void create_primary_group_snapshot();
  void handle_create_primary_group_snapshot(int r);

  void create_primary_image_snapshots();
  void handle_create_primary_image_snapshots(int r);

  void update_primary_group_snapshot();
  void handle_update_primary_group_snapshot(int r);

  void set_mirror_images_enabled();
  void handle_set_mirror_images_enabled(int r);

  void set_mirror_group_enabled();
  void handle_set_mirror_group_enabled(int r);

  void notify_mirroring_watcher();
  void handle_notify_mirroring_watcher(int r);

  // Cleanup
  void disable_mirror_group();
  void handle_disable_mirror_group(int r);

  void get_mirror_images_for_cleanup();
  void handle_get_mirror_images_for_cleanup(int r);

  void disable_mirror_images();
  void handle_disable_mirror_images(int r);

  void remove_primary_group_snapshot();
  void handle_remove_primary_group_snapshot(int r);

  void remove_mirror_images();
  void handle_remove_mirror_images(int r);

  void remove_mirror_group();
  void handle_remove_mirror_group(int r);

  void close_images();
  void handle_close_images(int r);
  void finish(int r);
};

} // namespace snapshot
} // namespace mirror
} // namespace librbd

extern template class
librbd::mirror::snapshot::GroupMirrorEnableUpdateRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_GROUP_MIRROR_ENABLE_UPDATE_REQUEST_H
