// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_GROUP_DEMOTE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_GROUP_DEMOTE_REQUEST_H

#include "include/buffer.h"
#include "include/rbd/librbd.hpp"
#include "common/ceph_mutex.h"
#include "common/Timer.h"
#include "librbd/internal.h"
#include "librbd/Utils.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/mirror/Types.h"
#include <string>
#include <set>
#include <vector>
#include <list>

struct Context;

namespace librbd {
struct ImageCtx;

namespace mirror {

template <typename ImageCtxT = librbd::ImageCtx>
class GroupDemoteRequest {
public:
  static GroupDemoteRequest *create(librados::IoCtx &io_ctx,
                                     const std::string &group_id,
                                     const std::string &group_name,
                                     Context *on_finish) {
    return new GroupDemoteRequest(io_ctx, group_id, group_name,
                                   on_finish);
  }

  GroupDemoteRequest(librados::IoCtx &io_ctx,
                      const std::string &group_id,
                      const std::string &group_name,
                      Context *on_finish)
    : m_group_ioctx(io_ctx), m_group_id(group_id), m_group_name(group_name),
      m_on_finish(on_finish), m_cct(reinterpret_cast<CephContext*>(io_ctx.cct())) {
}

  void send();

private:

  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_MIRROR_GROUP_INFO
   *    |
   *    v                   (if group is empty)
   * PREPARE_GROUP_IMAGES------------------------------->
   *    |                                               |
   *    v                                               |
   * ACQUIRE_EXCLUSIVE_LOCKS                            |
   *    |                                               |
   *    v  (incomplete)                                 |
   * CREATE_PRIMARY_GROUP_SNAPSHOT <--------------------/
   *    |                        |
   *    v                        v----------------------\
   * ENABLE_NON_PRIMARY_FEATURES        (if no images)  |
   *    |                                               |
   *    v                                               |
   * CREATE_IMAGES_PRIMARY_SNAPSHOTS                    |
        |                                               |
   *    v  (complete)                                   |
   * UPDATE_PRIMARY_GROUP_SNAPSHOT <--------------------/
   *    |
   *    v
   * GROUP_UNLINK_PEER
   *    |
   *    v   (skip if not needed)
   * RELEASE_EXCLUSIVE_LOCKS
   *    |
   *    v
   * CLOSE_IMAGES
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  librados::IoCtx &m_group_ioctx;
  const std::string m_group_id;
  const std::string m_group_name;
  Context *m_on_finish;
  CephContext *m_cct;

  cls::rbd::MirrorGroup m_mirror_group;
  std::set<std::string> m_mirror_peer_uuids;
  std::vector<cls::rbd::GroupImageStatus> m_images;
  std::vector<ImageCtxT *> m_image_ctxs;
  std::vector<bool> m_locks_acquired;
  bool m_excl_locks_acquired = false;

  int m_ret_val = 0;
  cls::rbd::GroupSnapshot m_group_snap;
  PromotionState m_promotion_state;

  std::vector<cls::rbd::MirrorImage> m_mirror_images;
  std::vector<uint64_t> m_snap_ids;
  std::vector<std::string> m_global_image_ids;

  void get_mirror_group();
  void handle_get_mirror_group(int r);

  void get_mirror_group_info();
  void handle_get_mirror_group_info(int r);

  void prepare_group_images();
  void handle_prepare_group_images(int r);

  void acquire_exclusive_locks();
  void handle_acquire_exclusive_locks(int r);

  void create_primary_group_snapshot();
  void handle_create_primary_group_snapshot(int r);

  void enable_non_primary_features();
  void handle_enable_non_primary_features(int r);

  void create_images_primary_snapshots();
  void handle_create_images_primary_snapshots(int r);

  void update_primary_group_snapshot();
  void handle_update_primary_group_snapshot(int r);

  void group_unlink_peer();
  void handle_group_unlink_peer(int r);

  void release_exclusive_locks();
  void handle_release_exclusive_locks(int r);

  void close_images();
  void handle_close_images(int r);

  void remove_primary_group_snapshot();
  void handle_remove_primary_group_snapshot(int r);

  void finish(int r);
};


} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::GroupDemoteRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_GROUP_DEMOTE_REQUEST_H
