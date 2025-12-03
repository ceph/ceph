// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_GROUP_DISABLE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_GROUP_DISABLE_REQUEST_H

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
struct obj_watch_t;

namespace librbd {
struct ImageCtx;

namespace mirror {

template <typename ImageCtxT = librbd::ImageCtx>
class GroupDisableRequest {
public:
  static GroupDisableRequest *create(librados::IoCtx &io_ctx,
                                     const std::string &group_id,
                                     const std::string &group_name,
                                     bool force,
                                     Context *on_finish) {
    return new GroupDisableRequest(io_ctx, group_id, group_name, force,
                                   on_finish);
  }

  GroupDisableRequest(librados::IoCtx &io_ctx,
                      const std::string &group_id,
                      const std::string &group_name,
                      bool force,
                      Context *on_finish)
    : m_group_ioctx(io_ctx), m_group_id(group_id), m_group_name(group_name), m_force(force),
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
   *    v               (if group is empty)
   * LIST_GROUP_IMAGES---------------------------------->
   *    |                                               |
   *    v                                               |
   * OPEN_IMAGES                                        |
   *    |                                               |
   *    v                                               |
   * REFRESH_IMAGES                                     |
   *    |                                               |
   *    v                                               |
   * GET_IMAGES_MIRROR_MODE                             |
   *    |                                               |
   *    v                                               |
   * GET_IMAGES_MIRROR_INFO                             |
   *    |                                               |
   *    v                                               |
   * CHECK_IMAGES_CHILD_MIRRORING                       |
   *    |                                               |
   *    v                                               |
   * UPDATE_IMAGES_READ_ONLY_MASK                       |
   *    |                                               |
   *    v                                               |
   * REMOVE_SNAPSHOT_KEYS                               |
   *    |                                               |
   *    v                                               |
   * SET_MIRROR_GROUP_DISABLING <-----------------------|
   *    |                     |
        |                     v---(if group is empty)----\
   *    v                                                |
   * SEND_PROMOTE_IMAGES----------------------           |
   *    |     (skip if images are primary)    |          |
   *    v                                     |          |
   * SEND_REFRESH_IMAGES                      |          |
   *    |                                     |          |
   *    v                                     |          |
   * REMOVE_MIRROR_SNAPSHOTS <---------------/           |
   *    |                                                |
   *    v                                                |
   * SEND_REMOVE_SNAP                                    |
   *    |                                                |
   *    v                                                |
   * SEND_REMOVE_MIRROR_IMAGES                           |
   *    |                                                |
   *    v                                                |
   * CLOSE_IMAGES                                        |
   *    |                                                |
   *    v                                                |
   * LIST_GROUP_SNAPSHOTS <------------------------------/
   *    |
   *    v
   * REMOVE_GROUP_MIRROR_SNAPS
   *    |
   *    v
   * MIRROR_GROUP_REMOVE
   *    |
   *    v
   * NOTIFY_MIRRORING_WATCHER
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  librados::IoCtx &m_group_ioctx;
  librados::IoCtx m_default_ns_ioctx;
  const std::string m_group_id;
  const std::string m_group_name;
  bool m_force;
  Context *m_on_finish;
  CephContext *m_cct;

  std::vector<cls::rbd::GroupSnapshot> m_snaps;
  std::vector<bool> m_is_primary;

  cls::rbd::MirrorGroup m_mirror_group;
  std::set<std::string> m_mirror_peer_uuids;
  cls::rbd::GroupImageSpec m_start_after;
  std::vector<cls::rbd::GroupImageStatus> m_images;
  std::vector<ImageCtxT *> m_image_ctxs;
  ceph::bufferlist m_out_bl;


  int m_ret_val = 0;
  cls::rbd::GroupSnapshot m_group_snap;
  PromotionState m_promotion_state;
  mutable ceph::mutex m_lock =
    ceph::make_mutex("mirror::GroupDisableRequest::m_lock");

  std::vector<cls::rbd::MirrorImage> m_mirror_images;
  std::vector<uint64_t> m_snap_ids;
  std::vector<std::string> m_global_image_ids;
  std::vector<PromotionState> m_images_promotion_states;
  std::vector<std::string> m_images_primary_mirror_uuids;

  bool m_images_disabled = false;

  void get_mirror_group_info();
  void handle_get_mirror_group_info(int r);

  void list_group_images();
  void handle_list_group_images(int r);

  void open_images();
  void handle_open_images(int r);

  void refresh_images();

  void get_images_mirror_mode();

  void get_images_mirror_info();
  void handle_get_images_mirror_info(int r);

  void check_images_child_mirroring();

  void update_images_read_only_mask();
  void handle_update_images_read_only_mask(int r);

  void remove_snapshot_keys();

  void set_mirror_group_disabling();
  void handle_set_mirror_group_disabling(int r);

  void set_mirror_images_disabling();
  void handle_set_mirror_images_disabling(int r);

  void send_promote_images();
  void handle_send_promote_images(int r);

  void send_refresh_images();
  void handle_send_refresh_images(int r);

  void remove_mirror_snapshots();

  void send_remove_snap(size_t i, const cls::rbd::SnapshotNamespace &snap_namespace,
    const std::string &snap_name, Context *on_finish);

  void send_remove_mirror_images();
  void handle_send_remove_mirror_images(int r);

  void close_images();
  void handle_close_images(int r);

  void list_group_snapshots();
  void handle_list_group_snapshots(int r);

  void remove_group_mirror_snaps();

  void mirror_group_remove();

  void notify_mirroring_watcher();
  void handle_notify_mirroring_watcher(int r);

  void finish(int r);
};


} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::GroupDisableRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_GROUP_DISABLE_REQUEST_H
