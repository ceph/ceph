// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_GROUP_PREPARE_IMAGES_REQUEST_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_GROUP_PREPARE_IMAGES_REQUEST_H

#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "librbd/ImageWatcher.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/internal.h"
#include "librbd/mirror/snapshot/Types.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/mirror/Types.h"

#include <string>
#include <set>

struct Context;

namespace librbd {

struct ImageCtx;

namespace mirror {
namespace snapshot {

template <typename ImageCtxT = librbd::ImageCtx>
class GroupPrepareImagesRequest {
public:
  static GroupPrepareImagesRequest *create(
      librados::IoCtx& group_ioctx, const std::string& group_id,
      std::vector<librbd::ImageCtx *>& image_ctxs,
      std::vector<cls::rbd::GroupImageStatus>& images,
      std::vector<cls::rbd::MirrorImage>* mirror_images,
      std::vector<bool>* is_primary,
      std::vector<bool>* is_image_disabled,
      std::set<std::string>* mirror_peer_uuids,
      const std::string& api_name, bool force, Context *on_finish) {
    return new GroupPrepareImagesRequest(
      group_ioctx, group_id, image_ctxs, images, mirror_images, is_primary,
      is_image_disabled, mirror_peer_uuids, api_name, force, on_finish);
  }

  GroupPrepareImagesRequest(
    librados::IoCtx& group_ioctx, const std::string& group_id,
    std::vector<librbd::ImageCtx *>& image_ctxs,
    std::vector<cls::rbd::GroupImageStatus>& images,
    std::vector<cls::rbd::MirrorImage>* mirror_images,
    std::vector<bool>* is_primary,
    std::vector<bool>* is_image_disabled,
    std::set<std::string>* mirror_peer_uuids,
    const std::string& api_name, bool force, Context *on_finish);

  void send();

private:
  /**
   *@verbatim
   *
   *   <start>-------------->-------------\
   *       |                              |
   *       v                              |
   *   GET_MIRROR_PEER_LIST            (disable)
   *       |                              |
   *       v                              |
   *   LIST_GROUP_IMAGES <----------------/
   *       |            \
   *       |             \-----(enable)----->CHECK_MIRROR_IMAGES_DISABLED
   *       |          ___________________________/
   *       |         |
   *       v         V
   *   OPEN_GROUP_IMAGES ------------------(enable, r=0)-------------------->|
   *       |            \                                                    |
   *       |             \---(disable)-->REFRESH_IMAGES                      |
   *       |                                   |                             |
   *       |                                   v                             |
   *       |                              GET_IMAGES_MIRROR_MODE             |
   *       |          ___________________________/                           |
   *       |         |                                                       |
   *       v         v                                                       |
   *   GET_IMAGES_MIRROR_INFO ------------(create snapshot, r=0)------------>|
   *       |         \                                                       |
   *       |          \------(disable)--->CHECK_IMAGES_CHILD_MIRRORING       |
   *       |           __________________________/                           |
   *       |          |                                                      |
   *       v          v                                                      |
   *     UPDATE_IMAGES_READ_ONLY_MASK                                        |
   *       |                                                                 |
   *       v                                                                 v
   *   <finish>-----------------------<--------------------------------------
   *
   * @endverbatim
   */

  librados::IoCtx& m_group_ioctx;
  std::string m_group_id;
  std::vector<ImageCtx *>& m_image_ctxs;
  std::vector<cls::rbd::GroupImageStatus>& m_images;
  std::vector<cls::rbd::MirrorImage>* m_mirror_images;
  std::vector<bool>* m_is_primary; // for disable
  std::vector<bool>* m_is_image_disabled; //for disable
  std::set<std::string>* m_mirror_peer_uuids;
  std::string m_api_name;
  bool m_force;
  Context *m_on_finish;

  CephContext *m_cct;

  librados::IoCtx m_default_ns_ioctx;
  bufferlist m_outbl;
  std::vector<bufferlist> m_out_bls; // for enable
  std::vector<mirror::PromotionState> m_images_promotion_states;
  std::vector<std::string> m_images_primary_mirror_uuids;
  NoOpProgressContext m_prog_ctx;
  int m_ret_code = 0;
  cls::rbd::GroupImageSpec m_start_after;

  void get_mirror_peer_list();
  void handle_get_mirror_peer_list(int r);

  void list_group_images();
  void handle_list_group_images(int r);

  void check_mirror_images_disabled();
  void handle_check_mirror_images_disabled(int r);

  void open_group_images();
  void handle_open_group_images(int r);

  void refresh_images();
  void get_images_mirror_mode();

  void get_images_mirror_info();
  void handle_get_images_mirror_info(int r);

  void check_images_child_mirroring();

  void update_images_read_only_mask();
  void handle_update_images_read_only_mask(int r);

  void finish(int r);
};

} // namespace snapshot
} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::snapshot::GroupPrepareImagesRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_GROUP_PREPARE_IMAGES_REQUEST_H
