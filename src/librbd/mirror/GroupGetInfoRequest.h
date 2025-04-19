// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_GROUP_GET_INFO_REQUEST_H
#define CEPH_LIBRBD_MIRROR_GROUP_GET_INFO_REQUEST_H

#include "include/buffer.h"
#include "include/types.h"
#include "include/rados/librados.hpp"
#include "librbd/ImageCtx.h"
#include "librbd/mirror/Types.h"
#include "cls/rbd/cls_rbd_types.h"

struct Context;

namespace librbd {

struct ImageCtx;

namespace mirror {

template <typename ImageCtxT = librbd::ImageCtx>
class GroupGetInfoRequest {
public:
  static GroupGetInfoRequest *create(librados::IoCtx& group_ioctx,
                                     const std::string& group_name,
                                     const std::string& group_id,
                                     cls::rbd::MirrorGroup *mirror_group,
                                     PromotionState *promotion_state,
                                     Context *on_finish) {
    return new GroupGetInfoRequest(group_ioctx, group_name, group_id,
                                   mirror_group, promotion_state, on_finish);
  }

  GroupGetInfoRequest(librados::IoCtx& group_ioctx,
                      const std::string& group_name,
                      const std::string& group_id,
                      cls::rbd::MirrorGroup *mirror_group,
                      PromotionState *promotion_state,
                      Context *on_finish)
    : m_group_ioctx(group_ioctx), m_group_name(group_name),
      m_group_id(group_id), m_mirror_group(mirror_group),
      m_promotion_state(promotion_state), m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET ID
   *    |
   *    v
   * GET INFO
   *    |
   *    v
   * GET LAST MIRROR SNAPSHOT STATE
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  librados::IoCtx m_group_ioctx;
  const std::string m_group_name;
  std::string m_group_id;
  cls::rbd::MirrorGroup *m_mirror_group;
  PromotionState *m_promotion_state;
  std::vector<cls::rbd::GroupSnapshot> m_group_snaps;
  Context *m_on_finish;

  bufferlist m_outbl;

  void get_id();
  void handle_get_id(int r);

  void get_info();
  void handle_get_info(int r);

  void get_last_mirror_snapshot_state();
  void handle_get_last_mirror_snapshot_state(int r);

  void finish(int r);
};

} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::GroupGetInfoRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_GROUP_GET_INFO_REQUEST_H
