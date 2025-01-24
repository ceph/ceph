// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_GROUP_GET_INFO_REQUEST_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_GROUP_GET_INFO_REQUEST_H

#include "include/buffer.h"
#include "include/types.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"
#include "cls/rbd/cls_rbd_types.h"

struct Context;

namespace librbd {

struct ImageCtx;

namespace mirror {
namespace snapshot {

template <typename ImageCtxT = librbd::ImageCtx>
class GroupGetInfoRequest {
public:
  static GroupGetInfoRequest *create(librados::IoCtx& group_ioctx,
                                     const std::string& group_name,
                                     mirror_group_info_t *mirror_group_info,
                                     Context *on_finish) {
    return new GroupGetInfoRequest(group_ioctx, group_name, mirror_group_info,
                                   on_finish);
  }

  GroupGetInfoRequest(librados::IoCtx& group_ioctx,
                      const std::string& group_name,
                      mirror_group_info_t *mirror_group_info,
                      Context *on_finish)
    : m_group_ioctx(group_ioctx), m_group_name(group_name),
      m_mirror_group_info(mirror_group_info), m_on_finish(on_finish) {
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
  mirror_group_info_t *m_mirror_group_info;
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

} // namespace snapshot
} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::snapshot::GroupGetInfoRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_GROUP_GET_INFO_REQUEST_H
