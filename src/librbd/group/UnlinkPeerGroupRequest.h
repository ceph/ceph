// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_UNLINK_PEER_GROUP_REQUST_H
#define CEPH_LIBRBD_UNLINK_PEER_GROUP_REQUST_H

#include "include/int_types.h"
#include "include/types.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_types.h"

#include <string>
#include <vector>

class Context;

namespace librbd {

struct ImageCtx;

namespace group {

template <typename ImageCtxT = librbd::ImageCtx>
class UnlinkPeerGroupRequest {
public:
  static UnlinkPeerGroupRequest *create(
      librados::IoCtx &group_io_ctx, const std::string &group_id,
      std::vector<ImageCtx *> *image_ctxs,
      Context *on_finish) {
    return new UnlinkPeerGroupRequest(group_io_ctx, group_id,
                                      image_ctxs, on_finish);
  }

  UnlinkPeerGroupRequest(librados::IoCtx &group_io_ctx,
                       const std::string &group_id,
                       std::vector<ImageCtx *> *image_ctxs,
                       Context *on_finish)
    : m_group_io_ctx(group_io_ctx), m_group_id(group_id),
      m_image_ctxs(image_ctxs), m_on_finish(on_finish) {
    m_cct = (CephContext *)group_io_ctx.cct();
  }

  void send();

private:
  librados::IoCtx &m_group_io_ctx;
  const std::string m_group_id;
  std::vector<ImageCtx *> *m_image_ctxs;
  Context *m_on_finish;

  CephContext *m_cct;

  std::vector<cls::rbd::GroupSnapshot> m_group_snaps;
  std::string m_remove_gp_snap_id;

  void list_group_snaps();
  void handle_list_group_snaps(int r);

  void unlink_peer();

  void remove_group_snapshot(cls::rbd::GroupSnapshot group_snap);
  void remove_image_snapshot(ImageCtx *image_ctx, uint64_t snap_id,
                             C_Gather *gather_ctx);
  void handle_remove_group_snapshot(int r);

  void finish(int r);
};

} // namespace group
} // namespace librbd

extern template class librbd::group::UnlinkPeerGroupRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_UNLINK_PEER_GROUP_REQUST_H
