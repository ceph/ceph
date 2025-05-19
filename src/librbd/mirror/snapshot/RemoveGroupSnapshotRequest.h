// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_REMOVE_GROUP_SNAPSHOT_REQUEST_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_REMOVE_GROUP_SNAPSHOT_REQUEST_H

#include "include/int_types.h"
#include "include/types.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_types.h"

#include <string>
#include <vector>

class Context;

namespace librbd {

struct ImageCtx;

namespace mirror {
namespace snapshot {

template <typename ImageCtxT = librbd::ImageCtx>
class RemoveGroupSnapshotRequest {
public:
  static RemoveGroupSnapshotRequest *create(
      librados::IoCtx &group_io_ctx, const std::string &group_id,
      cls::rbd::GroupSnapshot *group_snap, std::vector<ImageCtx *> *image_ctxs,
      Context *on_finish) {
    return new RemoveGroupSnapshotRequest(group_io_ctx, group_id,
                                          group_snap, image_ctxs,
                                          on_finish);
  }

  RemoveGroupSnapshotRequest(librados::IoCtx &group_io_ctx,
                             const std::string &group_id,
                             cls::rbd::GroupSnapshot *group_snap,
                             std::vector<ImageCtx *> *image_ctxs,
                             Context *on_finish)
    : m_group_io_ctx(group_io_ctx), m_group_id(group_id),
      m_group_snap(group_snap), m_image_ctxs(image_ctxs),
      m_on_finish(on_finish) {
    m_cct = (CephContext *)group_io_ctx.cct();
  }

  void send();

private:
  librados::IoCtx &m_group_io_ctx;
  const std::string m_group_id;
  cls::rbd::GroupSnapshot *m_group_snap = nullptr;
  std::vector<ImageCtx *> *m_image_ctxs;
  Context *m_on_finish;

  CephContext *m_cct;

  void remove_group_image_snapshots();
  void handle_remove_group_image_snapshots(int r);

  void remove_group_snapshot();
  void handle_remove_group_snapshot(int r);

  void remove_image_snapshot(ImageCtx *image_ctx, uint64_t snap_id,
                             C_Gather *ctx);
  void finish(int r);
};

} // namespace snapshot
} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::snapshot::RemoveGroupSnapshotRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_REMOVE_GROUP_SNAPSHOT_REQUEST_H
