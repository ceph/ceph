// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_REMOVE_STANDALONE_SNAPSHOTS_REQUEST_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_REMOVE_STANDALONE_SNAPSHOTS_REQUEST_H

#include "cls/rbd/cls_rbd_types.h"
#include <deque>

class Context;

namespace librbd {
struct ImageCtx;
namespace mirror {
namespace snapshot {

// Only group teardown may discard standalone snapshots with pending peers.
template <typename ImageCtxT = ImageCtx>
class RemoveStandaloneSnapshotsRequest {
public:
  static RemoveStandaloneSnapshotsRequest *create(
      ImageCtxT *image_ctx, const cls::rbd::GroupSpec& group_spec,
      Context *on_finish) {
    return new RemoveStandaloneSnapshotsRequest(image_ctx, group_spec,
                                               on_finish);
  }

  RemoveStandaloneSnapshotsRequest(
      ImageCtxT *image_ctx, const cls::rbd::GroupSpec& group_spec,
      Context *on_finish)
    : m_image_ctx(image_ctx), m_group_spec(group_spec), m_on_finish(on_finish) {
  }

  void send();

private:
  ImageCtxT *m_image_ctx;
  cls::rbd::GroupSpec m_group_spec;
  Context *m_on_finish;
  bufferlist m_out_bl;
  std::deque<std::pair<cls::rbd::SnapshotNamespace, std::string>> m_snapshots;

  void handle_get_mirror_image(int r);
  void handle_refresh(int r);
  void remove_snapshot();
  void handle_remove_snapshot(int r);
  void finish(int r);
};

} // namespace snapshot
} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::snapshot::RemoveStandaloneSnapshotsRequest<
  librbd::ImageCtx>;

#endif
