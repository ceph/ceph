// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_REMOVE_IMAGE_STATE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_REMOVE_IMAGE_STATE_REQUEST_H

#include "include/buffer.h"
#include "include/types.h"

struct Context;

namespace librbd {

struct ImageCtx;

namespace mirror {
namespace snapshot {

template <typename ImageCtxT = librbd::ImageCtx>
class RemoveImageStateRequest {
public:
  static RemoveImageStateRequest *create(ImageCtxT *image_ctx, uint64_t snap_id,
                                         Context *on_finish) {
      return new RemoveImageStateRequest(image_ctx, snap_id, on_finish);
  }

  RemoveImageStateRequest(ImageCtxT *image_ctx, uint64_t snap_id,
                          Context *on_finish)
    : m_image_ctx(image_ctx), m_snap_id(snap_id), m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_OBJECT_COUNT
   *    |
   *    v
   * REMOVE_OBJECT (repeat for
   *    |           every object)
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT *m_image_ctx;
  uint64_t m_snap_id;
  Context *m_on_finish;

  bufferlist m_bl;

  size_t m_object_count = 0;

  void get_object_count();
  void handle_get_object_count(int r);

  void remove_object();
  void handle_remove_object(int r);

  void finish(int r);
};

} // namespace snapshot
} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::snapshot::RemoveImageStateRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_REMOVE_IMAGE_STATE_REQUEST_H
