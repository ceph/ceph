// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_GET_IMAGE_STATE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_GET_IMAGE_STATE_REQUEST_H

#include "include/buffer.h"
#include "include/types.h"

struct Context;

namespace librbd {

struct ImageCtx;

namespace mirror {
namespace snapshot {

struct ImageState;

template <typename ImageCtxT = librbd::ImageCtx>
class GetImageStateRequest {
public:
  static GetImageStateRequest *create(ImageCtxT *image_ctx, uint64_t snap_id,
                                      ImageState *image_state,
                                      Context *on_finish) {
    return new GetImageStateRequest(image_ctx, snap_id, image_state, on_finish);
  }

  GetImageStateRequest(ImageCtxT *image_ctx, uint64_t snap_id,
                       ImageState *image_state, Context *on_finish)
    : m_image_ctx(image_ctx), m_snap_id(snap_id), m_image_state(image_state),
      m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * READ_OBJECT (repeat for
   *    |         every object)
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT *m_image_ctx;
  uint64_t m_snap_id;
  ImageState *m_image_state;
  Context *m_on_finish;

  bufferlist m_bl;
  bufferlist m_state_bl;

  size_t m_object_count = 0;
  size_t m_object_index = 0;

  void read_object();
  void handle_read_object(int r);

  void finish(int r);
};

} // namespace snapshot
} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::snapshot::GetImageStateRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_GET_IMAGE_STATE_REQUEST_H
