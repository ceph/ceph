// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_WRITE_IMAGE_STATE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_WRITE_IMAGE_STATE_REQUEST_H

#include "librbd/mirror/snapshot/Types.h"

#include <map>
#include <string>

struct Context;

namespace librbd {

struct ImageCtx;

namespace mirror {
namespace snapshot {

template <typename ImageCtxT = librbd::ImageCtx>
class WriteImageStateRequest {
public:
  static WriteImageStateRequest *create(ImageCtxT *image_ctx, uint64_t snap_id,
                                        const ImageState &image_state,
                                        Context *on_finish) {
    return new WriteImageStateRequest(image_ctx, snap_id, image_state,
                                      on_finish);
  }

  WriteImageStateRequest(ImageCtxT *image_ctx, uint64_t snap_id,
                         const ImageState &image_state, Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * WRITE_OBJECT (repeat for
   *    |          every object)
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT *m_image_ctx;
  uint64_t m_snap_id;
  ImageState m_image_state;
  Context *m_on_finish;

  bufferlist m_bl;

  const size_t m_object_size;
  size_t m_object_count = 0;

  void write_object();
  void handle_write_object(int r);

  void finish(int r);
};

} // namespace snapshot
} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::snapshot::WriteImageStateRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_WRITE_IMAGE_STATE_REQUEST_H
