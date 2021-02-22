// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_SET_IMAGE_STATE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_SET_IMAGE_STATE_REQUEST_H

#include "librbd/mirror/snapshot/Types.h"

#include <map>
#include <string>

struct Context;

namespace librbd {

struct ImageCtx;

namespace mirror {
namespace snapshot {

template <typename ImageCtxT = librbd::ImageCtx>
class SetImageStateRequest {
public:
  static SetImageStateRequest *create(ImageCtxT *image_ctx, uint64_t snap_id,
                                      Context *on_finish) {
    return new SetImageStateRequest(image_ctx, snap_id, on_finish);
  }

  SetImageStateRequest(ImageCtxT *image_ctx, uint64_t snap_id,
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
   * GET_NAME
   *    |
   *    v
   * GET_SNAP_LIMIT
   *    |
   *    v
   * GET_METADATA
   *    |
   *    v
   * WRITE_IMAGE_STATE
   *    |
   *    v
   * UPDATE_PRIMARY_SNAPSHOT
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT *m_image_ctx;
  uint64_t m_snap_id;
  Context *m_on_finish;

  ImageState m_image_state;

  bufferlist m_bl;
  bufferlist m_state_bl;

  void get_name();
  void handle_get_name(int r);

  void get_snap_limit();
  void handle_get_snap_limit(int r);

  void get_metadata();
  void handle_get_metadata(int r);

  void write_image_state();
  void handle_write_image_state(int r);

  void update_primary_snapshot();
  void handle_update_primary_snapshot(int r);

  void finish(int r);
};

} // namespace snapshot
} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::snapshot::SetImageStateRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_SET_IMAGE_STATE_REQUEST_H
