// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OPERATION_SNAPSHOT_ROLLBACK_REQUEST_H
#define CEPH_LIBRBD_OPERATION_SNAPSHOT_ROLLBACK_REQUEST_H

#include "librbd/operation/Request.h"
#include "librbd/internal.h"
#include <string>

class Context;

namespace librbd {

class ImageCtx;
class ProgressContext;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class SnapshotRollbackRequest : public Request<ImageCtxT> {
public:
  /**
   * Snap Rollback goes through the following state machine:
   *
   * @verbatim
   *
   * <start> ---------\
   *  .               |
   *  .               v
   *  .         STATE_RESIZE_IMAGE
   *  .               |
   *  . (skip path)   v
   *  . . . . > STATE_ROLLBACK_OBJECT_MAP
   *  .               |
   *  .               v
   *  . . . . > STATE_ROLLBACK_OBJECTS . . .
   *                  |                    .
   *                  v                    .
   *            STATE_INVALIDATE_CACHE     .
   *                  |                    .
   *                  v                    .
   *              <finish> < . . . . . . . .
   *
   * @endverbatim
   *
   * The _RESIZE_IMAGE state is skipped if the image doesn't need to be resized.
   * The _ROLLBACK_OBJECT_MAP state is skipped if the object map isn't enabled.
   * The _INVALIDATE_CACHE state is skipped if the cache isn't enabled.
   */
  enum State {
    STATE_RESIZE_IMAGE,
    STATE_ROLLBACK_OBJECT_MAP,
    STATE_ROLLBACK_OBJECTS,
    STATE_INVALIDATE_CACHE
  };

  SnapshotRollbackRequest(ImageCtxT &image_ctx, Context *on_finish,
                          const std::string &snap_name, uint64_t snap_id,
                          uint64_t snap_size, ProgressContext &prog_ctx);

protected:
  virtual void send_op();
  virtual bool should_complete(int r);

  virtual journal::Event create_event() const {
    return journal::SnapRollbackEvent(0, m_snap_name);
  }

private:
  std::string m_snap_name;
  uint64_t m_snap_id;
  uint64_t m_snap_size;
  ProgressContext &m_prog_ctx;

  NoOpProgressContext m_no_op_prog_ctx;
  State m_state;

  void send_resize_image();
  void send_rollback_object_map();
  void send_rollback_objects();
  bool send_invalidate_cache();

};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::SnapshotRollbackRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_SNAPSHOT_ROLLBACK_REQUEST_H
