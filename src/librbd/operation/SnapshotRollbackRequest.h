// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OPERATION_SNAPSHOT_ROLLBACK_REQUEST_H
#define CEPH_LIBRBD_OPERATION_SNAPSHOT_ROLLBACK_REQUEST_H

#include "librbd/operation/Request.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include "librbd/journal/Types.h"
#include <string>

class Context;

namespace librbd {

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
   *                  |
   *                  v
   *            STATE_BLOCK_WRITES
   *                  |
   *                  v
   *            STATE_RESIZE_IMAGE (skip if resize not
   *                  |             required)
   *                  v
   *            STATE_ROLLBACK_OBJECT_MAP (skip if object
   *                  |                    map disabled)
   *                  v
   *            STATE_ROLLBACK_OBJECTS
   *                  |
   *                  v
   *            STATE_REFRESH_OBJECT_MAP  (skip if object
   *                  |                    map disabled)
   *                  v
   *            STATE_INVALIDATE_CACHE (skip if cache
   *                  |                 disabled)
   *                  v
   *              <finish>
   *
   * @endverbatim
   *
   * The _RESIZE_IMAGE state is skipped if the image doesn't need to be resized.
   * The _ROLLBACK_OBJECT_MAP state is skipped if the object map isn't enabled.
   * The _INVALIDATE_CACHE state is skipped if the cache isn't enabled.
   */

  SnapshotRollbackRequest(ImageCtxT &image_ctx, Context *on_finish,
                          const std::string &snap_name, uint64_t snap_id,
                          uint64_t snap_size, ProgressContext &prog_ctx);
  virtual ~SnapshotRollbackRequest();

protected:
  virtual void send_op();
  virtual bool should_complete(int r) {
    return true;
  }

  virtual journal::Event create_event(uint64_t op_tid) const {
    return journal::SnapRollbackEvent(op_tid, m_snap_name);
  }

private:
  std::string m_snap_name;
  uint64_t m_snap_id;
  uint64_t m_snap_size;
  ProgressContext &m_prog_ctx;

  NoOpProgressContext m_no_op_prog_ctx;

  bool m_blocking_writes = false;
  decltype(ImageCtxT::object_map) m_object_map;

  void send_block_writes();
  Context *handle_block_writes(int *result);

  void send_resize_image();
  Context *handle_resize_image(int *result);

  void send_rollback_object_map();
  Context *handle_rollback_object_map(int *result);

  void send_rollback_objects();
  Context *handle_rollback_objects(int *result);

  Context *send_refresh_object_map();
  Context *handle_refresh_object_map(int *result);

  Context *send_invalidate_cache();
  Context *handle_invalidate_cache(int *result);

  void apply();
};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::SnapshotRollbackRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_SNAPSHOT_ROLLBACK_REQUEST_H
