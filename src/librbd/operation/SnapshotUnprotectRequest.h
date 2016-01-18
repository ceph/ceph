// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OPERATION_SNAPSHOT_UNPROTECT_REQUEST_H
#define CEPH_LIBRBD_OPERATION_SNAPSHOT_UNPROTECT_REQUEST_H

#include "librbd/operation/Request.h"
#include <iosfwd>
#include <string>

class Context;

namespace librbd {

class ImageCtx;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class SnapshotUnprotectRequest : public Request<ImageCtxT> {
public:
  /**
   * Snap Unprotect goes through the following state machine:
   *
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * STATE_UNPROTECT_SNAP_START
   *    |
   *    v
   * STATE_SCAN_POOL_CHILDREN * * * * > STATE_UNPROTECT_SNAP_ROLLBACK
   *    |                                  |
   *    v                                  |
   * STATE_UNPROTECT_SNAP_FINISH           |
   *    |                                  |
   *    v                                  |
   * <finish> <----------------------------/
   *
   * @endverbatim
   *
   * If the unprotect operation needs to abort, the error path is followed
   * to rollback the unprotect in-progress status on the image.
   */
  enum State {
    STATE_UNPROTECT_SNAP_START,
    STATE_SCAN_POOL_CHILDREN,
    STATE_UNPROTECT_SNAP_FINISH,
    STATE_UNPROTECT_SNAP_ROLLBACK
  };

  SnapshotUnprotectRequest(ImageCtxT &image_ctx, Context *on_finish,
		           const std::string &snap_name);

protected:
  virtual void send_op();
  virtual bool should_complete(int r);

  virtual int filter_return_code(int r) const {
    if (m_ret_val < 0) {
      return m_ret_val;
    }
    return 0;
  }

  virtual journal::Event create_event(uint64_t op_tid) const {
    return journal::SnapUnprotectEvent(op_tid, m_snap_name);
  }

private:
  std::string m_snap_name;
  State m_state;

  int m_ret_val;
  uint64_t m_snap_id;

  bool should_complete_error();

  void send_unprotect_snap_start();
  void send_scan_pool_children();
  void send_unprotect_snap_finish();
  void send_unprotect_snap_rollback();

  int verify_and_send_unprotect_snap_start();
};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::SnapshotUnprotectRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_SNAPSHOT_UNPROTECT_REQUEST_H
