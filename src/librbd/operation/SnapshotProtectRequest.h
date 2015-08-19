// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OPERATION_SNAPSHOT_PROTECT_REQUEST_H
#define CEPH_LIBRBD_OPERATION_SNAPSHOT_PROTECT_REQUEST_H

#include "librbd/operation/Request.h"
#include <iosfwd>
#include <string>

class Context;

namespace librbd {

class ImageCtx;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class SnapshotProtectRequest : public Request<ImageCtxT> {
public:
  /**
   * Snap Protect goes through the following state machine:
   *
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * STATE_PROTECT_SNAP
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   *
   */
  enum State {
    STATE_PROTECT_SNAP
  };

  SnapshotProtectRequest(ImageCtxT &image_ctx, Context *on_finish,
		         const std::string &snap_name);

protected:
  virtual void send_op();
  virtual bool should_complete(int r);

  virtual journal::Event create_event() const {
    return journal::SnapProtectEvent(0, m_snap_name);
  }

private:
  std::string m_snap_name;
  State m_state;

  void send_protect_snap();

  int verify_and_send_protect_snap();
};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::SnapshotProtectRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_SNAPSHOT_PROTECT_REQUEST_H
