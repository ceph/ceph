// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OPERATION_SNAPSHOT_LIMIT_REQUEST_H
#define CEPH_LIBRBD_OPERATION_SNAPSHOT_LIMIT_REQUEST_H

#include "librbd/operation/Request.h"
#include <iosfwd>
#include <string>

class Context;

namespace librbd {

class ImageCtx;

namespace operation {

template <typename ImageCtxT = ImageCtx>
class SnapshotLimitRequest : public Request<ImageCtxT> {
public:
  SnapshotLimitRequest(ImageCtxT &image_ctx, Context *on_finish,
		       uint64_t limit);

protected:
  virtual void send_op();
  virtual bool should_complete(int r);

  virtual journal::Event create_event(uint64_t op_tid) const {
    return journal::SnapLimitEvent(op_tid, m_snap_limit);
  }

private:
  uint64_t m_snap_limit;

  void send_limit_snaps();
};

} // namespace operation
} // namespace librbd

extern template class librbd::operation::SnapshotLimitRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATION_SNAPSHOT_LIMIT_REQUEST_H
