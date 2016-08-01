// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/SnapshotLimitRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::SnapshotLimitRequest: "

namespace librbd {
namespace operation {

template <typename I>
SnapshotLimitRequest<I>::SnapshotLimitRequest(I &image_ctx,
					      Context *on_finish,
					      uint64_t limit)
  : Request<I>(image_ctx, on_finish), m_snap_limit(limit) {
}

template <typename I>
void SnapshotLimitRequest<I>::send_op() {
  send_limit_snaps();
}

template <typename I>
bool SnapshotLimitRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
  }
  return true;
}

template <typename I>
void SnapshotLimitRequest<I>::send_limit_snaps() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  {
    RWLock::RLocker md_locker(image_ctx.md_lock);
    RWLock::RLocker snap_locker(image_ctx.snap_lock);

    librados::ObjectWriteOperation op;
    cls_client::snapshot_set_limit(&op, m_snap_limit);

    librados::AioCompletion *rados_completion =
      this->create_callback_completion();
    int r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid, rados_completion,
					 &op);
    assert(r == 0);
    rados_completion->release();
  }
}

} // namespace operation
} // namespace librbd

template class librbd::operation::SnapshotLimitRequest<librbd::ImageCtx>;
