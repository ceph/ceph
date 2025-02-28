// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/SnapshotLimitRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"

#include <shared_mutex> // for std::shared_lock

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
  ldout(cct, 5) << this << " " << __func__ << " r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
  }
  return true;
}

template <typename I>
void SnapshotLimitRequest<I>::send_limit_snaps() {
  I &image_ctx = this->m_image_ctx;
  ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  {
    std::shared_lock image_locker{image_ctx.image_lock};

    librados::ObjectWriteOperation op;
    cls_client::snapshot_set_limit(&op, m_snap_limit);

    librados::AioCompletion *rados_completion =
      this->create_callback_completion();
    int r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid, rados_completion,
					 &op);
    ceph_assert(r == 0);
    rados_completion->release();
  }
}

} // namespace operation
} // namespace librbd

template class librbd::operation::SnapshotLimitRequest<librbd::ImageCtx>;
