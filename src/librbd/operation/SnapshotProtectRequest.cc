// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/SnapshotProtectRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::SnapshotProtectRequest: "

namespace librbd {
namespace operation {

namespace {

template <typename I>
std::ostream& operator<<(std::ostream& os,
                         const typename SnapshotProtectRequest<I>::State& state) {
  switch(state) {
  case SnapshotProtectRequest<I>::STATE_PROTECT_SNAP:
    os << "PROTECT_SNAP";
    break;
  }
  return os;
}

} // anonymous namespace

template <typename I>
SnapshotProtectRequest<I>::SnapshotProtectRequest(I &image_ctx,
                                                  Context *on_finish,
                                                  const std::string &snap_name)
  : Request<I>(image_ctx, on_finish), m_snap_name(snap_name) {
}

template <typename I>
void SnapshotProtectRequest<I>::send_op() {
  send_protect_snap();
}

template <typename I>
bool SnapshotProtectRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": state=" << m_state << ", "
                << "r=" << r << dendl;
  if (r < 0) {
    lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
  }
  return true;
}

template <typename I>
void SnapshotProtectRequest<I>::send_protect_snap() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  m_state = STATE_PROTECT_SNAP;

  int r = verify_and_send_protect_snap();
  if (r < 0) {
    this->async_complete(r);
    return;
  }
}

template <typename I>
int SnapshotProtectRequest<I>::verify_and_send_protect_snap() {
  I &image_ctx = this->m_image_ctx;
  RWLock::RLocker md_locker(image_ctx.md_lock);
  RWLock::RLocker snap_locker(image_ctx.snap_lock);

  CephContext *cct = image_ctx.cct;
  if ((image_ctx.features & RBD_FEATURE_LAYERING) == 0) {
    lderr(cct) << "image must support layering" << dendl;
    return -ENOSYS;
  }

  uint64_t snap_id = image_ctx.get_snap_id(m_snap_name);
  if (snap_id == CEPH_NOSNAP) {
    return -ENOENT;
  }

  bool is_protected;
  int r = image_ctx.is_snap_protected(snap_id, &is_protected);
  if (r < 0) {
    return r;
  }

  if (is_protected) {
    return -EBUSY;
  }

  librados::ObjectWriteOperation op;
  cls_client::set_protection_status(&op, snap_id,
                                    RBD_PROTECTION_STATUS_PROTECTED);

  librados::AioCompletion *rados_completion =
    this->create_callback_completion();
  r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid, rados_completion,
                                     &op);
  assert(r == 0);
  rados_completion->release();
  return 0;
}

} // namespace operation
} // namespace librbd

template class librbd::operation::SnapshotProtectRequest<librbd::ImageCtx>;
