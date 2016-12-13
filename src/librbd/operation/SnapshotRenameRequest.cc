// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/SnapshotRenameRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::SnapshotRenameRequest: "

namespace librbd {
namespace operation {

namespace {

template <typename I>
std::ostream& operator<<(std::ostream& os,
                         const typename SnapshotRenameRequest<I>::State& state) {
  switch(state) {
  case SnapshotRenameRequest<I>::STATE_RENAME_SNAP:
    os << "RENAME_SNAP";
    break;
  }
  return os;
}

} // anonymous namespace

template <typename I>
SnapshotRenameRequest<I>::SnapshotRenameRequest(I &image_ctx,
						Context *on_finish,
						uint64_t snap_id,
						const std::string &snap_name)
  : Request<I>(image_ctx, on_finish), m_snap_id(snap_id), m_snap_name(snap_name) {
}

template <typename I>
journal::Event SnapshotRenameRequest<I>::create_event(uint64_t op_tid) const {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.snap_lock.is_locked());

  std::string src_snap_name;
  auto snap_info_it = image_ctx.snap_info.find(m_snap_id);
  if (snap_info_it != image_ctx.snap_info.end()) {
    src_snap_name = snap_info_it->second.name;
  }

  return journal::SnapRenameEvent(op_tid, m_snap_id, src_snap_name,
                                  m_snap_name);
}

template <typename I>
void SnapshotRenameRequest<I>::send_op() {
  send_rename_snap();
}

template <typename I>
bool SnapshotRenameRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": state=" << m_state << ", "
                << "r=" << r << dendl;
  if (r < 0) {
    if (r == -EEXIST) {
      ldout(cct, 1) << "snapshot already exists" << dendl;
    } else {
      lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
    }
  }
  return true;
}

template <typename I>
void SnapshotRenameRequest<I>::send_rename_snap() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());
  RWLock::RLocker md_locker(image_ctx.md_lock);
  RWLock::RLocker snap_locker(image_ctx.snap_lock);

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  m_state = STATE_RENAME_SNAP;

  librados::ObjectWriteOperation op;
  if (image_ctx.old_format) {
    cls_client::old_snapshot_rename(&op, m_snap_id, m_snap_name);
  } else {
    cls_client::snapshot_rename(&op, m_snap_id, m_snap_name);
  }

  librados::AioCompletion *rados_completion = this->create_callback_completion();
  int r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid,
                                       rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

} // namespace operation
} // namespace librbd

template class librbd::operation::SnapshotRenameRequest<librbd::ImageCtx>;
