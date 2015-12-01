// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/SnapshotRenameRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::SnapshotRenameRequest: "

namespace librbd {
namespace operation {

namespace {

std::ostream& operator<<(std::ostream& os,
                         const SnapshotRenameRequest::State& state) {
  switch(state) {
  case SnapshotRenameRequest::STATE_RENAME_SNAP:
    os << "RENAME_SNAP";
    break;
  }
  return os;
}

} // anonymous namespace

SnapshotRenameRequest::SnapshotRenameRequest(ImageCtx &image_ctx,
                                             Context *on_finish,
                                             uint64_t snap_id,
                                             const std::string &snap_name)
  : Request(image_ctx, on_finish), m_snap_id(snap_id), m_snap_name(snap_name) {
}

void SnapshotRenameRequest::send_op() {
  send_rename_snap();
}

bool SnapshotRenameRequest::should_complete(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << ": state=" << m_state << ", "
                << "r=" << r << dendl;
  if (r < 0) {
    lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
  }
  return true;
}

void SnapshotRenameRequest::send_rename_snap() {
  assert(m_image_ctx.owner_lock.is_locked());
  RWLock::RLocker md_locker(m_image_ctx.md_lock);
  RWLock::RLocker snap_locker(m_image_ctx.snap_lock);

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << __func__ << dendl;

  m_state = STATE_RENAME_SNAP;

  librados::ObjectWriteOperation op;
  if (m_image_ctx.old_format) {
    cls_client::old_snapshot_rename(&op, m_snap_id, m_snap_name);
  } else {
    if (m_image_ctx.image_watcher->is_lock_owner()) {
      m_image_ctx.image_watcher->assert_header_locked(&op);
    }
    cls_client::snapshot_rename(&op, m_snap_id, m_snap_name);
  }

  librados::AioCompletion *rados_completion = create_callback_completion();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid,
                                         rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

} // namespace operation
} // namespace librbd

