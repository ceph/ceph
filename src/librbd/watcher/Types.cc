// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/watcher/Types.h"
#include "librbd/Watcher.h"
#include "common/dout.h"

#include "librbd/ImageCtx.h"
#include "librbd/MirroringWatcher.h"
#include "librbd/ImageWatcher.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::Watcher: "

namespace librbd {
namespace watcher {

C_NotifyAck::C_NotifyAck(Watcher *watcher, uint64_t notify_id,
                         uint64_t handle)
  : watcher(watcher), cct(watcher->m_cct), notify_id(notify_id),
    handle(handle) {
  ldout(cct, 10) << this << " C_NotifyAck start: id=" << notify_id << ", "
                 << "handle=" << handle << dendl;
}

void C_NotifyAck::finish(int r) {
  assert(r == 0);
  ldout(cct, 10) << this << " C_NotifyAck finish: id=" << notify_id << ", "
                 << "handle=" << handle << dendl;
  watcher->acknowledge_notify(notify_id, handle, out);
}

} // namespace watcher
} // namespace librbd

template struct librbd::watcher::HandlePayloadVisitor<
    librbd::MirroringWatcher<librbd::ImageCtx>>;

template struct librbd::watcher::HandlePayloadVisitor<
    librbd::ImageWatcher<librbd::ImageCtx>>;
