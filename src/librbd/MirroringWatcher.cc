// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/MirroringWatcher.h"
#include "include/rbd_types.h"
#include "common/errno.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::MirroringWatcher: "

namespace librbd {

using namespace mirroring_watcher;

namespace {

static const uint64_t NOTIFY_TIMEOUT_MS = 5000;

} // anonymous namespace

template <typename I>
MirroringWatcher<I>::MirroringWatcher(librados::IoCtx &io_ctx,
                                      ContextWQT *work_queue)
  : ObjectWatcher<I>(io_ctx, work_queue) {
}

template <typename I>
std::string MirroringWatcher<I>::get_oid() const {
  return RBD_MIRRORING;
}

template <typename I>
int MirroringWatcher<I>::notify_mode_updated(librados::IoCtx &io_ctx,
                                             cls::rbd::MirrorMode mirror_mode) {
  CephContext *cct = reinterpret_cast<CephContext*>(io_ctx.cct());
  ldout(cct, 20) << dendl;

  bufferlist bl;
  ::encode(NotifyMessage{ModeUpdatedPayload{mirror_mode}}, bl);

  int r = io_ctx.notify2(RBD_MIRRORING, bl, NOTIFY_TIMEOUT_MS, nullptr);
  if (r < 0) {
    lderr(cct) << ": error encountered sending mode updated notification: "
               << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

template <typename I>
int MirroringWatcher<I>::notify_image_updated(
    librados::IoCtx &io_ctx, cls::rbd::MirrorImageState mirror_image_state,
    const std::string &image_id, const std::string &global_image_id) {
  CephContext *cct = reinterpret_cast<CephContext*>(io_ctx.cct());
  ldout(cct, 20) << dendl;

  bufferlist bl;
  ::encode(NotifyMessage{ImageUpdatedPayload{mirror_image_state, image_id,
                                             global_image_id}},
           bl);

  int r = io_ctx.notify2(RBD_MIRRORING, bl, NOTIFY_TIMEOUT_MS, nullptr);
  if (r < 0) {
    lderr(cct) << ": error encountered sending image updated notification: "
               << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

} // namespace librbd

template class librbd::MirroringWatcher<librbd::ImageCtx>;
