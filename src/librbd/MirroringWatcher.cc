// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/MirroringWatcher.h"
#include "include/rbd_types.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::MirroringWatcher: "

namespace librbd {

template <typename I>
MirroringWatcher<I>::MirroringWatcher(librados::IoCtx &io_ctx,
                                      ContextWQT *work_queue)
  : ObjectWatcher<I>(io_ctx, work_queue) {
}

template <typename I>
std::string MirroringWatcher<I>::get_oid() const {
  return RBD_MIRRORING;
}

} // namespace librbd

template class librbd::MirroringWatcher<librbd::ImageCtx>;
