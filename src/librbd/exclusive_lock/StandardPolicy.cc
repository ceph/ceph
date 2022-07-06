// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/exclusive_lock/StandardPolicy.h"
#include "librbd/ImageCtx.h"
#include "librbd/ExclusiveLock.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ExclusiveLock::StandardPolicy "

namespace librbd {
namespace exclusive_lock {

template <typename I>
int StandardPolicy<I>::lock_requested(bool force) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx->owner_lock));
  ceph_assert(m_image_ctx->exclusive_lock != nullptr);

  ldout(m_image_ctx->cct, 20) << this << " " << __func__ << ": force=" << force
			      << dendl;

  return -EROFS;
}

} // namespace exclusive_lock
} // namespace librbd

template class librbd::exclusive_lock::StandardPolicy<librbd::ImageCtx>;
