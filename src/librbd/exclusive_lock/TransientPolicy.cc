// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "librbd/exclusive_lock/TransientPolicy.h"
#include "librbd/ImageCtx.h"
#include "librbd/ExclusiveLock.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ExclusiveLock::TransientPolicy "

namespace librbd {
namespace exclusive_lock {

template <typename I>
int TransientPolicy<I>::lock_requested(bool force) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx->owner_lock));
  ceph_assert(m_image_ctx->exclusive_lock != nullptr);

  ldout(m_image_ctx->cct, 20) << this << " " << __func__ << ": force=" << force
			      << dendl;

  // it's expected that the lock will be released shortly -- the peer
  // will block waiting for that to happen
  return 0;
}

} // namespace exclusive_lock
} // namespace librbd

template class librbd::exclusive_lock::TransientPolicy<librbd::ImageCtx>;
