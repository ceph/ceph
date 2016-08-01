// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/exclusive_lock/StandardPolicy.h"
#include "librbd/ImageCtx.h"
#include "librbd/ExclusiveLock.h"

namespace librbd {
namespace exclusive_lock {

void StandardPolicy::lock_requested(bool force) {
  assert(m_image_ctx->owner_lock.is_locked());
  assert(m_image_ctx->exclusive_lock != nullptr);

  // release the lock upon request (ignore forced requests)
  m_image_ctx->exclusive_lock->release_lock(nullptr);
}

} // namespace exclusive_lock
} // namespace librbd

