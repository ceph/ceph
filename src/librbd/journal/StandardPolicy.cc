// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/journal/StandardPolicy.h"
#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::journal::StandardPolicy: "

namespace librbd {
namespace journal {

void StandardPolicy::allocate_tag_on_lock(Context *on_finish) {
  assert(m_image_ctx->journal != nullptr);

  if (!m_image_ctx->journal->is_tag_owner()) {
    lderr(m_image_ctx->cct) << "local image not promoted" << dendl;
    m_image_ctx->op_work_queue->queue(on_finish, -EPERM);
    return;
  }

  m_image_ctx->journal->allocate_tag(Journal<>::LOCAL_MIRROR_UUID, on_finish);
}

void StandardPolicy::cancel_external_replay(Context *on_finish) {
  // external replay is only handled by rbd-mirror
  assert(false);
}

} // namespace journal
} // namespace librbd
