// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/journal/StandardPolicy.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/asio/ContextWQ.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::journal::StandardPolicy: "

namespace librbd {
namespace journal {

template<typename I>
void StandardPolicy<I>::allocate_tag_on_lock(Context *on_finish) {
  ceph_assert(m_image_ctx->journal != nullptr);

  if (!m_image_ctx->journal->is_tag_owner()) {
    lderr(m_image_ctx->cct) << "local image not promoted" << dendl;
    m_image_ctx->op_work_queue->queue(on_finish, -EPERM);
    return;
  }

  m_image_ctx->journal->allocate_local_tag(on_finish);
}

} // namespace journal
} // namespace librbd

template class librbd::journal::StandardPolicy<librbd::ImageCtx>;
