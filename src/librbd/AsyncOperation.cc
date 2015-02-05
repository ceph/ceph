// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "librbd/AsyncOperation.h"
#include "librbd/ImageCtx.h"
#include "common/dout.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::AsyncOperation: "

namespace librbd {

void AsyncOperation::start_op(ImageCtx &image_ctx) {
  assert(m_image_ctx == NULL);
  m_image_ctx = &image_ctx;

  ldout(m_image_ctx->cct, 20) << this << " " << __func__ << dendl; 
  Mutex::Locker l(m_image_ctx->async_ops_lock);
  m_image_ctx->async_ops.push_back(&m_xlist_item);
}

void AsyncOperation::finish_op() {
  ldout(m_image_ctx->cct, 20) << this << " " << __func__ << dendl;
  {
    Mutex::Locker l(m_image_ctx->async_ops_lock);
    assert(m_xlist_item.remove_myself());
  }

  while (!m_flush_contexts.empty()) {
    Context *flush_ctx = m_flush_contexts.front();
    m_flush_contexts.pop_front();

    ldout(m_image_ctx->cct, 20) << "completed flush: " << flush_ctx << dendl;
    flush_ctx->complete(0);
  }
}

void AsyncOperation::add_flush_context(Context *on_finish) {
  assert(m_image_ctx->async_ops_lock.is_locked());
  m_flush_contexts.push_back(on_finish);
} 

} // namespace librbd
