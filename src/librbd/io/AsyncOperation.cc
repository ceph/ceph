// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/AsyncOperation.h"
#include "librbd/ImageCtx.h"
#include "librbd/asio/ContextWQ.h"
#include "common/dout.h"
#include "include/ceph_assert.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::AsyncOperation: "

namespace librbd {
namespace io {

namespace {

struct C_CompleteFlushes : public Context {
  ImageCtx *image_ctx;
  std::list<Context *> flush_contexts;

  explicit C_CompleteFlushes(ImageCtx *image_ctx,
                             std::list<Context *> &&flush_contexts)
    : image_ctx(image_ctx), flush_contexts(std::move(flush_contexts)) {
  }
  void finish(int r) override {
    std::shared_lock owner_locker{image_ctx->owner_lock};
    while (!flush_contexts.empty()) {
      Context *flush_ctx = flush_contexts.front();
      flush_contexts.pop_front();

      ldout(image_ctx->cct, 20) << "completed flush: " << flush_ctx << dendl;
      flush_ctx->complete(0);
    }
  }
};

} // anonymous namespace

void AsyncOperation::start_op(ImageCtx &image_ctx) {
  ceph_assert(m_image_ctx == NULL);
  m_image_ctx = &image_ctx;

  ldout(m_image_ctx->cct, 20) << this << " " << __func__ << dendl;
  std::lock_guard l{m_image_ctx->async_ops_lock};
  m_image_ctx->async_ops.push_front(&m_xlist_item);
}

void AsyncOperation::finish_op() {
  ldout(m_image_ctx->cct, 20) << this << " " << __func__ << dendl;

  {
    std::lock_guard l{m_image_ctx->async_ops_lock};
    xlist<AsyncOperation *>::iterator iter(&m_xlist_item);
    ++iter;
    ceph_assert(m_xlist_item.remove_myself());

    // linked list stored newest -> oldest ops
    if (!iter.end() && !m_flush_contexts.empty()) {
      ldout(m_image_ctx->cct, 20) << "moving flush contexts to previous op: "
                                  << *iter << dendl;
      (*iter)->m_flush_contexts.insert((*iter)->m_flush_contexts.end(),
                                       m_flush_contexts.begin(),
                                       m_flush_contexts.end());
      return;
    }
  }

  if (!m_flush_contexts.empty()) {
    C_CompleteFlushes *ctx = new C_CompleteFlushes(m_image_ctx,
                                                   std::move(m_flush_contexts));
    m_image_ctx->op_work_queue->queue(ctx);
  }
}

void AsyncOperation::flush(Context* on_finish) {
  {
    std::lock_guard locker{m_image_ctx->async_ops_lock};
    xlist<AsyncOperation *>::iterator iter(&m_xlist_item);
    ++iter;

    // linked list stored newest -> oldest ops
    if (!iter.end()) {
      (*iter)->m_flush_contexts.push_back(on_finish);
      return;
    }
  }

  m_image_ctx->op_work_queue->queue(on_finish);
}

} // namespace io
} // namespace librbd
