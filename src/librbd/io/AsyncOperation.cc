// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "librbd/io/AsyncOperation.h"
#include "include/ceph_assert.h"
#include "common/dout.h"
#include "librbd/AsioEngine.h"
#include "librbd/ImageCtx.h"

#include <shared_mutex> // for std::shared_lock

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

void AsyncOperation::start_op(ImageCtx &image_ctx, bool writes) {
  ceph_assert(m_image_ctx == NULL);
  m_image_ctx = &image_ctx;
  m_writes = writes;

  ldout(m_image_ctx->cct, 20) << this << " " << __func__ << dendl;
  std::lock_guard l{m_image_ctx->async_ops_lock};
  m_image_ctx->async_ops.push_front(&m_xlist_item);
}

void AsyncOperation::finish_op() {
  ldout(m_image_ctx->cct, 20) << this << " " << __func__ << dendl;

  std::list<Context *> flush_contexts;
  {
    std::lock_guard l{m_image_ctx->async_ops_lock};
    auto older_op = find_older_op(false);
    auto older_writes_op = find_older_op(true);
    ceph_assert(m_xlist_item.remove_myself());

    if (!m_flush_contexts.empty()) {
      if (older_op != nullptr) {
        ldout(m_image_ctx->cct, 20) << "moving flush contexts to previous op: "
                                    << older_op << dendl;
        older_op->m_flush_contexts.splice(older_op->m_flush_contexts.end(),
                                          m_flush_contexts);
      } else {
        flush_contexts.splice(flush_contexts.end(), m_flush_contexts);
      }
    }

    if (!m_flush_writes_contexts.empty()) {
      if (older_writes_op != nullptr) {
        ldout(m_image_ctx->cct, 20) << "moving flush writes contexts to "
                                    << "previous op: " << older_writes_op
                                    << dendl;
        older_writes_op->m_flush_writes_contexts.splice(
          older_writes_op->m_flush_writes_contexts.end(),
          m_flush_writes_contexts);
      } else {
        flush_contexts.splice(flush_contexts.end(), m_flush_writes_contexts);
      }
    }
  }

  if (!flush_contexts.empty()) {
    C_CompleteFlushes *ctx = new C_CompleteFlushes(m_image_ctx,
                                                   std::move(flush_contexts));
    m_image_ctx->asio_engine->post(ctx, 0);
  }
}

void AsyncOperation::flush(Context* on_finish) {
  {
    std::lock_guard locker{m_image_ctx->async_ops_lock};
    if (auto older_op = find_older_op(false); older_op != nullptr) {
      older_op->m_flush_contexts.push_back(on_finish);
      return;
    }
  }

  m_image_ctx->asio_engine->post(on_finish, 0);
}

void AsyncOperation::flush_writes(Context* on_finish) {
  {
    std::lock_guard locker{m_image_ctx->async_ops_lock};
    if (auto older_op = find_older_op(true); older_op != nullptr) {
      older_op->m_flush_writes_contexts.push_back(on_finish);
      return;
    }
  }

  m_image_ctx->asio_engine->post(on_finish, 0);
}

AsyncOperation* AsyncOperation::find_older_op(bool writes_only) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx->async_ops_lock));

  // linked list stored newest -> oldest ops
  xlist<AsyncOperation *>::iterator iter(&m_xlist_item);
  for (++iter; !iter.end(); ++iter) {
    if (!writes_only || (*iter)->m_writes) {
      return *iter;
    }
  }
  return nullptr;
}

} // namespace io
} // namespace librbd
