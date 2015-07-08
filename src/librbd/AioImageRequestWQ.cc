// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/AioImageRequestWQ.h"
#include "librbd/AioImageRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"

namespace librbd {

void AioImageRequestWQ::aio_read(AioCompletion *c, uint64_t off, size_t len,
                                 char *buf, bufferlist *pbl, int op_flags) {
  if (m_image_ctx.non_blocking_aio) {
    queue(new AioImageRead(m_image_ctx, c, off, len, buf, pbl, op_flags));
  } else {
    AioImageRequest::read(&m_image_ctx, c, off, len, buf, pbl, op_flags);
  }
}

void AioImageRequestWQ::aio_write(AioCompletion *c, uint64_t off, size_t len,
                                  const char *buf, int op_flags) {
  if (m_image_ctx.non_blocking_aio) {
    queue(new AioImageWrite(m_image_ctx, c, off, len, buf, op_flags));
  } else {
    AioImageRequest::write(&m_image_ctx, c, off, len, buf, op_flags);
  }
}

void AioImageRequestWQ::aio_discard(AioCompletion *c, uint64_t off,
                                    uint64_t len) {
  if (m_image_ctx.non_blocking_aio) {
    queue(new AioImageDiscard(m_image_ctx, c, off, len));
  } else {
    AioImageRequest::discard(&m_image_ctx, c, off, len);
  }
}

void AioImageRequestWQ::aio_flush(AioCompletion *c) {
  if (m_image_ctx.non_blocking_aio) {
    queue(new AioImageFlush(m_image_ctx, c));
  } else {
    AioImageRequest::flush(&m_image_ctx, c);
  }
}

bool AioImageRequestWQ::writes_empty() const {
  Mutex::Locker locker(m_lock);
  return (m_queued_writes > 0);
}

void *AioImageRequestWQ::_void_dequeue() {
  AioImageRequest *peek_item = front();
  if (peek_item == NULL) {
    return NULL;
  }

  {
    if (peek_item->is_write_op()) {
      Mutex::Locker locker(m_lock);
      if (m_writes_suspended) {
        return NULL;
      }
      ++m_in_progress_writes;
    }
  }

  AioImageRequest *item = reinterpret_cast<AioImageRequest *>(
    ThreadPool::PointerWQ<AioImageRequest>::_void_dequeue());
  assert(peek_item == item);
  return item;
}

void AioImageRequestWQ::process(AioImageRequest *req) {
  req->send();

  {
    Mutex::Locker locker(m_lock);
    if (req->is_write_op()) {
      assert(m_queued_writes > 0);
      --m_queued_writes;

      assert(m_in_progress_writes > 0);
      if (--m_in_progress_writes == 0) {
        m_cond.Signal();
      }
    }
  }
  delete req;
}

void AioImageRequestWQ::queue(AioImageRequest *req) {
  {
    Mutex::Locker locker(m_lock);
    if (req->is_write_op()) {
      ++m_queued_writes;
    }
  }
  ThreadPool::PointerWQ<AioImageRequest>::queue(req);
}

} // namespace librbd
