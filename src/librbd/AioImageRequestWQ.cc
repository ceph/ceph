// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/AioImageRequestWQ.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"

namespace librbd {

namespace {

class C_AioReadWQ : public Context {
public:
  C_AioReadWQ(ImageCtx *ictx, uint64_t off, size_t len, char *buf,
              bufferlist *pbl, AioCompletion *c, int op_flags)
    : m_ictx(ictx), m_off(off), m_len(len), m_buf(buf), m_pbl(pbl), m_comp(c),
      m_op_flags(op_flags) {
  }
protected:
  virtual void finish(int r) {
    aio_read(m_ictx, m_off, m_len, m_buf, m_pbl, m_comp, m_op_flags);
  }
private:
  ImageCtx *m_ictx;
  uint64_t m_off;
  uint64_t m_len;
  char *m_buf;
  bufferlist *m_pbl;
  AioCompletion *m_comp;
  int m_op_flags;
};

class C_AioWriteWQ : public Context {
public:
  C_AioWriteWQ(ImageCtx *ictx, uint64_t off, size_t len, const char *buf,
               AioCompletion *c, int op_flags)
    : m_ictx(ictx), m_off(off), m_len(len), m_buf(buf), m_comp(c),
      m_op_flags(op_flags) {
  }
protected:
  virtual void finish(int r) {
    aio_write(m_ictx, m_off, m_len, m_buf, m_comp, m_op_flags);
  }
private:
  ImageCtx *m_ictx;
  uint64_t m_off;
  uint64_t m_len;
  const char *m_buf;
  AioCompletion *m_comp;
  int m_op_flags;
};

class C_AioDiscardWQ : public Context {
public:
  C_AioDiscardWQ(ImageCtx *ictx, uint64_t off, uint64_t len, AioCompletion *c)
    : m_ictx(ictx), m_off(off), m_len(len), m_comp(c) {
  }
protected:
  virtual void finish(int r) {
    aio_discard(m_ictx, m_off, m_len, m_comp);
  }
private:
  ImageCtx *m_ictx;
  uint64_t m_off;
  uint64_t m_len;
  AioCompletion *m_comp;
};

class C_AioFlushWQ : public Context {
public:
  C_AioFlushWQ(ImageCtx *ictx, AioCompletion *c)
    : m_ictx(ictx), m_comp(c) {
  }
protected:
  virtual void finish(int r) {
    aio_flush(m_ictx, m_comp);
  }
private:
  ImageCtx *m_ictx;
  AioCompletion *m_comp;
};

} // anonymous namespace

void AioImageRequestWQ::aio_read(ImageCtx *ictx, uint64_t off, size_t len,
                                 char *buf, bufferlist *pbl, AioCompletion *c,
                                 int op_flags) {
  if (ictx->non_blocking_aio) {
    queue(new C_AioReadWQ(ictx, off, len, buf, pbl, c, op_flags),
          Metadata(false, c));
  } else {
    librbd::aio_read(ictx, off, len, buf, pbl, c, op_flags);
  }
}

void AioImageRequestWQ::aio_write(ImageCtx *ictx, uint64_t off, size_t len,
                                  const char *buf, AioCompletion *c,
                                  int op_flags) {
  if (ictx->non_blocking_aio) {
    queue(new C_AioWriteWQ(ictx, off, len, buf, c, op_flags),
          Metadata(true, c));
  } else {
    librbd::aio_write(ictx, off, len, buf, c, op_flags);
  }
}

void AioImageRequestWQ::aio_discard(ImageCtx *ictx, uint64_t off, uint64_t len,
                                    AioCompletion *c) {
  if (ictx->non_blocking_aio) {
    queue(new C_AioDiscardWQ(ictx, off, len, c), Metadata(true, c));
  } else {
    librbd::aio_discard(ictx, off, len, c);
  }
}

void AioImageRequestWQ::aio_flush(ImageCtx *ictx, AioCompletion *c) {
  if (ictx->non_blocking_aio) {
    queue(new C_AioFlushWQ(ictx, c), Metadata(false, c));
  } else {
    librbd::aio_flush(ictx, c);
  }
}

bool AioImageRequestWQ::writes_empty() const {
  Mutex::Locker locker(m_lock);
  for (ceph::unordered_map<Context *, Metadata>::const_iterator it =
         m_context_metadata.begin();
       it != m_context_metadata.end(); ++it) {
    if (it->second.write_op) {
      return false;
    }
  }
  return true;
}

void *AioImageRequestWQ::_void_dequeue() {
  Context *peek_item = front();
  if (peek_item == NULL) {
    return NULL;
  }

  {
    Mutex::Locker locker(m_lock);
    ceph::unordered_map<Context *, Metadata>::iterator it =
      m_context_metadata.find(peek_item);
    assert(it != m_context_metadata.end());

    if (it->second.write_op) {
      if (m_writes_suspended) {
        return NULL;
      }
      ++m_in_progress_writes;
    }
  }

  Context *item = reinterpret_cast<Context *>(
    ThreadPool::PointerWQ<Context>::_void_dequeue());
  assert(peek_item == item);
  return item;
}

void AioImageRequestWQ::process(Context *ctx) {
  Metadata metadata;
  {
    Mutex::Locker locker(m_lock);
    ceph::unordered_map<Context *, Metadata>::iterator it =
      m_context_metadata.find(ctx);
    assert(it != m_context_metadata.end());

    metadata = it->second;
    m_context_metadata.erase(it);
  }

  // TODO
  ctx->complete(0);

  {
    Mutex::Locker locker(m_lock);
    if (metadata.write_op) {
      assert(m_in_progress_writes > 0);
      if (--m_in_progress_writes == 0) {
        m_cond.Signal();
      }
    }
  }
}

void AioImageRequestWQ::queue(Context *ctx, const Metadata &metadata) {
  {
    Mutex::Locker locker(m_lock);
    m_context_metadata[ctx] = metadata;
  }
  ThreadPool::PointerWQ<Context>::queue(ctx);
}

} // namespace librbd
