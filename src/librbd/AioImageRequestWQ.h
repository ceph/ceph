// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_AIO_IMAGE_REQUEST_WQ_H
#define CEPH_LIBRBD_AIO_IMAGE_REQUEST_WQ_H

#include "common/WorkQueue.h"
#include "common/Mutex.h"
#include "include/unordered_map.h"

namespace librbd {

class AioCompletion;
class ImageCtx;

class AioImageRequestWQ : protected ThreadPool::PointerWQ<Context> {
public:
  AioImageRequestWQ(const string &name, time_t ti, ThreadPool *tp)
    : ThreadPool::PointerWQ<Context>(name, ti, 0, tp),
      m_lock("AioImageRequestWQ::m_lock"), m_writes_suspended(false),
      m_in_progress_writes(0) {
  }

  void aio_read(ImageCtx *ictx, uint64_t off, size_t len, char *buf,
                bufferlist *pbl, AioCompletion *c, int op_flags);
  void aio_write(ImageCtx *ictx, uint64_t off, size_t len, const char *buf,
                 AioCompletion *c, int op_flags);
  void aio_discard(ImageCtx *ictx, uint64_t off, uint64_t len,
                   AioCompletion *c);
  void aio_flush(ImageCtx *ictx, AioCompletion *c);

  using ThreadPool::PointerWQ<Context>::drain;

  bool writes_empty() const;
  inline bool writes_suspended() const {
    Mutex::Locker locker(m_lock);
    return m_writes_suspended;
  }

  void suspend_writes() {
    Mutex::Locker locker(m_lock);
    while (m_in_progress_writes > 0) {
      m_cond.Wait(m_lock);
    }
  }

  void resume_writes() {
    {
      Mutex::Locker locker(m_lock);
      m_writes_suspended = false;
    }
    signal();
  }
protected:
  virtual void _clear() {
    ThreadPool::PointerWQ<Context>::_clear();
    m_context_metadata.clear();
  }

  virtual void *_void_dequeue();
  virtual void process(Context *ctx);
private:
  struct Metadata {
    bool write_op;
    AioCompletion *aio_comp;

    Metadata() : write_op(false), aio_comp(NULL) {}
    Metadata(bool _write_op, AioCompletion *_aio_comp)
      : write_op(_write_op), aio_comp(_aio_comp) {}
  };

  mutable Mutex m_lock;
  Cond m_cond;
  bool m_writes_suspended;
  uint32_t m_in_progress_writes;
  ceph::unordered_map<Context *, Metadata> m_context_metadata;

  void queue(Context *ctx, const Metadata &metadata);
};

} // namespace librbd

#endif // CEPH_LIBRBD_AIO_IMAGE_REQUEST_WQ_H
