// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_AIO_IMAGE_REQUEST_WQ_H
#define CEPH_LIBRBD_AIO_IMAGE_REQUEST_WQ_H

#include "common/WorkQueue.h"
#include "common/Mutex.h"

namespace librbd {

class AioCompletion;
class AioImageRequest;
class ImageCtx;

class AioImageRequestWQ : protected ThreadPool::PointerWQ<AioImageRequest> {
public:
  AioImageRequestWQ(const string &name, time_t ti, ThreadPool *tp)
    : ThreadPool::PointerWQ<AioImageRequest>(name, ti, 0, tp),
      m_lock("AioImageRequestWQ::m_lock"), m_writes_suspended(false),
      m_in_progress_writes(0), m_queued_writes(0) {
  }

  void aio_read(ImageCtx *ictx, uint64_t off, size_t len, char *buf,
                bufferlist *pbl, AioCompletion *c, int op_flags);
  void aio_write(ImageCtx *ictx, uint64_t off, size_t len, const char *buf,
                 AioCompletion *c, int op_flags);
  void aio_discard(ImageCtx *ictx, uint64_t off, uint64_t len,
                   AioCompletion *c);
  void aio_flush(ImageCtx *ictx, AioCompletion *c);

  using ThreadPool::PointerWQ<AioImageRequest>::drain;

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
  virtual void *_void_dequeue();
  virtual void process(AioImageRequest *req);
private:
  mutable Mutex m_lock;
  Cond m_cond;
  bool m_writes_suspended;
  uint32_t m_in_progress_writes;
  uint32_t m_queued_writes;

  void queue(AioImageRequest *req);
};

} // namespace librbd

#endif // CEPH_LIBRBD_AIO_IMAGE_REQUEST_WQ_H
