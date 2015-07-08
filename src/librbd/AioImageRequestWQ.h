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
  AioImageRequestWQ(ImageCtx *image_ctx, const string &name, time_t ti,
                    ThreadPool *tp)
    : ThreadPool::PointerWQ<AioImageRequest>(name, ti, 0, tp),
      m_image_ctx(*image_ctx), m_lock("AioImageRequestWQ::m_lock"),
      m_writes_suspended(false), m_in_progress_writes(0), m_queued_writes(0) {
  }

  ssize_t read(uint64_t off, size_t len, char *buf, int op_flags);
  ssize_t write(uint64_t off, size_t len, const char *buf, int op_flags);
  int discard(uint64_t off, uint64_t len);

  void aio_read(AioCompletion *c, uint64_t off, size_t len, char *buf,
                bufferlist *pbl, int op_flags);
  void aio_write(AioCompletion *c, uint64_t off, size_t len, const char *buf,
                 int op_flags);
  void aio_discard(AioCompletion *c, uint64_t off, uint64_t len);
  void aio_flush(AioCompletion *c);

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
  ImageCtx &m_image_ctx;
  mutable Mutex m_lock;
  Cond m_cond;
  bool m_writes_suspended;
  uint32_t m_in_progress_writes;
  uint32_t m_queued_writes;

  bool is_lock_required();
  void queue(AioImageRequest *req);
};

} // namespace librbd

#endif // CEPH_LIBRBD_AIO_IMAGE_REQUEST_WQ_H
