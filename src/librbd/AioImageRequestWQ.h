// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_AIO_IMAGE_REQUEST_WQ_H
#define CEPH_LIBRBD_AIO_IMAGE_REQUEST_WQ_H

#include "include/Context.h"
#include "include/atomic.h"
#include "common/Cond.h"
#include "common/RWLock.h"
#include "common/WorkQueue.h"
#include <list>

namespace librbd {

class AioCompletion;
class AioImageRequest;
class ImageCtx;

class AioImageRequestWQ : protected ThreadPool::PointerWQ<AioImageRequest> {
public:
  AioImageRequestWQ(ImageCtx *image_ctx, const string &name, time_t ti,
                    ThreadPool *tp);

  ssize_t read(uint64_t off, uint64_t len, char *buf, int op_flags);
  ssize_t write(uint64_t off, uint64_t len, const char *buf, int op_flags);
  int discard(uint64_t off, uint64_t len);

  void aio_read(AioCompletion *c, uint64_t off, uint64_t len, char *buf,
                bufferlist *pbl, int op_flags, bool native_async=true);
  void aio_write(AioCompletion *c, uint64_t off, uint64_t len, const char *buf,
                 int op_flags, bool native_async=true);
  void aio_discard(AioCompletion *c, uint64_t off, uint64_t len, bool native_async=true);
  void aio_flush(AioCompletion *c, bool native_async=true);

  using ThreadPool::PointerWQ<AioImageRequest>::drain;
  using ThreadPool::PointerWQ<AioImageRequest>::empty;

  inline bool writes_empty() const {
    RWLock::RLocker locker(m_lock);
    return (m_queued_writes.read() == 0);
  }

  inline bool writes_blocked() const {
    RWLock::RLocker locker(m_lock);
    return (m_write_blockers > 0);
  }

  void shut_down(Context *on_shutdown);

  void block_writes();
  void block_writes(Context *on_blocked);
  void unblock_writes();

protected:
  virtual void *_void_dequeue();
  virtual void process(AioImageRequest *req);

private:
  typedef std::list<Context *> Contexts;

  struct C_RefreshFinish : public Context {
    AioImageRequestWQ *aio_work_queue;
    AioImageRequest *aio_image_request;

    C_RefreshFinish(AioImageRequestWQ *aio_work_queue,
                    AioImageRequest *aio_image_request)
      : aio_work_queue(aio_work_queue), aio_image_request(aio_image_request) {
    }
    virtual void finish(int r) override {
      aio_work_queue->handle_refreshed(r, aio_image_request);
    }
  };

  struct C_BlockedWrites : public Context {
    AioImageRequestWQ *aio_work_queue;
    C_BlockedWrites(AioImageRequestWQ *_aio_work_queue)
      : aio_work_queue(_aio_work_queue) {
    }

    virtual void finish(int r) {
      aio_work_queue->handle_blocked_writes(r);
    }
  };

  ImageCtx &m_image_ctx;
  mutable RWLock m_lock;
  Contexts m_write_blocker_contexts;
  uint32_t m_write_blockers;
  atomic_t m_in_progress_writes;
  atomic_t m_queued_writes;
  atomic_t m_in_flight_ops;

  bool m_refresh_in_progress;

  bool m_shutdown;
  Context *m_on_shutdown;

  int start_in_flight_op(AioCompletion *c);
  void finish_in_flight_op();

  bool is_journal_required() const;
  bool is_lock_required() const;
  void queue(AioImageRequest *req);

  void handle_refreshed(int r, AioImageRequest *req);
  void handle_blocked_writes(int r);
};

} // namespace librbd

#endif // CEPH_LIBRBD_AIO_IMAGE_REQUEST_WQ_H
