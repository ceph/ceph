// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_AIO_IMAGE_REQUEST_WQ_H
#define CEPH_LIBRBD_AIO_IMAGE_REQUEST_WQ_H

#include "include/Context.h"
#include "common/WorkQueue.h"
#include "common/Mutex.h"
#include "librbd/ImageWatcher.h"

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
    Mutex::Locker locker(m_lock);
    return (m_queued_writes == 0);
  }

  inline bool writes_blocked() const {
    Mutex::Locker locker(m_lock);
    return (m_write_blockers > 0);
  }

  void block_writes();
  void block_writes(Context *on_blocked);
  void unblock_writes();

  void register_lock_listener();

protected:
  virtual void *_void_dequeue();
  virtual void process(AioImageRequest *req);

private:
  typedef std::list<Context *> Contexts;

  struct LockListener : public ImageWatcher::Listener {
    AioImageRequestWQ *aio_work_queue;
    LockListener(AioImageRequestWQ *_aio_work_queue)
      : aio_work_queue(_aio_work_queue) {
    }

    virtual bool handle_requested_lock() {
      return true;
    }
    virtual void handle_lock_updated(ImageWatcher::LockUpdateState state) {
      aio_work_queue->handle_lock_updated(state);
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
  mutable Mutex m_lock;
  Contexts m_write_blocker_contexts;
  uint32_t m_write_blockers;
  uint32_t m_in_progress_writes;
  uint32_t m_queued_writes;

  LockListener m_lock_listener;
  bool m_blocking_writes;

  bool is_journal_required() const;
  bool is_lock_required() const;
  void queue(AioImageRequest *req);

  void handle_lock_updated(ImageWatcher::LockUpdateState state);
  void handle_blocked_writes(int r);
};

} // namespace librbd

#endif // CEPH_LIBRBD_AIO_IMAGE_REQUEST_WQ_H
