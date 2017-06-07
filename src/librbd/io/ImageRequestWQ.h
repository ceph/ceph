// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_IMAGE_REQUEST_WQ_H
#define CEPH_LIBRBD_IO_IMAGE_REQUEST_WQ_H

#include "include/Context.h"
#include "common/RWLock.h"
#include "common/WorkQueue.h"

#include <list>
#include <atomic>

namespace librbd {

class ImageCtx;

namespace io {

class AioCompletion;
template <typename> class ImageRequest;
class ReadResult;

class ImageRequestWQ : protected ThreadPool::PointerWQ<ImageRequest<ImageCtx> > {
public:
  ImageRequestWQ(ImageCtx *image_ctx, const string &name, time_t ti,
                 ThreadPool *tp);

  ssize_t read(uint64_t off, uint64_t len, ReadResult &&read_result,
               int op_flags);
  ssize_t write(uint64_t off, uint64_t len, bufferlist &&bl, int op_flags);
  ssize_t discard(uint64_t off, uint64_t len, bool skip_partial_discard);
  ssize_t writesame(uint64_t off, uint64_t len, bufferlist &&bl, int op_flags);

  void aio_read(AioCompletion *c, uint64_t off, uint64_t len,
                ReadResult &&read_result, int op_flags, bool native_async=true);
  void aio_write(AioCompletion *c, uint64_t off, uint64_t len,
                 bufferlist &&bl, int op_flags, bool native_async=true);
  void aio_discard(AioCompletion *c, uint64_t off, uint64_t len,
                   bool skip_partial_discard, bool native_async=true);
  void aio_flush(AioCompletion *c, bool native_async=true);
  void aio_writesame(AioCompletion *c, uint64_t off, uint64_t len,
                     bufferlist &&bl, int op_flags, bool native_async=true);

  using ThreadPool::PointerWQ<ImageRequest<ImageCtx> >::drain;

  using ThreadPool::PointerWQ<ImageRequest<ImageCtx> >::empty;

  void shut_down(Context *on_shutdown);

  bool is_lock_required() const;
  bool is_lock_request_needed() const;

  inline bool writes_blocked() const {
    RWLock::RLocker locker(m_lock);
    return (m_write_blockers > 0);
  }

  int block_writes();
  void block_writes(Context *on_blocked);
  void unblock_writes();

  void set_require_lock_on_read();
  void clear_require_lock_on_read();

protected:
  void *_void_dequeue() override;
  void process(ImageRequest<ImageCtx> *req) override;

private:
  typedef std::list<Context *> Contexts;

  struct C_RefreshFinish : public Context {
    ImageRequestWQ *aio_work_queue;
    ImageRequest<ImageCtx> *aio_image_request;

    C_RefreshFinish(ImageRequestWQ *aio_work_queue,
                    ImageRequest<ImageCtx> *aio_image_request)
      : aio_work_queue(aio_work_queue), aio_image_request(aio_image_request) {
    }
    void finish(int r) override {
      aio_work_queue->handle_refreshed(r, aio_image_request);
    }
  };

  struct C_BlockedWrites : public Context {
    ImageRequestWQ *aio_work_queue;
    C_BlockedWrites(ImageRequestWQ *_aio_work_queue)
      : aio_work_queue(_aio_work_queue) {
    }

    void finish(int r) override {
      aio_work_queue->handle_blocked_writes(r);
    }
  };

  ImageCtx &m_image_ctx;
  mutable RWLock m_lock;
  Contexts m_write_blocker_contexts;
  uint32_t m_write_blockers;
  bool m_require_lock_on_read = false;
  std::atomic<unsigned> m_in_progress_writes { 0 };
  std::atomic<unsigned> m_queued_reads { 0 };
  std::atomic<unsigned> m_queued_writes { 0 };
  std::atomic<unsigned> m_in_flight_ops { 0 };

  bool m_refresh_in_progress;

  bool m_shutdown;
  Context *m_on_shutdown;

  inline bool writes_empty() const {
    RWLock::RLocker locker(m_lock);
    return (m_queued_writes == 0);
  }

  void finish_queued_op(ImageRequest<ImageCtx> *req);
  void finish_in_progress_write();

  int start_in_flight_op(AioCompletion *c);
  void finish_in_flight_op();

  void queue(ImageRequest<ImageCtx> *req);

  void handle_refreshed(int r, ImageRequest<ImageCtx> *req);
  void handle_blocked_writes(int r);
};

} // namespace io
} // namespace librbd

#endif // CEPH_LIBRBD_IO_IMAGE_REQUEST_WQ_H
