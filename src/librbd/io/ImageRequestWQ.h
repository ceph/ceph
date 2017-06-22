// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_IMAGE_REQUEST_WQ_H
#define CEPH_LIBRBD_IO_IMAGE_REQUEST_WQ_H

#include "include/Context.h"
#include "common/RWLock.h"
#include "common/WorkQueue.h"
#include "librbd/io/Types.h"

#include <list>
#include <atomic>

namespace librbd {

class ImageCtx;

namespace io {

class AioCompletion;
template <typename> class ImageRequest;
class ReadResult;

template <typename ImageCtxT = librbd::ImageCtx>
class ImageRequestWQ
  : public ThreadPool::PointerWQ<ImageRequest<ImageCtxT> > {
public:
  ImageRequestWQ(ImageCtxT *image_ctx, const string &name, time_t ti,
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

  using ThreadPool::PointerWQ<ImageRequest<ImageCtxT> >::drain;

  using ThreadPool::PointerWQ<ImageRequest<ImageCtxT> >::empty;

  void shut_down(Context *on_shutdown);

  inline bool writes_blocked() const {
    RWLock::RLocker locker(m_lock);
    return (m_write_blockers > 0);
  }

  int block_writes();
  void block_writes(Context *on_blocked);
  void unblock_writes();

  void set_require_lock(Direction direction, bool enabled);

protected:
  void *_void_dequeue() override;
  void process(ImageRequest<ImageCtxT> *req) override;

private:
  typedef std::list<Context *> Contexts;

  struct C_AcquireLock;
  struct C_BlockedWrites;
  struct C_RefreshFinish;

  ImageCtxT &m_image_ctx;
  mutable RWLock m_lock;
  Contexts m_write_blocker_contexts;
  uint32_t m_write_blockers = 0;
  bool m_require_lock_on_read = false;
  bool m_require_lock_on_write = false;
  std::atomic<unsigned> m_queued_reads { 0 };
  std::atomic<unsigned> m_queued_writes { 0 };
  std::atomic<unsigned> m_in_flight_ios { 0 };
  std::atomic<unsigned> m_in_flight_writes { 0 };
  std::atomic<unsigned> m_io_blockers { 0 };

  bool m_shutdown = false;
  Context *m_on_shutdown = nullptr;

  bool is_lock_required(bool write_op) const;

  inline bool require_lock_on_read() const {
    RWLock::RLocker locker(m_lock);
    return m_require_lock_on_read;
  }
  inline bool writes_empty() const {
    RWLock::RLocker locker(m_lock);
    return (m_queued_writes == 0);
  }

  void finish_queued_io(ImageRequest<ImageCtxT> *req);
  void finish_in_flight_write();

  int start_in_flight_io(AioCompletion *c);
  void finish_in_flight_io();
  void fail_in_flight_io(int r, ImageRequest<ImageCtxT> *req);

  void queue(ImageRequest<ImageCtxT> *req);

  void handle_acquire_lock(int r, ImageRequest<ImageCtxT> *req);
  void handle_refreshed(int r, ImageRequest<ImageCtxT> *req);
  void handle_blocked_writes(int r);
};

} // namespace io
} // namespace librbd

extern template class librbd::io::ImageRequestWQ<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IO_IMAGE_REQUEST_WQ_H
