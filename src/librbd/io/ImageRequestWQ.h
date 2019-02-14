// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_IMAGE_REQUEST_WQ_H
#define CEPH_LIBRBD_IO_IMAGE_REQUEST_WQ_H

#include "include/Context.h"
#include "common/RWLock.h"
#include "common/Throttle.h"
#include "common/WorkQueue.h"
#include "librbd/io/Types.h"

#include <list>
#include <atomic>

namespace librbd {

class ImageCtx;

namespace io {

class AioCompletion;
template <typename> class ImageDispatchSpec;
class ReadResult;

template <typename ImageCtxT = librbd::ImageCtx>
class ImageRequestWQ
  : public ThreadPool::PointerWQ<ImageDispatchSpec<ImageCtxT> > {
public:
  ImageRequestWQ(ImageCtxT *image_ctx, const string &name, time_t ti,
                 ThreadPool *tp);
  ~ImageRequestWQ();

  ssize_t read(uint64_t off, uint64_t len, ReadResult &&read_result,
               int op_flags);
  ssize_t write(uint64_t off, uint64_t len, bufferlist &&bl, int op_flags);
  ssize_t discard(uint64_t off, uint64_t len,
                  uint32_t discard_granularity_bytes);
  ssize_t writesame(uint64_t off, uint64_t len, bufferlist &&bl, int op_flags);
  ssize_t compare_and_write(uint64_t off, uint64_t len,
                            bufferlist &&cmp_bl, bufferlist &&bl,
                            uint64_t *mismatch_off, int op_flags);
  int flush();

  void aio_read(AioCompletion *c, uint64_t off, uint64_t len,
                ReadResult &&read_result, int op_flags, bool native_async=true);
  void aio_write(AioCompletion *c, uint64_t off, uint64_t len,
                 bufferlist &&bl, int op_flags, bool native_async=true);
  void aio_discard(AioCompletion *c, uint64_t off, uint64_t len,
                   uint32_t discard_granularity_bytes, bool native_async=true);
  void aio_flush(AioCompletion *c, bool native_async=true);
  void aio_writesame(AioCompletion *c, uint64_t off, uint64_t len,
                     bufferlist &&bl, int op_flags, bool native_async=true);
  void aio_compare_and_write(AioCompletion *c, uint64_t off,
                             uint64_t len, bufferlist &&cmp_bl,
                             bufferlist &&bl, uint64_t *mismatch_off,
                             int op_flags, bool native_async=true);

  using ThreadPool::PointerWQ<ImageDispatchSpec<ImageCtxT> >::drain;
  using ThreadPool::PointerWQ<ImageDispatchSpec<ImageCtxT> >::empty;

  void shut_down(Context *on_shutdown);

  inline bool writes_blocked() const {
    RWLock::RLocker locker(m_lock);
    return (m_write_blockers > 0);
  }

  int block_writes();
  void block_writes(Context *on_blocked);
  void unblock_writes();

  void wait_on_writes_unblocked(Context *on_unblocked);

  void set_require_lock(Direction direction, bool enabled);

  void apply_qos_schedule_tick_min(uint64_t tick);

  void apply_qos_limit(const uint64_t flag, uint64_t limit, uint64_t burst);
protected:
  void *_void_dequeue() override;
  void process(ImageDispatchSpec<ImageCtxT> *req) override;
  bool _empty() override {
    return (ThreadPool::PointerWQ<ImageDispatchSpec<ImageCtxT>>::_empty() &&
            m_io_throttled.load() == 0);
  }


private:
  typedef std::list<Context *> Contexts;

  struct C_AcquireLock;
  struct C_BlockedWrites;
  struct C_RefreshFinish;

  ImageCtxT &m_image_ctx;
  mutable RWLock m_lock;
  Contexts m_write_blocker_contexts;
  uint32_t m_write_blockers = 0;
  Contexts m_unblocked_write_waiter_contexts;
  bool m_require_lock_on_read = false;
  bool m_require_lock_on_write = false;
  std::atomic<unsigned> m_queued_reads { 0 };
  std::atomic<unsigned> m_queued_writes { 0 };
  std::atomic<unsigned> m_in_flight_ios { 0 };
  std::atomic<unsigned> m_in_flight_writes { 0 };
  std::atomic<unsigned> m_io_blockers { 0 };
  std::atomic<unsigned> m_io_throttled { 0 };

  std::list<std::pair<uint64_t, TokenBucketThrottle*> > m_throttles;
  uint64_t m_qos_enabled_flag = 0;

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

  bool needs_throttle(ImageDispatchSpec<ImageCtxT> *item);

  void finish_queued_io(ImageDispatchSpec<ImageCtxT> *req);
  void finish_in_flight_write();

  int start_in_flight_io(AioCompletion *c);
  void finish_in_flight_io();
  void fail_in_flight_io(int r, ImageDispatchSpec<ImageCtxT> *req);

  void queue(ImageDispatchSpec<ImageCtxT> *req);

  void handle_acquire_lock(int r, ImageDispatchSpec<ImageCtxT> *req);
  void handle_refreshed(int r, ImageDispatchSpec<ImageCtxT> *req);
  void handle_blocked_writes(int r);

  void handle_throttle_ready(int r, ImageDispatchSpec<ImageCtxT> *item, uint64_t flag);
};

} // namespace io
} // namespace librbd

extern template class librbd::io::ImageRequestWQ<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IO_IMAGE_REQUEST_WQ_H
