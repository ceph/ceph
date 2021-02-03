// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_AIO_COMPLETION_H
#define CEPH_LIBRBD_IO_AIO_COMPLETION_H

#include "common/ceph_time.h"
#include "include/common_fwd.h"
#include "include/Context.h"
#include "include/utime.h"
#include "include/rbd/librbd.hpp"

#include "librbd/ImageCtx.h"
#include "librbd/io/AsyncOperation.h"
#include "librbd/io/ReadResult.h"
#include "librbd/io/Types.h"

#include <atomic>
#include <condition_variable>
#include <mutex>

struct Context;

namespace librbd {
namespace io {

/**
 * AioCompletion is the overall completion for a single
 * rbd I/O request. It may be composed of many AioObjectRequests,
 * which each go to a single object.
 *
 * The retrying of individual requests is handled at a lower level,
 * so all AioCompletion cares about is the count of outstanding
 * requests. The number of expected individual requests should be
 * set initially using set_request_count() prior to issuing the
 * requests.  This ensures that the completion will not be completed
 * within the caller's thread of execution (instead via a librados
 * context or via a thread pool context for cache read hits).
 */
struct AioCompletion {
  typedef enum {
    AIO_STATE_PENDING = 0,
    AIO_STATE_CALLBACK,
    AIO_STATE_COMPLETE,
  } aio_state_t;

  mutable std::mutex lock;
  std::condition_variable cond;

  callback_t complete_cb = nullptr;
  void *complete_arg = nullptr;
  rbd_completion_t rbd_comp = nullptr;

  /// note: only using atomic for built-in memory barrier
  std::atomic<aio_state_t> state{AIO_STATE_PENDING};

  std::atomic<ssize_t> rval{0};
  std::atomic<int> error_rval{0};
  std::atomic<uint32_t> ref{1};
  std::atomic<uint32_t> pending_count{0};   ///< number of requests/blocks
  std::atomic<bool> released{false};

  ImageCtx *ictx = nullptr;
  coarse_mono_time start_time;
  aio_type_t aio_type = AIO_TYPE_NONE;

  ReadResult read_result;

  AsyncOperation async_op;

  bool event_notify = false;
  bool was_armed = false;
  bool external_callback = false;

  Context* image_dispatcher_ctx = nullptr;

  template <typename T, void (T::*MF)(int)>
  static void callback_adapter(completion_t cb, void *arg) {
    AioCompletion *comp = reinterpret_cast<AioCompletion *>(cb);
    T *t = reinterpret_cast<T *>(arg);
    (t->*MF)(comp->get_return_value());
    comp->release();
  }

  static AioCompletion *create(void *cb_arg, callback_t cb_complete,
                               rbd_completion_t rbd_comp) {
    AioCompletion *comp = new AioCompletion();
    comp->set_complete_cb(cb_arg, cb_complete);
    comp->rbd_comp = (rbd_comp != nullptr ? rbd_comp : comp);
    return comp;
  }

  template <typename T, void (T::*MF)(int) = &T::complete>
  static AioCompletion *create(T *obj) {
    AioCompletion *comp = new AioCompletion();
    comp->set_complete_cb(obj, &callback_adapter<T, MF>);
    comp->rbd_comp = comp;
    return comp;
  }

  template <typename T, void (T::*MF)(int) = &T::complete>
  static AioCompletion *create_and_start(T *obj, ImageCtx *image_ctx,
                                         aio_type_t type) {
    AioCompletion *comp = create<T, MF>(obj);
    comp->init_time(image_ctx, type);
    comp->start_op();
    return comp;
  }

  AioCompletion() {
  }

  ~AioCompletion() {
  }

  int wait_for_complete();

  void finalize();

  inline bool is_initialized(aio_type_t type) const {
    std::unique_lock<std::mutex> locker(lock);
    return ((ictx != nullptr) && (aio_type == type));
  }
  inline bool is_started() const {
    std::unique_lock<std::mutex> locker(lock);
    return async_op.started();
  }

  void block(CephContext* cct);
  void unblock(CephContext* cct);

  void init_time(ImageCtx *i, aio_type_t t);
  void start_op();
  void fail(int r);

  void complete();

  void set_complete_cb(void *cb_arg, callback_t cb) {
    complete_cb = cb;
    complete_arg = cb_arg;
  }

  void set_request_count(uint32_t num);
  void add_request() {
    ceph_assert(pending_count > 0);
    get();
  }
  void complete_request(ssize_t r);

  bool is_complete();

  ssize_t get_return_value();

  void get() {
    ceph_assert(ref > 0);
    ++ref;
  }
  void release() {
    bool previous_released = released.exchange(true);
    ceph_assert(!previous_released);
    put();
  }
  void put() {
    uint32_t previous_ref = ref--;
    ceph_assert(previous_ref > 0);

    if (previous_ref == 1) {
      delete this;
    }
  }

  void set_event_notify(bool s) {
    event_notify = s;
  }

  void *get_arg() {
    return complete_arg;
  }

private:
  void queue_complete();
  void complete_external_callback();
  void complete_event_socket();
  void notify_callbacks_complete();
};

class C_AioRequest : public Context {
public:
  C_AioRequest(AioCompletion *completion) : m_completion(completion) {
    m_completion->add_request();
  }
  ~C_AioRequest() override {}
  void finish(int r) override {
    m_completion->complete_request(r);
  }
protected:
  AioCompletion *m_completion;
};

} // namespace io
} // namespace librbd

#endif // CEPH_LIBRBD_IO_AIO_COMPLETION_H
