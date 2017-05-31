// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_AIO_COMPLETION_H
#define CEPH_LIBRBD_IO_AIO_COMPLETION_H

#include "common/Cond.h"
#include "common/Mutex.h"
#include "include/Context.h"
#include "include/utime.h"
#include "include/rbd/librbd.hpp"

#include "librbd/ImageCtx.h"
#include "librbd/io/AsyncOperation.h"
#include "librbd/io/ReadResult.h"
#include "librbd/io/Types.h"

class CephContext;

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

  mutable Mutex lock;
  Cond cond;
  aio_state_t state;
  ssize_t rval;
  callback_t complete_cb;
  void *complete_arg;
  rbd_completion_t rbd_comp;
  uint32_t pending_count;   ///< number of requests
  uint32_t blockers;
  int ref;
  bool released;
  ImageCtx *ictx;
  utime_t start_time;
  aio_type_t aio_type;

  ReadResult read_result;

  AsyncOperation async_op;

  uint64_t journal_tid;
  xlist<AioCompletion*>::item m_xlist_item;
  bool event_notify;

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

  AioCompletion() : lock("AioCompletion::lock", true, false),
                    state(AIO_STATE_PENDING), rval(0), complete_cb(NULL),
                    complete_arg(NULL), rbd_comp(NULL),
                    pending_count(0), blockers(1),
                    ref(1), released(false), ictx(NULL),
                    aio_type(AIO_TYPE_NONE),
                    journal_tid(0), m_xlist_item(this), event_notify(false) {
  }

  ~AioCompletion() {
  }

  int wait_for_complete();

  void finalize(ssize_t rval);

  inline bool is_initialized(aio_type_t type) const {
    Mutex::Locker locker(lock);
    return ((ictx != nullptr) && (aio_type == type));
  }
  inline bool is_started() const {
    Mutex::Locker locker(lock);
    return async_op.started();
  }

  void init_time(ImageCtx *i, aio_type_t t);
  void start_op(bool ignore_type = false);
  void fail(int r);

  void complete();

  void set_complete_cb(void *cb_arg, callback_t cb) {
    complete_cb = cb;
    complete_arg = cb_arg;
  }

  void set_request_count(uint32_t num);
  void add_request() {
    lock.Lock();
    assert(pending_count > 0);
    lock.Unlock();
    get();
  }
  void complete_request(ssize_t r);

  void associate_journal_event(uint64_t tid);

  bool is_complete();

  ssize_t get_return_value();

  void get() {
    lock.Lock();
    assert(ref > 0);
    ref++;
    lock.Unlock();
  }
  void release() {
    lock.Lock();
    assert(!released);
    released = true;
    put_unlock();
  }
  void put() {
    lock.Lock();
    put_unlock();
  }
  void put_unlock() {
    assert(ref > 0);
    int n = --ref;
    lock.Unlock();
    if (!n) {
      if (ictx) {
        if (event_notify) {
          ictx->completed_reqs_lock.Lock();
          m_xlist_item.remove_myself();
          ictx->completed_reqs_lock.Unlock();
        }
        if (aio_type == AIO_TYPE_CLOSE ||
            (aio_type == AIO_TYPE_OPEN && rval < 0)) {
          delete ictx;
        }
      }
      delete this;
    }
  }

  void block() {
    Mutex::Locker l(lock);
    ++blockers;
  }
  void unblock() {
    Mutex::Locker l(lock);
    assert(blockers > 0);
    --blockers;
    if (pending_count == 0 && blockers == 0) {
      finalize(rval);
      complete();
    }
  }

  void set_event_notify(bool s) {
    Mutex::Locker l(lock);
    event_notify = s;
  }

  void *get_arg() {
    return complete_arg;
  }
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
