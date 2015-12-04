// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_AIOCOMPLETION_H
#define CEPH_LIBRBD_AIOCOMPLETION_H

#include "common/Cond.h"
#include "common/Mutex.h"
#include "include/Context.h"
#include "include/utime.h"
#include "include/rbd/librbd.hpp"

#include "librbd/AsyncOperation.h"
#include "librbd/ImageCtx.h"

#include "osdc/Striper.h"

class CephContext;

namespace librbd {

  class AioObjectRead;

  typedef enum {
    AIO_TYPE_READ = 0,
    AIO_TYPE_WRITE,
    AIO_TYPE_DISCARD,
    AIO_TYPE_FLUSH,
    AIO_TYPE_NONE,
  } aio_type_t;

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
    Mutex lock;
    Cond cond;
    bool done;
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

    Striper::StripedReadResult destriper;
    bufferlist *read_bl;
    char *read_buf;
    size_t read_buf_len;

    AsyncOperation async_op;

    uint64_t journal_tid;
    xlist<AioCompletion*>::item m_xlist_item;
    bool event_notify;

    AioCompletion() : lock("AioCompletion::lock", true, false),
		      done(false), rval(0), complete_cb(NULL),
		      complete_arg(NULL), rbd_comp(NULL),
		      pending_count(0), blockers(1),
		      ref(1), released(false), ictx(NULL),
		      aio_type(AIO_TYPE_NONE),
		      read_bl(NULL), read_buf(NULL), read_buf_len(0),
                      journal_tid(0),
                      m_xlist_item(this), event_notify(false) {
    }
    ~AioCompletion() {
    }

    int wait_for_complete();

    void finalize(CephContext *cct, ssize_t rval);

    void init_time(ImageCtx *i, aio_type_t t);
    void start_op(ImageCtx *i, aio_type_t t);
    void fail(CephContext *cct, int r);

    void complete(CephContext *cct);

    void set_complete_cb(void *cb_arg, callback_t cb) {
      complete_cb = cb;
      complete_arg = cb_arg;
    }

    void set_request_count(CephContext *cct, uint32_t num);
    void add_request() {
      lock.Lock();
      assert(pending_count > 0);
      lock.Unlock();
      get();
    }
    void complete_request(CephContext *cct, ssize_t r);

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
        if (ictx && event_notify) {
          ictx->completed_reqs_lock.Lock();
          m_xlist_item.remove_myself();
          ictx->completed_reqs_lock.Unlock();
        }
        delete this;
      }
    }

    void block() {
      Mutex::Locker l(lock);
      ++blockers;
    }
    void unblock(CephContext *cct) {
      Mutex::Locker l(lock);
      assert(blockers > 0);
      --blockers;
      if (pending_count == 0 && blockers == 0) {
        finalize(cct, rval);
        complete(cct);
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
    C_AioRequest(CephContext *cct, AioCompletion *completion)
      : m_cct(cct), m_completion(completion) {
      m_completion->add_request();
    }
    virtual ~C_AioRequest() {}
    virtual void finish(int r) {
      m_completion->complete_request(m_cct, r);
    }
  protected:
    CephContext *m_cct;
    AioCompletion *m_completion;
  };

  class C_AioRead : public C_AioRequest {
  public:
    C_AioRead(CephContext *cct, AioCompletion *completion)
      : C_AioRequest(cct, completion), m_req(NULL) {
    }
    virtual ~C_AioRead() {}
    virtual void finish(int r);
    void set_req(AioObjectRead *req) {
      m_req = req;
    }
  private:
    AioObjectRead *m_req;
  };

  class C_CacheRead : public Context {
  public:
    explicit C_CacheRead(ImageCtx *ictx, AioObjectRead *req)
      : m_image_ctx(*ictx), m_req(req), m_enqueued(false) {}
    virtual void complete(int r);
  protected:
    virtual void finish(int r);
  private:
    ImageCtx &m_image_ctx;
    AioObjectRead *m_req;
    bool m_enqueued;
  };
}

#endif
