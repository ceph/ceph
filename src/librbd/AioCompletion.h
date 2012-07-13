// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_AIOCOMPLETION_H
#define CEPH_LIBRBD_AIOCOMPLETION_H

#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/perf_counters.h"
#include "include/utime.h"
#include "include/rbd/librbd.hpp"

#include "librbd/ImageCtx.h"
#include "librbd/internal.h"

namespace librbd {
  typedef enum {
    AIO_TYPE_READ = 0,
    AIO_TYPE_WRITE,
    AIO_TYPE_DISCARD
  } aio_type_t;

  struct AioBlockCompletion;
  struct AioCompletion {
    Mutex lock;
    Cond cond;
    bool done;
    ssize_t rval;
    callback_t complete_cb;
    void *complete_arg;
    rbd_completion_t rbd_comp;
    int pending_count;
    int ref;
    bool released;
    ImageCtx *ictx;
    utime_t start_time;
    aio_type_t aio_type;

    AioCompletion() : lock("AioCompletion::lock", true),
		      done(false), rval(0), complete_cb(NULL),
		      complete_arg(NULL), rbd_comp(NULL), pending_count(1),
		      ref(1), released(false) { 
    }
    ~AioCompletion() {
    }

    int wait_for_complete() {
      lock.Lock();
      while (!done)
	cond.Wait(lock);
      lock.Unlock();
      return 0;
    }

    void add_block_completion(AioBlockCompletion *aio_completion) {
      lock.Lock();
      pending_count++;
      lock.Unlock();
      get();
    }

    void finish_adding_completions() {
      lock.Lock();
      assert(pending_count);
      int count = --pending_count;
      if (!count) {
	complete();
      }
      lock.Unlock();
    }

    void init_time(ImageCtx *i, aio_type_t t) {
      ictx = i;
      aio_type = t;
      start_time = ceph_clock_now(ictx->cct);
    }

    void complete() {
      utime_t elapsed;
      assert(lock.is_locked());
      elapsed = ceph_clock_now(ictx->cct) - start_time;
      if (complete_cb) {
	complete_cb(rbd_comp, complete_arg);
      }
      switch (aio_type) {
      case AIO_TYPE_READ: 
	ictx->perfcounter->finc(l_librbd_aio_rd_latency, elapsed); break;
      case AIO_TYPE_WRITE:
	ictx->perfcounter->finc(l_librbd_aio_wr_latency, elapsed); break;
      case AIO_TYPE_DISCARD:
	ictx->perfcounter->finc(l_librbd_aio_discard_latency, elapsed); break;
      default: break;
      }
      done = true;
      cond.Signal();
    }

    void set_complete_cb(void *cb_arg, callback_t cb) {
      complete_cb = cb;
      complete_arg = cb_arg;
    }

    void complete_block(AioBlockCompletion *block_completion, ssize_t r);

    ssize_t get_return_value() {
      lock.Lock();
      ssize_t r = rval;
      lock.Unlock();
      return r;
    }

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
      if (!n)
	delete this;
    }
  };

  struct AioBlockCompletion : Context {
    CephContext *cct;
    AioCompletion *completion;
    uint64_t ofs;
    size_t len;
    char *buf;
    std::map<uint64_t,uint64_t> m;
    ceph::bufferlist data_bl;
    librados::ObjectWriteOperation write_op;

    AioBlockCompletion(CephContext *cct_, AioCompletion *aio_completion,
		       uint64_t _ofs, size_t _len, char *_buf)
      : cct(cct_), completion(aio_completion),
	ofs(_ofs), len(_len), buf(_buf) {}
    virtual ~AioBlockCompletion() {}
    virtual void finish(int r);
  };
}

#endif
