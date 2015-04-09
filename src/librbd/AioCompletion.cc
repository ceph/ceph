// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"

#include "librbd/AioRequest.h"
#include "librbd/internal.h"

#include "librbd/AioCompletion.h"

#ifdef WITH_LTTNG
#include "tracing/librbd.h"
#else
#define tracepoint(...)
#endif

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::AioCompletion: "

namespace librbd {

  void AioCompletion::finish_adding_requests(CephContext *cct)
  {
    ldout(cct, 20) << "AioCompletion::finish_adding_requests " << (void*)this << " pending " << pending_count << dendl;
    unblock(cct);
  }

  int AioCompletion::wait_for_complete() {
    tracepoint(librbd, aio_wait_for_complete_enter, this);
    lock.Lock();
    while (!done)
      cond.Wait(lock);
    lock.Unlock();
    tracepoint(librbd, aio_wait_for_complete_exit, 0);
    return 0;
  }

  void AioCompletion::finalize(CephContext *cct, ssize_t rval)
  {
    ldout(cct, 20) << "AioCompletion::finalize() " << (void*)this << " rval " << rval << " read_buf " << (void*)read_buf
		   << " read_bl " << (void*)read_bl << dendl;
    if (rval >= 0 && aio_type == AIO_TYPE_READ) {
      // FIXME: make the destriper write directly into a buffer so
      // that we avoid shuffling pointers and copying zeros around.
      bufferlist bl;
      destriper.assemble_result(cct, bl, true);

      if (read_buf) {
	assert(bl.length() == read_buf_len);
	bl.copy(0, read_buf_len, read_buf);
	ldout(cct, 20) << "AioCompletion::finalize() copied resulting " << bl.length()
		       << " bytes to " << (void*)read_buf << dendl;
      }
      if (read_bl) {
	ldout(cct, 20) << "AioCompletion::finalize() moving resulting " << bl.length()
		       << " bytes to bl " << (void*)read_bl << dendl;
	read_bl->claim(bl);
      }
    }
  }

  void AioCompletion::complete(CephContext *cct) {
    tracepoint(librbd, aio_complete_enter, this, rval);
    utime_t elapsed;
    assert(lock.is_locked());
    elapsed = ceph_clock_now(cct) - start_time;
    switch (aio_type) {
    case AIO_TYPE_READ:
      ictx->perfcounter->tinc(l_librbd_aio_rd_latency, elapsed); break;
    case AIO_TYPE_WRITE:
      ictx->perfcounter->tinc(l_librbd_aio_wr_latency, elapsed); break;
    case AIO_TYPE_DISCARD:
      ictx->perfcounter->tinc(l_librbd_aio_discard_latency, elapsed); break;
    case AIO_TYPE_FLUSH:
      ictx->perfcounter->tinc(l_librbd_aio_flush_latency, elapsed); break;
    default:
      lderr(cct) << "completed invalid aio_type: " << aio_type << dendl;
      break;
    }

    // note: possible for image to be closed after op marked finished
    if (async_op.started()) {
      async_op.finish_op();
    }

    if (complete_cb) {
      complete_cb(rbd_comp, complete_arg);
    }
    done = true;
    cond.Signal();
    tracepoint(librbd, aio_complete_exit);
  }

  void AioCompletion::fail(CephContext *cct, int r)
  {
    lderr(cct) << "AioCompletion::fail() " << this << ": " << cpp_strerror(r)
               << dendl;
    lock.Lock();
    assert(pending_count == 0);
    rval = r;
    complete(cct);
    put_unlock();
  }

  void AioCompletion::complete_request(CephContext *cct, ssize_t r)
  {
    ldout(cct, 20) << "AioCompletion::complete_request() "
		   << (void *)this << " complete_cb=" << (void *)complete_cb
		   << " pending " << pending_count << dendl;
    lock.Lock();
    if (rval >= 0) {
      if (r < 0 && r != -EEXIST)
	rval = r;
      else if (r > 0)
	rval += r;
    }
    assert(pending_count);
    int count = --pending_count;
    if (!count && blockers == 0) {
      finalize(cct, rval);
      complete(cct);
    }
    put_unlock();
  }

  bool AioCompletion::is_complete() {
    tracepoint(librbd, aio_is_complete_enter, this);
    bool done;
    {
      Mutex::Locker l(lock);
      done = this->done;
    }
    tracepoint(librbd, aio_is_complete_exit, done);
    return done;
  }

  ssize_t AioCompletion::get_return_value() {
    tracepoint(librbd, aio_get_return_value_enter, this);
    lock.Lock();
    ssize_t r = rval;
    lock.Unlock();
    tracepoint(librbd, aio_get_return_value_exit, r);
    return r;
  }

  void C_AioRead::finish(int r)
  {
    ldout(m_cct, 10) << "C_AioRead::finish() " << this << " r = " << r << dendl;
    if (r >= 0 || r == -ENOENT) { // this was a sparse_read operation
      ldout(m_cct, 10) << " got " << m_req->m_ext_map
		       << " for " << m_req->m_buffer_extents
		       << " bl " << m_req->data().length() << dendl;
      // reads from the parent don't populate the m_ext_map and the overlap
      // may not be the full buffer.  compensate here by filling in m_ext_map
      // with the read extent when it is empty.
      if (m_req->m_ext_map.empty())
	m_req->m_ext_map[m_req->m_object_off] = m_req->data().length();

      m_completion->lock.Lock();
      m_completion->destriper.add_partial_sparse_result(
	  m_cct, m_req->data(), m_req->m_ext_map, m_req->m_object_off,
	  m_req->m_buffer_extents);
      m_completion->lock.Unlock();
      r = m_req->m_object_len;
    }
    m_completion->complete_request(m_cct, r);
  }

  void C_CacheRead::finish(int r)
  {
    m_req->complete(r);
  }
}
