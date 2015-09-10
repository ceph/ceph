// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/WorkQueue.h"

#include "librbd/AioObjectRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"

#include "librbd/AioCompletion.h"
#include "librbd/Journal.h"
#include <boost/variant/apply_visitor.hpp>
#include <boost/variant/static_visitor.hpp>

#ifdef WITH_LTTNG
#include "tracing/librbd.h"
#else
#define tracepoint(...)
#endif

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::AioCompletion: "

namespace librbd {

namespace {

struct FinalizeReadResult : public boost::static_visitor<void> {
  CephContext *cct;
  bufferlist &bl;

  FinalizeReadResult(CephContext *_cct, bufferlist &_bl) : cct(_cct), bl(_bl) {
  }

  void operator()(read_result::Linear &linear) const {
    assert(bl.length() <= linear.buf_len);
    bl.copy(0, bl.length(), linear.buf);
    ldout(cct, 20) << "AioCompletion::finalize() copied resulting "
                   << bl.length() << " bytes to " << (void*)linear.buf << dendl;
  }

  void operator()(read_result::Vector &vector) const {
    ldout(cct, 20) << "AioCompletion::finalize() moving resulting "
                   << bl.length() << " bytes to iovec " << (void*)vector.iov
                   << dendl;

    bufferlist::iterator it = bl.begin();
    size_t length = bl.length();
    size_t offset = 0;
    int idx = 0;
    for (; offset < length && idx < vector.iov_count; idx++) {
      size_t len = MIN(vector.iov[idx].iov_len, length - offset);
      it.copy(len, static_cast<char *>(vector.iov[idx].iov_base));
      offset += len;
    }
    assert(offset == bl.length());
  }

  void operator()(read_result::Bufferlist &bufferlist) const {
    ldout(cct, 20) << "AioCompletion::finalize() moving resulting "
                   << bl.length() << " bytes to bl " << (void*)bufferlist.bl
                   << dendl;
    bufferlist.bl->claim(bl);
  }
};

} // anonymous namespace

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
    ldout(cct, 20) << "AioCompletion::finalize() " << (void*)this << " "
                   << "rval=" << rval << dendl;
    if (rval >= 0 && aio_type == AIO_TYPE_READ) {
      // FIXME: make the destriper write directly into a buffer so
      // that we avoid shuffling pointers and copying zeros around.
      bufferlist bl;
      destriper.assemble_result(cct, bl, true);
      boost::apply_visitor(FinalizeReadResult(cct, bl), read_result);
    }
  }

  void AioCompletion::complete(CephContext *cct) {
    tracepoint(librbd, aio_complete_enter, this, rval);
    utime_t elapsed;
    assert(lock.is_locked());
    elapsed = ceph_clock_now(cct) - start_time;
    switch (aio_type) {
    case AIO_TYPE_OPEN:
    case AIO_TYPE_CLOSE:
      break;
    case AIO_TYPE_READ:
      ictx->perfcounter->tinc(l_librbd_rd_latency, elapsed); break;
    case AIO_TYPE_WRITE:
      ictx->perfcounter->tinc(l_librbd_wr_latency, elapsed); break;
    case AIO_TYPE_DISCARD:
      ictx->perfcounter->tinc(l_librbd_discard_latency, elapsed); break;
    case AIO_TYPE_FLUSH:
      ictx->perfcounter->tinc(l_librbd_aio_flush_latency, elapsed); break;
    default:
      lderr(cct) << "completed invalid aio_type: " << aio_type << dendl;
      break;
    }

    // inform the journal that the op has successfully committed
    if (journal_tid != 0) {
      assert(ictx->journal != NULL);
      ictx->journal->commit_io_event(journal_tid, rval);
    }

    // note: possible for image to be closed after op marked finished
    done = true;
    if (async_op.started()) {
      async_op.finish_op();
    }

    if (complete_cb) {
      lock.Unlock();
      complete_cb(rbd_comp, complete_arg);
      lock.Lock();
    }

    if (ictx && event_notify && ictx->event_socket.is_valid()) {
      ictx->completed_reqs_lock.Lock();
      ictx->completed_reqs.push_back(&m_xlist_item);
      ictx->completed_reqs_lock.Unlock();
      ictx->event_socket.notify();
    }
    cond.Signal();
    tracepoint(librbd, aio_complete_exit);
  }

  void AioCompletion::init_time(ImageCtx *i, aio_type_t t) {
    if (ictx == NULL) {
      ictx = i;
      aio_type = t;
      start_time = ceph_clock_now(ictx->cct);
    }
  }

  void AioCompletion::start_op(ImageCtx *i, aio_type_t t) {
    init_time(i, t);

    Mutex::Locker locker(lock);
    if (!done && !async_op.started()) {
      async_op.start_op(*ictx);
    }
  }

  void AioCompletion::fail(CephContext *cct, int r)
  {
    lderr(cct) << this << " " << __func__ << ": " << cpp_strerror(r)
               << dendl;
    lock.Lock();
    assert(pending_count == 0);
    rval = r;
    complete(cct);
    put_unlock();
  }

  void AioCompletion::set_request_count(CephContext *cct, uint32_t count) {
    ldout(cct, 20) << this << " " << __func__ << ": pending=" << count << dendl;
    lock.Lock();
    assert(pending_count == 0);
    pending_count = count;
    lock.Unlock();

    // if no pending requests, completion will fire now
    unblock(cct);
  }

  void AioCompletion::complete_request(CephContext *cct, ssize_t r)
  {
    lock.Lock();
    if (rval >= 0) {
      if (r < 0 && r != -EEXIST)
	rval = r;
      else if (r > 0)
	rval += r;
    }
    assert(pending_count);
    int count = --pending_count;

    ldout(cct, 20) << this << " " << __func__ << ": cb=" << complete_cb << ", "
                   << "pending=" << pending_count << dendl;
    if (!count && blockers == 0) {
      finalize(cct, rval);
      complete(cct);
    }
    put_unlock();
  }

  void AioCompletion::associate_journal_event(uint64_t tid) {
    Mutex::Locker l(lock);
    assert(!done);
    journal_tid = tid;
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
    C_AioRequest::finish(r);
  }

  void C_CacheRead::complete(int r) {
    if (!m_enqueued) {
      // cache_lock creates a lock ordering issue -- so re-execute this context
      // outside the cache_lock
      m_enqueued = true;
      m_image_ctx.op_work_queue->queue(this, r);
      return;
    }
    Context::complete(r);
  }

  void C_CacheRead::finish(int r)
  {
    m_req->complete(r);
  }
}
