// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/ObjectRecorder.h"
#include "journal/Future.h"
#include "journal/Utils.h"
#include "include/ceph_assert.h"
#include "common/Timer.h"
#include "cls/journal/cls_journal_client.h"

#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << "ObjectRecorder: " << this << " " \
                           << __func__ << " (" << m_oid << "): "

using namespace cls::journal;
using std::shared_ptr;

namespace journal {

ObjectRecorder::ObjectRecorder(librados::IoCtx &ioctx, const std::string &oid,
                               uint64_t object_number, shared_ptr<Mutex> lock,
                               ContextWQ *work_queue, SafeTimer &timer,
                               Mutex &timer_lock, Handler *handler,
                               uint8_t order, uint32_t flush_interval,
                               uint64_t flush_bytes, double flush_age,
                               uint64_t max_in_flight_appends)
  : RefCountedObject(NULL, 0), m_oid(oid), m_object_number(object_number),
    m_cct(NULL), m_op_work_queue(work_queue), m_timer(timer),
    m_timer_lock(timer_lock), m_handler(handler), m_order(order),
    m_soft_max_size(1 << m_order), m_flush_interval(flush_interval),
    m_flush_bytes(flush_bytes), m_flush_age(flush_age),
    m_max_in_flight_appends(max_in_flight_appends), m_flush_handler(this),
    m_lock(lock), m_append_tid(0), m_pending_bytes(0),
    m_size(0), m_overflowed(false), m_object_closed(false),
    m_in_flight_flushes(false), m_aio_scheduled(false) {
  m_ioctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext*>(m_ioctx.cct());
  ceph_assert(m_handler != NULL);
  ldout(m_cct, 20) << dendl;
}

ObjectRecorder::~ObjectRecorder() {
  ldout(m_cct, 20) << dendl;
  ceph_assert(m_append_task == NULL);
  ceph_assert(m_append_buffers.empty());
  ceph_assert(m_in_flight_tids.empty());
  ceph_assert(m_in_flight_appends.empty());
  ceph_assert(!m_aio_scheduled);
}

bool ObjectRecorder::append_unlock(AppendBuffers &&append_buffers) {
  ceph_assert(m_lock->is_locked());
  ldout(m_cct, 20) << "count=" << append_buffers.size() << dendl;

  FutureImplPtr last_flushed_future;
  bool schedule_append = false;

  if (m_overflowed) {
    m_append_buffers.insert(m_append_buffers.end(),
                            append_buffers.begin(), append_buffers.end());
    m_lock->Unlock();
    ldout(m_cct, 20) << "already overflowed" << dendl;
    return false;
  }

  for (AppendBuffers::const_iterator iter = append_buffers.begin();
       iter != append_buffers.end(); ++iter) {
    if (append(*iter, &schedule_append)) {
      last_flushed_future = iter->first;
    }
  }

  if (last_flushed_future) {
    flush(last_flushed_future);
    m_lock->Unlock();
  } else {
    m_lock->Unlock();
    if (schedule_append) {
      schedule_append_task();
    } else {
      cancel_append_task();
    }
  }
  return (!m_object_closed && !m_overflowed &&
          m_size + m_pending_bytes >= m_soft_max_size);
}

void ObjectRecorder::flush(Context *on_safe) {
  ldout(m_cct, 20) << dendl;

  cancel_append_task();
  Future future;
  {
    Mutex::Locker locker(*m_lock);

    // if currently handling flush notifications, wait so that
    // we notify in the correct order (since lock is dropped on
    // callback)
    if (m_in_flight_flushes) {
      m_in_flight_flushes_cond.Wait(*(m_lock.get()));
    }

    // attach the flush to the most recent append
    if (!m_append_buffers.empty()) {
      future = Future(m_append_buffers.rbegin()->first);

      flush_appends(true);
    } else if (!m_pending_buffers.empty()) {
      future = Future(m_pending_buffers.rbegin()->first);
    } else if (!m_in_flight_appends.empty()) {
      AppendBuffers &append_buffers = m_in_flight_appends.rbegin()->second;
      ceph_assert(!append_buffers.empty());
      future = Future(append_buffers.rbegin()->first);
    }
  }

  if (future.is_valid()) {
    future.flush(on_safe);
  } else {
    on_safe->complete(0);
  }
}

void ObjectRecorder::flush(const FutureImplPtr &future) {
  ldout(m_cct, 20) << "flushing " << *future << dendl;

  ceph_assert(m_lock->is_locked());

  if (future->get_flush_handler().get() != &m_flush_handler) {
    // if we don't own this future, re-issue the flush so that it hits the
    // correct journal object owner
    future->flush();
    return;
  } else if (future->is_flush_in_progress()) {
    return;
  }

  if (m_object_closed || m_overflowed) {
    return;
  }

  AppendBuffers::reverse_iterator r_it;
  for (r_it = m_append_buffers.rbegin(); r_it != m_append_buffers.rend();
       ++r_it) {
    if (r_it->first == future) {
      break;
    }
  }
  ceph_assert(r_it != m_append_buffers.rend());

  auto it = (++r_it).base();
  ceph_assert(it != m_append_buffers.end());
  ++it;

  AppendBuffers flush_buffers;
  flush_buffers.splice(flush_buffers.end(), m_append_buffers,
                       m_append_buffers.begin(), it);
  send_appends(&flush_buffers);
}

void ObjectRecorder::claim_append_buffers(AppendBuffers *append_buffers) {
  ldout(m_cct, 20) << dendl;

  ceph_assert(m_lock->is_locked());
  ceph_assert(m_in_flight_tids.empty());
  ceph_assert(m_in_flight_appends.empty());
  ceph_assert(m_object_closed || m_overflowed);
  append_buffers->splice(append_buffers->end(), m_append_buffers,
                         m_append_buffers.begin(), m_append_buffers.end());
}

bool ObjectRecorder::close() {
  ceph_assert(m_lock->is_locked());

  ldout(m_cct, 20) << dendl;

  cancel_append_task();

  flush_appends(true);

  ceph_assert(!m_object_closed);
  m_object_closed = true;
  return (m_in_flight_tids.empty() && !m_in_flight_flushes && !m_aio_scheduled);
}

void ObjectRecorder::handle_append_task() {
  ceph_assert(m_timer_lock.is_locked());
  m_append_task = NULL;

  Mutex::Locker locker(*m_lock);
  flush_appends(true);
}

void ObjectRecorder::cancel_append_task() {
  Mutex::Locker locker(m_timer_lock);
  if (m_append_task != NULL) {
    ldout(m_cct, 20) << dendl;
    m_timer.cancel_event(m_append_task);
    m_append_task = NULL;
  }
}

void ObjectRecorder::schedule_append_task() {
  Mutex::Locker locker(m_timer_lock);
  if (m_append_task == nullptr && m_flush_age > 0) {
    ldout(m_cct, 20) << dendl;
    m_append_task = m_timer.add_event_after(
      m_flush_age, new FunctionContext([this](int) {
	  handle_append_task();
	}));
  }
}

bool ObjectRecorder::append(const AppendBuffer &append_buffer,
                            bool *schedule_append) {
  ceph_assert(m_lock->is_locked());
  ldout(m_cct, 20) << "bytes=" << append_buffer.second.length() << dendl;

  bool flush_requested = false;
  if (!m_object_closed && !m_overflowed) {
    flush_requested = append_buffer.first->attach(&m_flush_handler);
  }

  m_append_buffers.push_back(append_buffer);
  m_pending_bytes += append_buffer.second.length();

  if (!flush_appends(false)) {
    *schedule_append = true;
  }
  return flush_requested;
}

bool ObjectRecorder::flush_appends(bool force) {
  ceph_assert(m_lock->is_locked());
  ldout(m_cct, 20) << "force=" << force << dendl;
  if (m_object_closed || m_overflowed) {
    ldout(m_cct, 20) << "already closed or overflowed" << dendl;
    return true;
  }

  if (m_append_buffers.empty() ||
      (!force &&
       m_size + m_pending_bytes < m_soft_max_size &&
       (m_flush_interval > 0 && m_append_buffers.size() < m_flush_interval) &&
       (m_flush_bytes > 0 && m_pending_bytes < m_flush_bytes))) {
    ldout(m_cct, 20) << "batching append" << dendl;
    return false;
  }

  m_pending_bytes = 0;
  AppendBuffers append_buffers;
  append_buffers.swap(m_append_buffers);
  send_appends(&append_buffers);
  return true;
}

void ObjectRecorder::handle_append_flushed(uint64_t tid, int r) {
  ldout(m_cct, 20) << "tid=" << tid << ", r=" << r << dendl;

  AppendBuffers append_buffers;
  {
    m_lock->Lock();
    auto tid_iter = m_in_flight_tids.find(tid);
    ceph_assert(tid_iter != m_in_flight_tids.end());
    m_in_flight_tids.erase(tid_iter);

    InFlightAppends::iterator iter = m_in_flight_appends.find(tid);
    if (r == -EOVERFLOW || m_overflowed) {
      if (iter != m_in_flight_appends.end()) {
        ldout(m_cct, 10) << "append overflowed" << dendl;
        m_overflowed = true;
      } else {
        // must have seen an overflow on a previous append op
        ceph_assert(r == -EOVERFLOW && m_overflowed);
      }

      // notify of overflow once all in-flight ops are complete
      if (m_in_flight_tids.empty() && !m_aio_scheduled) {
        m_append_buffers.splice(m_append_buffers.begin(), m_pending_buffers);
        append_overflowed();
        notify_handler_unlock();
      } else {
        m_lock->Unlock();
      }
      return;
    }

    ceph_assert(iter != m_in_flight_appends.end());
    append_buffers.swap(iter->second);
    ceph_assert(!append_buffers.empty());

    m_in_flight_appends.erase(iter);
    m_in_flight_flushes = true;
    m_lock->Unlock();
  }

  // Flag the associated futures as complete.
  for (AppendBuffers::iterator buf_it = append_buffers.begin();
       buf_it != append_buffers.end(); ++buf_it) {
    ldout(m_cct, 20) << *buf_it->first << " marked safe" << dendl;
    buf_it->first->safe(r);
  }

  // wake up any flush requests that raced with a RADOS callback
  m_lock->Lock();
  m_in_flight_flushes = false;
  m_in_flight_flushes_cond.Signal();

  if (!m_aio_scheduled) {
    if (m_in_flight_appends.empty() &&
        (m_object_closed || m_aio_sent_size >= m_soft_max_size)) {
      if (m_aio_sent_size >= m_soft_max_size) {
        ldout(m_cct, 20) << " soft max size reached, notifying overflow"
                         << dendl;
        m_overflowed = true;
      }
      // all remaining unsent appends should be redirected to new object
      m_append_buffers.splice(m_append_buffers.begin(), m_pending_buffers);
      notify_handler_unlock();
    } else if (!m_pending_buffers.empty()) {
      m_aio_scheduled = true;
      m_lock->Unlock();
      send_appends_aio();
    } else {
      m_lock->Unlock();
    }
  } else {
    m_lock->Unlock();
  }
}

void ObjectRecorder::append_overflowed() {
  ldout(m_cct, 10) << dendl;

  ceph_assert(m_lock->is_locked());
  ceph_assert(!m_in_flight_appends.empty());

  cancel_append_task();

  InFlightAppends in_flight_appends;
  in_flight_appends.swap(m_in_flight_appends);

  AppendBuffers restart_append_buffers;
  for (InFlightAppends::iterator it = in_flight_appends.begin();
       it != in_flight_appends.end(); ++it) {
    restart_append_buffers.insert(restart_append_buffers.end(),
                                  it->second.begin(), it->second.end());
  }

  restart_append_buffers.splice(restart_append_buffers.end(),
                                m_append_buffers,
                                m_append_buffers.begin(),
                                m_append_buffers.end());
  restart_append_buffers.swap(m_append_buffers);

  for (AppendBuffers::const_iterator it = m_append_buffers.begin();
       it != m_append_buffers.end(); ++it) {
    ldout(m_cct, 20) << "overflowed " << *it->first << dendl;
    it->first->detach();
  }
}

void ObjectRecorder::send_appends(AppendBuffers *append_buffers) {
  ldout(m_cct, 20) << dendl;
  ceph_assert(m_lock->is_locked());
  ceph_assert(!append_buffers->empty());

  for (AppendBuffers::iterator it = append_buffers->begin();
       it != append_buffers->end(); ++it) {
    ldout(m_cct, 20) << "flushing " << *it->first << dendl;
    it->first->set_flush_in_progress();
    m_size += it->second.length();
  }

  m_pending_buffers.splice(m_pending_buffers.end(), *append_buffers,
                           append_buffers->begin(), append_buffers->end());
  if (!m_aio_scheduled) {
    m_op_work_queue->queue(new FunctionContext([this] (int r) {
        send_appends_aio();
    }));
    m_aio_scheduled = true;
  }
}

void ObjectRecorder::send_appends_aio() {
  ldout(m_cct, 20) << dendl;
  librados::AioCompletion *rados_completion;
  {
    Mutex::Locker locker(*m_lock);
    m_aio_scheduled = false;

    if (m_pending_buffers.empty()) {
      ldout(m_cct, 10) << "pending buffers empty" << dendl;
      return;
    }

    if (m_max_in_flight_appends != 0 &&
        m_in_flight_tids.size() >= m_max_in_flight_appends) {
      ldout(m_cct, 10) << "max in flight appends reached" << dendl;
      return;
    }

    if (m_aio_sent_size >= m_soft_max_size) {
      ldout(m_cct, 10) << "soft max size reached" << dendl;
      return;
    }

    uint64_t append_tid = m_append_tid++;
    m_in_flight_tids.insert(append_tid);

    ldout(m_cct, 10) << "flushing journal tid=" << append_tid << dendl;

    librados::ObjectWriteOperation op;
    client::guard_append(&op, m_soft_max_size);
    auto append_buffers = &m_in_flight_appends[append_tid];

    size_t append_bytes = 0;
    for (auto it = m_pending_buffers.begin(); it != m_pending_buffers.end(); ) {
      ldout(m_cct, 20) << "flushing " << *it->first << dendl;
      op.append(it->second);
      op.set_op_flags2(CEPH_OSD_OP_FLAG_FADVISE_DONTNEED);
      m_aio_sent_size += it->second.length();
      append_bytes += it->second.length();
      append_buffers->push_back(*it);
      it = m_pending_buffers.erase(it);
      if (m_aio_sent_size >= m_soft_max_size) {
        break;
      }
    }
    rados_completion = librados::Rados::aio_create_completion(
        new C_AppendFlush(this, append_tid), nullptr,
        utils::rados_ctx_callback);
    int r = m_ioctx.aio_operate(m_oid, rados_completion, &op);
    ceph_assert(r == 0);
    ldout(m_cct, 20) << "append_bytes=" << append_bytes << dendl;
  }
  rados_completion->release();
}

void ObjectRecorder::notify_handler_unlock() {
  ceph_assert(m_lock->is_locked());
  if (m_object_closed) {
    m_lock->Unlock();
    m_handler->closed(this);
  } else {
    // TODO need to delay completion until after aio_notify completes
    m_lock->Unlock();
    m_handler->overflow(this);
  }
}

} // namespace journal
