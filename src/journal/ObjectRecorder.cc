// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/ObjectRecorder.h"
#include "journal/Future.h"
#include "journal/Utils.h"
#include "include/ceph_assert.h"
#include "common/Clock.h" // for ceph_clock_now()
#include "common/Timer.h"
#include "common/errno.h"
#include "cls/journal/cls_journal_client.h"

#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << "ObjectRecorder: " << this << " " \
                           << __func__ << " (" << m_oid << "): "

using namespace cls::journal;
using std::shared_ptr;

namespace journal {

ObjectRecorder::ObjectRecorder(librados::IoCtx &ioctx, std::string_view oid,
                               uint64_t object_number, ceph::mutex* lock,
                               ContextWQ *work_queue, Handler *handler,
                               uint8_t order, int32_t max_in_flight_appends)
  : m_oid(oid), m_object_number(object_number),
    m_op_work_queue(work_queue), m_handler(handler),
    m_order(order), m_soft_max_size(1 << m_order),
    m_max_in_flight_appends(max_in_flight_appends),
    m_lock(lock)
{
  m_ioctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext*>(m_ioctx.cct());
  ceph_assert(m_handler != NULL);

  librados::Rados rados(m_ioctx);
  int8_t require_osd_release = 0;
  int r = rados.get_min_compatible_osd(&require_osd_release);
  if (r < 0) {
    ldout(m_cct, 0) << "failed to retrieve min OSD release: "
                    << cpp_strerror(r) << dendl;
  }
  m_compat_mode = require_osd_release < CEPH_RELEASE_OCTOPUS;

  ldout(m_cct, 20) << dendl;
}

ObjectRecorder::~ObjectRecorder() {
  ldout(m_cct, 20) << dendl;
  ceph_assert(m_pending_buffers.empty());
  ceph_assert(m_in_flight_tids.empty());
  ceph_assert(m_in_flight_appends.empty());
}

void ObjectRecorder::set_append_batch_options(int flush_interval,
                                              uint64_t flush_bytes,
                                              double flush_age) {
  ldout(m_cct, 5) << "flush_interval=" << flush_interval << ", "
                  << "flush_bytes=" << flush_bytes << ", "
                  << "flush_age=" << flush_age << dendl;

  ceph_assert(ceph_mutex_is_locked(*m_lock));
  m_flush_interval = flush_interval;
  m_flush_bytes = flush_bytes;
  m_flush_age = flush_age;
}

bool ObjectRecorder::append(AppendBuffers &&append_buffers) {
  ldout(m_cct, 20) << "count=" << append_buffers.size() << dendl;

  ceph_assert(ceph_mutex_is_locked(*m_lock));

  ceph::ref_t<FutureImpl> last_flushed_future;
  auto flush_handler = get_flush_handler();
  for (auto& append_buffer : append_buffers) {
    ldout(m_cct, 20) << *append_buffer.first << ", "
                     << "size=" << append_buffer.second.length() << dendl;
    bool flush_requested = append_buffer.first->attach(flush_handler);
    if (flush_requested) {
      last_flushed_future = append_buffer.first;
    }

    m_pending_buffers.push_back(append_buffer);
    m_pending_bytes += append_buffer.second.length();
  }

  return send_appends(!!last_flushed_future, last_flushed_future);
}

void ObjectRecorder::flush(Context *on_safe) {
  ldout(m_cct, 20) << dendl;

  Future future;
  {
    std::unique_lock locker{*m_lock};

    // if currently handling flush notifications, wait so that
    // we notify in the correct order (since lock is dropped on
    // callback)
    while (m_in_flight_callbacks > 0) {
      m_in_flight_callbacks_cond.wait(locker);
    }

    // attach the flush to the most recent append
    if (!m_pending_buffers.empty()) {
      future = Future(m_pending_buffers.rbegin()->first);
    } else if (!m_in_flight_appends.empty()) {
      AppendBuffers &append_buffers = m_in_flight_appends.rbegin()->second;
      ceph_assert(!append_buffers.empty());
      future = Future(append_buffers.rbegin()->first);
    }
  }

  if (future.is_valid()) {
    // cannot be invoked while the same lock context
    m_op_work_queue->queue(new LambdaContext(
      [future, on_safe] (int r) mutable {
        future.flush(on_safe);
      }));
  } else {
    on_safe->complete(0);
  }
}

void ObjectRecorder::flush(const ceph::ref_t<FutureImpl>& future) {
  ldout(m_cct, 20) << "flushing " << *future << dendl;

  std::unique_lock locker{*m_lock};
  auto flush_handler = future->get_flush_handler();
  auto my_handler = get_flush_handler();
  if (flush_handler != my_handler) {
    // if we don't own this future, re-issue the flush so that it hits the
    // correct journal object owner
    future->flush();
    return;
  } else if (future->is_flush_in_progress()) {
    return;
  }

  if (!m_object_closed && !m_overflowed && send_appends(true, future)) {
    ++m_in_flight_callbacks;
    notify_handler_unlock(locker, true);
  }
}

void ObjectRecorder::claim_append_buffers(AppendBuffers *append_buffers) {
  ldout(m_cct, 20) << dendl;

  ceph_assert(ceph_mutex_is_locked(*m_lock));
  ceph_assert(m_in_flight_tids.empty());
  ceph_assert(m_in_flight_appends.empty());
  ceph_assert(m_object_closed || m_overflowed);

  for (auto& append_buffer : m_pending_buffers) {
    ldout(m_cct, 20) << "detached " << *append_buffer.first << dendl;
    append_buffer.first->detach();
  }
  append_buffers->splice(append_buffers->end(), m_pending_buffers,
                         m_pending_buffers.begin(), m_pending_buffers.end());
}

bool ObjectRecorder::close() {
  ceph_assert(ceph_mutex_is_locked(*m_lock));

  ldout(m_cct, 20) << dendl;

  send_appends(true, {});

  ceph_assert(!m_object_closed);
  m_object_closed = true;

  if (!m_in_flight_tids.empty() || m_in_flight_callbacks > 0) {
    m_object_closed_notify = true;
    return false;
  }

  return true;
}

void ObjectRecorder::handle_append_flushed(uint64_t tid, int r) {
  ldout(m_cct, 20) << "tid=" << tid << ", r=" << r << dendl;

  std::unique_lock locker{*m_lock};
  ++m_in_flight_callbacks;

  auto tid_iter = m_in_flight_tids.find(tid);
  ceph_assert(tid_iter != m_in_flight_tids.end());
  m_in_flight_tids.erase(tid_iter);

  InFlightAppends::iterator iter = m_in_flight_appends.find(tid);
  ceph_assert(iter != m_in_flight_appends.end());

  bool notify_overflowed = false;
  AppendBuffers append_buffers;
  if (r == -EOVERFLOW) {
    ldout(m_cct, 10) << "append overflowed: "
                     << "idle=" << m_in_flight_tids.empty() << ", "
                     << "previous_overflow=" << m_overflowed << dendl;
    if (m_in_flight_tids.empty()) {
      append_overflowed();
    }

    if (!m_object_closed && !m_overflowed) {
      notify_overflowed = true;
    }
    m_overflowed = true;
  } else {
    append_buffers.swap(iter->second);
    ceph_assert(!append_buffers.empty());

    for (auto& append_buffer : append_buffers) {
      auto length = append_buffer.second.length();
      m_object_bytes += length;

      ceph_assert(m_in_flight_bytes >= length);
      m_in_flight_bytes -= length;
    }
    ldout(m_cct, 20) << "object_bytes=" << m_object_bytes << dendl;

    m_in_flight_appends.erase(iter);
  }
  locker.unlock();

  // Flag the associated futures as complete.
  for (auto& append_buffer : append_buffers) {
    ldout(m_cct, 20) << *append_buffer.first << " marked safe" << dendl;
    append_buffer.first->safe(r);
  }

  // attempt to kick off more appends to the object
  locker.lock();
  if (!m_object_closed && !m_overflowed && send_appends(false, {})) {
    notify_overflowed = true;
  }

  ldout(m_cct, 20) << "pending tids=" << m_in_flight_tids << dendl;

  // notify of overflow if one just occurred or indicate that all in-flight
  // appends have completed on a closed object (or wake up stalled flush
  // requests that was waiting for this strand to complete).
  notify_handler_unlock(locker, notify_overflowed);
}

void ObjectRecorder::append_overflowed() {
  ldout(m_cct, 10) << dendl;

  ceph_assert(ceph_mutex_is_locked(*m_lock));
  ceph_assert(!m_in_flight_appends.empty());

  InFlightAppends in_flight_appends;
  in_flight_appends.swap(m_in_flight_appends);

  AppendBuffers restart_append_buffers;
  for (InFlightAppends::iterator it = in_flight_appends.begin();
       it != in_flight_appends.end(); ++it) {
    restart_append_buffers.insert(restart_append_buffers.end(),
                                  it->second.begin(), it->second.end());
  }

  restart_append_buffers.splice(restart_append_buffers.end(),
                                m_pending_buffers,
                                m_pending_buffers.begin(),
                                m_pending_buffers.end());
  restart_append_buffers.swap(m_pending_buffers);
}

bool ObjectRecorder::send_appends(bool force, ceph::ref_t<FutureImpl> flush_future) {
  ldout(m_cct, 20) << dendl;

  ceph_assert(ceph_mutex_is_locked(*m_lock));
  if (m_object_closed || m_overflowed) {
    ldout(m_cct, 20) << "already closed or overflowed" << dendl;
    return false;
  }

  if (m_pending_buffers.empty()) {
    ldout(m_cct, 20) << "append buffers empty" << dendl;
    return false;
  }

  if (!force &&
      ((m_flush_interval > 0 && m_pending_buffers.size() >= m_flush_interval) ||
       (m_flush_bytes > 0 && m_pending_bytes >= m_flush_bytes) ||
       (m_flush_age > 0 && !m_last_flush_time.is_zero() &&
        m_last_flush_time + m_flush_age <= ceph_clock_now()))) {
    ldout(m_cct, 20) << "forcing batch flush" << dendl;
    force = true;
  }

  // start tracking flush time after the first append event
  if (m_last_flush_time.is_zero()) {
    m_last_flush_time = ceph_clock_now();
  }

  auto max_in_flight_appends = m_max_in_flight_appends;
  if (m_flush_interval > 0 || m_flush_bytes > 0 || m_flush_age > 0) {
    if (!force && max_in_flight_appends == 0) {
      ldout(m_cct, 20) << "attempting to batch AIO appends" << dendl;
      max_in_flight_appends = 1;
    }
  } else if (max_in_flight_appends < 0) {
    max_in_flight_appends = 0;
  }

  if (!force && max_in_flight_appends != 0 &&
      static_cast<int32_t>(m_in_flight_tids.size()) >= max_in_flight_appends) {
    ldout(m_cct, 10) << "max in flight appends reached" << dendl;
    return false;
  }

  librados::ObjectWriteOperation op;
  if (m_compat_mode) {
    client::guard_append(&op, m_soft_max_size);
  }

  size_t append_bytes = 0;
  AppendBuffers append_buffers;
  bufferlist append_bl;
  for (auto it = m_pending_buffers.begin(); it != m_pending_buffers.end(); ) {
    auto& future = it->first;
    auto& bl = it->second;
    auto size = m_object_bytes + m_in_flight_bytes + append_bytes + bl.length();
    if (size == m_soft_max_size) {
      ldout(m_cct, 10) << "object at capacity (" << size << ") " << *future << dendl;
      m_overflowed = true;
    } else if (size > m_soft_max_size) {
      ldout(m_cct, 10) << "object beyond capacity (" << size << ") " << *future << dendl;
      m_overflowed = true;
      break;
    }

    bool flush_break = (force && flush_future && flush_future == future);
    ldout(m_cct, 20) << "flushing " << *future << dendl;
    future->set_flush_in_progress();

    if (m_compat_mode) {
      op.append(bl);
      op.set_op_flags2(CEPH_OSD_OP_FLAG_FADVISE_DONTNEED);
    } else {
      append_bl.append(bl);
    }

    append_bytes += bl.length();
    append_buffers.push_back(*it);
    it = m_pending_buffers.erase(it);

    if (flush_break) {
      ldout(m_cct, 20) << "stopping at requested flush future" << dendl;
      break;
    }
  }

  if (append_bytes > 0) {
    m_last_flush_time = ceph_clock_now();

    uint64_t append_tid = m_append_tid++;
    m_in_flight_tids.insert(append_tid);
    m_in_flight_appends[append_tid].swap(append_buffers);
    m_in_flight_bytes += append_bytes;

    ceph_assert(m_pending_bytes >= append_bytes);
    m_pending_bytes -= append_bytes;

    if (!m_compat_mode) {
      client::append(&op, m_soft_max_size, append_bl);
    }

    auto rados_completion = librados::Rados::aio_create_completion(
      new C_AppendFlush(this, append_tid), utils::rados_ctx_callback);
    int r = m_ioctx.aio_operate(m_oid, rados_completion, &op);
    ceph_assert(r == 0);
    rados_completion->release();
    ldout(m_cct, 20) << "flushing journal tid=" << append_tid << ", "
                     << "append_bytes=" << append_bytes << ", "
                     << "in_flight_bytes=" << m_in_flight_bytes << ", "
                     << "pending_bytes=" << m_pending_bytes << dendl;
  }

  return m_overflowed;
}

void ObjectRecorder::wake_up_flushes() {
  ceph_assert(ceph_mutex_is_locked(*m_lock));
  --m_in_flight_callbacks;
  if (m_in_flight_callbacks == 0) {
    m_in_flight_callbacks_cond.notify_all();
  }
}

void ObjectRecorder::notify_handler_unlock(
    std::unique_lock<ceph::mutex>& locker, bool notify_overflowed) {
  ceph_assert(ceph_mutex_is_locked(*m_lock));
  ceph_assert(m_in_flight_callbacks > 0);

  if (!m_object_closed && notify_overflowed) {
    // TODO need to delay completion until after aio_notify completes
    ldout(m_cct, 10) << "overflow" << dendl;
    ceph_assert(m_overflowed);

    locker.unlock();
    m_handler->overflow(this);
    locker.lock();
  }

  // wake up blocked flush requests
  wake_up_flushes();

  // An overflow notification might have blocked a close. A close
  // notification could lead to the immediate destruction of this object
  // so the object shouldn't be referenced anymore
  bool object_closed_notify = false;
  if (m_in_flight_tids.empty()) {
    std::swap(object_closed_notify, m_object_closed_notify);
  }
  ceph_assert(m_object_closed || !object_closed_notify);
  locker.unlock();

  if (object_closed_notify) {
    ldout(m_cct, 10) << "closed" << dendl;
    m_handler->closed(this);
  }
}

} // namespace journal
