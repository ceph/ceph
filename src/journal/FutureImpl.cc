// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/FutureImpl.h"
#include "journal/Utils.h"

namespace journal {

FutureImpl::FutureImpl(uint64_t tag_tid, uint64_t entry_tid,
                       uint64_t commit_tid)
  : m_tag_tid(tag_tid),
    m_entry_tid(entry_tid),
    m_commit_tid(commit_tid),
    m_consistent_ack(this)
{
}

void FutureImpl::init(const ceph::ref_t<FutureImpl> &prev_future) {
  // chain ourself to the prior future (if any) to that we known when the
  // journal is consistent
  if (prev_future) {
    m_prev_future = prev_future;
    m_prev_future->wait(&m_consistent_ack);
  } else {
    m_consistent_ack.complete(0);
  }
}

void FutureImpl::flush(Context *on_safe) {

  bool complete;
  FlushHandlers flush_handlers;
  ceph::ref_t<FutureImpl> prev_future;
  {
    std::lock_guard locker{m_lock};
    complete = (m_safe && m_consistent);
    if (!complete) {
      if (on_safe != nullptr) {
        m_contexts.push_back(on_safe);
      }

      prev_future = prepare_flush(&flush_handlers, m_lock);
    }
  }

  // instruct prior futures to flush as well
  while (prev_future) {
    prev_future = prev_future->prepare_flush(&flush_handlers);
  }

  if (complete && on_safe != NULL) {
    on_safe->complete(m_return_value);
  } else if (!flush_handlers.empty()) {
    // attached to journal object -- instruct it to flush all entries through
    // this one.  possible to become detached while lock is released, so flush
    // will be re-requested by the object if it doesn't own the future
    for (auto &pair : flush_handlers) {
      pair.first->flush(pair.second);
    }
  }
}

ceph::ref_t<FutureImpl> FutureImpl::prepare_flush(FlushHandlers *flush_handlers) {
  std::lock_guard locker{m_lock};
  return prepare_flush(flush_handlers, m_lock);
}

ceph::ref_t<FutureImpl> FutureImpl::prepare_flush(FlushHandlers *flush_handlers,
                                        ceph::mutex &lock) {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  if (m_flush_state == FLUSH_STATE_NONE) {
    m_flush_state = FLUSH_STATE_REQUESTED;

    auto h = m_flush_handler;
    if (h) {
      flush_handlers->try_emplace(std::move(h), this);
    }
  }
  return m_prev_future;
}

void FutureImpl::wait(Context *on_safe) {
  ceph_assert(on_safe != NULL);
  {
    std::lock_guard locker{m_lock};
    if (!m_safe || !m_consistent) {
      m_contexts.push_back(on_safe);
      return;
    }
  }

  on_safe->complete(m_return_value);
}

bool FutureImpl::is_complete() const {
  std::lock_guard locker{m_lock};
  return m_safe && m_consistent;
}

int FutureImpl::get_return_value() const {
  std::lock_guard locker{m_lock};
  ceph_assert(m_safe && m_consistent);
  return m_return_value;
}

bool FutureImpl::attach(FlushHandler::ref flush_handler) {
  std::lock_guard locker{m_lock};
  ceph_assert(!m_flush_handler);
  m_flush_handler = std::move(flush_handler);
  return m_flush_state != FLUSH_STATE_NONE;
}

void FutureImpl::safe(int r) {
  m_lock.lock();
  ceph_assert(!m_safe);
  m_safe = true;
  if (m_return_value == 0) {
    m_return_value = r;
  }

  m_flush_handler.reset();
  if (m_consistent) {
    finish_unlock();
  } else {
    m_lock.unlock();
  }
}

void FutureImpl::consistent(int r) {
  m_lock.lock();
  ceph_assert(!m_consistent);
  m_consistent = true;
  m_prev_future.reset();
  if (m_return_value == 0) {
    m_return_value = r;
  }

  if (m_safe) {
    finish_unlock();
  } else {
    m_lock.unlock();
  }
}

void FutureImpl::finish_unlock() {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  ceph_assert(m_safe && m_consistent);

  Contexts contexts;
  contexts.swap(m_contexts);

  m_lock.unlock();
  for (Contexts::iterator it = contexts.begin();
       it != contexts.end(); ++it) {
    (*it)->complete(m_return_value);
  }
}

std::ostream &operator<<(std::ostream &os, const FutureImpl &future) {
  os << "Future[tag_tid=" << future.m_tag_tid << ", "
     << "entry_tid=" << future.m_entry_tid << ", "
     << "commit_tid=" << future.m_commit_tid << "]";
  return os;
}

} // namespace journal
