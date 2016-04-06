// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/FutureImpl.h"
#include "journal/Utils.h"

namespace journal {

FutureImpl::FutureImpl(uint64_t tag_tid, uint64_t entry_tid,
                       uint64_t commit_tid)
  : RefCountedObject(NULL, 0), m_tag_tid(tag_tid), m_entry_tid(entry_tid),
    m_commit_tid(commit_tid),
    m_lock(utils::unique_lock_name("FutureImpl::m_lock", this)), m_safe(false),
    m_consistent(false), m_return_value(0), m_flush_state(FLUSH_STATE_NONE),
    m_consistent_ack(this) {
}

void FutureImpl::init(const FutureImplPtr &prev_future) {
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
  FlushHandlerPtr flush_handler;
  {
    Mutex::Locker locker(m_lock);
    complete = (m_safe && m_consistent);
    if (!complete) {
      if (on_safe != NULL) {
        m_contexts.push_back(on_safe);
      }

      if (m_flush_state == FLUSH_STATE_NONE) {
        m_flush_state = FLUSH_STATE_REQUESTED;
        flush_handler = m_flush_handler;

        // walk the chain backwards up to <splay width> futures
        if (m_prev_future) {
          m_prev_future->flush();
        }
      }
    }
  }

  if (complete && on_safe != NULL) {
    on_safe->complete(m_return_value);
  } else if (flush_handler) {
    // attached to journal object -- instruct it to flush all entries through
    // this one.  possible to become detached while lock is released, so flush
    // will be re-requested by the object if it doesn't own the future
    flush_handler->flush(this);
  }
}

void FutureImpl::wait(Context *on_safe) {
  assert(on_safe != NULL);
  {
    Mutex::Locker locker(m_lock);
    if (!m_safe || !m_consistent) {
      m_contexts.push_back(on_safe);
      return;
    }
  }

  on_safe->complete(m_return_value);
}

bool FutureImpl::is_complete() const {
  Mutex::Locker locker(m_lock);
  return m_safe && m_consistent;
}

int FutureImpl::get_return_value() const {
  Mutex::Locker locker(m_lock);
  assert(m_safe && m_consistent);
  return m_return_value;
}

bool FutureImpl::attach(const FlushHandlerPtr &flush_handler) {
  Mutex::Locker locker(m_lock);
  assert(!m_flush_handler);
  m_flush_handler = flush_handler;
  return m_flush_state != FLUSH_STATE_NONE;
}

void FutureImpl::safe(int r) {
  m_lock.Lock();
  assert(!m_safe);
  m_safe = true;
  if (m_return_value == 0) {
    m_return_value = r;
  }

  m_flush_handler.reset();
  if (m_consistent) {
    finish_unlock();
  } else {
    m_lock.Unlock();
  }
}

void FutureImpl::consistent(int r) {
  m_lock.Lock();
  assert(!m_consistent);
  m_consistent = true;
  m_prev_future.reset();
  if (m_return_value == 0) {
    m_return_value = r;
  }

  if (m_safe) {
    finish_unlock();
  } else {
    m_lock.Unlock();
  }
}

void FutureImpl::finish_unlock() {
  assert(m_lock.is_locked());
  assert(m_safe && m_consistent);

  Contexts contexts;
  contexts.swap(m_contexts);

  m_lock.Unlock();
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

void intrusive_ptr_add_ref(FutureImpl::FlushHandler *p) {
  p->get();
}

void intrusive_ptr_release(FutureImpl::FlushHandler *p) {
  p->put();
}

} // namespace journal
