// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "common/OrderedThrottle.h"
#include "include/ceph_assert.h"

void C_OrderedThrottle::finish(int r) {
  m_ordered_throttle->finish_op(m_tid, r);
}

OrderedThrottle::OrderedThrottle(uint64_t max, bool ignore_enoent)
  : m_max(max), m_ignore_enoent(ignore_enoent) {}

OrderedThrottle::~OrderedThrottle() {
  std::lock_guard l(m_lock);
  ceph_assert(waiters == 0);
}

C_OrderedThrottle *OrderedThrottle::start_op(Context *on_finish) {
  ceph_assert(on_finish);

  std::unique_lock l(m_lock);
  uint64_t tid = m_next_tid++;
  m_tid_result[tid] = Result(on_finish);
  auto ctx = std::make_unique<C_OrderedThrottle>(this, tid);

  complete_pending_ops(l);
  while (m_max == m_current) {
    ++waiters;
    m_cond.wait(l);
    --waiters;
    complete_pending_ops(l);
  }
  ++m_current;

  return ctx.release();
}

void OrderedThrottle::end_op(int r) {
  std::lock_guard l(m_lock);
  ceph_assert(m_current > 0);

  if (r < 0 && m_ret_val == 0 && (r != -ENOENT || !m_ignore_enoent)) {
    m_ret_val = r;
  }
  --m_current;
  m_cond.notify_all();
}

void OrderedThrottle::finish_op(uint64_t tid, int r) {
  std::lock_guard l(m_lock);

  auto it = m_tid_result.find(tid);
  ceph_assert(it != m_tid_result.end());

  it->second.finished = true;
  it->second.ret_val = r;
  m_cond.notify_all();
}

bool OrderedThrottle::pending_error() const {
  std::lock_guard l(m_lock);
  return (m_ret_val < 0);
}

int OrderedThrottle::wait_for_ret() {
  std::unique_lock l(m_lock);
  complete_pending_ops(l);

  while (m_current > 0) {
    ++waiters;
    m_cond.wait(l);
    --waiters;
    complete_pending_ops(l);
  }
  return m_ret_val;
}

void OrderedThrottle::complete_pending_ops(std::unique_lock<std::mutex>& l) {
  while (true) {
    auto it = m_tid_result.begin();
    if (it == m_tid_result.end() || it->first != m_complete_tid ||
        !it->second.finished) {
      break;
    }

    Result result = it->second;
    m_tid_result.erase(it);

    l.unlock();
    result.on_finish->complete(result.ret_val);
    l.lock();

    ++m_complete_tid;
  }
}
