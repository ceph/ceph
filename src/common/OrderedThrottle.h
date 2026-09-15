// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_ORDERED_THROTTLE_H
#define CEPH_ORDERED_THROTTLE_H

#include <condition_variable>
#include <cstdint>
#include <map>
#include <mutex>

#include "include/Context.h"

class OrderedThrottle;

class C_OrderedThrottle : public Context {
public:
  C_OrderedThrottle(OrderedThrottle *ordered_throttle, uint64_t tid)
    : m_ordered_throttle(ordered_throttle), m_tid(tid) {
  }

protected:
  void finish(int r) override;

private:
  OrderedThrottle *m_ordered_throttle;
  uint64_t m_tid;
};

/**
 * @class OrderedThrottle
 * Throttles the maximum number of active requests and completes them in order
 *
 * Operations can complete out-of-order but their associated Context callback
 * will completed in-order during invocation of start_op() and wait_for_ret()
 */
class OrderedThrottle {
public:
  OrderedThrottle(uint64_t max, bool ignore_enoent);
  ~OrderedThrottle();

  C_OrderedThrottle *start_op(Context *on_finish);
  void end_op(int r);

  bool pending_error() const;
  int wait_for_ret();

protected:
  friend class C_OrderedThrottle;

  void finish_op(uint64_t tid, int r);

private:
  struct Result {
    bool finished;
    int ret_val;
    Context *on_finish;

    Result(Context *_on_finish = NULL)
      : finished(false), ret_val(0), on_finish(_on_finish) {
    }
  };

  typedef std::map<uint64_t, Result> TidResult;

  mutable std::mutex m_lock;
  std::condition_variable m_cond;
  uint64_t m_max;
  uint64_t m_current = 0;
  int m_ret_val = 0;
  bool m_ignore_enoent;

  uint64_t m_next_tid = 0;
  uint64_t m_complete_tid = 0;

  TidResult m_tid_result;

  void complete_pending_ops(std::unique_lock<std::mutex>& l);
  uint32_t waiters = 0;
};

#endif
