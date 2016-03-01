// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_THROTTLE_H
#define CEPH_JOURNAL_THROTTLE_H

#include "common/Throttle.h"

#include <list>
#include <deque>
#include <condition_variable>
#include <thread>
#include <vector>
#include <chrono>
#include <iostream>

/**
 * JournalThrottle
 *
 * Throttle designed to implement dynamic throttling as the journal fills
 * up.  The goal is to not delay ops at all when the journal is relatively
 * empty, delay ops somewhat as the journal begins to fill (with the delay
 * getting linearly longer as the journal fills up to a high water mark),
 * and to delay much more aggressively (though still linearly with usage)
 * until we hit the max value.
 *
 * The implementation simply wraps BackoffThrottle with a queue of
 * journaled but not synced ops.
 *
 * The usage pattern is as follows:
 * 1) Call get(seq, bytes) before taking the op_queue_throttle
 * 2) Once the journal is flushed, flush(max_op_id_flushed)
 */
class JournalThrottle {
  BackoffThrottle throttle;

  std::mutex lock;
  /// deque<id, count>
  std::deque<std::pair<uint64_t, uint64_t> > journaled_ops;
  using locker = std::unique_lock<std::mutex>;

public:
  /**
   * set_params
   *
   * Sets params.  If the params are invalid, returns false
   * and populates errstream (if non-null) with a user compreshensible
   * explanation.
   */
  bool set_params(
    double low_threshhold,
    double high_threshhold,
    double expected_throughput,
    double high_multiple,
    double max_multiple,
    uint64_t throttle_max,
    std::ostream *errstream);

  /**
   * gets specified throttle for id mono_id, waiting as necessary
   *
   * @param c [in] amount to take
   * @return duration waited
   */
  std::chrono::duration<double> get(uint64_t c);

  /**
   * take
   *
   * Takes specified throttle without waiting
   */
  uint64_t take(uint64_t c);

  /**
   * register_throttle_seq
   *
   * Registers a sequence number with an amount of throttle to
   * release upon flush()
   *
   * @param seq [in] seq
   */
  void register_throttle_seq(uint64_t seq, uint64_t c);


  /**
   * Releases throttle held by ids <= mono_id
   *
   * @param mono_id [in] id up to which to flush
   * @returns pair<ops_flushed, bytes_flushed>
   */
  std::pair<uint64_t, uint64_t> flush(uint64_t mono_id);

  uint64_t get_current();
  uint64_t get_max();

  JournalThrottle(
    unsigned expected_concurrency ///< [in] determines size of conds
    ) : throttle(expected_concurrency) {}
};

#endif
