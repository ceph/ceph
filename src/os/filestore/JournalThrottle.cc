// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "JournalThrottle.h"
#include "include/assert.h"

bool JournalThrottle::set_params(
  double _low_threshhold,
  double _high_threshhold,
  double _expected_throughput,
  double _high_multiple,
  double _max_multiple,
  uint64_t _throttle_max,
  std::ostream *errstream)
{
  return throttle.set_params(
    _low_threshhold,
    _high_threshhold,
    _expected_throughput,
    _high_multiple,
    _max_multiple,
    _throttle_max,
    errstream);
}

std::chrono::duration<double> JournalThrottle::get(uint64_t c)
{
  return throttle.get(c);
}

uint64_t JournalThrottle::take(uint64_t c)
{
  return throttle.take(c);
}

void JournalThrottle::register_throttle_seq(uint64_t seq, uint64_t c)
{
  locker l(lock);
  journaled_ops.push_back(std::make_pair(seq, c));
}

std::pair<uint64_t, uint64_t> JournalThrottle::flush(uint64_t mono_id)
{
  uint64_t to_put_bytes = 0;
  uint64_t to_put_ops = 0;
  {
    locker l(lock);
    while (!journaled_ops.empty() &&
	   journaled_ops.front().first <= mono_id) {
      to_put_bytes += journaled_ops.front().second;
      to_put_ops++;
      journaled_ops.pop_front();
    }
  }
  throttle.put(to_put_bytes);
  return make_pair(to_put_ops, to_put_bytes);
}

uint64_t JournalThrottle::get_current()
{
  return throttle.get_current();
}

uint64_t JournalThrottle::get_max()
{
  return throttle.get_max();
}
