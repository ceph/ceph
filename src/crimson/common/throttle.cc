// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "throttle.h"

namespace crimson::common {

int64_t Throttle::take(int64_t c)
{
  if (max == 0u) {
    return 0;
  }
  count += c;
  return count;
}

int64_t Throttle::put(int64_t c)
{
  if (max == 0u) {
    return 0;
  }
  if (!c) {
    return count;
  }
  on_free_slots.signal();
  count -= c;
  return count;
}

seastar::future<> Throttle::get(size_t c)
{
  if (max == 0u) {
    return seastar::make_ready_future<>();
  }
  pending++;
  return on_free_slots.wait([this, c] {
    return !_should_wait(c);
  }).then([this, c] {
    pending--;
    count += c;
    return seastar::make_ready_future<>();
  });
}

void Throttle::reset_max(size_t m) {
  if (max == m) {
    return;
  }

  if (m > max) {
    on_free_slots.signal();
  }
  max = m;
}

bool Throttle::_should_wait(size_t c) const {
  if (!max) {
    return false;
  }
  return ((c <= max && count + c > max) || // normally stay under max
          (c >= max && count > max));      // except for large c
}

} // namespace crimson::common
