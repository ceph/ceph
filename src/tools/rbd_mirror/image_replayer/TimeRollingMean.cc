// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/image_replayer/TimeRollingMean.h"
#include "common/Clock.h"

namespace rbd {
namespace mirror {
namespace image_replayer {

void TimeRollingMean::operator()(uint32_t value) {
  auto time = ceph_clock_now();
  if (m_last_time.is_zero()) {
    m_last_time = time;
  } else if (m_last_time.sec() < time.sec()) {
    auto sec = m_last_time.sec();
    while (sec++ < time.sec()) {
      m_rolling_mean(m_sum);
      m_sum = 0;
    }

    m_last_time = time;
  }

  m_sum += value;
}

double TimeRollingMean::get_average() const {
  return boost::accumulators::rolling_mean(m_rolling_mean);
}

} // namespace image_replayer
} // namespace mirror
} // namespace rbd
