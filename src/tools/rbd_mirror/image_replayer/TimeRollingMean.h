// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_TIME_ROLLING_MEAN_H
#define RBD_MIRROR_IMAGE_REPLAYER_TIME_ROLLING_MEAN_H

#include "include/utime.h"
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/rolling_mean.hpp>

namespace rbd {
namespace mirror {
namespace image_replayer {

class TimeRollingMean {
public:

  void operator()(uint32_t value);

  double get_average() const;

private:
  typedef boost::accumulators::accumulator_set<
    uint64_t, boost::accumulators::stats<
      boost::accumulators::tag::rolling_mean>> RollingMean;

  utime_t m_last_time;
  uint64_t m_sum = 0;

  RollingMean m_rolling_mean{
    boost::accumulators::tag::rolling_window::window_size = 30};

};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

#endif // RBD_MIRROR_IMAGE_REPLAYER_TIME_ROLLING_MEAN_H
