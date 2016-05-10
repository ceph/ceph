// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#pragma once


#include <unistd.h>
#include <assert.h>
#include <sys/time.h>
#include <math.h>

#include <limits>
#include <cmath>
#include <chrono>


namespace crimson {
  namespace dmclock {
    // we're using double to represent time, but we could change it by
    // changing the following declarations (and by making sure a min
    // function existed)
    using Time = double;
    static const Time TimeZero = 0.0;
    static const Time TimeMax = std::numeric_limits<Time>::max();
    static const double NaN = nan("");


    inline Time get_time() {
      struct timeval now;
      auto result = gettimeofday(&now, NULL);
      (void) result;
      assert(0 == result);
      return now.tv_sec + (now.tv_usec / 1000000.0);
    }

    std::string format_time(const Time& time, uint modulo = 1000);

    void debugger();

    template<typename T>
    class ProfileTimer {
      using clock = std::chrono::steady_clock;

      uint count = 0;
      typename T::rep sum = 0;
      typename T::rep sum_squares = 0;

      bool is_timing = false;
      clock::time_point start_time;

    public:

      ProfileTimer() {
      }

      void start() {
	assert(!is_timing);
	start_time = clock::now;
	is_timing = true;
      }

      void stop() {
	assert(is_timing);
	T duration = std::chrono::duration_cast<T>(clock::now - start_time);
	typename T::rep count = duration.count();
	sum += count;
	sum_squares += count * count;
	is_timing = false;
      }

      uint get_count() { return count; }
      double get_mean() { return sum / count; }
      double get_std_dev() {
	if (count < 2) return 0;
	typename T::rep variance =
	  (count * sum_squares - sum * sum) / (count * (count - 1));
	return sqrt(double(variance));
      }
    };
  } // namespace dmclock
} // namespace crimson
