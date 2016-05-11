// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#pragma once


#include <cmath>
#include <chrono>


namespace crimson {
  namespace dmclock {

    template<typename T>
    class ProfileTimer {
      using clock = std::chrono::steady_clock;

      uint count = 0;
      typename T::rep sum = 0;
      typename T::rep sum_squares = 0;
      typename T::rep low = 0;
      typename T::rep high = 0;

      bool is_timing = false;
      clock::time_point start_time;

    public:

      ProfileTimer() {
      }

      void start() {
	assert(!is_timing);
	start_time = clock::now();
	is_timing = true;
      }

      void stop() {
	assert(is_timing);
	T duration = std::chrono::duration_cast<T>(clock::now() - start_time);
	typename T::rep duration_count = duration.count();
	sum += duration_count;
	sum_squares += duration_count * duration_count;
	if (0 == count) {
	  low = duration_count;
	  high = duration_count;
	} else {
	  if (duration_count < low) low = duration_count;
	  else if (duration_count > high) high = duration_count;
	}
	++count;
	is_timing = false;
      }

      uint get_count() const { return count; }
      typename T::rep get_sum() const { return sum; }
      typename T::rep get_low() const { return low; }
      typename T::rep get_high() const { return high; }
      double get_mean() const { return sum / count; }
      double get_std_dev() const {
	if (count < 2) return 0;
	typename T::rep variance =
	  (count * sum_squares - sum * sum) / (count * (count - 1));
	return sqrt(double(variance));
      }
    }; // class ProfileTimer

  } // namespace dmclock
} // namespace crimson
