// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#pragma once


#include <cmath>
#include <chrono>


namespace crimson {
  template<typename T>
  class ProfileBase {

  protected:

    using clock = std::chrono::steady_clock;

    uint count = 0;
    typename T::rep sum = 0;
    typename T::rep sum_squares = 0;
    typename T::rep low = 0;
    typename T::rep high = 0;

  public:

    uint get_count() const { return count; }
    typename T::rep get_sum() const { return sum; }
    typename T::rep get_low() const { return low; }
    typename T::rep get_high() const { return high; }
    double get_mean() const {
      if (0 == count) return nan("");
      return sum / double(count); }
    double get_std_dev() const {
      if (0 == count) return nan("");
      double variance =
	(count * sum_squares - sum * sum) / double(count * count);
      return sqrt(variance);
    }
  }; // class ProfileBase


  // forward declaration for friend
  template<typename T>
  class ProfileCombiner;


  template<typename T>
  class ProfileTimer : public ProfileBase<T> {
    friend ProfileCombiner<T>;

    using super = ProfileBase<T>;

    bool is_timing = false;
    typename super::clock::time_point start_time;

  public:

    ProfileTimer() {
    }

    void start() {
      assert(!is_timing);
      start_time = super::clock::now();
      is_timing = true;
    }

    void stop() {
      assert(is_timing);
      T duration = std::chrono::duration_cast<T>(super::clock::now() - start_time);
      typename T::rep duration_count = duration.count();
      this->sum += duration_count;
      this->sum_squares += duration_count * duration_count;
      if (0 == this->count) {
	this->low = duration_count;
	this->high = duration_count;
      } else {
	if (duration_count < this->low) this->low = duration_count;
	else if (duration_count > this->high) this->high = duration_count;
      }
      ++this->count;
      is_timing = false;
    }
  };  // class ProfileTimer


  template<typename T>
  class ProfileCombiner : public ProfileBase<T> {

    using super = ProfileBase<T>;

  public:

    ProfileCombiner() {}

    void combine(const ProfileTimer<T>& timer) {
      if (0 == this->count) {
	this->low = timer.low;
	this->high = timer.high;
      } else {
	if (timer.low < this->low) this->low = timer.low;
	else if (timer.high > this->high) this->high = timer.high;
      }
      this->count += timer.count;
      this->sum += timer.sum;
      this->sum_squares += timer.sum_squares;
    }
  }; // class ProfileCombiner
} // namespace crimson
