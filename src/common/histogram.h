// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 * Copyright 2013 Inktank
 */

#ifndef CEPH_HISTOGRAM_H
#define CEPH_HISTOGRAM_H

#include "include/encoding.h"
#include "include/intarith.h"
#include <fstream>
#include <limits>
#include <list>

namespace ceph {
class Formatter;
}
template <typename T = uint64_t> // default type is unit_64

class adaptive_linear_hist_t {
private:
  std::vector<uint64_t> bucket_count;
  T bucket_size;
  T min_value_observed = std::numeric_limits<T>::max();
  T max_value_observed = std::numeric_limits<T>::min();
  T min_value_allowed;
  T max_value_allowed;
  size_t bucket_min;
  size_t bucket_max;
  uint64_t total_count = 0;

public:
  // constructor takes in interval size which is the size of buckets
  adaptive_linear_hist_t(T interval_size, T min_val, T max_val)
    : bucket_size(interval_size),
      min_value_allowed(min_val),
      max_value_allowed(max_val) 
  {
    // Determine which bucket the min value goes in
    // Determine which bucket the max value goes in
    // Set the size of bucket_count to be (bucket_max - bucket_min)
    bucket_min = static_cast<size_t>(min_value_allowed/bucket_size);
    bucket_max = static_cast<size_t>(max_value_allowed/bucket_size);
    
    size_t num_buckets = bucket_max-bucket_min+1;
    bucket_count.assign(num_buckets+2,0);   
  }

  // figure out which bucket a value goes in 
  // in a vector bucket==index, so increment the count of that index 
  void record_value(T value) {
    if (value<min_value_allowed){
      bucket_count[0]+=1;
      total_count+=1;
      return;
    }

    else if (value > max_value_allowed){
      bucket_count.back()+=1;
      total_count+=1;
      return;
    }

    size_t bucket = static_cast<size_t>(value / bucket_size);

    //index is offset by 1 becuase we made the 0th index to collect values less than min_val
    bucket_count[bucket-bucket_min+1]++;

    if (value < min_value_observed) {
      min_value_observed = value;
    }
    if (value > max_value_observed) {
      max_value_observed = value;
    }
    total_count += 1;
  }
  // find the value of the latency at the ith percentile
  // find the latency bucket such that the count of latencies upto that bucket
  // is >=threshold threshold is calculated by
  // percentage*num_latency_observations_recorded

  T value_at_percentile(double percentile) {
    double running_total = 0.0;
    double threshold = total_count * percentile / 100.0;
    for (int i = 0; i < bucket_count.size(); ++i) {
      running_total += bucket_count[i];
      if (running_total >= threshold) {
        if (i==0){
          //lower than min_allowed_value
          return std::numeric_limits<T>::min();
        }
        else if (i==bucket_count.size()-1){
          return std::numeric_limits<T>::max();
        }
        T offset = static_cast<T> (i-1)*bucket_size;
        return offset+min_value_allowed;
      }
    }
    return max_value_observed;
  }
  T get_min_value() { 
    return min_value_observed; 
  }

  T get_max_value() { 
    return max_value_observed; 
  }
  T get_total_count() { 
    return total_count; 
  }
  
  // allows me to print to whatever i want cout , file ,log file
  void print_stats(std::ostream &out) {
    out << "Min value is" << get_min_value() <<"\n";
    out << "Max latency is" << get_max_value() <<"\n";
    out << "total count of objects is " << get_total_count() <<"\n";
  }

  // export the histogram as a csv but dont default write to a file output
  // write to std:outstream
  // creating an x and y axis formatter because if the buckets are structs or
  // vectors cant print them directly
  void export_csv(std::ostream &out, std::string x_axis_label,
                  std::string y_axis_label,
                  std::function<std::string(T)> x_axis_formatter,
                  std::function<std::string(uint64_t)> y_axis_formatter) {
    out << x_axis_label << "," << y_axis_label << "\n";
    for (int i = 0; i < bucket_count.size(); ++i) {
      uint64_t count = 0;
      if (bucket_count[i] > 0) {
        count = bucket_count[i];
      }
      T bucket_we_want;
      if (i==0){
         bucket_we_want =std::numeric_limits<T>::min();
      }
      else if (i==bucket_count.size()-1){
         bucket_we_want =std::numeric_limits<T>::max();
      }
      else{
        bucket_we_want = static_cast<T>(i-1) * bucket_size +min_value_allowed;
      }
      out << x_axis_formatter(bucket_we_want) << ","
          << y_axis_formatter(bucket_count[i]) << "\n";
    }
  }
};

/**
 * power of 2 histogram
 */
struct pow2_hist_t { //
  /**
   * histogram
   *
   * bin size is 2^index
   * value is count of elements that are <= the current bin but > the previous
   * bin.
   */
  std::vector<int32_t> h;

private:
  /// expand to at least another's size
  void _expand_to(unsigned s) {
    if (s > h.size())
      h.resize(s, 0);
  }
  /// drop useless trailing 0's
  void _contract() {
    unsigned p = h.size();
    while (p > 0 && h[p - 1] == 0)
      --p;
    h.resize(p);
  }

public:
  void clear() { h.clear(); }
  bool empty() const { return h.empty(); }
  void set_bin(int bin, int32_t count) {
    _expand_to(bin + 1);
    h[bin] = count;
    _contract();
  }

  void add(int32_t v) {
    int bin = cbits(v);
    _expand_to(bin + 1);
    h[bin]++;
    _contract();
  }

  bool operator==(const pow2_hist_t &r) const { return h == r.h; }

  /// get a value's position in the histogram.
  ///
  /// positions are represented as values in the range [0..1000000]
  /// (millionths on the unit interval).
  ///
  /// @param v [in] value (non-negative)
  /// @param lower [out] pointer to lower-bound (0..1000000)
  /// @param upper [out] pointer to the upper bound (0..1000000)
  int get_position_micro(int32_t v, uint64_t *lower, uint64_t *upper) {
    if (v < 0)
      return -1;
    unsigned bin = cbits(v);
    uint64_t lower_sum = 0, upper_sum = 0, total = 0;
    for (unsigned i = 0; i < h.size(); ++i) {
      if (i <= bin)
        upper_sum += h[i];
      if (i < bin)
        lower_sum += h[i];
      total += h[i];
    }
    if (total > 0) {
      *lower = lower_sum * 1000000 / total;
      *upper = upper_sum * 1000000 / total;
    }
    return 0;
  }

  void add(const pow2_hist_t &o) {
    _expand_to(o.h.size());
    for (unsigned p = 0; p < o.h.size(); ++p)
      h[p] += o.h[p];
    _contract();
  }
  void sub(const pow2_hist_t &o) {
    _expand_to(o.h.size());
    for (unsigned p = 0; p < o.h.size(); ++p)
      h[p] -= o.h[p];
    _contract();
  }

  int32_t upper_bound() const { return 1 << h.size(); }

  /// decay histogram by N bits (default 1, for a halflife)
  void decay(int bits = 1);

  void dump(ceph::Formatter *f) const;
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  static void generate_test_instances(std::list<pow2_hist_t *> &o);
};
WRITE_CLASS_ENCODER(pow2_hist_t)

#endif /* CEPH_HISTOGRAM_H */
