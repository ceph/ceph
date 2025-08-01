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

#include <list>
#include "include/encoding.h"
#include "include/intarith.h"
#include <limits>
#include <fstream>

namespace ceph {
  class Formatter;
}
/*
class to store histogram as a dictionary
dint want to use unnecessary space for buckets that had 0 count 
*/
class adaptive_linear_hist_t{
  private:
  std::map<double,int>latency_buckets_counts;
  double user_specified_granularity;
  int user_min_value;
  int user_max_value;
  double min_latency_observed;
  double max_latency_observed;
  double total_latency_count=0.0;

  public:
  //constructor takes in the min latency the user thinks exists, the max latency the user thinks will be outputed 
  // interval size is the size of buckets 
  // usually max_value is duration passed in milli sec 
  // min value is 1 millisec
  //interval_size is passed in by user 
  adaptive_linear_hist_t(int min_value, int max_value,int interval_size){
    this->user_min_value=min_value;
    this->user_max_value=max_value;
    this->user_specified_granularity=interval_size;
    this->min_latency_observed=std::numeric_limits<double>::infinity();
    this->max_latency_observed=0.0;
  }
  //store the latency value in a bucket 
  //latency bucket is the lower bound of each bucket
  //we round latency_bucket lower bound to 3 sig figs to avoid rounding errors

  void record_latencies(double latency_value){
    int bucket_number= latency_value/user_specified_granularity;
    double bucket_lower_bound=std::round(bucket_number * user_specified_granularity * 1000.0) / 1000.0;
    latency_buckets_counts[bucket_lower_bound]++;
    if (latency_value<min_latency_observed){
      min_latency_observed=latency_value;
    }
    if (latency_value>max_latency_observed){
      max_latency_observed=latency_value;
    }
    total_latency_count+=1;
  }
  // find the value of the latency at the ith percentile
  // find the latency bucket such that the count of latencies upto that bucket is >=threshold 
  // threshold is calculated by percentage*num_latency_observations_recorded

  double value_at_percentile(double percentile){
    double running_total=0.0;
    double threshold=total_latency_count*percentile/100.0;
    for (const auto& [latency, count] : latency_buckets_counts) {
        running_total += count;
        if (running_total >= threshold) {
            return latency;
        }
    }
    return std::numeric_limits<double>::infinity();
  }
  double get_min_latency(){
    return min_latency_observed;
  }
  double get_max_latency(){
    return max_latency_observed;
  }
  //does cout work here? 
  /* 
  void print_stats() {
    // this always returns an intger output but we can control bucket size by
    // adjusting the granularity size,essential saying 3 means that buckets have
    // size 10^-3
    //LOG_PREFIX(print_stats);
    //cout<<"Min latency is {}"<< get_min_latency()<<dendl;
    //ERROR("Max latency is {}", get_max_latency());
  }
  */

  // see my idea was to define a vector that had size (max_observed -min_observed)/granualrity buckets
  //so that if there were any buckets in between that had 0 count we would be able to see them 
  //but i think a smarter way would be to iterate through the dictionary and if we are missing a 
  //bucket we can add it to the csv 

  void export_csv(std::string filename){
    std::ofstream myfile(filename); //open the file and call it something in this case myfile 
    myfile << "latency(ms),count\n";
    double min_bucket = latency_buckets_counts.begin()->first;
    double max_bucket = latency_buckets_counts.rbegin()->first;
    for(double bucket=min_bucket; bucket<=max_bucket;bucket+=user_specified_granularity ){
      double rounded = std::round(bucket * 1000.0) / 1000.0;
      int count=0;
      if (latency_buckets_counts.find(rounded)!=latency_buckets_counts.end()){
        count=latency_buckets_counts[rounded];
      }
      myfile<<rounded<<","<<count<<"\n";
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
   * value is count of elements that are <= the current bin but > the previous bin.
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
    while (p > 0 && h[p-1] == 0)
      --p;
    h.resize(p);
  }

public:
  void clear() {
    h.clear();
  }
  bool empty() const {
    return h.empty();
  }
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

  bool operator==(const pow2_hist_t &r) const {
    return h == r.h;
  }

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
    for (unsigned i=0; i<h.size(); ++i) {
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

  void add(const pow2_hist_t& o) {
    _expand_to(o.h.size());
    for (unsigned p = 0; p < o.h.size(); ++p)
      h[p] += o.h[p];
    _contract();
  }
  void sub(const pow2_hist_t& o) {
    _expand_to(o.h.size());
    for (unsigned p = 0; p < o.h.size(); ++p)
      h[p] -= o.h[p];
    _contract();
  }

  int32_t upper_bound() const {
    return 1 << h.size();
  }

  /// decay histogram by N bits (default 1, for a halflife)
  void decay(int bits = 1);

  void dump(ceph::Formatter *f) const;
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  static void generate_test_instances(std::list<pow2_hist_t*>& o);
};
WRITE_CLASS_ENCODER(pow2_hist_t)

#endif /* CEPH_HISTOGRAM_H */
