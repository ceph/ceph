// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_COMMON_STDDEV_H
#define CEPH_COMMON_STDDEV_H

#include <cmath>

// Calculate standard deviation using rapid calculation methods:
// https://en.wikipedia.org/wiki/Standard_deviation#Rapid_calculation_methods
class Stddev {
 public:
  Stddev() : count_(0), avg_(0), sum_(0) {}

  void enter(const double& value) {
    if (count_ == 0) {
      avg_ = value;
      sum_ = 0.0;
    } else {
      double delta = value - avg_;
      avg_ += delta / (count_ + 1.0);
      sum_ += delta * (value - avg_);
    }

    count_++;
  }

  double value() const {
    double stddev = 0.0;
    if (count_ > 1) {
      stddev = std::sqrt(sum_ / (count_ - 1));
    }
    return stddev;
  }

 private:
  int count_;
  double avg_;
  double sum_;
};

#endif  // CEPH_COMMON_STDDEV_H
