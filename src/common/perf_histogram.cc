// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 OVH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/perf_histogram.h"

#include <limits>

void PerfHistogramCommon::dump_formatted_axis(
    ceph::Formatter *f, const PerfHistogramCommon::axis_config_d &ac) {
  f->open_object_section("axis");

  // Dump axis configuration
  f->dump_string("name", ac.m_name);
  f->dump_int("min", ac.m_min);
  f->dump_int("quant_size", ac.m_quant_size);
  f->dump_int("buckets", ac.m_buckets);
  switch (ac.m_scale_type) {
    case SCALE_LINEAR:
      f->dump_string("scale_type", "linear");
      break;
    case SCALE_LOG2:
      f->dump_string("scale_type", "log2");
      break;
    default:
      ceph_abort_msg("Invalid scale type");
  }

  {
    // Dump concrete ranges for axis buckets
    f->open_array_section("ranges");
    auto ranges = get_axis_bucket_ranges(ac);
    for (int i = 0; i < ac.m_buckets; ++i) {
      f->open_object_section("bucket");
      if (i > 0) {
        f->dump_int("min", ranges[i].first);
      }
      if (i < ac.m_buckets - 1) {
        f->dump_int("max", ranges[i].second);
      }
      f->close_section();
    }
    f->close_section();
  }

  f->close_section();
}

int64_t get_quants(int64_t i, PerfHistogramCommon::scale_type_d st) {
  switch (st) {
    case PerfHistogramCommon::SCALE_LINEAR:
      return i;
    case PerfHistogramCommon::SCALE_LOG2:
      return int64_t(1) << (i - 1);
  }
  ceph_abort_msg("Invalid scale type");
}

int64_t PerfHistogramCommon::get_bucket_for_axis(
    int64_t value, const PerfHistogramCommon::axis_config_d &ac) {
  if (value < ac.m_min) {
    return 0;
  }

  value -= ac.m_min;
  value /= ac.m_quant_size;

  switch (ac.m_scale_type) {
    case SCALE_LINEAR:
      return std::min<int64_t>(value + 1, ac.m_buckets - 1);

    case SCALE_LOG2:
      for (int64_t i = 1; i < ac.m_buckets; ++i) {
        if (value < get_quants(i, SCALE_LOG2)) {
          return i;
        }
      }
      return ac.m_buckets - 1;
  }
  ceph_abort_msg("Invalid scale type");
}

std::vector<std::pair<int64_t, int64_t>>
PerfHistogramCommon::get_axis_bucket_ranges(
    const PerfHistogramCommon::axis_config_d &ac) {
  std::vector<std::pair<int64_t, int64_t>> ret;
  ret.resize(ac.m_buckets);

  // First bucket is for value < min
  int64_t min = ac.m_min;
  for (int64_t i = 1; i < ac.m_buckets - 1; i++) {
    int64_t max_exclusive =
        ac.m_min + get_quants(i, ac.m_scale_type) * ac.m_quant_size;

    // Dump bucket range
    ret[i].first = min;
    ret[i].second = max_exclusive - 1;

    // Shift min to next bucket
    min = max_exclusive;
  }

  // Fill up first and last element, note that in case m_buckets == 1
  // those will point to the same element, the order is important here
  ret.front().second = ac.m_min - 1;
  ret.back().first = min;

  ret.front().first = std::numeric_limits<int64_t>::min();
  ret.back().second = std::numeric_limits<int64_t>::max();
  return ret;
}
