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

#ifndef CEPH_COMMON_PERF_HISTOGRAM_H
#define CEPH_COMMON_PERF_HISTOGRAM_H

#include "common/Formatter.h"
#include "include/int_types.h"

#include <array>
#include <atomic>
#include <memory>
#include <cassert>

class PerfHistogramCommon {
public:
  enum scale_type_d : uint8_t {
    SCALE_LINEAR = 1,
    SCALE_LOG2 = 2,
  };

  struct axis_config_d {
    const char *m_name = nullptr;
    scale_type_d m_scale_type = SCALE_LINEAR;
    int64_t m_min = 0;
    int64_t m_quant_size = 0;
    int32_t m_buckets = 0;
    axis_config_d() = default;
    axis_config_d(const char* name,
		  scale_type_d scale_type,
		  int64_t min,
		  int64_t quant_size,
		  int32_t buckets)
      : m_name(name),
	m_scale_type(scale_type),
	m_min(min),
	m_quant_size(quant_size),
	m_buckets(buckets)
    {}
  };

protected:
  /// Dump configuration of one axis to a formatter
  static void dump_formatted_axis(ceph::Formatter *f, const axis_config_d &ac);

  /// Quantize given value and convert to bucket number on given axis
  static int64_t get_bucket_for_axis(int64_t value, const axis_config_d &ac);

  /// Calculate inclusive ranges of axis values for each bucket on that axis
  static std::vector<std::pair<int64_t, int64_t>> get_axis_bucket_ranges(
      const axis_config_d &ac);
};

/// PerfHistogram does trace a histogram of input values. It's an extended
/// version of a standard histogram which does trace characteristics of a single
/// one value only. In this implementation, values can be traced in multiple
/// dimensions - i.e. we can create a histogram of input request size (first
/// dimension) and processing latency (second dimension). Creating standard
/// histogram out of such multidimensional one is trivial and requires summing
/// values across dimensions we're not interested in.
template <int DIM = 2>
class PerfHistogram : public PerfHistogramCommon {
public:
  /// Initialize new histogram object
  PerfHistogram(std::initializer_list<axis_config_d> axes_config) {
    assert(axes_config.size() == DIM &&
           "Invalid number of axis configuration objects");

    int i = 0;
    for (const auto &ac : axes_config) {
      assert(ac.m_buckets > 0 && "Must have at least one bucket on axis");
      assert(ac.m_quant_size > 0 &&
             "Quantization unit must be non-zero positive integer value");

      m_axes_config[i++] = ac;
    }

    m_rawData.reset(new std::atomic<uint64_t>[get_raw_size()]);
  }

  /// Copy from other histogram object
  PerfHistogram(const PerfHistogram &other)
      : m_axes_config(other.m_axes_config) {
    int64_t size = get_raw_size();
    m_rawData.reset(new std::atomic<uint64_t>[size]);
    for (int64_t i = 0; i < size; i++) {
      m_rawData[i] = other.m_rawData[i];
    }
  }

  /// Set all histogram values to 0
  void reset() {
    auto size = get_raw_size();
    for (auto i = size; --i >= 0;) {
      m_rawData[i] = 0;
    }
  }

  /// Increase counter for given axis values by one
  template <typename... T>
  void inc(T... axis) {
    auto index = get_raw_index_for_value(axis...);
    m_rawData[index] += 1;
  }

  /// Increase counter for given axis buckets by one
  template <typename... T>
  void inc_bucket(T... bucket) {
    auto index = get_raw_index_for_bucket(bucket...);
    m_rawData[index] += 1;
  }

  /// Read value from given bucket
  template <typename... T>
  uint64_t read_bucket(T... bucket) const {
    auto index = get_raw_index_for_bucket(bucket...);
    return m_rawData[index];
  }

  /// Dump data to a Formatter object
  void dump_formatted(ceph::Formatter *f) const {
    // Dump axes configuration
    f->open_array_section("axes");
    for (auto &ac : m_axes_config) {
      dump_formatted_axis(f, ac);
    }
    f->close_section();

    // Dump histogram values
    dump_formatted_values(f);
  }

protected:
  /// Raw data stored as linear space, internal indexes are calculated on
  /// demand.
  std::unique_ptr<std::atomic<uint64_t>[]> m_rawData;

  /// Configuration of axes
  std::array<axis_config_d, DIM> m_axes_config;

  /// Dump histogram counters to a formatter
  void dump_formatted_values(ceph::Formatter *f) const {
    visit_values([f](int) { f->open_array_section("values"); },
                 [f](int64_t value) { f->dump_unsigned("value", value); },
                 [f](int) { f->close_section(); });
  }

  /// Get number of all histogram counters
  int64_t get_raw_size() {
    int64_t ret = 1;
    for (const auto &ac : m_axes_config) {
      ret *= ac.m_buckets;
    }
    return ret;
  }

  /// Calculate m_rawData index from axis values
  template <typename... T>
  int64_t get_raw_index_for_value(T... axes) const {
    static_assert(sizeof...(T) == DIM, "Incorrect number of arguments");
    return get_raw_index_internal<0>(get_bucket_for_axis, 0, axes...);
  }

  /// Calculate m_rawData index from axis bucket numbers
  template <typename... T>
  int64_t get_raw_index_for_bucket(T... buckets) const {
    static_assert(sizeof...(T) == DIM, "Incorrect number of arguments");
    return get_raw_index_internal<0>(
        [](int64_t bucket, const axis_config_d &ac) {
          assert(bucket >= 0 && "Bucket index can not be negative");
          assert(bucket < ac.m_buckets && "Bucket index too large");
          return bucket;
        },
        0, buckets...);
  }

  template <int level = 0, typename F, typename... T>
  int64_t get_raw_index_internal(F bucket_evaluator, int64_t startIndex,
                                 int64_t value, T... tail) const {
    static_assert(level + 1 + sizeof...(T) == DIM,
                  "Internal consistency check");
    auto &ac = m_axes_config[level];
    auto bucket = bucket_evaluator(value, ac);
    return get_raw_index_internal<level + 1>(
        bucket_evaluator, ac.m_buckets * startIndex + bucket, tail...);
  }

  template <int level, typename F>
  int64_t get_raw_index_internal(F, int64_t startIndex) const {
    static_assert(level == DIM, "Internal consistency check");
    return startIndex;
  }

  /// Visit all histogram counters, call onDimensionEnter / onDimensionLeave
  /// when starting / finishing traversal
  /// on given axis, call onValue when dumping raw histogram counter value.
  template <typename FDE, typename FV, typename FDL>
  void visit_values(FDE onDimensionEnter, FV onValue, FDL onDimensionLeave,
                    int level = 0, int startIndex = 0) const {
    if (level == DIM) {
      onValue(m_rawData[startIndex]);
      return;
    }

    onDimensionEnter(level);
    auto &ac = m_axes_config[level];
    startIndex *= ac.m_buckets;
    for (int32_t i = 0; i < ac.m_buckets; ++i, ++startIndex) {
      visit_values(onDimensionEnter, onValue, onDimensionLeave, level + 1,
                   startIndex);
    }
    onDimensionLeave(level);
  }
};

#endif
