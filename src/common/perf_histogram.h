// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

#include "common/Formatter.h"
#include "include/ceph_assert.h"

class PerfHistogramCommon {
public:
  enum scale_type_d : uint8_t {
    SCALE_LINEAR = 1,
    SCALE_LOG2 = 2,
  };

  enum axis_unit_d : uint8_t {
    AXIS_UNIT_NONE,
    AXIS_UNIT_NANOSECONDS,
    AXIS_UNIT_BYTES,
  };

  static const char* axis_unit_name(axis_unit_d u) {
    switch (u) {
      case AXIS_UNIT_NANOSECONDS: return "nanoseconds";
      case AXIS_UNIT_BYTES: return "bytes";
      case AXIS_UNIT_NONE:
      default: return "none";
    }
  }

  struct axis_config_d {
    const char *m_name = nullptr;
    scale_type_d m_scale_type = SCALE_LINEAR;
    int64_t m_min = 0;
    int64_t m_quant_size = 0;
    int32_t m_buckets = 0;
    axis_unit_d m_unit = AXIS_UNIT_NONE;
    axis_config_d() = default;
    axis_config_d(const char* name,
		  scale_type_d scale_type,
		  int64_t min,
		  int64_t quant_size,
		  int32_t buckets,
                  axis_unit_d unit = AXIS_UNIT_NONE)
      : m_name(name),
	m_scale_type(scale_type),
	m_min(min),
	m_quant_size(quant_size),
	m_buckets(buckets),
        m_unit(unit)
    {}

    static constexpr axis_config_d latency(const char* name, int64_t quant_ns = 100, int buckets = 32) {
      return {name, SCALE_LOG2, 0, quant_ns, buckets, AXIS_UNIT_NANOSECONDS};
    }
    // Prometheus-style buckets
    static constexpr axis_config_d web_latency(const char* name, int buckets = 16) {
      return {name, SCALE_LOG2, 0, 1'000'000 /*1ms floor*/, buckets, AXIS_UNIT_NANOSECONDS};
    }
    static constexpr axis_config_d bytes(const char* name, int64_t quant_bytes=512, int buckets = 32) {
      return {name, SCALE_LOG2, 0, quant_bytes, buckets, AXIS_UNIT_BYTES};
    }
    static constexpr axis_config_d count(
        const char* name, int buckets, int64_t quant = 1) {
      return {name, SCALE_LINEAR, 0, quant, buckets, AXIS_UNIT_NONE};
    }
  };

  virtual ~PerfHistogramCommon() = default;
  virtual int dims() const = 0;
  virtual void reset() = 0;
  virtual uint64_t get_sum(int dim = 0) const = 0;
  virtual void dump_formatted(ceph::Formatter* f) const = 0;
  virtual void observe(int64_t x, int64_t y) = 0;
  virtual std::unique_ptr<PerfHistogramCommon> clone() const = 0;

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
    ceph_assert(axes_config.size() == DIM &&
		"Invalid number of axis configuration objects");

    int i = 0;
    for (const auto &ac : axes_config) {
      ceph_assertf(ac.m_buckets > 0, "Must have at least one bucket on axis");
      ceph_assertf(ac.m_quant_size > 0,
             "Quantization unit must be non-zero positive integer value");

      m_axes_config[i++] = ac;
    }

    m_rawData.reset(new std::atomic<uint64_t>[get_raw_size()] {});
  }

  /// Copy from other histogram object
  PerfHistogram(const PerfHistogram &other)
      : m_axes_config(other.m_axes_config) {
    int64_t size = get_raw_size();
    m_rawData.reset(new std::atomic<uint64_t>[size] {});
    for (int64_t i = 0; i < size; i++) {
      m_rawData[i] = other.m_rawData[i].load();
    }
    for (int i = 0; i < DIM; i++) {
      m_sum[i] = other.m_sum[i].load();
    }
  }

  std::unique_ptr<PerfHistogramCommon> clone() const override {
    return std::make_unique<PerfHistogram<DIM>>(*this);
  }

  int dims() const override { return DIM; }

  uint64_t get_sum(int dim) const override { return m_sum[dim].load(); }

  /// Set all histogram values to 0
  void reset() override {
    auto size = get_raw_size();
    for (auto i = size; --i >= 0;) {
      m_rawData[i] = 0;
    }
    for (auto& s : m_sum) {
      s = 0;
    }
  }

  void observe(int64_t x, int64_t y) override {
    if constexpr (DIM == 1) {
      inc(x);
    } else if constexpr (DIM == 2) {
      inc(x, y);
    } else {
      ceph_abort_msg("observe only supports 1D/2D histograms");
    }
  }

  /// Increase counter for given axis values by one
  template <typename... T>
  void inc(T... axis) {
    auto index = get_raw_index_for_value(axis...);
    m_rawData[index]++;
    int i = 0;
    ((m_sum[i++] += axis), ...);
  }

  /// Increase counter for given axis buckets by one
  template <typename... T>
  void inc_bucket(T... bucket) {
    auto index = get_raw_index_for_bucket(bucket...);
    m_rawData[index]++;
  }

  /// Read value from given bucket
  template <typename... T>
  uint64_t read_bucket(T... bucket) const {
    auto index = get_raw_index_for_bucket(bucket...);
    return m_rawData[index];
  }

  /// Dump data to a Formatter object
  void dump_formatted(ceph::Formatter *f) const override {
    // Dump axes configuration
    f->open_array_section("axes");
    for (auto &ac : m_axes_config) {
      dump_formatted_axis(f, ac);
    }
    f->close_section();
    f->open_array_section("sum");
    for (int i = 0; i < DIM; i++) {
      f->dump_unsigned("sum", m_sum[i].load());
    }
    f->close_section();

    // Dump histogram values
    dump_formatted_values(f);
  }

protected:
  /// Raw data stored as linear space, internal indexes are calculated on
  /// demand.
  std::unique_ptr<std::atomic<uint64_t>[]> m_rawData;

  /// Running sum of observed values along the histogram's value axis.
  std::array<std::atomic<uint64_t>, DIM> m_sum{};

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
          ceph_assertf(bucket >= 0, "Bucket index can not be negative");
          ceph_assertf(bucket < ac.m_buckets, "Bucket index too large");
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
