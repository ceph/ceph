// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MGR_MDS_PERF_METRIC_TYPES_H
#define CEPH_MGR_MDS_PERF_METRIC_TYPES_H

#include <regex>
#include <vector>
#include <iostream>

#include "include/denc.h"
#include "include/stringify.h"

#include "mds/mdstypes.h"
#include "mgr/Types.h"

typedef std::vector<std::string> MDSPerfMetricSubKey; // array of regex match
typedef std::vector<MDSPerfMetricSubKey> MDSPerfMetricKey;

enum class MDSPerfMetricSubKeyType : uint8_t {
  MDS_RANK = 0,
  CLIENT_ID = 1,
};

struct MDSPerfMetricSubKeyDescriptor {
  MDSPerfMetricSubKeyType type = static_cast<MDSPerfMetricSubKeyType>(-1);
  std::string regex_str;
  std::regex regex;

  bool is_supported() const {
    switch (type) {
    case MDSPerfMetricSubKeyType::MDS_RANK:
    case MDSPerfMetricSubKeyType::CLIENT_ID:
      return true;
    default:
      return false;
    }
  }

  MDSPerfMetricSubKeyDescriptor() {
  }
  MDSPerfMetricSubKeyDescriptor(MDSPerfMetricSubKeyType type, const std::string &regex_str)
    : type(type), regex_str(regex_str) {
  }

  bool operator<(const MDSPerfMetricSubKeyDescriptor &other) const {
    if (type < other.type) {
      return true;
    }
    if (type > other.type) {
      return false;
    }
    return regex_str < other.regex_str;
  }

  DENC(MDSPerfMetricSubKeyDescriptor, v, p) {
    DENC_START(1, 1, p);
    denc(v.type, p);
    denc(v.regex_str, p);
    DENC_FINISH(p);
  }
};
WRITE_CLASS_DENC(MDSPerfMetricSubKeyDescriptor)

std::ostream& operator<<(std::ostream& os, const MDSPerfMetricSubKeyDescriptor &d);
typedef std::vector<MDSPerfMetricSubKeyDescriptor> MDSPerfMetricKeyDescriptor;

template<>
struct denc_traits<MDSPerfMetricKeyDescriptor> {
  static constexpr bool supported = true;
  static constexpr bool bounded = false;
  static constexpr bool featured = false;
  static constexpr bool need_contiguous = true;
  static void bound_encode(const MDSPerfMetricKeyDescriptor& v, size_t& p) {
    p += sizeof(uint32_t);
    const auto size = v.size();
    if (size) {
      size_t per = 0;
      denc(v.front(), per);
      p +=  per * size;
    }
  }
  static void encode(const MDSPerfMetricKeyDescriptor& v,
		     ceph::buffer::list::contiguous_appender& p) {
    denc_varint(v.size(), p);
    for (auto& i : v) {
      denc(i, p);
    }
  }
  static void decode(MDSPerfMetricKeyDescriptor& v,
                     ceph::buffer::ptr::const_iterator& p) {
    unsigned num;
    denc_varint(num, p);
    v.clear();
    v.reserve(num);
    for (unsigned i=0; i < num; ++i) {
      MDSPerfMetricSubKeyDescriptor d;
      denc(d, p);
      if (!d.is_supported()) {
        v.clear();
        return;
      }
      try {
        d.regex = d.regex_str.c_str();
      } catch (const std::regex_error& e) {
        v.clear();
        return;
      }
      if (d.regex.mark_count() == 0) {
        v.clear();
        return;
      }
      v.push_back(std::move(d));
    }
  }
};

enum class MDSPerformanceCounterType : uint8_t {
  CAP_HIT_METRIC = 0,
  READ_LATENCY_METRIC = 1,
  WRITE_LATENCY_METRIC = 2,
  METADATA_LATENCY_METRIC = 3,
};

struct MDSPerformanceCounterDescriptor {
  MDSPerformanceCounterType type = static_cast<MDSPerformanceCounterType>(-1);

  bool is_supported() const {
    switch(type) {
    case MDSPerformanceCounterType::CAP_HIT_METRIC:
    case MDSPerformanceCounterType::READ_LATENCY_METRIC:
    case MDSPerformanceCounterType::WRITE_LATENCY_METRIC:
    case MDSPerformanceCounterType::METADATA_LATENCY_METRIC:
      return true;
    default:
      return false;
    }
  }

  MDSPerformanceCounterDescriptor() {
  }
  MDSPerformanceCounterDescriptor(MDSPerformanceCounterType type) : type(type) {
  }

  bool operator<(const MDSPerformanceCounterDescriptor &other) const {
    return type < other.type;
  }

  bool operator==(const MDSPerformanceCounterDescriptor &other) const {
    return type == other.type;
  }

  bool operator!=(const MDSPerformanceCounterDescriptor &other) const {
    return type != other.type;
  }

  DENC(MDSPerformanceCounterDescriptor, v, p) {
    DENC_START(1, 1, p);
    denc(v.type, p);
    DENC_FINISH(p);
  }

  void pack_counter(const PerformanceCounter &c, ceph::buffer::list *bl) const;
  void unpack_counter(ceph::buffer::list::const_iterator& bl, PerformanceCounter *c) const;
};
WRITE_CLASS_DENC(MDSPerformanceCounterDescriptor)

std::ostream& operator<<(std::ostream &os, const MDSPerformanceCounterDescriptor &d);
typedef std::vector<MDSPerformanceCounterDescriptor> MDSPerformanceCounterDescriptors;

template<>
struct denc_traits<MDSPerformanceCounterDescriptors> {
  static constexpr bool supported = true;
  static constexpr bool bounded = false;
  static constexpr bool featured = false;
  static constexpr bool need_contiguous = true;
  static void bound_encode(const MDSPerformanceCounterDescriptors& v, size_t& p) {
    p += sizeof(uint32_t);
    const auto size = v.size();
    if (size) {
      size_t per = 0;
      denc(v.front(), per);
      p +=  per * size;
    }
  }
  static void encode(const MDSPerformanceCounterDescriptors& v,
                     ceph::buffer::list::contiguous_appender& p) {
    denc_varint(v.size(), p);
    for (auto& i : v) {
      denc(i, p);
    }
  }
  static void decode(MDSPerformanceCounterDescriptors& v,
                     ceph::buffer::ptr::const_iterator& p) {
    unsigned num;
    denc_varint(num, p);
    v.clear();
    v.reserve(num);
    for (unsigned i=0; i < num; ++i) {
      MDSPerformanceCounterDescriptor d;
      denc(d, p);
      if (d.is_supported()) {
        v.push_back(std::move(d));
      }
    }
  }
};

struct MDSPerfMetricLimit {
  MDSPerformanceCounterDescriptor order_by;
  uint64_t max_count;

  MDSPerfMetricLimit() {
  }
  MDSPerfMetricLimit(const MDSPerformanceCounterDescriptor &order_by, uint64_t max_count)
    : order_by(order_by), max_count(max_count) {
  }

  bool operator<(const MDSPerfMetricLimit &other) const {
    if (order_by != other.order_by) {
      return order_by < other.order_by;
    }

    return max_count < other.max_count;
  }

  DENC(MDSPerfMetricLimit, v, p) {
    DENC_START(1, 1, p);
    denc(v.order_by, p);
    denc(v.max_count, p);
    DENC_FINISH(p);
  }
};
WRITE_CLASS_DENC(MDSPerfMetricLimit)

std::ostream &operator<<(std::ostream &os, const MDSPerfMetricLimit &limit);
typedef std::set<MDSPerfMetricLimit> MDSPerfMetricLimits;

struct MDSPerfMetricQuery {
  MDSPerfMetricKeyDescriptor key_descriptor;
  MDSPerformanceCounterDescriptors performance_counter_descriptors;

  MDSPerfMetricQuery() {
  }
  MDSPerfMetricQuery(const MDSPerfMetricKeyDescriptor &key_descriptor,
                     const MDSPerformanceCounterDescriptors &performance_counter_descriptors)
    : key_descriptor(key_descriptor),
      performance_counter_descriptors(performance_counter_descriptors)
  {
  }

  bool operator<(const MDSPerfMetricQuery &other) const {
    if (key_descriptor < other.key_descriptor) {
      return true;
    }
    if (key_descriptor > other.key_descriptor) {
      return false;
    }
    return performance_counter_descriptors < other.performance_counter_descriptors;
  }

  template <typename L>
  bool get_key(L&& get_sub_key, MDSPerfMetricKey *key) const {
    for (auto &sub_key_descriptor : key_descriptor) {
      MDSPerfMetricSubKey sub_key;
      if (!get_sub_key(sub_key_descriptor, &sub_key)) {
        return false;
      }
      key->push_back(sub_key);
    }
    return true;
  }

  void get_performance_counter_descriptors(MDSPerformanceCounterDescriptors *descriptors) const {
    *descriptors = performance_counter_descriptors;
  }

  template <typename L>
  void update_counters(L &&update_counter, PerformanceCounters *counters) const {
    auto it = counters->begin();
    for (auto &descriptor : performance_counter_descriptors) {
      // TODO: optimize
      if (it == counters->end()) {
        counters->push_back(PerformanceCounter());
        it = std::prev(counters->end());
      }
      update_counter(descriptor, &(*it));
      it++;
    }
  }

  DENC(MDSPerfMetricQuery, v, p) {
    DENC_START(1, 1, p);
    denc(v.key_descriptor, p);
    denc(v.performance_counter_descriptors, p);
    DENC_FINISH(p);
  }

  void pack_counters(const PerformanceCounters &counters, ceph::buffer::list *bl) const;
};
WRITE_CLASS_DENC(MDSPerfMetricQuery)

std::ostream &operator<<(std::ostream &os, const MDSPerfMetricQuery &query);

struct MDSPerfMetrics {
  MDSPerformanceCounterDescriptors performance_counter_descriptors;
  std::map<MDSPerfMetricKey, ceph::buffer::list> group_packed_performance_counters;

  DENC(MDSPerfMetrics, v, p) {
    DENC_START(1, 1, p);
    denc(v.performance_counter_descriptors, p);
    denc(v.group_packed_performance_counters, p);
    DENC_FINISH(p);
  }
};

struct MDSPerfMetricReport {
  std::map<MDSPerfMetricQuery, MDSPerfMetrics> reports;
  // set of active ranks that have delayed (stale) metrics
  std::set<mds_rank_t> rank_metrics_delayed;

  DENC(MDSPerfMetricReport, v, p) {
    DENC_START(1, 1, p);
    denc(v.reports, p);
    denc(v.rank_metrics_delayed, p);
    DENC_FINISH(p);
  }
};

WRITE_CLASS_DENC(MDSPerfMetrics)
WRITE_CLASS_DENC(MDSPerfMetricReport)

#endif // CEPH_MGR_MDS_PERF_METRIC_TYPES_H
