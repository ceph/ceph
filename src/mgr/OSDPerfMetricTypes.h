// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef OSD_PERF_METRIC_H_
#define OSD_PERF_METRIC_H_

#include "include/denc.h"
#include "include/stringify.h"

#include <regex>

typedef std::vector<std::string> OSDPerfMetricSubKey; // array of regex match
typedef std::vector<OSDPerfMetricSubKey> OSDPerfMetricKey;

enum class OSDPerfMetricSubKeyType : uint8_t {
  CLIENT_ID = 0,
  CLIENT_ADDRESS = 1,
  POOL_ID = 2,
  NAMESPACE = 3,
  OSD_ID = 4,
  PG_ID = 5,
  OBJECT_NAME = 6,
  SNAP_ID = 7,
};

struct OSDPerfMetricSubKeyDescriptor {
  OSDPerfMetricSubKeyType type = static_cast<OSDPerfMetricSubKeyType>(-1);
  std::string regex_str;
  std::regex regex;

  bool is_supported() const {
    switch (type) {
    case OSDPerfMetricSubKeyType::CLIENT_ID:
    case OSDPerfMetricSubKeyType::CLIENT_ADDRESS:
    case OSDPerfMetricSubKeyType::POOL_ID:
    case OSDPerfMetricSubKeyType::NAMESPACE:
    case OSDPerfMetricSubKeyType::OSD_ID:
    case OSDPerfMetricSubKeyType::PG_ID:
    case OSDPerfMetricSubKeyType::OBJECT_NAME:
    case OSDPerfMetricSubKeyType::SNAP_ID:
      return true;
    default:
      return false;
    }
  }

  OSDPerfMetricSubKeyDescriptor() {
  }

  OSDPerfMetricSubKeyDescriptor(OSDPerfMetricSubKeyType type,
                                const std::string regex)
    : type(type), regex_str(regex) {
  }

  bool operator<(const OSDPerfMetricSubKeyDescriptor &other) const {
    if (type < other.type) {
      return true;
    }
    if (type > other.type) {
      return false;
    }
    return regex_str < other.regex_str;
  }

  DENC(OSDPerfMetricSubKeyDescriptor, v, p) {
    DENC_START(1, 1, p);
    denc(v.type, p);
    denc(v.regex_str, p);
    DENC_FINISH(p);
  }
};
WRITE_CLASS_DENC(OSDPerfMetricSubKeyDescriptor)

std::ostream& operator<<(std::ostream& os,
                         const OSDPerfMetricSubKeyDescriptor &d);

typedef std::vector<OSDPerfMetricSubKeyDescriptor> OSDPerfMetricKeyDescriptor;

template<>
struct denc_traits<OSDPerfMetricKeyDescriptor> {
  static constexpr bool supported = true;
  static constexpr bool bounded = false;
  static constexpr bool featured = false;
  static constexpr bool need_contiguous = true;
  static void bound_encode(const OSDPerfMetricKeyDescriptor& v, size_t& p) {
    p += sizeof(uint32_t);
    const auto size = v.size();
    if (size) {
      size_t per = 0;
      denc(v.front(), per);
      p +=  per * size;
    }
  }
  static void encode(const OSDPerfMetricKeyDescriptor& v,
		     ceph::buffer::list::contiguous_appender& p) {
    denc_varint(v.size(), p);
    for (auto& i : v) {
      denc(i, p);
    }
  }
  static void decode(OSDPerfMetricKeyDescriptor& v,
                     ceph::buffer::ptr::const_iterator& p) {
    unsigned num;
    denc_varint(num, p);
    v.clear();
    v.reserve(num);
    for (unsigned i=0; i < num; ++i) {
      OSDPerfMetricSubKeyDescriptor d;
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

typedef std::pair<uint64_t,uint64_t> PerformanceCounter;
typedef std::vector<PerformanceCounter> PerformanceCounters;

enum class PerformanceCounterType : uint8_t {
  OPS = 0,
  WRITE_OPS = 1,
  READ_OPS = 2,
  BYTES = 3,
  WRITE_BYTES = 4,
  READ_BYTES = 5,
  LATENCY = 6,
  WRITE_LATENCY = 7,
  READ_LATENCY = 8,
};

struct PerformanceCounterDescriptor {
  PerformanceCounterType type = static_cast<PerformanceCounterType>(-1);

  bool is_supported() const {
    switch (type) {
    case PerformanceCounterType::OPS:
    case PerformanceCounterType::WRITE_OPS:
    case PerformanceCounterType::READ_OPS:
    case PerformanceCounterType::BYTES:
    case PerformanceCounterType::WRITE_BYTES:
    case PerformanceCounterType::READ_BYTES:
    case PerformanceCounterType::LATENCY:
    case PerformanceCounterType::WRITE_LATENCY:
    case PerformanceCounterType::READ_LATENCY:
      return true;
    default:
      return false;
    }
  }

  PerformanceCounterDescriptor() {
  }

  PerformanceCounterDescriptor(PerformanceCounterType type) : type(type) {
  }

  bool operator<(const PerformanceCounterDescriptor &other) const {
    return type < other.type;
  }

  bool operator==(const PerformanceCounterDescriptor &other) const {
    return type == other.type;
  }

  bool operator!=(const PerformanceCounterDescriptor &other) const {
    return type != other.type;
  }

  DENC(PerformanceCounterDescriptor, v, p) {
    DENC_START(1, 1, p);
    denc(v.type, p);
    DENC_FINISH(p);
  }

  void pack_counter(const PerformanceCounter &c, ceph::buffer::list *bl) const;
  void unpack_counter(ceph::buffer::list::const_iterator& bl,
                      PerformanceCounter *c) const;
};
WRITE_CLASS_DENC(PerformanceCounterDescriptor)

std::ostream& operator<<(std::ostream& os,
                         const PerformanceCounterDescriptor &d);

typedef std::vector<PerformanceCounterDescriptor> PerformanceCounterDescriptors;

template<>
struct denc_traits<PerformanceCounterDescriptors> {
  static constexpr bool supported = true;
  static constexpr bool bounded = false;
  static constexpr bool featured = false;
  static constexpr bool need_contiguous = true;
  static void bound_encode(const PerformanceCounterDescriptors& v, size_t& p) {
    p += sizeof(uint32_t);
    const auto size = v.size();
    if (size) {
      size_t per = 0;
      denc(v.front(), per);
      p +=  per * size;
    }
  }
  static void encode(const PerformanceCounterDescriptors& v,
                     ceph::buffer::list::contiguous_appender& p) {
    denc_varint(v.size(), p);
    for (auto& i : v) {
      denc(i, p);
    }
  }
  static void decode(PerformanceCounterDescriptors& v,
                     ceph::buffer::ptr::const_iterator& p) {
    unsigned num;
    denc_varint(num, p);
    v.clear();
    v.reserve(num);
    for (unsigned i=0; i < num; ++i) {
      PerformanceCounterDescriptor d;
      denc(d, p);
      if (d.is_supported()) {
        v.push_back(std::move(d));
      }
    }
  }
};

struct OSDPerfMetricLimit {
  PerformanceCounterDescriptor order_by;
  uint64_t max_count = 0;

  OSDPerfMetricLimit() {
  }

  OSDPerfMetricLimit(const PerformanceCounterDescriptor &order_by,
                     uint64_t max_count)
    : order_by(order_by), max_count(max_count) {
  }

  bool operator<(const OSDPerfMetricLimit &other) const {
    if (order_by != other.order_by) {
      return order_by < other.order_by;
    }
    return max_count < other.max_count;
  }

  DENC(OSDPerfMetricLimit, v, p) {
    DENC_START(1, 1, p);
    denc(v.order_by, p);
    denc(v.max_count, p);
    DENC_FINISH(p);
  }
};
WRITE_CLASS_DENC(OSDPerfMetricLimit)

std::ostream& operator<<(std::ostream& os, const OSDPerfMetricLimit &limit);

typedef std::set<OSDPerfMetricLimit> OSDPerfMetricLimits;

typedef int OSDPerfMetricQueryID;

struct OSDPerfMetricQuery {
  bool operator<(const OSDPerfMetricQuery &other) const {
    if (key_descriptor < other.key_descriptor) {
      return true;
    }
    if (key_descriptor > other.key_descriptor) {
      return false;
    }
    return (performance_counter_descriptors <
            other.performance_counter_descriptors);
  }

  OSDPerfMetricQuery() {
  }

  OSDPerfMetricQuery(
      const OSDPerfMetricKeyDescriptor &key_descriptor,
      const PerformanceCounterDescriptors &performance_counter_descriptors)
    : key_descriptor(key_descriptor),
      performance_counter_descriptors(performance_counter_descriptors) {
  }

  template <typename L>
  bool get_key(L&& get_sub_key, OSDPerfMetricKey *key) const {
    for (auto &sub_key_descriptor : key_descriptor) {
      OSDPerfMetricSubKey sub_key;
      if (!get_sub_key(sub_key_descriptor, &sub_key)) {
        return false;
      }
      key->push_back(sub_key);
    }
    return true;
  }

  DENC(OSDPerfMetricQuery, v, p) {
    DENC_START(1, 1, p);
    denc(v.key_descriptor, p);
    denc(v.performance_counter_descriptors, p);
    DENC_FINISH(p);
  }

  void get_performance_counter_descriptors(
      PerformanceCounterDescriptors *descriptors) const {
    *descriptors = performance_counter_descriptors;
  }

  template <typename L>
  void update_counters(L &&update_counter,
                       PerformanceCounters *counters) const {
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

  void pack_counters(const PerformanceCounters &counters, ceph::buffer::list *bl) const;

  OSDPerfMetricKeyDescriptor key_descriptor;
  PerformanceCounterDescriptors performance_counter_descriptors;
};
WRITE_CLASS_DENC(OSDPerfMetricQuery)

std::ostream& operator<<(std::ostream& os, const OSDPerfMetricQuery &query);

struct OSDPerfMetricReport {
  PerformanceCounterDescriptors performance_counter_descriptors;
  std::map<OSDPerfMetricKey, ceph::buffer::list> group_packed_performance_counters;

  DENC(OSDPerfMetricReport, v, p) {
    DENC_START(1, 1, p);
    denc(v.performance_counter_descriptors, p);
    denc(v.group_packed_performance_counters, p);
    DENC_FINISH(p);
  }
};
WRITE_CLASS_DENC(OSDPerfMetricReport)

#endif // OSD_PERF_METRIC_H_

