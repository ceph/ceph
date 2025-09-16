// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MGR_MDS_PERF_METRIC_TYPES_H
#define CEPH_MGR_MDS_PERF_METRIC_TYPES_H

#include <regex>
#include <vector>
#include <iosfwd>

#include "include/cephfs/types.h" // for mds_rank_t
#include "include/denc.h"
#include "include/stringify.h"
#include "include/utime.h"
#include "common/Formatter.h"

#include "mds/mdstypes.h"
#include "mgr/Types.h"

typedef std::vector<std::string> MDSPerfMetricSubKey; // array of regex match
typedef std::vector<MDSPerfMetricSubKey> MDSPerfMetricKey;

enum class MDSPerfMetricSubKeyType : uint8_t {
  MDS_RANK = 0,
  CLIENT_ID = 1,
  SUBVOLUME_PATH = 2,
};

struct MDSPerfMetricSubKeyDescriptor {
  MDSPerfMetricSubKeyType type = static_cast<MDSPerfMetricSubKeyType>(-1);
  std::string regex_str;
  std::regex regex;

  bool is_supported() const {
    switch (type) {
    case MDSPerfMetricSubKeyType::MDS_RANK:
    case MDSPerfMetricSubKeyType::CLIENT_ID:
    case MDSPerfMetricSubKeyType::SUBVOLUME_PATH:
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
  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("type", static_cast<uint8_t>(type));
    f->dump_string("regex_str", regex_str);
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
      p += per * size;
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
  DENTRY_LEASE_METRIC = 4,
  OPENED_FILES_METRIC = 5,
  PINNED_ICAPS_METRIC = 6,
  OPENED_INODES_METRIC = 7,
  READ_IO_SIZES_METRIC = 8,
  WRITE_IO_SIZES_METRIC = 9,
  AVG_READ_LATENCY_METRIC = 10,
  STDEV_READ_LATENCY_METRIC = 11,
  AVG_WRITE_LATENCY_METRIC = 12,
  STDEV_WRITE_LATENCY_METRIC = 13,
  AVG_METADATA_LATENCY_METRIC = 14,
  STDEV_METADATA_LATENCY_METRIC = 15,
  SUBV_READ_IOPS_METRIC = 16,
  SUBV_WRITE_IOPS_METRIC = 17,
  SUBV_READ_THROUGHPUT_METRIC = 18,
  SUBV_WRITE_THROUGHPUT_METRIC = 19,
  SUBV_AVG_READ_LATENCY_METRIC = 20,
  SUBV_AVG_WRITE_LATENCY_METRIC = 21
};

struct MDSPerformanceCounterDescriptor {
  MDSPerformanceCounterType type = static_cast<MDSPerformanceCounterType>(-1);

  bool is_supported() const {
    switch(type) {
    case MDSPerformanceCounterType::CAP_HIT_METRIC:
    case MDSPerformanceCounterType::READ_LATENCY_METRIC:
    case MDSPerformanceCounterType::WRITE_LATENCY_METRIC:
    case MDSPerformanceCounterType::METADATA_LATENCY_METRIC:
    case MDSPerformanceCounterType::DENTRY_LEASE_METRIC:
    case MDSPerformanceCounterType::OPENED_FILES_METRIC:
    case MDSPerformanceCounterType::PINNED_ICAPS_METRIC:
    case MDSPerformanceCounterType::OPENED_INODES_METRIC:
    case MDSPerformanceCounterType::READ_IO_SIZES_METRIC:
    case MDSPerformanceCounterType::WRITE_IO_SIZES_METRIC:
    case MDSPerformanceCounterType::AVG_READ_LATENCY_METRIC:
    case MDSPerformanceCounterType::STDEV_READ_LATENCY_METRIC:
    case MDSPerformanceCounterType::AVG_WRITE_LATENCY_METRIC:
    case MDSPerformanceCounterType::STDEV_WRITE_LATENCY_METRIC:
    case MDSPerformanceCounterType::AVG_METADATA_LATENCY_METRIC:
    case MDSPerformanceCounterType::STDEV_METADATA_LATENCY_METRIC:
    case MDSPerformanceCounterType::SUBV_READ_IOPS_METRIC:
    case MDSPerformanceCounterType::SUBV_WRITE_IOPS_METRIC:
    case MDSPerformanceCounterType::SUBV_READ_THROUGHPUT_METRIC:
    case MDSPerformanceCounterType::SUBV_WRITE_THROUGHPUT_METRIC:
    case MDSPerformanceCounterType::SUBV_AVG_READ_LATENCY_METRIC:
    case MDSPerformanceCounterType::SUBV_AVG_WRITE_LATENCY_METRIC:
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
  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("type", static_cast<uint8_t>(type));
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
      p += per * size;
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
  void dump(ceph::Formatter *f) const {
    f->dump_object("order_by", order_by);
    f->dump_unsigned("max_count", max_count);
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

  void dump(ceph::Formatter *f) const {
    f->dump_stream("key_descriptor") << key_descriptor;
    f->dump_stream("performance_counter_descriptors") << performance_counter_descriptors;
  }

  void pack_counters(const PerformanceCounters &counters, ceph::buffer::list *bl) const;
};
WRITE_CLASS_DENC(MDSPerfMetricQuery)

std::ostream &operator<<(std::ostream &os, const MDSPerfMetricQuery &query);

struct MDSPerfCollector : PerfCollector {
  std::map<MDSPerfMetricKey, PerformanceCounters> counters;
  std::set<mds_rank_t> delayed_ranks;
  utime_t last_updated_mono;

  MDSPerfCollector(MetricQueryID query_id)
      : PerfCollector(query_id) {
  }
};

struct MDSPerfMetrics {
  MDSPerformanceCounterDescriptors performance_counter_descriptors;
  std::map<MDSPerfMetricKey, ceph::buffer::list> group_packed_performance_counters;

  DENC(MDSPerfMetrics, v, p) {
    DENC_START(1, 1, p);
    denc(v.performance_counter_descriptors, p);
    denc(v.group_packed_performance_counters, p);
    DENC_FINISH(p);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_stream("performance_counter_descriptors") << performance_counter_descriptors;
    f->open_array_section("group_packed_performance_counters");
    for (auto &i : group_packed_performance_counters) {
      f->dump_stream("key") << i.first;
      f->dump_stream("value") << i.second;
    }
    f->close_section();
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
  void dump(ceph::Formatter *f) const {
    f->open_array_section("reports");
    for (auto &i : reports) {
      f->open_object_section("query");
      f->dump_object("query",i.first);
      f->close_section();
      f->open_object_section("metrics");
      f->dump_object("metrics",i.second);
      f->close_section();
    }
    f->close_section();
  }
  static std::list<MDSPerfMetricReport> generate_test_instances() {
    std::list<MDSPerfMetricReport> o;
    o.emplace_back();
    o.emplace_back();
    o.back().reports.emplace(MDSPerfMetricQuery(), MDSPerfMetrics());
    o.back().rank_metrics_delayed.insert(1);
    return o;
  }
};

// all latencies are converted to millisec during the aggregation
struct AggregatedSubvolumeMetric {
    std::string subvolume_path;

    uint64_t read_iops = 0;
    uint64_t write_iops = 0;
    uint64_t read_tpBs = 0;
    uint64_t write_tBps = 0;

    uint64_t min_read_latency = std::numeric_limits<uint64_t>::max();
    uint64_t max_read_latency = 0;
    uint64_t avg_read_latency = 0;

    uint64_t min_write_latency = std::numeric_limits<uint64_t>::max();
    uint64_t max_write_latency = 0;
    uint64_t avg_write_latency = 0;

    uint64_t time_window_last_end_sec = 0;
    uint64_t time_window_last_dur_sec = 0;

    void dump(ceph::Formatter* f) const {
      f->dump_string("subvolume_path", subvolume_path);
      f->dump_unsigned("read_iops", read_iops);
      f->dump_unsigned("write_iops", write_iops);
      f->dump_unsigned("read_tpBs", read_tpBs);
      f->dump_unsigned("write_tBps", write_tBps);

      f->dump_unsigned("min_read_latency_ns", min_read_latency);
      f->dump_unsigned("max_read_latency_ns", max_read_latency);
      f->dump_unsigned("avg_read_latency_ns", avg_read_latency);

      f->dump_unsigned("min_write_latency_ns", min_write_latency);
      f->dump_unsigned("max_write_latency_ns", max_write_latency);
      f->dump_unsigned("avg_write_latency_ns", avg_write_latency);

      f->dump_unsigned("time_window_sec_end", time_window_last_end_sec);
      f->dump_unsigned("time_window_sec_dur", time_window_last_dur_sec);
    }
};

using TimePoint = std::chrono::steady_clock::time_point;
using Duration  = std::chrono::steady_clock::duration;

template <typename T>
struct DataPoint {
    TimePoint timestamp;
    T value;
};

/**
* @brief Holds a collection of I/O performance metrics for a specific storage subvolume.
*
* Simple sliding window to hold values for some period of time, allows to iterate over values
* to calculate whatever is needed.
* See for_each_value usage in the MetricsHandler.cc
*/
template <typename T>
class SlidingWindowTracker {
public:
    explicit SlidingWindowTracker(uint64_t window_duration_seconds)
            : window_duration(std::chrono::seconds(window_duration_seconds))
    {}

    void add_value(const T& value, TimePoint timestamp = std::chrono::steady_clock::now()) {
      std::unique_lock lock(data_lock);
      data_points.push_back({timestamp, value});
    }

    // prune old data
    void update() {
      std::unique_lock lock(data_lock);
      prune_old_data(std::chrono::steady_clock::now());
    }

    // Call function on each value in window
    template <typename Fn>
    void for_each_value(Fn&& fn) const {
      std::shared_lock lock(data_lock);
      for (const auto& dp : data_points) {
        fn(dp.value);
      }
    }

    uint64_t get_current_window_duration_sec() const {
      std::shared_lock lock(data_lock);
      if (data_points.size() < 2) {
        return 0;
      }
      auto duration = data_points.back().timestamp - data_points.front().timestamp;
      return std::chrono::duration_cast<std::chrono::seconds>(duration).count();
    }

    bool is_empty() const {
      std::shared_lock lock(data_lock);
      return data_points.empty();
    }

   uint64_t get_time_from_last_sample() const {
    if (data_points.empty()) {
      return std::numeric_limits<uint64_t>::max();
    }
    auto duration = std::chrono::steady_clock::now() - data_points.back().timestamp;
    return std::chrono::duration_cast<std::chrono::seconds>(duration).count();
   }
 private:
    void prune_old_data(TimePoint now) {
      TimePoint window_start = now - window_duration;
      while (!data_points.empty() && data_points.front().timestamp < window_start) {
        data_points.pop_front();
      }
    }

    mutable std::shared_mutex data_lock;
    std::deque<DataPoint<T>> data_points;
    Duration window_duration;
};

WRITE_CLASS_DENC(MDSPerfMetrics)
WRITE_CLASS_DENC(MDSPerfMetricReport)

#endif // CEPH_MGR_MDS_PERF_METRIC_TYPES_H
