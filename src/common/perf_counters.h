// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 * Copyright (C) 2017 OVH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef CEPH_COMMON_PERF_COUNTERS_H
#define CEPH_COMMON_PERF_COUNTERS_H

#include <string>
#include <vector>
#include <memory>
#include <atomic>
#include <cstdint>

#include "common/perf_histogram.h"
#include "include/utime.h"
#include "include/common_fwd.h"
#include "common/ceph_mutex.h"
#include "common/ceph_time.h"

namespace TOPNSPC::common {
  class CephContext;
  class PerfCountersBuilder;
  class PerfCounters;
}

enum perfcounter_type_d : uint8_t
{
  PERFCOUNTER_NONE = 0,
  PERFCOUNTER_TIME = 0x1,       // float (measuring seconds)
  PERFCOUNTER_U64 = 0x2,        // integer (note: either TIME or U64 *must* be set)
  PERFCOUNTER_LONGRUNAVG = 0x4, // paired counter + sum (time)
  PERFCOUNTER_COUNTER = 0x8,    // counter (vs gauge)
  PERFCOUNTER_HISTOGRAM = 0x10, // histogram (vector) of values
};

enum unit_t : uint8_t
{
  UNIT_BYTES,
  UNIT_NONE
};

/* Class for constructing a PerfCounters object.
 *
 * This class performs some validation that the parameters we have supplied are
 * correct in create_perf_counters().
 *
 * In the future, we will probably get rid of the first/last arguments, since
 * PerfCountersBuilder can deduce them itself.
 */
namespace TOPNSPC::common {
class PerfCountersBuilder
{
public:
  PerfCountersBuilder(CephContext *cct, const std::string &name,
		    int first, int last);
  ~PerfCountersBuilder();

  // prio values: higher is better, and higher values get included in
  // 'ceph daemonperf' (and similar) results.
  // Use of priorities enables us to add large numbers of counters
  // internally without necessarily overwhelming consumers.
  enum {
    PRIO_CRITICAL = 10,
    // 'interesting' is the default threshold for `daemonperf` output
    PRIO_INTERESTING = 8,
    // `useful` is the default threshold for transmission to ceph-mgr
    // and inclusion in prometheus/influxdb plugin output
    PRIO_USEFUL = 5,
    PRIO_UNINTERESTING = 2,
    PRIO_DEBUGONLY = 0,
  };
  void add_u64(int key, const char *name,
	       const char *description=NULL, const char *nick = NULL,
	       int prio=0, int unit=UNIT_NONE);
  void add_u64_counter(int key, const char *name,
		       const char *description=NULL,
		       const char *nick = NULL,
		       int prio=0, int unit=UNIT_NONE);
  void add_u64_avg(int key, const char *name,
		   const char *description=NULL,
		   const char *nick = NULL,
		   int prio=0, int unit=UNIT_NONE);
  void add_time(int key, const char *name,
		const char *description=NULL,
		const char *nick = NULL,
		int prio=0);
  void add_time_avg(int key, const char *name,
		    const char *description=NULL,
		    const char *nick = NULL,
		    int prio=0);
  void add_u64_counter_histogram(
    int key, const char* name,
    PerfHistogramCommon::axis_config_d x_axis_config,
    PerfHistogramCommon::axis_config_d y_axis_config,
    const char *description=NULL,
    const char* nick = NULL,
    int prio=0, int unit=UNIT_NONE);

  void set_prio_default(int prio_)
  {
    prio_default = prio_;
  }

  PerfCounters* create_perf_counters();
private:
  PerfCountersBuilder(const PerfCountersBuilder &rhs);
  PerfCountersBuilder& operator=(const PerfCountersBuilder &rhs);
  void add_impl(int idx, const char *name,
                const char *description, const char *nick, int prio, int ty, int unit=UNIT_NONE,
                std::unique_ptr<PerfHistogram<>> histogram = nullptr);

  PerfCounters *m_perf_counters;

  int prio_default = 0;
};

/*
 * A PerfCounters object is usually associated with a single subsystem.
 * It contains counters which we modify to track performance and throughput
 * over time. 
 *
 * PerfCounters can track several different types of values:
 * 1) integer values & counters
 * 2) floating-point values & counters
 * 3) floating-point averages
 * 4) 2D histograms of quantized value pairs
 *
 * The difference between values, counters and histograms is in how they are initialized
 * and accessed. For a counter, use the inc(counter, amount) function (note
 * that amount defaults to 1 if you don't set it). For a value, use the
 * set(index, value) function. For histogram use the hinc(value1, value2) function.
 * (For time, use the tinc and tset variants.)
 *
 * If for some reason you would like to reset your counters, you can do so using
 * the set functions even if they are counters, and you can also
 * increment your values if for some reason you wish to.
 *
 * For the time average, it returns the current value and
 * the "avgcount" member when read off. avgcount is incremented when you call
 * tinc. Calling tset on an average is an error and will assert out.
 */
class PerfCounters
{
public:
  /** Represents a PerfCounters data element. */
  struct perf_counter_data_any_d {
    perf_counter_data_any_d()
      : name(NULL),
        description(NULL),
        nick(NULL),
	 type(PERFCOUNTER_NONE),
	 unit(UNIT_NONE)
    {}
    perf_counter_data_any_d(const perf_counter_data_any_d& other)
      : name(other.name),
        description(other.description),
        nick(other.nick),
	 type(other.type),
	 unit(other.unit),
	 u64(other.u64.load()) {
      auto a = other.read_avg();
      u64 = a.first;
      avgcount = a.second;
      avgcount2 = a.second;
      if (other.histogram) {
        histogram.reset(new PerfHistogram<>(*other.histogram));
      }
    }

    const char *name;
    const char *description;
    const char *nick;
    uint8_t prio = 0;
    enum perfcounter_type_d type;
    enum unit_t unit;
    std::atomic<uint64_t> u64 = { 0 };
    std::atomic<uint64_t> avgcount = { 0 };
    std::atomic<uint64_t> avgcount2 = { 0 };
    std::unique_ptr<PerfHistogram<>> histogram;

    void reset()
    {
      if (type != PERFCOUNTER_U64) {
	    u64 = 0;
	    avgcount = 0;
	    avgcount2 = 0;
      }
      if (histogram) {
        histogram->reset();
      }
    }

    // read <sum, count> safely by making sure the post- and pre-count
    // are identical; in other words the whole loop needs to be run
    // without any intervening calls to inc, set, or tinc.
    std::pair<uint64_t,uint64_t> read_avg() const {
      uint64_t sum, count;
      do {
	count = avgcount2;
	sum = u64;
      } while (avgcount != count);
      return { sum, count };
    }
  };

  template <typename T>
  struct avg_tracker {
    std::pair<uint64_t, T> last;
    std::pair<uint64_t, T> cur;
    avg_tracker() : last(0, 0), cur(0, 0) {}
    T current_avg() const {
      if (cur.first == last.first)
        return 0;
      return (cur.second - last.second) / (cur.first - last.first);
    }
    void consume_next(const std::pair<uint64_t, T> &next) {
      last = cur;
      cur = next;
    }
  };

  ~PerfCounters();

  void inc(int idx, uint64_t v = 1);
  void dec(int idx, uint64_t v = 1);
  void set(int idx, uint64_t v);
  uint64_t get(int idx) const;

  void tset(int idx, utime_t v);
  void tset(int idx, ceph::timespan v);
  void tinc(int idx, utime_t v);
  void tinc(int idx, ceph::timespan v);
  utime_t tget(int idx) const;

  void hinc(int idx, int64_t x, int64_t y);

  void reset();
  void dump_formatted(ceph::Formatter *f, bool schema, bool dump_labeled,
                      const std::string &counter = "") const {
    dump_formatted_generic(f, schema, false, dump_labeled, counter);
  }
  void dump_formatted_histograms(ceph::Formatter *f, bool schema,
                                 const std::string &counter = "") const {
    dump_formatted_generic(f, schema, true, false, counter);
  }
  std::pair<uint64_t, uint64_t> get_tavg_ns(int idx) const;

  const std::string& get_name() const;
  void set_name(std::string s) {
    m_name = s;
  }

  /// adjust priority values by some value
  void set_prio_adjust(int p) {
    prio_adjust = p;
  }

  int get_adjusted_priority(int p) const {
    return std::max(std::min(p + prio_adjust,
                             (int)PerfCountersBuilder::PRIO_CRITICAL),
                    0);
  }

private:
  PerfCounters(CephContext *cct, const std::string &name,
	     int lower_bound, int upper_bound);
  PerfCounters(const PerfCounters &rhs);
  PerfCounters& operator=(const PerfCounters &rhs);
  void dump_formatted_generic(ceph::Formatter *f, bool schema, bool histograms,
                              bool dump_labeled,
                              const std::string &counter = "") const;

  typedef std::vector<perf_counter_data_any_d> perf_counter_data_vec_t;

  CephContext *m_cct;
  int m_lower_bound;
  int m_upper_bound;
  std::string m_name;

  int prio_adjust = 0;

#if !defined(WITH_SEASTAR) || defined(WITH_ALIEN)
  const std::string m_lock_name;
  /** Protects m_data */
  ceph::mutex m_lock;
#endif

  perf_counter_data_vec_t m_data;

  friend class PerfCountersBuilder;
  friend class PerfCountersCollectionImpl;
};

class SortPerfCountersByName {
public:
  bool operator()(const PerfCounters* lhs, const PerfCounters* rhs) const {
    return (lhs->get_name() < rhs->get_name());
  }
};

typedef std::set <PerfCounters*, SortPerfCountersByName> perf_counters_set_t;

/*
 * PerfCountersCollectionImp manages PerfCounters objects for a Ceph process.
 */
class PerfCountersCollectionImpl
{
public:
  PerfCountersCollectionImpl();
  ~PerfCountersCollectionImpl();
  void add(PerfCounters *l);
  void remove(PerfCounters *l);
  void clear();
  bool reset(const std::string &name);

  void dump_formatted(ceph::Formatter *f, bool schema, bool dump_labeled,
                      const std::string &logger = "",
                      const std::string &counter = "") const {
    dump_formatted_generic(f, schema, false, dump_labeled, logger, counter);
  }

  void dump_formatted_histograms(ceph::Formatter *f, bool schema,
                                 const std::string &logger = "",
                                 const std::string &counter = "") const {
    dump_formatted_generic(f, schema, true, false, logger, counter);
  }

  // A reference to a perf_counter_data_any_d, with an accompanying
  // pointer to the enclosing PerfCounters, in order that the consumer
  // can see the prio_adjust
  class PerfCounterRef
  {
    public:
    PerfCounters::perf_counter_data_any_d *data;
    PerfCounters *perf_counters;
  };
  typedef std::map<std::string,
          PerfCounterRef> CounterMap;

  void with_counters(std::function<void(const CounterMap &)>) const;

private:
  void dump_formatted_generic(ceph::Formatter *f, bool schema, bool histograms,
                              bool dump_labeled,
                              const std::string &logger = "",
                              const std::string &counter = "") const;

  perf_counters_set_t m_loggers;

  CounterMap by_path; 
};


class PerfGuard {
  const ceph::real_clock::time_point start;
  PerfCounters* const counters;
  const int event;

public:
  PerfGuard(PerfCounters* const counters,
            const int event)
  : start(ceph::real_clock::now()),
    counters(counters),
    event(event) {
  }

  ~PerfGuard() {
    counters->tinc(event, ceph::real_clock::now() - start);
  }
};

}
#endif
