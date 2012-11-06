// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef CEPH_COMMON_PERF_COUNTERS_H
#define CEPH_COMMON_PERF_COUNTERS_H

#include "common/config_obs.h"
#include "common/Mutex.h"
#include "include/buffer.h"

#include <stdint.h>
#include <string>
#include <vector>

class CephContext;
class PerfCountersBuilder;
class PerfCountersCollectionTest;

enum perfcounter_type_d
{
  PERFCOUNTER_NONE = 0,
  PERFCOUNTER_FLOAT = 0x1,
  PERFCOUNTER_U64 = 0x2,
  PERFCOUNTER_LONGRUNAVG = 0x4,
  PERFCOUNTER_COUNTER = 0x8,
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
 *
 * The difference between values and counters is in how they are initialized
 * and accessed. For a counter, use the inc(counter, amount) function (note
 * that amount defaults to 1 if you don't set it). For a value, use the
 * set(index, value) function.
 * (For floats, use the finc and fset variants.)
 *
 * If for some reason you would like to reset your counters, you can do so using
 * the set functions even if they are counters, and you can also
 * increment your values if for some reason you wish to.
 *
 * For the floating-point average, it returns the current value and
 * the "avgcount" member when read off. avgcount is incremented when you call
 * finc. Calling fset on an average is an error and will assert out.
 */
class PerfCounters
{
public:
  ~PerfCounters();

  void inc(int idx, uint64_t v = 1);
  void dec(int idx, uint64_t v = 1);
  void set(int idx, uint64_t v);
  uint64_t get(int idx) const;

  void fset(int idx, double v);
  void finc(int idx, double v);
  double fget(int idx) const;

  void write_json_to_buf(ceph::bufferlist& bl, bool schema);

  const std::string& get_name() const;
  void set_name(std::string s) {
    m_name = s;
  }

private:
  PerfCounters(CephContext *cct, const std::string &name,
	     int lower_bound, int upper_bound);
  PerfCounters(const PerfCounters &rhs);
  PerfCounters& operator=(const PerfCounters &rhs);

  /** Represents a PerfCounters data element. */
  struct perf_counter_data_any_d {
    perf_counter_data_any_d();
    void write_schema_json(char *buf, size_t buf_sz) const;
    void  write_json(char *buf, size_t buf_sz) const;

    const char *name;
    enum perfcounter_type_d type;
    union {
      uint64_t u64;
      double dbl;
    } u;
    uint64_t avgcount;
  };
  typedef std::vector<perf_counter_data_any_d> perf_counter_data_vec_t;

  CephContext *m_cct;
  int m_lower_bound;
  int m_upper_bound;
  std::string m_name;
  const std::string m_lock_name;

  /** Protects m_data */
  mutable Mutex m_lock;

  perf_counter_data_vec_t m_data;

  friend class PerfCountersBuilder;
};

class SortPerfCountersByName {
public:
  bool operator()(const PerfCounters* lhs, const PerfCounters* rhs) const {
    return (lhs->get_name() < rhs->get_name());
  }
};

typedef std::set <PerfCounters*, SortPerfCountersByName> perf_counters_set_t;

/*
 * PerfCountersCollection manages PerfCounters objects for a Ceph process.
 */
class PerfCountersCollection
{
public:
  PerfCountersCollection(CephContext *cct);
  ~PerfCountersCollection();
  void add(class PerfCounters *l);
  void remove(class PerfCounters *l);
  void clear();
  void write_json_to_buf(ceph::bufferlist& bl, bool schema);
private:
  CephContext *m_cct;

  /** Protects m_loggers */
  mutable Mutex m_lock;

  perf_counters_set_t m_loggers;

  friend class PerfCountersCollectionTest;
};

/* Class for constructing a PerfCounters object.
 *
 * This class peforms some validation that the parameters we have supplied are
 * correct in create_perf_counters().
 *
 * In the future, we will probably get rid of the first/last arguments, since
 * PerfCountersBuilder can deduce them itself.
 */
class PerfCountersBuilder
{
public:
  PerfCountersBuilder(CephContext *cct, const std::string &name,
		    int first, int last);
  ~PerfCountersBuilder();
  void add_u64(int key, const char *name);
  void add_u64_counter(int key, const char *name);
  void add_u64_avg(int key, const char *name);
  void add_fl(int key, const char *name);
  void add_fl_avg(int key, const char *name);
  PerfCounters* create_perf_counters();
private:
  PerfCountersBuilder(const PerfCountersBuilder &rhs);
  PerfCountersBuilder& operator=(const PerfCountersBuilder &rhs);
  void add_impl(int idx, const char *name, int ty);

  PerfCounters *m_perf_counters;
};

#endif
