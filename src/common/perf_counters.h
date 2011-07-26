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
 * This object is thread-safe. However, it is better to avoid sharing
 * a PerfCounters object between multiple threads to avoid cacheline ping-pong.
 */
class PerfCounters
{
public:
  ~PerfCounters();

  void inc(int idx, uint64_t v = 1);
  void set(int idx, uint64_t v);
  uint64_t get(int idx) const;

  void fset(int idx, double v);
  void finc(int idx, double v);
  double fget(int idx) const;

  void write_json_to_buf(std::vector <char> &buffer, bool schema);

  const std::string& get_name() const;

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
  const std::string m_name;
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
  void logger_add(class PerfCounters *l);
  void logger_remove(class PerfCounters *l);
  void logger_clear();
  void write_json_to_buf(std::vector <char> &buffer, bool schema);
private:
  bool init(const std::string &uri);
  void shutdown();

  CephContext *m_cct;

  /** Protects m_loggers */
  mutable Mutex m_lock;

  int m_shutdown_fd;
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
