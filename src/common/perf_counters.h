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
class ProfLoggerBuilder;
class ProfLoggerCollectionTest;

/*
 * A ProfLogger is usually associated with a single subsystem.
 * It contains counters which we modify to track performance and throughput
 * over time. 
 *
 * ProfLogger is thread-safe. However, it is better to avoid sharing
 * ProfLoggers between multiple threads to avoid cacheline ping-pong.
 */
class ProfLogger
{
public:
  ~ProfLogger();

  void inc(int idx, uint64_t v = 1);
  void set(int idx, uint64_t v);
  uint64_t get(int idx) const;

  void fset(int idx, double v);
  void finc(int idx, double v);
  double fget(int idx) const;

  void write_json_to_buf(std::vector <char> &buffer);

  const std::string& get_name() const;

private:
  ProfLogger(CephContext *cct, const std::string &name,
	     int lower_bound, int upper_bound);
  ProfLogger(const ProfLogger &rhs);
  ProfLogger& operator=(const ProfLogger &rhs);

  /** Represents a ProfLogger data element. */
  struct prof_log_data_any_d {
    prof_log_data_any_d();
    const char *name;
    int type;
    union {
      uint64_t u64;
      double dbl;
    } u;
    uint64_t count;
  };
  typedef std::vector<prof_log_data_any_d> prof_log_data_vec_t;

  CephContext *m_cct;
  int m_lower_bound;
  int m_upper_bound;
  const std::string m_name;
  const std::string m_lock_name;

  /** Protects m_data */
  mutable Mutex m_lock;

  prof_log_data_vec_t m_data;

  friend class ProfLoggerBuilder;
};

class SortProfLoggersByName {
public:
  bool operator()(const ProfLogger* lhs, const ProfLogger* rhs) const {
    return (lhs->get_name() < rhs->get_name());
  }
};

typedef std::set <ProfLogger*, SortProfLoggersByName> prof_logger_set_t;

/*
 * ProfLoggerCollection manages the set of ProfLoggers for a Ceph process.
 */
class ProfLoggerCollection
{
public:
  ProfLoggerCollection(CephContext *cct);
  ~ProfLoggerCollection();
  void logger_add(class ProfLogger *l);
  void logger_remove(class ProfLogger *l);
  void logger_clear();
  void write_json_to_buf(std::vector <char> &buffer);
private:
  bool init(const std::string &uri);
  void shutdown();

  CephContext *m_cct;

  /** Protects m_loggers */
  mutable Mutex m_lock;

  int m_shutdown_fd;
  prof_logger_set_t m_loggers;

  friend class ProfLoggerCollectionTest;
};

/* Class for constructing ProfLoggers.
 *
 * This class peforms some validation that the parameters we have supplied are
 * correct in create_proflogger().
 *
 * In the future, we will probably get rid of the first/last arguments, since
 * ProfLoggerBuilder can deduce them itself.
 */
class ProfLoggerBuilder
{
public:
  ProfLoggerBuilder(CephContext *cct, const std::string &name,
		    int first, int last);
  ~ProfLoggerBuilder();
  void add_u64(int key, const char *name);
  void add_fl(int key, const char *name);
  void add_fl_avg(int key, const char *name);
  ProfLogger* create_proflogger();
private:
  ProfLoggerBuilder(const ProfLoggerBuilder &rhs);
  ProfLoggerBuilder& operator=(const ProfLoggerBuilder &rhs);
  void add_impl(int idx, const char *name, int ty, uint64_t count);

  ProfLogger *m_prof_logger;
};

#endif
