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


#ifndef CEPH_PROF_LOG_H
#define CEPH_PROF_LOG_H

#include "common/config_obs.h"
#include "common/Mutex.h"

#include <stdint.h>
#include <string>
#include <vector>

class ProfLoggerBuilder;
class ProfLoggerCollectionTest;
class CephContext;
class Thread;

/*
 * ProfLog manages the profiler logging for a Ceph process.
 */
class ProfLoggerCollection : public md_config_obs_t
{
public:
  ProfLoggerCollection(CephContext *cct);
  ~ProfLoggerCollection();
  virtual const char** get_tracked_conf_keys() const;
  virtual void handle_conf_change(const md_config_t *conf,
			  const std::set <std::string> &changed);
  void logger_add(class ProfLogger *l);
  void logger_remove(class ProfLogger *l);
private:
  bool init(const std::string &uri);
  void shutdown();

  CephContext *m_cct;
  Thread* m_thread;

  /** Protects m_loggers */
  Mutex m_lock;

  int m_shutdown_fd;
  std::set <ProfLogger*> m_loggers;

  friend class ProfLogThread;
  friend class ProfLoggerCollectionTest;
};

class ProfLogger
{
public:
  ~ProfLogger();

  void inc(int idx, uint64_t v = 1);
  void set(int idx, uint64_t v);
  uint64_t get(int idx);

  void fset(int idx, double v);
  void finc(int idx, double v);
  double fget(int idx);

  void write_json_to_fp(FILE *fp);

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

  /** Protects m_data */
  Mutex m_lock;

  prof_log_data_vec_t m_data;

  friend class ProfLoggerBuilder;
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
