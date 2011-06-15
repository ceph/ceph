// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef CEPH_LOGGER_H
#define CEPH_LOGGER_H

#include "common/config.h"
#include "common/Clock.h"
#include "common/ProfLogType.h"
#include "common/Timer.h"
#include "include/types.h"

#include <string>
#include <fstream>
#include <vector>

class ProfLogger;

class ProfLoggerCollection
{
public:
  ProfLoggerCollection(CephContext *cct_);
  ~ProfLoggerCollection();
  void logger_reopen_all();
  void logger_reset_all();
  void logger_add(class ProfLogger *l);
  void logger_remove(class ProfLogger *l);
  void flush_all_loggers();
  void logger_tare(utime_t when);
  void logger_start();
private:
  Mutex lock; // big lock.  lame, but this way I protect ProfLogType too!
  SafeTimer logger_timer;
  Context *logger_event;
  list<ProfLogger*> logger_list;
  utime_t start;
  int last_flush; // in seconds since start
  bool need_reopen;
  bool need_reset;
  CephContext *cct;
};

class ProfLoggerConfObs : public md_config_obs_t {
public:
  ProfLoggerConfObs(ProfLoggerCollection *coll_);
  ~ProfLoggerConfObs();
  virtual const char** get_tracked_conf_keys() const;
  virtual void handle_conf_change(const md_config_t *conf,
			  const std::set <std::string> &changed);
private:
  ProfLoggerCollection *coll;
};

class ProfLogger {
 protected:
   CephContext *cct;
  // my type
  std::string name, filename;
  ProfLogType *type;

  bool need_open;
  bool need_reset;
  bool need_close;

  // values for this instance
  std::vector<int64_t> vals;
  std::vector<double> fvals;
  std::vector< std::vector<double> > vals_to_avg;  // for calculating variance

  std::ofstream out;

  // what i've written
  //int last_logged;
  int wrote_header_last;

  void _open_log();

 private:
  Mutex *lock;

 public:
  ProfLogger(CephContext *cct_, const std::string &n, ProfLogType *t) :
    cct(cct_), name(n), type(t),
    need_open(true), need_reset(false), need_close(false),
    vals(t->num_keys), fvals(t->num_keys), vals_to_avg(t->num_keys),
    wrote_header_last(10000), lock(NULL) { }
  ~ProfLogger();

  int64_t inc(int f, int64_t v = 1);
  int64_t set(int f, int64_t v);
  int64_t get(int f);

  double fset(int f, double v);
  double finc(int f, double v);
  double favg(int f, double v);

  void _flush(bool need_reopen, bool need_reset, int last_flush);

  void reopen();
  void reset();
  void close();

  friend class ProfLoggerCollection;
};

#endif
