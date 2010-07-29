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

#include "include/types.h"
#include "Clock.h"

#include <string>
#include <fstream>
#include <vector>
using std::vector;
using std::string;
using std::ofstream;


#include "LogType.h"

extern void logger_reopen_all();
extern void logger_reset_all();
extern void logger_add(class Logger *l);
extern void logger_remove(class Logger *l);
extern void logger_tare(utime_t when);
extern void logger_start();

class Logger {
 protected:
  // my type
  string name, filename;
  LogType *type;

  bool need_open;
  bool need_reset;
  bool need_close;

  // values for this instance
  vector<int64_t> vals;
  vector<double> fvals;
  vector< vector<double> > vals_to_avg;  // for calculating variance

  ofstream out;

  // what i've written
  //int last_logged;
  int wrote_header_last;

  void _open_log();

 public:
  Logger(string n, LogType *t) :
    name(n), type(t),
    need_open(true), need_reset(false), need_close(false),
    vals(t->num_keys), fvals(t->num_keys), vals_to_avg(t->num_keys),
    wrote_header_last(10000) { }
  ~Logger();

  int64_t inc(int f, int64_t v = 1);
  int64_t set(int f, int64_t v);
  int64_t get(int f);

  double fset(int f, double v);
  double finc(int f, double v);
  double favg(int f, double v);

  void _flush();

  void reopen();
  void reset();
  void close();
};

#endif
