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


#ifndef __LOGGER_H
#define __LOGGER_H

#include "include/types.h"
#include "Clock.h"

#include <string>
#include <fstream>
#include <vector>
using std::vector;
using std::string;
using std::ofstream;


#include "LogType.h"


class Logger {
 protected:
  // my type
  string name, filename;
  bool append;
  LogType *type;
  bool open;

  // values for this instance
  vector<__s64> vals;
  vector<double> fvals;
  vector< vector<double> > vals_to_avg;  // for calculating variance

  ofstream out;

  // what i've written
  //int last_logged;
  int wrote_header_last;

 public:
  Logger(string n, LogType *t, bool ap=false) :
    name(n), append(ap), type(t), open(false),
    vals(t->num_keys), fvals(t->num_keys), vals_to_avg(t->num_keys),
    wrote_header_last(10000) {
    _open_log();
  }
  ~Logger();

  void _open_log();

  __s64 inc(int f, __s64 v = 1);
  __s64 set(int f, __s64 v);
  __s64 get(int f);

  double fset(int f, double v);
  double finc(int f, double v);
  double favg(int f, double v);

  //void flush();
  void _flush(bool reset=true);

  void set_start(utime_t s);
};

#endif
