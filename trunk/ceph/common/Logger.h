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
  // values for this instance
  vector<long> vals;
  vector<double> fvals;
  vector< vector<double> > vals_to_avg;

  void maybe_resize(unsigned s) {
    while (s >= vals.size()) {
      vals.push_back(0);
      fvals.push_back(0.0);
      vals_to_avg.push_back(vector<double>());
    }
  }

  // my type
  LogType *type;
  int version;

  string filename;
  ofstream out;

  // what i've written
  //int last_logged;
  int wrote_header;
  int wrote_header_last;

 public:
  Logger(string fn, LogType *type, bool append=false);
  ~Logger();

  long inc(const char *s, long v = 1);
  long set(const char *s, long v);
  long get(const char *s);

  double fset(const char *s, double v);
  double finc(const char *s, double v);
  double favg(const char *s, double v);

  //void flush();
  void _flush();

  void set_start(utime_t s);
};

#endif
