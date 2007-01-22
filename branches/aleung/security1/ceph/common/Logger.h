// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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
#include "Mutex.h"

#include <string>
#include <fstream>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;

#include "LogType.h"




class Logger {
 protected:
  //hash_map<const char*, long, hash<const char*>, eqstr> vals;
  //hash_map<const char*, double, hash<const char*>, eqstr> fvals;
  vector<long> vals;
  vector<double> fvals;

  //Mutex lock;
  LogType *type;

  utime_t start;
  int last_logged;
  int interval;
  int wrote_header;
  int wrote_header_last;

  string filename;

  int version;

  ofstream out;
  bool open;

 public:
  Logger(string fn, LogType *type);
  ~Logger();

  void set_start(const utime_t& a) { start = a; }
  utime_t& get_start() { return start; }

  long inc(const char *s, long v = 1);
  long set(const char *s, long v);
  long get(const char *s);

  double fset(const char *s, double v);
  double finc(const char *s, double v);

  void flush(bool force = false);
};

#endif
