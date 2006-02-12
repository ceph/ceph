// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
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
  hash_map<const char*, long, hash<const char*>, eqstr> vals;
  hash_map<const char*, double, hash<const char*>, eqstr> fvals;

  //Mutex lock;
  LogType *type;

  utime_t start;
  int last_logged;
  int interval;
  int wrote_header;
  int wrote_header_last;

  string filename;

  ofstream out;
  bool open;

 public:
  Logger(string fn, LogType *type);
  ~Logger();

  long inc(const char *s, long v = 1);
  long set(const char *s, long v);
  long get(const char *s);

  double fset(const char *s, double v);
  double finc(const char *s, double v);
  
  void flush(bool force = false);
};

#endif
