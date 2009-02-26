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


#ifndef __LOGTYPE_H
#define __LOGTYPE_H

#include "include/types.h"

#include <vector>
using std::vector;

class LogType {
 protected:
  int first_key, num_keys;
  vector<const char*> key_name;
  vector<bool> inc_keys, avg_keys;

  friend class Logger;

 public:
  LogType(int first, int tail) :
    first_key(first), num_keys(tail-first),
    key_name(num_keys), inc_keys(num_keys), avg_keys(num_keys) {
    for (int i=0; i<num_keys; i++) {
      key_name[i] = 0;
      inc_keys[i] = 0;
      avg_keys[i] = 0;
    }
  }
  int lookup_key(int key, bool isnew=false) {
    int i = key - first_key;
    assert(i >= 0 && i < num_keys);
    assert(isnew || key_name[i]);
    return i;
  }
  void add_key(int key, const char *name, bool is_inc, bool is_avg) {
    int i = lookup_key(key, true);
    key_name[i] = name;
    inc_keys[i] = is_inc;
    avg_keys[i] = is_avg;
  }
  void add_inc(int key, const char *name) {
    return add_key(key, name, true, false);
  }
  void add_set(int key, const char *name) {
    return add_key(key, name, false, false);
  }
  void add_avg(int key, const char *name) {
    return add_key(key, name, true, true);
  }
};

#endif
