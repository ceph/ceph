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


#ifndef __LOGTYPE_H
#define __LOGTYPE_H

#include "include/types.h"

#include <string>
#include <fstream>
using namespace std;
#include <ext/hash_map>
#include <ext/hash_set>
using namespace __gnu_cxx;

#include "Mutex.h"


class LogType {
 protected:
  hash_map<__uint64_t, int> keymap;  
  vector<const char*>   keys;
  set<int>              inc_keys;

  int version;

  // HACK to avoid the hash table as often as possible...
  // cache recent key name lookups in a small ring buffer
  const static int cache_keys = 10;
  __uint64_t kc_ptr[cache_keys];
  int kc_val[cache_keys];
  int kc_pos;

  friend class Logger;

 public:
  LogType() {
    version = 1;

    for (int i=0;i<cache_keys;i++)
      kc_ptr[i] = 0;
    kc_pos = 0;
  }
  int add_key(const char* key, bool is_inc) {
    int i = lookup_key(key);
    if (i >= 0) return i;

    i = keys.size();
    keys.push_back(key);

#ifdef __LP64__
    __uint64_t p = (__uint64_t)key;
#else
    __uint64_t p = (__uint32_t)key;
#endif
    keymap[p] = i;
    if (is_inc) inc_keys.insert(i);

    version++;
    return i;
  }
  int add_inc(const char* key) {
    return add_key(key, true);
  }
  int add_set(const char *key) {
    return add_key(key, false);
  }
  
  bool have_key(const char* key) {
    return lookup_key(key) < 0;
  }

  int lookup_key(const char* key) {
#ifdef __LP64__
    __uint64_t p = (__uint64_t)key;
#else
    __uint64_t p = (__uint32_t)key;
#endif

    if (keymap.count(p)) 
      return keymap[p];

    // try kc ringbuffer
    int pos = kc_pos-1;
    for (int j=0; j<cache_keys; j++) {
      if (pos < 0) pos = cache_keys - 1;
      if (kc_ptr[pos] == p) return kc_val[pos];
      pos--;
    }

    for (unsigned i=0; i<keys.size(); i++)
      if (strcmp(keys[i], key) == 0) {
        keymap[p] = i;

        // put in kc ringbuffer
        kc_ptr[kc_pos] = p;
        kc_val[kc_pos] = i;
        kc_pos++;
        if (kc_pos == cache_keys) kc_pos = 0;

        return i; 
      }
    return -1;
  }

};

#endif
