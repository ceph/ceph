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

#ifndef __MON_MONITORSTORE_H
#define __MON_MONITORSTORE_H

#include "include/types.h"
#include "include/buffer.h"

#include <string.h>

class MonitorStore {
  string dir;

public:
  MonitorStore(char *d) : dir(d) {
  }
  ~MonitorStore() {
  }

  void mkfs();  // wipe
  void mount();

  // ints (stored as ascii)
  version_t get_int(const char *a, const char *b=0);
  void put_int(version_t v, const char *a, const char *b=0);

  // buffers
  // ss and sn varieties.
  bool exists_bl_ss(const char *a, const char *b=0);
  int get_bl_ss(bufferlist& bl, const char *a, const char *b);
  int put_bl_ss(bufferlist& bl, const char *a, const char *b);
  bool exists_bl_sn(const char *a, version_t b) {
    char bs[20];
#ifdef __LP64__
    sprintf(bs, "%lu", b);
#else
    sprintf(bs, "%llu", b);
#endif
    return exists_bl_ss(a, bs);
  }
  int get_bl_sn(bufferlist& bl, const char *a, version_t b) {
    char bs[20];
#ifdef __LP64__
    sprintf(bs, "%lu", b);
#else
    sprintf(bs, "%llu", b);
#endif
    return get_bl_ss(bl, a, bs);
  }
  int put_bl_sn(bufferlist& bl, const char *a, version_t b) {
    char bs[20];
#ifdef __LP64__
    sprintf(bs, "%lu", b);
#else
    sprintf(bs, "%llu", b);
#endif
    return put_bl_ss(bl, a, bs);
  }

  /*
  version_t get_incarnation() { return get_int("incarnation"); }
  void set_incarnation(version_t i) { set_int(i, "incarnation"); }
  
  version_t get_last_proposal() { return get_int("last_proposal"); }
  void set_last_proposal(version_t i) { set_int(i, "last_proposal"); }
  */
};


#endif
