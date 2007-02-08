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

#ifndef __MON_MONITORSTORE_H
#define __MON_MONITORSTORE_H

#include "include/types.h"
#include "include/buffer.h"

#include <string.h>

class MonitorStore {
  string dir;

  void init();

public:
  MonitorStore(char *d) : dir(d) {
    init();
  }
  ~MonitorStore() {
  }

  void mkfs();  // wipe

  // ints (stored as ascii)
  version_t get_int(const char *a, const char *b=0);
  void put_int(version_t v, const char *a, const char *b=0);

  // buffers
  bool exists_bl(const char *a, const char *b=0);
  int get_bl(bufferlist& bl, const char *a, const char *b);
  int put_bl(bufferlist& bl, const char *a, const char *b);
  bool exists_bl(const char *a, unsigned b) {
    char bs[16];
    sprintf(bs, "%0u", b);
    return exists_bl(a, bs);
  }
  int get_bl(bufferlist& bl, const char *a, version_t b) {
    char bs[16];
    sprintf(bs, "%0llu", b);
    return get_bl(bl, a, bs);
  }
  int put_bl(bufferlist& bl, const char *a, version_t b) {
    char bs[16];
    sprintf(bs, "%0llu", b);
    return put_bl(bl, a, bs);
  }

  /*
  version_t get_incarnation() { return get_int("incarnation"); }
  void set_incarnation(version_t i) { set_int(i, "incarnation"); }
  
  version_t get_last_proposal() { return get_int("last_proposal"); }
  void set_last_proposal(version_t i) { set_int(i, "last_proposal"); }
  */
};


#endif
