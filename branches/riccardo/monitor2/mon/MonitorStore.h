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

class MonitorStore {
  const char *dir;

  version_t get_int(const char *a, const char *b=0);
  void set_int(version_t v, const char *a, const char *b=0);

  int get_bl(const char *nm, bufferlist& bl);
  int put_bl(const char *nm, bufferlist& bl);

  void init();

public:
  MonitorStore(const char *d) :
    dir(d) {
    init();
  }

  version_t get_incarnation() { return get_int("incarnation"); }
  void set_incarnation(version_t i) { set_int(i, "incarnation"); }
  
  version_t get_last_proposal() { return get_int("last_proposal"); }
  void set_last_proposal(version_t i) { set_int(i, "last_proposal"); }
};


#endif
