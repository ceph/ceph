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
#ifndef __CAPCACHE_H
#define __CAPCACHE_H

//#include <iostream>
//using namespace std;

//#include "crypto/ExtCap.h"

class CapCache {
private:
  map <cap_id_t, ExtCap> verified_caps;
  
public:
  CapCache() {
  }
  bool prev_verified(cap_id_t cap_id) {
    return verified_caps.count(cap_id); 
  }
  void insert(ExtCap *cap) {
    verified_caps[cap->get_id()] = (*cap);
  }
  void remove(cap_id_t cap_id) {
    verified_caps.erase(cap_id);
  }
};

#endif
