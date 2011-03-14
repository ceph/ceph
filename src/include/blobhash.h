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

#ifndef CEPH_BLOBHASH_H
#define CEPH_BLOBHASH_H

#include "hash.h"

/*
- this is to make some of the STL types work with 64 bit values, string hash keys, etc.
- added when i was using an old STL.. maybe try taking these out and see if things 
  compile now?
*/

class blobhash {
public:
  uint32_t operator()(const char *p, unsigned len) {
    static rjhash<uint32_t> H;
    uint32_t acc = 0;
    while (len >= sizeof(acc)) {
      acc ^= *(uint32_t*)p;
      p += sizeof(uint32_t);
      len -= sizeof(uint32_t);
    }
    int sh = 0;
    while (len) {
      acc ^= (uint32_t)*p << sh;
      sh += 8;
      len--;
      p++;
    }
    return H(acc);
  }
};


#endif
