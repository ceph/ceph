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
  size_t operator()(const char *p, unsigned len) {
    static rjhash<size_t> H;
    size_t acc = 0;
    while (len >= sizeof(size_t)) {
      acc ^= *(size_t*)p;
      p += sizeof(size_t);
      len -= sizeof(size_t);
    }   
    int sh = 0;
    while (len) {
      acc ^= (size_t)*p << sh;
      sh += 8;
      len--;
      p++;
    }
    return H(acc);
  }
};


#endif
