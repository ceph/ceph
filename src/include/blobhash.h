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

#include <cstdint>
#include "hash.h"

class blobhash {
public:
  uint32_t operator()(const void* p, size_t len) {
    static rjhash<std::uint32_t> H;
    std::uint32_t acc = 0;
    auto buf = static_cast<const unsigned char*>(p);
    while (len >= sizeof(acc)) {
      acc ^= unaligned_load(buf);
      buf += sizeof(std::uint32_t);
      len -= sizeof(std::uint32_t);
    }
    // handle the last few bytes of p[-(len % 4):]
    switch (len) {
    case 3:
      acc ^= buf[2] << 16;
      [[fallthrough]];
    case 2:
      acc ^= buf[1] << 8;
      [[fallthrough]];
    case 1:
      acc ^= buf[0];
    }
    return H(acc);
  }
private:
  static inline std::uint32_t unaligned_load(const unsigned char* p) {
    std::uint32_t result;
    __builtin_memcpy(&result, p, sizeof(result));
    return result;
  }
};


#endif
