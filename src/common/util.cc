// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "include/util.h"

// test if an entire buf is zero in 8-byte chunks
bool buf_is_zero(const char *buf, size_t len)
{
  size_t ofs;
  int chunk = sizeof(uint64_t);

  for (ofs = 0; ofs < len; ofs += sizeof(uint64_t)) {
    if (*(uint64_t *)(buf + ofs) != 0) {
      return false;
    }
  }
  for (ofs = (len / chunk) * chunk; ofs < len; ofs++) {
    if (buf[ofs] != '\0') {
      return false;
    }
  }
  return true;
}


