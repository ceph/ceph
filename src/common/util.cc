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

#include <errno.h>

#include "include/util.h"
#include "common/errno.h"
#include "common/strtol.h"

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


int64_t unit_to_bytesize(string val, ostream *pss)
{
  if (val.empty()) {
    if (pss)
      *pss << "value is empty!";
    return -EINVAL;
  }

  char c = val[val.length()-1];
  int modifier = 0;
  if (!::isdigit(c)) {
    if (val.length() < 2) {
      if (pss)
        *pss << "invalid value: " << val;
      return -EINVAL;
    }
    val = val.substr(0,val.length()-1);
    switch (c) {
    case 'B':
      break;
    case 'K':
      modifier = 10;
      break;
    case 'M':
      modifier = 20;
      break;
    case 'G':
      modifier = 30;
      break;
    case 'T':
      modifier = 40;
      break;
    case 'P':
      modifier = 50;
      break;
    case 'E':
      modifier = 60;
      break;
    default:
      if (pss)
        *pss << "unrecognized modifier '" << c << "'" << std::endl;
      return -EINVAL;
    }
  }

  if (val[0] == '+' || val[0] == '-') {
    if (pss)
      *pss << "expected numerical value, got: " << val;
    return -EINVAL;
  }

  string err;
  int64_t r = strict_strtoll(val.c_str(), 10, &err);
  if ((r == 0) && !err.empty()) {
    if (pss)
      *pss << err;
    return -1;
  }
  if (r < 0) {
    if (pss)
      *pss << "unable to parse positive integer '" << val << "'";
    return -1;
  }
  return (r * (1LL << modifier));
}
