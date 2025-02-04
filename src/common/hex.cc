// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2008-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <vector>
#include "common/hex.h"

int hex2str(const char *s, int len, char *buf, int dest_len)
{
  int pos = 0;
  for (int i=0; i<len && pos<dest_len; i++) {
    if (i && !(i%8))
      pos += snprintf(&buf[pos], dest_len-pos, " ");
    if (i && !(i%16))
      pos += snprintf(&buf[pos], dest_len-pos, "\n");
    pos += snprintf(&buf[pos], dest_len-pos, "%.2x ", (int)(unsigned char)s[i]);
  }
  return pos;
}

std::string hexdump(const std::string &msg, const char *s, int len)
{
  const int buf_len = len*4;
  std::vector<char> buf(buf_len);
  hex2str(s, len, buf.data(), buf_len);
  return buf.data();
}
