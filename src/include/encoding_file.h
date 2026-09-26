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

#pragma once

#include "encoding_string.h"

namespace ceph {

/*
 * Encoders/decoders to read from current offset in a file handle and
 * encode/decode the data according to argument types.
 */
inline ssize_t decode_file(int fd, std::string &str)
{
  bufferlist bl;
  __u32 len = 0;
  bl.read_fd(fd, sizeof(len));
  decode(len, bl);
  bl.read_fd(fd, len);
  decode(str, bl);
  return bl.length();
}

inline ssize_t decode_file(int fd, bufferptr &bp)
{
  bufferlist bl;
  __u32 len = 0;
  bl.read_fd(fd, sizeof(len));
  decode(len, bl);
  bl.read_fd(fd, len);
  auto bli = std::cbegin(bl);

  decode(bp, bli);
  return bl.length();
}
}
