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

#include <string_view>

#include "encoding.h"

namespace std {

inline void encode(std::string_view s, bufferlist& bl, uint64_t features=0)
{
  using ceph::encode;

  __u32 len = s.length();
  encode(len, bl);
  if (len)
    bl.append(s.data(), len);
}
inline void encode_nohead(std::string_view s, bufferlist& bl)
{
  bl.append(s.data(), s.length());
}

} // namespace std

namespace ceph {

// const char* (encode only, string compatible)
inline void encode(const char *s, bufferlist& bl) 
{
  encode(std::string_view{s}, bl);
}

} // namespace ceph
