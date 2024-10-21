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

#include <string>

#include "encoding_string_view.h"
#include "denc_string.h"

namespace std {

inline void encode(const std::string& s, bufferlist& bl, uint64_t features=0)
{
  return encode(std::string_view(s), bl, features);
}
inline void decode(std::string& s, bufferlist::const_iterator& p)
{
  using ceph::decode;

  __u32 len;
  decode(len, p);
  s.clear();
  p.copy(len, s);
}

inline void encode_nohead(const std::string& s, bufferlist& bl)
{
  encode_nohead(std::string_view(s), bl);
}
inline void decode_nohead(unsigned len, std::string& s, bufferlist::const_iterator& p)
{
  s.clear();
  p.copy(len, s);
}

} // namespace std
