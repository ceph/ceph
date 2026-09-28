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
  encoding_detail::encode_bytes(s.data(), s.length(), bl);
}

inline void encode_nohead(std::string_view s, bufferlist& bl)
{
  encoding_detail::append_bytes(s.data(), s.length(), bl);
}

} // namespace std

namespace ceph {

// const char* (encode only, string compatible)
inline void encode(const char *s, bufferlist& bl) 
{
  encode(std::string_view{s}, bl);
}

} // namespace ceph
