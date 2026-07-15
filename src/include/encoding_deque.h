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

#include <deque>

#include "encoding.h"

namespace ceph {

template<class T, class Alloc>
inline void encode(const std::deque<T,Alloc>& ls, bufferlist& bl, uint64_t features);
template<class T, class Alloc>
inline void encode(const std::deque<T,Alloc>& ls, bufferlist& bl);
template<class T, class Alloc>
inline void decode(std::deque<T,Alloc>& ls, bufferlist::const_iterator& p);

template<class T, class Alloc>
inline void encode(const std::deque<T,Alloc>& ls, bufferlist& bl, uint64_t features)
{
  __u32 n = ls.size();
  encode(n, bl);
  for (auto p = ls.begin(); p != ls.end(); ++p)
    encode(*p, bl, features);
}
template<class T, class Alloc>
inline void encode(const std::deque<T,Alloc>& ls, bufferlist& bl)
{
  __u32 n = ls.size();
  encode(n, bl);
  for (auto p = ls.begin(); p != ls.end(); ++p)
    encode(*p, bl);
}
template<class T, class Alloc>
inline void decode(std::deque<T,Alloc>& ls, bufferlist::const_iterator& p)
{
  __u32 n;
  decode(n, p);
  ls.clear();
  while (n--) {
    ls.emplace_back();
    decode(ls.back(), p);
  }
}

} // namespace ceph
