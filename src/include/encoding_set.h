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

#include <set>

#include "denc_set.h"
#include "encoding.h"

namespace std {

// std::set<T>
template<class T, class Comp, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
  encode(const std::set<T,Comp,Alloc>& s, bufferlist& bl)
{
  using ceph::encode;

  __u32 n = (__u32)(s.size());
  encode(n, bl);
  for (auto p = s.begin(); p != s.end(); ++p)
    encode(*p, bl);
}
template<class T, class Comp, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
  decode(std::set<T,Comp,Alloc>& s, bufferlist::const_iterator& p)
{
  using ceph::decode;

  __u32 n;
  decode(n, p);
  s.clear();
  while (n--) {
    T v;
    decode(v, p);
    s.insert(v);
  }
}

template<class T, class Comp, class Alloc, typename traits=denc_traits<T>>
inline typename std::enable_if<!traits::supported>::type
  encode_nohead(const std::set<T,Comp,Alloc>& s, bufferlist& bl)
{
  using ceph::encode;

  for (auto p = s.begin(); p != s.end(); ++p)
    encode(*p, bl);
}
template<class T, class Comp, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
  decode_nohead(unsigned len, std::set<T,Comp,Alloc>& s, bufferlist::const_iterator& p)
{
  using ceph::decode;

  for (unsigned i=0; i<len; i++) {
    T v;
    decode(v, p);
    s.insert(v);
  }
}

// multiset
template<class T, class Comp, class Alloc>
inline void encode(const std::multiset<T,Comp,Alloc>& s, bufferlist& bl)
{
  using ceph::encode;

  __u32 n = (__u32)(s.size());
  encode(n, bl);
  for (auto p = s.begin(); p != s.end(); ++p)
    encode(*p, bl);
}
template<class T, class Comp, class Alloc>
inline void decode(std::multiset<T,Comp,Alloc>& s, bufferlist::const_iterator& p)
{
  using ceph::decode;

  __u32 n;
  decode(n, p);
  s.clear();
  while (n--) {
    T v;
    decode(v, p);
    s.insert(v);
  }
}

} // namespace std
