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

#include "encoding.h"

#include <unordered_map>

namespace std {

template<class T, class U, class Hash, class Pred, class Alloc>
inline void encode(const std::unordered_map<T,U,Hash,Pred,Alloc>& m, bufferlist& bl,
		   uint64_t features)
{
  using ceph::encode;

  __u32 n = (__u32)(m.size());
  encode(n, bl);
  for (auto p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl, features);
    encode(p->second, bl, features);
  }
}
template<class T, class U, class Hash, class Pred, class Alloc>
inline void encode(const std::unordered_map<T,U,Hash,Pred,Alloc>& m, bufferlist& bl)
{
  using ceph::encode;

  __u32 n = (__u32)(m.size());
  encode(n, bl);
  for (auto p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl);
    encode(p->second, bl);
  }
}
template<class T, class U, class Hash, class Pred, class Alloc>
inline void decode(std::unordered_map<T,U,Hash,Pred,Alloc>& m, bufferlist::const_iterator& p)
{
  using ceph::decode;

  __u32 n;
  decode(n, p);
  m.clear();
  while (n--) {
    T k;
    decode(k, p);
    decode(m[k], p);
  }
}

template <std::move_constructible T, std::move_constructible U, class Hash, class Pred, class Alloc>
inline void decode(std::unordered_map<T, U, Hash, Pred, Alloc>& m, bufferlist::const_iterator& p)
{
  using ceph::decode;

  __u32 n;
  decode(n, p);
  m.clear();
  while (n--) {
    T k;
    U v;
    decode(k, p);
    decode(v, p);
    m.emplace(std::move(k), std::move(v));
  }
}

} // namespace std
