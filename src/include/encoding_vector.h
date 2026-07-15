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

#include <memory>
#include <vector>

#include "denc_vector.h"
#include "encoding.h"

namespace std {

// opaque byte vectors
inline void encode(std::vector<uint8_t>& v, bufferlist& bl)
{
  using ceph::encode;

  uint32_t len = v.size();
  encode(len, bl);
  if (len)
    bl.append((char *)v.data(), len);
}

inline void decode(std::vector<uint8_t>& v, bufferlist::const_iterator& p)
{
  using ceph::decode;

  uint32_t len;

  decode(len, p);
  v.resize(len);
  p.copy(len, (char *)v.data());
}

template<class T, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
  encode(const std::vector<T,Alloc>& v, bufferlist& bl, uint64_t features)
{
  using ceph::encode;

  __u32 n = (__u32)(v.size());
  encode(n, bl);
  for (auto p = v.begin(); p != v.end(); ++p)
    encode(*p, bl, features);
}
template<class T, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
  encode(const std::vector<T,Alloc>& v, bufferlist& bl)
{
  using ceph::encode;

  __u32 n = (__u32)(v.size());
  encode(n, bl);
  for (auto p = v.begin(); p != v.end(); ++p)
    encode(*p, bl);
}
template<class T, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
  decode(std::vector<T,Alloc>& v, bufferlist::const_iterator& p)
{
  using ceph::decode;

  __u32 n;
  decode(n, p);
  v.resize(n);
  for (__u32 i=0; i<n; i++) 
    decode(v[i], p);
}

template<class T, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
  encode_nohead(const std::vector<T,Alloc>& v, bufferlist& bl)
{
  using ceph::encode;

  for (auto p = v.begin(); p != v.end(); ++p)
    encode(*p, bl);
}
template<class T, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
  decode_nohead(unsigned len, std::vector<T,Alloc>& v, bufferlist::const_iterator& p)
{
  using ceph::decode;

  v.resize(len);
  for (__u32 i=0; i<v.size(); i++) 
    decode(v[i], p);
}

// vector (shared_ptr)
template<class T,class Alloc>
inline void encode(const std::vector<std::shared_ptr<T>,Alloc>& v,
		   bufferlist& bl,
		   uint64_t features)
{
  using ceph::encode;

  __u32 n = (__u32)(v.size());
  encode(n, bl);
  for (const auto& ref : v) {
    if (ref)
      encode(*ref, bl, features);
    else
      encode(T(), bl, features);
  }
}
template<class T, class Alloc>
inline void encode(const std::vector<std::shared_ptr<T>,Alloc>& v,
		   bufferlist& bl)
{
  using ceph::encode;

  __u32 n = (__u32)(v.size());
  encode(n, bl);
  for (const auto& ref : v) {
    if (ref)
      encode(*ref, bl);
    else
      encode(T(), bl);
  }
}
template<class T, class Alloc>
inline void decode(std::vector<std::shared_ptr<T>,Alloc>& v,
		   bufferlist::const_iterator& p)
{
  using ceph::decode;

  __u32 n;
  decode(n, p);
  v.clear();
  v.reserve(n);
  while (n--) {
    auto ref = std::make_shared<T>();
    decode(*ref, p);
    v.emplace_back(std::move(ref));
  }
}

} // namespace std
