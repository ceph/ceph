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
#include <list>

#include "denc_list.h"
#include "encoding.h"

namespace std {

// std::list<T>
template<class T, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
  encode(const std::list<T, Alloc>& ls, bufferlist& bl)
{
  using ceph::encode;

  __u32 n = (__u32)(ls.size());  // c++11 std::list::size() is O(1)
  encode(n, bl);
  for (auto p = ls.begin(); p != ls.end(); ++p)
    encode(*p, bl);
}
template<class T, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
  encode(const std::list<T,Alloc>& ls, bufferlist& bl, uint64_t features)
{
  using ceph::encode;

  using counter_encode_t = ceph_le32;
  unsigned n = 0;
  auto filler = bl.append_hole(sizeof(counter_encode_t));
  for (const auto& item : ls) {
    // we count on our own because of buggy std::list::size() implementation
    // which doesn't follow the O(1) complexity constraint C++11 has brought.
    ++n;
    encode(item, bl, features);
  }
  counter_encode_t en;
  en = n;
  filler.copy_in(sizeof(en), reinterpret_cast<char*>(&en));
}

template<class T, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
  decode(std::list<T,Alloc>& ls, bufferlist::const_iterator& p)
{
  using ceph::decode;

  __u32 n;
  decode(n, p);
  ls.clear();
  while (n--) {
    ls.emplace_back();
    decode(ls.back(), p);
  }
}

// std::list<std::shared_ptr<T>>
template<class T, class Alloc>
inline void encode(const std::list<std::shared_ptr<T>, Alloc>& ls,
		   bufferlist& bl)
{
  using ceph::encode;

  __u32 n = (__u32)(ls.size());  // c++11 std::list::size() is O(1)
  encode(n, bl);
  for (const auto& ref : ls) {
    encode(*ref, bl);
  }
}
template<class T, class Alloc>
inline void encode(const std::list<std::shared_ptr<T>, Alloc>& ls,
		   bufferlist& bl, uint64_t features)
{
  using ceph::encode;

  __u32 n = (__u32)(ls.size());  // c++11 std::list::size() is O(1)
  encode(n, bl);
  for (const auto& ref : ls) {
    encode(*ref, bl, features);
  }
}
template<class T, class Alloc>
inline void decode(std::list<std::shared_ptr<T>, Alloc>& ls,
		   bufferlist::const_iterator& p)
{
  using ceph::decode;

  __u32 n;
  decode(n, p);
  ls.clear();
  while (n--) {
    auto ref = std::make_shared<T>();
    decode(*ref, p);
    ls.emplace_back(std::move(ref));
  }
}

} // namespace std
