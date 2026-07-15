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

#include <boost/container/flat_set.hpp>

#include "denc_flat_set.h"
#include "encoding.h"

namespace boost::container {

template<class T, class Comp, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
encode(const boost::container::flat_set<T, Comp, Alloc>& s, bufferlist& bl)
{
  using ceph::encode;

  __u32 n = (__u32)(s.size());
  encode(n, bl);
  for (const auto& e : s)
    encode(e, bl);
}
template<class T, class Comp, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
decode(boost::container::flat_set<T, Comp, Alloc>& s, bufferlist::const_iterator& p)
{
  using ceph::decode;

  __u32 n;
  decode(n, p);
  s.clear();
  s.reserve(n);
  while (n--) {
    T v;
    decode(v, p);
    s.insert(v);
  }
}

template<class T, class Comp, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
encode_nohead(const boost::container::flat_set<T, Comp, Alloc>& s,
	      bufferlist& bl)
{
  using ceph::encode;

  for (const auto& e : s)
    encode(e, bl);
}
template<class T, class Comp, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
decode_nohead(unsigned len, boost::container::flat_set<T, Comp, Alloc>& s,
	      bufferlist::iterator& p)
{
  using ceph::decode;

  s.reserve(len);
  for (unsigned i=0; i<len; i++) {
    T v;
    decode(v, p);
    s.insert(v);
  }
}

} // namespace boost::container
