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

#include <boost/container/small_vector.hpp>

#include "denc_small_vector.h"
#include "encoding.h"

namespace ceph {

template<class T, std::size_t N, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
encode(const boost::container::small_vector<T,N,Alloc>& v, bufferlist& bl, uint64_t features);
template<class T, std::size_t N, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
encode(const boost::container::small_vector<T,N,Alloc>& v, bufferlist& bl);
template<class T, std::size_t N, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
decode(boost::container::small_vector<T,N,Alloc>& v, bufferlist::const_iterator& p);
template<class T, std::size_t N, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
encode_nohead(const boost::container::small_vector<T,N,Alloc>& v, bufferlist& bl);
template<class T, std::size_t N, class Alloc, typename traits=denc_traits<T>>
inline std::enable_if_t<!traits::supported>
decode_nohead(unsigned len, boost::container::small_vector<T,N,Alloc>& v, bufferlist::const_iterator& p);

// small vector
template<class T, std::size_t N, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
  encode(const boost::container::small_vector<T,N,Alloc>& v, bufferlist& bl, uint64_t features)
{
  __u32 n = (__u32)(v.size());
  encode(n, bl);
  for (const auto& i : v)
    encode(i, bl, features);
}
template<class T, std::size_t N, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
  encode(const boost::container::small_vector<T,N,Alloc>& v, bufferlist& bl)
{
  __u32 n = (__u32)(v.size());
  encode(n, bl);
  for (const auto& i : v)
    encode(i, bl);
}
template<class T, std::size_t N, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
  decode(boost::container::small_vector<T,N,Alloc>& v, bufferlist::const_iterator& p)
{
  __u32 n;
  decode(n, p);
  v.resize(n);
  for (auto& i : v)
    decode(i, p);
}

template<class T, std::size_t N, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
  encode_nohead(const boost::container::small_vector<T,N,Alloc>& v, bufferlist& bl)
{
  for (const auto& i : v)
    encode(i, bl);
}
template<class T, std::size_t N, class Alloc, typename traits>
inline std::enable_if_t<!traits::supported>
  decode_nohead(unsigned len, boost::container::small_vector<T,N,Alloc>& v, bufferlist::const_iterator& p)
{
  v.resize(len);
  for (auto& i : v)
    decode(i, p);
}

} // namespace ceph
