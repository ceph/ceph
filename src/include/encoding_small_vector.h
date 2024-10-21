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

namespace boost::container {

template<class T, std::size_t N, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode(const boost::container::small_vector<T,N,Alloc>& v,
                   bufferlist& bl, uint64_t features)
{
  encoding_detail::encode_range(v, bl, features);
}
template<class T, std::size_t N, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode(const boost::container::small_vector<T,N,Alloc>& v,
                   bufferlist& bl)
{
  encoding_detail::encode_range(v, bl);
}
template<class T, std::size_t N, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void decode(boost::container::small_vector<T,N,Alloc>& v,
                   bufferlist::const_iterator& p)
{
  encoding_detail::decode_by_resize(v, p);
}

template<class T, std::size_t N, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode_nohead(const boost::container::small_vector<T,N,Alloc>& v,
                          bufferlist& bl)
{
  encoding_detail::encode_range_nohead(v, bl);
}
template<class T, std::size_t N, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void decode_nohead(unsigned len,
                          boost::container::small_vector<T,N,Alloc>& v,
                          bufferlist::const_iterator& p)
{
  encoding_detail::decode_by_resize_nohead(len, v, p);
}


} // namespace boost::container
