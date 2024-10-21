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
template<class T, class Comp, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode(const std::set<T,Comp,Alloc>& s, bufferlist& bl)
{
  encoding_detail::encode_range(s, bl);
}
template<class T, class Comp, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void decode(std::set<T,Comp,Alloc>& s, bufferlist::const_iterator& p)
{
  encoding_detail::decode_by_insert(s, p);
}

template<class T, class Comp, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode_nohead(const std::set<T,Comp,Alloc>& s, bufferlist& bl)
{
  encoding_detail::encode_range_nohead(s, bl);
}
template<class T, class Comp, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void decode_nohead(unsigned len, std::set<T,Comp,Alloc>& s,
                          bufferlist::const_iterator& p)
{
  encoding_detail::decode_by_insert(len, s, p);
}

// multiset
template<class T, class Comp, class Alloc>
inline void encode(const std::multiset<T,Comp,Alloc>& s, bufferlist& bl)
{
  encoding_detail::encode_range(s, bl);
}
template<class T, class Comp, class Alloc>
inline void decode(std::multiset<T,Comp,Alloc>& s, bufferlist::const_iterator& p)
{
  encoding_detail::decode_by_insert(s, p);
}

} // namespace std
