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

#include <boost/container/flat_map.hpp>

#include "denc_flat_map.h"
#include "encoding.h"

namespace boost::container {

template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::needs_legacy_encoding<t_traits, u_traits>
inline void encode(const boost::container::flat_map<T,U,Comp,Alloc>& m,
                   bufferlist& bl)
{
  encoding_detail::encode_pair_range(m, bl);
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::needs_legacy_encoding<t_traits, u_traits>
inline void encode(const boost::container::flat_map<T,U,Comp,Alloc>& m,
                   bufferlist& bl, uint64_t features)
{
  encoding_detail::encode_pair_range(m, bl, features);
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::needs_legacy_encoding<t_traits, u_traits>
inline void decode(boost::container::flat_map<T,U,Comp,Alloc>& m,
                   bufferlist::const_iterator& p)
{
  encoding_detail::decode_map_by_subscript(m, p);
}

// Compatibility-only: no production callers were found in-tree. Do not add new
// callers unless preserving the destination's current entries is necessary.
// Actual deprecation is a separate public-header compatibility decision.
// Duplicate decoded keys intentionally replace the existing mapped value for
// boost::container::flat_map.
template<class T, class U, class Comp, class Alloc>
[[maybe_unused]] inline void decode_noclear(
  boost::container::flat_map<T,U,Comp,Alloc>& m,
  bufferlist::const_iterator& p)
{
  const auto n = encoding_detail::decode_count(p);
  m.reserve(m.size() + n);
  encoding_detail::decode_map_entries_by_subscript(n, m, p);
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::needs_legacy_encoding<t_traits, u_traits>
inline void encode_nohead(const boost::container::flat_map<T,U,Comp,Alloc>& m,
                          bufferlist& bl)
{
  encoding_detail::encode_pair_range_nohead(m, bl);
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::needs_legacy_encoding<t_traits, u_traits>
inline void encode_nohead(const boost::container::flat_map<T,U,Comp,Alloc>& m,
                          bufferlist& bl, uint64_t features)
{
  encoding_detail::encode_pair_range_nohead(m, bl, features);
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::needs_legacy_encoding<t_traits, u_traits>
inline void decode_nohead(unsigned n,
                          boost::container::flat_map<T,U,Comp,Alloc>& m,
                          bufferlist::const_iterator& p)
{
  encoding_detail::decode_map_by_subscript(n, m, p);
}

} // namespace boost::container
