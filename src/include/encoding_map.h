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

#include <map>

#include "denc_map.h"
#include "encoding.h"

namespace std {

// map
template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::needs_legacy_encoding<t_traits, u_traits>
inline void encode(const std::map<T,U,Comp,Alloc>& m, bufferlist& bl)
{
  encoding_detail::encode_pair_range(m, bl);
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::needs_legacy_encoding<t_traits, u_traits>
inline void encode(const std::map<T,U,Comp,Alloc>& m, bufferlist& bl,
                   uint64_t features)
{
  encoding_detail::encode_pair_range(m, bl, features);
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::needs_legacy_encoding<t_traits, u_traits>
inline void decode(std::map<T,U,Comp,Alloc>& m, bufferlist::const_iterator& p)
{
  encoding_detail::decode_map(m, p);
}

// Compatibility helper for callers that intentionally manage clearing:
// actual deprecation is a separate public-header compatibility decision.
// Duplicate decoded keys intentionally preserve the existing mapped value for
// std::map.
template<class T, class U, class Comp, class Alloc>
inline void decode_noclear(std::map<T,U,Comp,Alloc>& m,
                           bufferlist::const_iterator& p)
{
  encoding_detail::decode_map_entries_no_clear(
    encoding_detail::decode_count(p), m, p);
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::needs_legacy_encoding<t_traits, u_traits>
inline void encode_nohead(const std::map<T,U,Comp,Alloc>& m, bufferlist& bl)
{
  encoding_detail::encode_pair_range_nohead(m, bl);
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::needs_legacy_encoding<t_traits, u_traits>
inline void encode_nohead(const std::map<T,U,Comp,Alloc>& m,
                          bufferlist& bl,
                          uint64_t features)
{
  encoding_detail::encode_pair_range_nohead(m, bl, features);
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits = denc_traits<T>, typename u_traits = denc_traits<U>>
requires encoding_detail::needs_legacy_encoding<t_traits, u_traits>
inline void decode_nohead(unsigned n, std::map<T,U,Comp,Alloc>& m,
                          bufferlist::const_iterator& p)
{
  encoding_detail::decode_map(n, m, p);
}

// multimap
template<class T, class U, class Comp, class Alloc>
inline void encode(const std::multimap<T,U,Comp,Alloc>& m, bufferlist& bl)
{
  encoding_detail::encode_pair_range(m, bl);
}
template<class T, class U, class Comp, class Alloc>
inline void decode(std::multimap<T,U,Comp,Alloc>& m,
                   bufferlist::const_iterator& p)
{
  m.clear();

  encoding_detail::for_each_count(encoding_detail::decode_count(p), [&m, &p] {
    using ceph::decode;
    auto tu = std::pair<T, U> {};
    decode(tu.first, p);
    auto it = m.insert(std::move(tu));
    decode(it->second, p);
  });
}

} // namespace std
