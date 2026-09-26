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

template<class T, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode(const std::vector<T,Alloc>& v, bufferlist& bl, uint64_t features)
{
  encoding_detail::encode_range(v, bl, features);
}
template<class T, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode(const std::vector<T,Alloc>& v, bufferlist& bl)
{
  encoding_detail::encode_range(v, bl);
}
template<class T, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void decode(std::vector<T,Alloc>& v, bufferlist::const_iterator& p)
{
  encoding_detail::decode_by_resize(v, p);
}

template<class T, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode_nohead(const std::vector<T,Alloc>& v, bufferlist& bl)
{
  encoding_detail::encode_range_nohead(v, bl);
}
template<class T, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void decode_nohead(unsigned len, std::vector<T,Alloc>& v,
                          bufferlist::const_iterator& p)
{
  encoding_detail::decode_by_resize_nohead(len, v, p);
}

// opaque byte vectors
inline void encode(std::vector<uint8_t>& v, bufferlist& bl)
{
  encoding_detail::encode_bytes(v.data(), v.size(), bl);
}

inline void decode(std::vector<uint8_t>& v, bufferlist::const_iterator& p)
{
  const auto len = encoding_detail::decode_count(p);
  v.resize(len);
  p.copy(len, (char *)v.data());
}

// vector (shared_ptr)
template<class T, class Alloc>
inline void encode(const std::vector<std::shared_ptr<T>,Alloc>& v,
                   bufferlist& bl,
                   uint64_t features)
{
  encoding_detail::encode_shared_ptr_range(v, bl, features);
}
template<class T, class Alloc>
inline void encode(const std::vector<std::shared_ptr<T>,Alloc>& v,
                   bufferlist& bl)
{
  encoding_detail::encode_shared_ptr_range(v, bl);
}
template<class T, class Alloc>
inline void decode(std::vector<std::shared_ptr<T>,Alloc>& v,
                   bufferlist::const_iterator& p)
{
  encoding_detail::decode_shared_ptr_sequence(v, p);
}

} // namespace std
