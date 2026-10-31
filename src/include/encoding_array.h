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

#include <array>

#include "denc_array.h"
#include "encoding.h"

namespace std {

template<class T, size_t N, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode(const std::array<T, N>& v, bufferlist& bl, uint64_t features)
{
  encoding_detail::encode_range_nohead(v, bl, features);
}
template<class T, size_t N, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode(const std::array<T, N>& v, bufferlist& bl)
{
  encoding_detail::encode_range_nohead(v, bl);
}
template<class T, size_t N, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void decode(std::array<T, N>& v, bufferlist::const_iterator& p)
{
  encoding_detail::decode_range_nohead(v, p);
}

} // namespace std
