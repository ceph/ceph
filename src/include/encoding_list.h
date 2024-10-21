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
template<class T, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode(const std::list<T, Alloc>& ls, bufferlist& bl)
{
  encoding_detail::encode_range(ls, bl);
}
template<class T, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void encode(const std::list<T,Alloc>& ls, bufferlist& bl, uint64_t features)
{
  encoding_detail::encode_range(ls, bl, features);
}

template<class T, class Alloc, typename traits = denc_traits<T>>
requires encoding_detail::needs_legacy_encoding<traits>
inline void decode(std::list<T,Alloc>& ls, bufferlist::const_iterator& p)
{
  encoding_detail::decode_by_emplace_back(ls, p);
}

// std::list<std::shared_ptr<T>>
template<class T, class Alloc>
inline void encode(const std::list<std::shared_ptr<T>, Alloc>& ls,
                   bufferlist& bl)
{
  encoding_detail::encode_shared_ptr_range(ls, bl);
}
template<class T, class Alloc>
inline void encode(const std::list<std::shared_ptr<T>, Alloc>& ls,
                   bufferlist& bl, uint64_t features)
{
  encoding_detail::encode_shared_ptr_range(ls, bl, features);
}
template<class T, class Alloc>
inline void decode(std::list<std::shared_ptr<T>, Alloc>& ls,
                   bufferlist::const_iterator& p)
{
  encoding_detail::decode_shared_ptr_sequence(ls, p);
}

} // namespace std
