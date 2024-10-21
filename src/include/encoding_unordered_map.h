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

#include "encoding.h"

#include <unordered_map>

namespace std {

template<class T, class U, class Hash, class Pred, class Alloc>
inline void encode(const std::unordered_map<T,U,Hash,Pred,Alloc>& m,
                   bufferlist& bl,
                   uint64_t features)
{
  encoding_detail::encode_pair_range(m, bl, features);
}
template<class T, class U, class Hash, class Pred, class Alloc>
inline void encode(const std::unordered_map<T,U,Hash,Pred,Alloc>& m,
                   bufferlist& bl)
{
  encoding_detail::encode_pair_range(m, bl);
}
template<class T, class U, class Hash, class Pred, class Alloc>
inline void decode(std::unordered_map<T,U,Hash,Pred,Alloc>& m,
                   bufferlist::const_iterator& p)
{
  encoding_detail::decode_map(m, p);
}

} // namespace std
