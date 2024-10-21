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

#include <unordered_set>

namespace std {

template<class T, class Hash, class Pred, class Alloc>
inline void encode(const std::unordered_set<T,Hash,Pred,Alloc>& m,
                   bufferlist& bl)
{
  encoding_detail::encode_range(m, bl);
}
template<class T, class Hash, class Pred, class Alloc>
inline void decode(std::unordered_set<T,Hash,Pred,Alloc>& m,
                   bufferlist::const_iterator& p)
{
  encoding_detail::decode_by_insert(m, p);
}

} // namespace std
