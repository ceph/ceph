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

namespace ceph {

template<class T, size_t N, typename traits>
inline std::enable_if_t<!traits::supported>
encode(const std::array<T, N>& v, bufferlist& bl, uint64_t features)
{
  for (const auto& e : v)
    encode(e, bl, features);
}
template<class T, size_t N, typename traits>
inline std::enable_if_t<!traits::supported>
encode(const std::array<T, N>& v, bufferlist& bl)
{
  for (const auto& e : v)
    encode(e, bl);
}
template<class T, size_t N, typename traits>
inline std::enable_if_t<!traits::supported>
decode(std::array<T, N>& v, bufferlist::const_iterator& p)
{
  for (auto& e : v)
    decode(e, p);
}

} // namespace ceph
