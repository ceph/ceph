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

#include <optional>

#include "denc_optional.h"
#include "encoding.h"

namespace std {

template<typename T>
inline void encode(const std::optional<T> &p, bufferlist &bl)
{
  using ceph::encode;

  __u8 present = static_cast<bool>(p);
  encode(present, bl);
  if (p)
    encode(*p, bl);
}

#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wuninitialized"
template<typename T>
inline void decode(std::optional<T> &p, bufferlist::const_iterator &bp)
{
  using ceph::decode;

  __u8 present;
  decode(present, bp);
  if (present) {
    p = T{};
    decode(*p, bp);
  } else {
    p = std::nullopt;
  }
}

} // namespace std
