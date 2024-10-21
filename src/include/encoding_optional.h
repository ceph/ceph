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

namespace ceph::encoding_detail {

template<typename T>
std::optional<T> decode_optional(bufferlist::const_iterator& p)
{
  __u8 present;
  decode(present, p);
  if (!present) {
    return std::nullopt;
  }

  T value{};
  decode(value, p);
  return value;
}

} // namespace ceph::encoding_detail

namespace std {

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic ignored "-Wuninitialized"

template<typename T>
inline void encode(const std::optional<T> &p, bufferlist &bl)
{
  ceph::encoding_detail::encode_optional(p, bl);
}

template<typename T>
inline void decode(std::optional<T> &p, bufferlist::const_iterator &bp)
{
  p = ceph::encoding_detail::decode_optional<T>(bp);
}

#pragma GCC diagnostic pop

} // namespace std
