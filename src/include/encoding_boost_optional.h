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

#include <boost/optional/optional_io.hpp>

#include "encoding.h"

namespace boost {

// boost optional
template<typename T>
inline void encode(const boost::optional<T> &p, bufferlist &bl)
{
  using ceph::encode;

  __u8 present = static_cast<bool>(p);
  encode(present, bl);
  if (p)
    encode(p.get(), bl);
}

#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wuninitialized"
template<typename T>
inline void decode(boost::optional<T> &p, bufferlist::const_iterator &bp)
{
  using ceph::decode;

  __u8 present;
  decode(present, bp);
  if (present) {
    p = T{};
    decode(p.get(), bp);
  } else {
    p = boost::none;
  }
}
#pragma GCC diagnostic pop
#pragma GCC diagnostic warning "-Wpragmas"

} // namespace boost
