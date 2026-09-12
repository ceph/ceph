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

#include "encoding_optional.h"

namespace boost {

template<typename T>
inline void encode(const boost::optional<T> &p, bufferlist &bl)
{
  encoding_detail::encode_optional(p, bl);
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic ignored "-Wuninitialized"

template<typename T>
inline void decode(boost::optional<T> &p, bufferlist::const_iterator &bp)
{
  auto decoded = encoding_detail::decode_optional<T>(bp);
  if (decoded) {
    p = std::move(*decoded);
    return;
  }

  p.reset();
}

#pragma GCC diagnostic pop

} // namespace boost
