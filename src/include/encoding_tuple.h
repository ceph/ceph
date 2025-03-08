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

#include <tuple>

#include "denc_tuple.h"
#include "encoding.h"
#include "common/convenience.h" // for ceph::for_each()

namespace ceph {

template<typename... Ts>
inline void encode(const std::tuple<Ts...> &t, bufferlist& bl)
{
  ceph::for_each(t, [&bl](const auto& e) {
      encode(e, bl);
    });
}
template<typename... Ts>
inline void decode(std::tuple<Ts...> &t, bufferlist::const_iterator &bp)
{
  ceph::for_each(t, [&bp](auto& e) {
      decode(e, bp);
    });
}

} // namespace ceph
