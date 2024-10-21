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

#include <boost/tuple/tuple.hpp>

#include "encoding.h"

namespace boost::tuples {

//triple boost::tuple
template<class A, class B, class C>
inline void encode(const boost::tuple<A, B, C> &t, bufferlist& bl)
{
  using ceph::encode;

  encode(boost::get<0>(t), bl);
  encode(boost::get<1>(t), bl);
  encode(boost::get<2>(t), bl);
}
template<class A, class B, class C>
inline void decode(boost::tuple<A, B, C> &t, bufferlist::const_iterator &bp)
{
  using ceph::decode;

  decode(boost::get<0>(t), bp);
  decode(boost::get<1>(t), bp);
  decode(boost::get<2>(t), bp);
}

} // namespace boost::tuples
