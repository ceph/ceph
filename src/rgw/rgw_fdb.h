// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
      
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 International Business Machines Corp. (IBM)
 *      
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#ifndef CEPH_RGW_FDB_CONVERSION_H
 #define CEPH_RGW_FDB_CONVERSION_H

/* Welcome, brave adventurer! This where Ceph types are converted back and forth 
between FDB's types! If you have a user type to add, this is the place!
*/
 
#include "include/buffer.h"

#include <span>
#include <cstdint>
#include <string_view>

// Hook into zpp_bit's system-- we'll need to clarify the relationship between libfdb's 
// extensions and the underlying serializer's:
// This is where "list" (aka bufferlist) lives:
namespace ceph::buffer {

// Note that "list" here is therefore "ceph::buffer::list":
constexpr auto serialize(auto& ar, list& bl)
{
 // It would be nice to figure out how to do this with less copying, but the
 // mysteries of buffer::list are mysterious:

 std::string o;
 auto r = ar(o);

 bl.clear();
 bl.append(o);

 return r;
}

constexpr auto serialize(auto& ar, const list& bl)
{
 // Likewise, not really sure there's a way to avoid the extra copy
 // here:
 return ar(bl.to_str());
}

} // namespace ceph::buffer

#endif
