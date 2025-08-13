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

#include "fdb/fdb.h"

#include <span>
#include <cstdint>
#include <string_view>

namespace ceph::libfdb::to {

// Be aware-- c_str() on buffer::list has different semantics than on std::string--
// for this reason, the parameter cannot itself be const (it's entirely fine to do that
// computation earlier and pass a span or string_view yourself if you don't want the 
// buffer::list to potentially mutate itself):
inline auto convert(ceph::buffer::list& bl) -> std::span<const std::uint8_t> {
// JFW: avoid making copy of bl in param
 return { (const std::uint8_t *)bl.c_str(), bl.length() };
}

} // namespace ceph::libfdb::to

namespace ceph::libfdb::from {

inline void convert(const std::span<const std::uint8_t>& in, ceph::buffer::list& out) {
 out.clear();
 out.append((char *)in.data(), in.size());
} 

} // namespace ceph::libfdb::from

#endif
