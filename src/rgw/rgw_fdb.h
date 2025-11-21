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

namespace ceph {

constexpr auto serialize(auto&  archive, ceph::buffer::list& to)
{
 return archive(std::span(to.c_str(), to.length()));
}

constexpr auto serialize(auto& archive, const ceph::buffer::list& from)
{
 // buffer::list c_str() may cause compaction, so const may not be something
 // we can really promise:
 auto& bl = const_cast<ceph::buffer::list&>(from);

 return archive(std::span(bl.c_str(), bl.length()));
}

inline auto serialize(ceph::buffer::list& from) -> std::vector<std::uint8_t> {
 return std::vector<std::uint8_t>(from.c_str(), from.length() + from.c_str());
}

inline void convert(const std::span<const std::uint8_t>& in, ceph::buffer::list& out) {
 // JFW: is this the most efficient/recommended way to populate a buffer::list like this..?
 out.clear();
 out.append((char *)in.data(), in.size());
}

} // namespace ceph

/*JFW:
namespace ceph::libfdb::to {

// Be aware-- c_str() on buffer::list has different semantics than on std::string--
// calling c_str() can potentially cause it to mutate, so we have to lie about its being
// const. :-/ If you know of a better way to do this, please do change this!
inline auto convert(const ceph::buffer::list& from) -> std::vector<std::uint8_t> {
 ceph::buffer::list& in = const_cast<ceph::buffer::list&>(from);
 return std::vector<std::uint8_t>(in.c_str(), in.length() + in.c_str());
}

} // namespace ceph::libfdb::to

namespace ceph::libfdb::from {

inline void convert(const std::span<const std::uint8_t>& in, ceph::buffer::list& out) {
 // JFW: is this the most efficient/recommended way to populate a buffer::list like this..?
 out.clear();
 out.append((char *)in.data(), in.size());
}

} // namespace ceph::libfdb::from
*/

#endif
