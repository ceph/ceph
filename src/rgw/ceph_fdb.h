// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025-2026 International Business Machines Corp. (IBM)
 *      
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#include <fmt/ranges.h>

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

inline auto convert(const ceph::buffer::list& from) -> std::vector<std::uint8_t>
{
 // uint32_t is what zpp::bits uses by default, but we could fiddle with that:
 std::vector<std::uint8_t> out_data(sizeof(std::uint32_t) + 1 + std::size(from)); 
 zpp::bits::out out(out_data);

 auto ptr = const_cast<ceph::buffer::list&>(from).c_str();

 std::span from_span(ptr, std::size(from) + ptr);

 out(from_span).or_throw();

 return out_data;
}

} // namespace ceph::libfdb::to

namespace ceph::libfdb::from {

inline void convert(const std::span<const std::uint8_t>& from, ceph::buffer::list& to) {
 to.append(sizeof(std::uint32_t) + reinterpret_cast<const char *>(from.data()), 
           std::size(from) - sizeof(std::uint32_t));
}

} // namespace ceph::libfdb::from

namespace ceph::libfdb::detail {

constexpr auto as_fdb_span(ceph::buffer::list& bl)
{
 // The c_str() function makes the buffer::list contiguous, but also appends an unwelcome NULL character--
 // maybe there's a way around this? Or maybe it's best to patch buffer::list? Here, we adjust with - 1
 // for now:
 auto p = bl.c_str();
 return std::span<const std::uint8_t>((const std::uint8_t *)p, std::size(bl) - 1);
}

} // namespace ceph::libfdb::detail

namespace ceph::buffer {

constexpr auto serialize(auto &archive, ceph::buffer::list &target)
{
 // This should be revisited after the library is separated from the larger
 // surrounding project-- essentially, we can't write directly /into/ the buffer::list
 // that I know of; yet, it would obviously be great to eliminate the copy
 // here. I believe that somewhere in zpp::bits there's probably a way to get the
 // archive to call a custom function-- but it is not clear to me at this time and
 // I need to move on for now, unfortunately:
 std::vector<std::uint8_t> out;

 auto r = archive(out);

fmt::format("JFW: out ({}):\n{}\n-----", out.size(), out);
 // JFW: this is really annoying, but again buffer::list is not something I find
 // easy to wrangle-- I'm not sure there's a straightforward way to just assign
 // a new value... so if you know what that is, please improve this:
 target.clear();
 target.append(out);

 return r;
}

constexpr auto serialize(auto &archive, const ceph::buffer::list& src)
{
 auto p = const_cast<ceph::buffer::list&>(src).c_str();
 return archive(std::span { p, std::size(src) });
}

} // namespace ceph::buffer

#endif
