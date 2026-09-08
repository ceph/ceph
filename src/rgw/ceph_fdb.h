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

#ifndef CEPH_FDB_H
 #define CEPH_FDB_H

/* Welcome, brave adventurer! This where Ceph types are converted back and forth 
between FDB's types! If you have a user type to add, this is the place!
*/
 
#include "include/buffer.h"

#include "fdb/fdb.h"

#include <span>
#include <vector>
#include <cstdint>
#include <string_view>

/*** Conversions: */

namespace ceph::libfdb::detail {

auto as_fdb_span(ceph::buffer::list& bl)
{
 // c_str() makes the buffer::list contiguous. Use length(), not C-string
 // rules, because buffer::list may contain arbitrary bytes:
 auto p = bl.c_str();

 return std::span<const std::uint8_t>(
          reinterpret_cast<const std::uint8_t *>(p), bl.length());
}

} // namespace ceph::libfdb::detail

namespace ceph::buffer {

auto serialize(auto& archive, ceph::buffer::list& target)
{
 // This should be revisited after the library is separated from the larger
 // surrounding project-- essentially, we can't write directly /into/ the buffer::list
 // that I know of; yet, it would obviously be great to eliminate the copy
 // here. I believe that somewhere in zpp::bits there's probably a way to get the
 // archive to call a custom function-- but it is not clear to me at this time and
 // I need to move on for now, unfortunately:
 std::vector<std::uint8_t> out;

 auto r = archive(out);

 // JFW: this is really annoying, but again buffer::list is not something I find
 // easy to wrangle-- I'm not sure there's a straightforward way to just assign
 // a new value... so if you know what that is, please improve this:
 target.clear();
 target.append(out);

 return r;
}

auto serialize(auto& archive, const ceph::buffer::list& src)
{
 std::vector<std::uint8_t> bytes;

 auto i = std::cbegin(src);

 // buffer::list copy already resizes the vector:
 i.copy(src.length(), bytes);

 return archive(std::span<const std::uint8_t> { bytes.data(), bytes.size() });
}

// This transliteration is one-way: we read from a buffer::ptr to an archive; writing 
// to a ceph::buffer::ptr as output seems pointless to me, as we already support spans 
// and buffer::list-- I feel in fact like we would be courting danger we don't need as it
// is a non-owning structure. 
auto serialize(auto& archive, const ceph::buffer::ptr& src)
{
 std::span<std::uint8_t> src_span((std::uint8_t *)src.c_str(), src.length());

 return archive(src_span);
}

} // namespace ceph::buffer

#endif
