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

#ifndef CEPH_FDB_CONVERSION_H
 #define CEPH_FDB_CONVERSION_H

#include "base.h"

#include "zpp_bits.h"

#include <span>
#include <string>
#include <vector>
#include <cstdint>
#include <concepts>
#include <functional>
#include <string_view>
#include <type_traits>
#include <system_error>

/* This module is for converting internal types "owned" by FoundationDB. They've initially been implemented in 
a conversion namespace with overloads, which is the same way that user conversions work, but especially with Concepts
this technique doesn't have to be used-- it is likely possible to avoid default construction and possible extra copies.
Since I'm building a prototype right now, it's more important to me on this pass to make things understandable and
clear, because past experience informs me that it's better to have a clear understanding of what the goals of "to"
and "from" versions actually are in relationship to user-level types than to have every nanosecond of performance
be available on day one.

The target of "to" conversions is not a USER type, but rather the FUNCTIONS provided inside of libfdb-- users
should NOT see the output of these or have to handle them outside of tests or edge-cases (and even then, I doubt it's
needed, though I won't work hard to stop it). 

I'm hoping that later down the line I can sit and spend more time with this-- it would be nice, for example, if we could
use memory provided by the caller.

Additionally, this mechanism is mostly obviated by forwarding the work to zpp_bits, but keeping it here provides an additional
hook "ahead" of that library, and indeed eliminates any actual dependency on it-- we use it to smooth out a few areas where
zpp_bits is designed for serialization ONLY rather than also playing nice with some conversions, especially those where the
input and output sizes of an array may not match.
*/
namespace ceph::libfdb::to {

inline auto convert(const auto& from, std::vector<std::uint8_t>& out_data) -> std::span<const std::uint8_t>
{
 out_data.clear();
 
 zpp::bits::out out(out_data);

 // zpp::bits won't write a size if we start with a fixed size array:
 // (see dynamic_extent):
 if constexpr (std::is_array_v<decltype(from)>) {
     out(std::span(from, std::size(from))).or_throw();

     return out_data;
 }

 out(from).or_throw();

 return out_data;
}

inline auto convert(const auto& from) -> std::vector<std::uint8_t>
{
 std::vector<std::uint8_t> out_data;
 convert(from, out_data);

 return out_data;
}

} // namespace ceph::libfdb::to

/* Map from FDB inputs from FDB TYPE to CONCRETE (i.e. copyable) userland types. Do NOT add 
non-FDB input sources here (or any non-matching user output sources). Do NOT add
non-owning targets, lest Antevorda be angered!: */
namespace ceph::libfdb::from {

inline void convert(const std::span<const std::uint8_t>& from, auto& to)
{
 zpp::bits::in zpp_in(from);
 zpp_in(to).or_throw();
}

template <std::invocable<const char *, size_t> OutputFunction>
inline void convert(const std::span<const std::uint8_t>& in, OutputFunction& fn)
{
 fn((const char *)in.data(), in.size()); 
}

} // namespace ceph::libfdb::from

namespace ceph::libfdb::detail {

template <typename ValueT>
inline std::pair<std::string, ValueT> to_decoded_kv_pair(const FDBKeyValue& kv)
{
 std::pair<std::string, ValueT> r;

 r.first.assign((const char *)kv.key, static_cast<std::string::size_type>(kv.key_length));

 try 
  {
     ceph::libfdb::from::convert(std::span<const std::uint8_t>(kv.value, kv.value_length), r.second);
  }
 catch (const std::system_error& e) {
     // Translate from underlying (e.g. zpp_bits) conversion error into the right type:
     // This is a bit bound to zpp_bits for the moment, but there's not a more direct way to distinguish this
     // from a different system_error. We could do that, by using zpp_bits' non-throwing modes and throwing a
     // special type, but this will do for now.
     throw ceph::libfdb::libfdb_exception(e.what());
  }

 return r;
}

} // namespace ceph::libfdb::detail

#endif
