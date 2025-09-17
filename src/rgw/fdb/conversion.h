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

#ifndef CEPH_FDB_CONVERSION_H
 #define CEPH_FDB_CONVERSION_H

#include "include/buffer.h"

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

namespace ceph::libfdb::detail {

// Utility function for dealing with buffers/stringlikes:
template <typename OutT>
inline void reify_from_buffer(std::span<const std::uint8_t> in, OutT& out)
{
#ifdef __cpp_lib_string_resize_and_overwrite
 if constexpr (requires { out.resize_and_overwrite(in.size(), [](char *, typename OutT::size_type sz) noexcept { return sz; }); }) {

  out.resize_and_overwrite(in.size(), [&in](char *out_p, OutT::size_type sz) noexcept {
            std::copy(in.data(), in.size() + in.data(), out_p);
	    return sz;
          });

    return;
 }
#endif

 // Can we get away with resizing?
 if constexpr (requires { out.resize(in.size()); }) {
  out.resize(in.size());
 }

 // Can it give us an iterator?
 if constexpr (requires { std::begin(out); }) {
  std::copy(in.data(), in.size() + in.data(), std::begin(out));
  return;
 } 

/* JFW: this needs work
 if constexpr (requires { out(std::uint8_t(0)); }) { <-- not sufficient, lots of things can take int!

use the concept std::output_iterator<decltype(out), std::uint8_t>() { decltype(out)(std::uint8_t(0)); }) {
  std::copy(buffer, buffer_len + buffer, out);
  return;
 }
*/
 static_assert("Ooops, I couldn't map a suitable output target.");
}

/* JFW:
// FoundationDB likes to deal in uint8_t buffers, so we'll need this somewhat often:
inline void buffer_to_string(const uint8_t *buffer, int buffer_len, std::string& out)
{
#ifdef __cpp_lib_string_resize_and_overwrite
      out.resize_and_overwrite(buffer_len, [&buffer, &buffer_len](char *out_p, std::string::size_type sz) noexcept {
            std::copy(buffer, buffer_len + buffer, out_p);
	    return sz;
          });
#else
    out.resize(buffer_len);
    std::copy(buffer, buffer_len + buffer, std::begin(out));
#endif
}
*/

} // namespace ceph::libfdb::detail

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
*/
namespace ceph::libfdb::to {

inline auto convert(const auto& from) -> std::vector<std::uint8_t>
{
 std::vector<std::uint8_t> out_data;
 
 zpp::bits::out out(out_data);

 // zpp::bits won't write a size if we start with a fixed size array:
 if constexpr (not std::is_array_v<decltype(from)>)
  out(from).or_throw();
 else 
  out(std::span(from, std::size(from))).or_throw();

 // Unfortunately, I see no real way to eliminate this extra copy:
 //return std::vector<std::uint8_t>(data, data.size() + data);
 return out_data;
}

/*
libfdb input type requirements:
-out] = zpp::bits::data_out();out] = zpp::bits::data_out();out] = zpp::bits::data_out(); could be const
- may be temporary: that is source input MUST be accessible (e.g. it could be a view) during
conversion
- the function converting these could well-be (and should be, when possible) implemented in
effect as a LENS view of the concrete, possibly temporary, input type. The same is true of the "input"
sources in the TO namespace: they are only types FROM THE DATABASE and user output targets which
are meant to be concrete. Do *NOT* serialize-TO something expecting the source to be alive, you will
unleash the fury of Chronos!

libfdb output type requirements:
- default constructable
- value constructable
- copy constructable
*/

/* JFW:
// identity:
inline auto convert(const std::int64_t in) 
{
 return in;
}

// identity:
inline auto convert(const std::span<const std::uint8_t> in)
{
 return in;
}

inline auto convert(const std::string_view in) -> std::span<const std::uint8_t>
{
 return { (const std::uint8_t *)in.data(), in.length() };
}

inline void convert(const auto& in, auto& out)
{
 static_assert("JFW: (to) you're asking me to convert that which cannot BE converted!");
}
*/

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

#endif
