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
#include <cstring>
#include <functional>
#include <string_view>
#include <type_traits>
#include <system_error>

/* This module is the conversion boundary between C++ values and the byte buffers used by FoundationDB. Most of
the serialization work is delegated to zpp_bits, but this layer has two jobs: it gives libfdb's gadgets a place
to live (abstracting array/span behavior, callback outputs, error translation, etc.), and it provides a clean point
for future features-- a good example would be caller-owned memory, which we currently don't support but certainly
could. It also provides a fixed point where another serialization library could be swapped in. */

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

inline void convert(const std::span<const std::uint8_t>& from, versionstamp& to)
{
 to.store_result(from);
}

inline void convert(const std::span<const std::uint8_t>& from, auto& to)
{
 zpp::bits::in zpp_in(from);
 zpp_in(to).or_throw();
}

template <std::invocable<const char *, size_t> OutputFunction>
inline void convert(const std::span<const std::uint8_t>& in, OutputFunction& write_output_fn)
{
 write_output_fn(reinterpret_cast<const char *>(in.data()), in.size());
}

} // namespace ceph::libfdb::from

namespace ceph::libfdb::detail {

inline std::string decode_zpp_string_value(const std::span<const std::uint8_t> from)
{
 constexpr auto size_prefix = sizeof(zpp::bits::default_size_type);

 if (from.size() < size_prefix) {
  throw ceph::libfdb::libfdb_exception("unable to decode string value");
 }

 zpp::bits::default_size_type size = 0;
 std::memcpy(&size, from.data(), size_prefix);

 if (from.size() - size_prefix != size) {
  throw ceph::libfdb::libfdb_exception("unable to decode string value");
 }

 const auto data = reinterpret_cast<const char *>(size_prefix + from.data());

 return std::string(data, static_cast<std::string::size_type>(size));
}

template <typename ValueT>
inline std::pair<std::string, ValueT> to_decoded_kv_pair(const FDBKeyValue& kv)
{
 std::pair<std::string, ValueT> r;

 r.first.assign(reinterpret_cast<const char *>(kv.key),
                static_cast<std::string::size_type>(kv.key_length));

 try 
  {
     ceph::libfdb::from::convert(std::span<const std::uint8_t>(kv.value, kv.value_length), r.second);
 }
 catch (const std::system_error& e) {
     // Decode failures still surface as libfdb operation failures to callers.
     throw ceph::libfdb::libfdb_exception(e.what());
  }

 return r;
}

template <>
inline std::pair<std::string, std::string> to_decoded_kv_pair<std::string>(const FDBKeyValue& kv)
{
 return {
  std::string(reinterpret_cast<const char *>(kv.key),
              static_cast<std::string::size_type>(kv.key_length)),
  decode_zpp_string_value(std::span<const std::uint8_t>(kv.value, kv.value_length))
 };
}

} // namespace ceph::libfdb::detail

#endif
