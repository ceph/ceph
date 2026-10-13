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
#include <string_view>

#include <cstdint>
#include <concepts>
#include <cstring>
#include <functional>
#include <type_traits>
#include <system_error>

/* This is the conversion boundary between C++ values and FoundationDB byte
 * buffers. Serialization is delegated to zpp_bits; this layer adapts callback
 * outputs, translates errors, and leaves a clean extension point for future
 * caller-owned memory or a different serializer. */

namespace ceph::libfdb::to {

inline auto convert(const auto& from,
                    std::vector<std::uint8_t>& out_data)
 -> std::span<const std::uint8_t>
{
 out_data.clear();

 zpp::bits::out out(out_data);
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

namespace detail {

template <typename OutputFunction>
concept output_function =
 requires(OutputFunction& write_output_fn, const char *data, std::size_t size) {
  { std::invoke(write_output_fn, data, size) } -> std::same_as<void>;
};

} // namespace detail

inline void convert(const std::span<const std::uint8_t>& from, versionstamp& to)
{
 to.store_result(from);
}

template <typename ToT>
requires (!std::invocable<ToT&, const char *, std::size_t>)
inline void convert(const std::span<const std::uint8_t>& from, ToT& to)
{
 zpp::bits::in zpp_in(from);
 zpp_in(to).or_throw();
}

template <typename OutputFunction>
requires detail::output_function<OutputFunction>
inline void convert(const std::span<const std::uint8_t>& in,
                    OutputFunction& write_output_fn)
{
 const auto input = ceph::libfdb::detail::as_string_view(in);

 std::invoke(write_output_fn, input.data(), input.size());
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

 const auto data = reinterpret_cast<const char *>(from.data() + size_prefix);

 return std::string(data, static_cast<std::string::size_type>(size));
}

template <typename ValueT>
inline std::pair<std::string, ValueT> to_decoded_kv_pair(const FDBKeyValue& kv)
try
{
 std::pair<std::string, ValueT> r;

 r.first = key_view(kv);
 ceph::libfdb::from::convert(value_view(kv), r.second);

 return r;
}
catch (const std::system_error& e)
{
 // Decode failures still surface as libfdb operation failures to callers:
 throw ceph::libfdb::libfdb_exception(e.what());
}

template <>
inline std::pair<std::string, std::string> to_decoded_kv_pair<std::string>(const FDBKeyValue& kv)
{
 return {
  std::string(key_view(kv)),
  decode_zpp_string_value(value_view(kv))
 };
}

} // namespace ceph::libfdb::detail

#endif
