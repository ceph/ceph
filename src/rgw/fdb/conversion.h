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

inline void convert(const std::span<const std::uint8_t>& from, versionstamp& to)
{
 to.store_result(from);
}

inline void convert(const std::span<const std::uint8_t>& from, auto& to)
{
 zpp::bits::in zpp_in(from);
 zpp_in(to).or_throw();
}

template <std::invocable<const char *, std::size_t> OutputFunction>
inline void convert(const std::span<const std::uint8_t>& in,
                    OutputFunction& write_output_fn)
{
 const auto input = detail::as_string_view(in);

 write_output_fn(input.data(), input.size());
}

} // namespace ceph::libfdb::from

namespace ceph::libfdb::detail {

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

} // namespace ceph::libfdb::detail

#endif
