// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */

#ifndef CEPH_FDB_KEY_H
 #define CEPH_FDB_KEY_H

#include <concepts>
#include <string_view>
#include <type_traits>

namespace ceph::libfdb::concepts {

template <typename StringViewLikeT>
concept stringview_convertible =
 std::convertible_to<std::remove_reference_t<StringViewLikeT> const&, std::string_view>;

template <typename KeyViewT>
concept libfdb_key_view = std::same_as<std::remove_cvref_t<KeyViewT>, std::string_view>;

} // namespace ceph::libfdb::concepts

namespace ceph::libfdb::detail {

// String-view-convertible keys need no adapter; other key types supply this
// operation through ADL. Key access borrows storage without copying the provider.
constexpr std::string_view libfdb_key_view(const concepts::stringview_convertible auto& key)
{
 return std::string_view(key);
}

template <typename KeyT>
concept libfdb_key_like =
 requires(const KeyT& key) {
  { libfdb_key_view(key) } -> std::same_as<std::string_view>;
 };

template <libfdb_key_like KeyT>
constexpr std::string_view as_libfdb_key_view(const KeyT& key)
{
 return libfdb_key_view(key);
}

} // namespace ceph::libfdb::detail

namespace ceph::libfdb::concepts {

template <typename KeyT>
concept libfdb_key = ::ceph::libfdb::detail::libfdb_key_like<KeyT>;

} // namespace ceph::libfdb::concepts

#endif
