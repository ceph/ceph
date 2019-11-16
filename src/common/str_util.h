// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMMON_STR_UTIL_H
#define CEPH_COMMON_STR_UTIL_H

#include <array>
#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>

namespace ceph {
// Because *somebody* built an entire operating system on the idea of
// NUL-terminated strings, and you don't want to have to allocate
// memory just to pass a filename to open(2).
//
// If the total length of source is less than N, copies the contents
// of source to dest, appending a NUL and returning true.
//
// Otherwise, returns false.
template<std::size_t N>
bool nul_terminated_copy(std::string_view source,
			 char* dest)
{
  if (source.size() < N) {
    auto i = std::copy(std::cbegin(source), std::cend(source),
		       dest);
    *i = '\0';
    return true;
  } else {
    return false;
  }
}


// Same thing with a C-style array
template<std::size_t N>
bool nul_terminated_copy(std::string_view source,
			 char (&dest)[N])
{
  if constexpr (N == 0) {
    return true;
  }
  return nul_terminated_copy<N>(source, &dest[0]);
}

// Same thing with a standard array
template<std::size_t N>
bool nul_terminated_copy(std::string_view source,
			 std::array<char, N>& dest)
{
  if constexpr (N == 0) {
    return true;
  }
  return nul_terminated_copy<N>(source, dest.data());
}

// Same as above, except with RVO instead of caller-provided-storage.
//
// If the total length of source is less than N, returns an optional
// array containing a NUL-terminated copy.
//
// Otherwise, returns an empty optional.
template<std::size_t N>
std::optional<std::array<char, N>>
nul_terminated_copy(std::string_view source)
{
  std::optional<std::array<char, N>> dest(std::in_place);
  auto b = nul_terminated_copy<N>(source, dest->data());
  if (!b)
    dest.reset();
  return dest;
}
}
#endif // CEPH_COMMON_STR_UTIL_H
