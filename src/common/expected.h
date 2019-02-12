// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMMON_EXPECTED_H
#define CEPH_COMMON_EXPECTED_H


#include <string_view>
#include <system_error>
#include <type_traits>

#include <boost/system/system_error.hpp>

#include <fmt/format.h>

#include "common/dout.h"
#include "common/ceph_context.h"
#include "include/expected.hpp"

namespace ceph::detail {
template<typename T, typename E>
T valuer(tl::expected<T, E>& v) {
  return v.value();
}
template<typename E>
void valuer(tl::expected<void, E>& v) {
  return;
}
template<typename T, typename E>
T valuer(const tl::expected<T, E>& v) {
  return v.value();
}
template<typename E>
void valuer(const tl::expected<void, E>& v) {
  return;
}
template<typename T, typename E>
T valuer(tl::expected<T, E>&& v) {
  return std::move(v).value();
}
template<typename E>
void valuer(tl::expected<void, E>&& v) {
  return;
}
}

// A Try macro, similar to Rust.
//
// `auto foo = TRY(fallible())` sets foo to the successful alternative
// of fallible on success or returns the error code wrapped in
// tl::unexpected() on failure.
#define TRY(m) ({                               \
  auto res = (m);                               \
  if (!res.has_value())                         \
    return ::tl::unexpected(res.error());       \
  ::ceph::detail::valuer(m);				\
})

// A Try macro, similar to Rust. But for functions returning an error code
//
// `auto foo = TRYE(fallible())` sets foo to the successful alternative
// of fallible on success or returns the error code on failure.
#define TRYE(m) ({                               \
  auto res = (m);                               \
  if (!res.has_value())                         \
    return res.error();				\
  ::ceph::detail::valuer(m);				\
})

// Since we have so many places that check the return value, log an
// error, and return the error, make a macro for that.
//
// `foo = TRY_V(fallible(), cct, 0, "Error: {}")` acts as above but
// logs the format string and error. The format string must contain
// exactly one `{}`, where the error value will be substituted.
#define TRY_V(m, cct, p, f) ({						\
      auto res = (m);							\
  if (!res.has_value())							\
    ldout(cct, p) << fmt::format(f, res.error().message()) << dendl;	\
  return ::tl::unexpected(res.error());					\
  ::ceph::detail::valuer(m);							\
})

// Like the TRY macro, but throw rather than returning the error code.
namespace ceph {
template<typename T>
auto try_throw(tl::expected<T, boost::system::error_code>&& v) {
  if (!v)
    throw boost::system::system_error(v.error());
  if constexpr (!std::is_void_v<T>)
    return *std::move(v);
  else
    return;
}

template<typename T>
auto try_throw(tl::expected<T, std::error_code>&& v) {
  if (!v)
    throw std::system_error(v.error());
  if constexpr (!std::is_void_v<T>)
    return *std::move(v);
  else
    return;
}
}

#endif
