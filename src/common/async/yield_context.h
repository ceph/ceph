// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018-2024 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <boost/range/begin.hpp>
#include <boost/range/end.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>

#include "acconfig.h"

#include <optional>

/// optional-like wrapper for a boost::asio::yield_context. operations that take
/// an optional_yield argument will, when passed a non-empty yield context,
/// suspend this coroutine instead of the blocking the thread of execution

namespace detail { using opt_t = std::optional<boost::asio::yield_context>; }

struct optional_yield final : detail::opt_t
{
 using detail::opt_t::opt_t;

 public:
 operator bool() const noexcept { return has_value(); }

 /// IMPORTANT: UB if non-empty; check for value first:
 boost::asio::yield_context& get_yield_context() const noexcept { return const_cast<value_type&>(**this); }
};

/// type tag object to construct an empty optional_yield:
static constexpr auto& null_yield = std::nullopt;

inline std::ostream& operator<<(std::ostream& os, const optional_yield& x)
{
 if(x) {
  os << *x;
 }

 return os;
}

