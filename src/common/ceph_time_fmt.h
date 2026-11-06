// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#pragma once

#include <time.h>

#include <fmt/chrono.h>

#include "ceph_time.h"



// concept helpers for the formatters:

template <typename TimeP>
concept UnsteadyTimepoint = !TimeP::clock::is_steady &&
    !std::same_as<typename TimeP::clock, std::chrono::system_clock> &&
    !std::same_as<typename TimeP::clock, std::chrono::utc_clock>;

template <typename TimeP>
concept SteadyTimepoint = TimeP::clock::is_steady;

namespace fmt {
template <UnsteadyTimepoint T>
struct formatter<T> {
  constexpr auto parse(fmt::format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const T& t, FormatContext& ctx) const
  {
    using namespace std::literals;
    std::tm bdt;
    time_t tt = T::clock::to_time_t(t);
    // C++26 supposedly has `std::localtime_r` in `<ctime>` but our
    // libstdc++ doesn't.
    localtime_r(&tt, &bdt);
    auto us = duration_cast<std::chrono::microseconds>(t.time_since_epoch() % 1s);
    return fmt::format_to(ctx.out(), "{:%FT%T}.{:06}{:%z}", bdt, us.count(), bdt);
  }
};

template <SteadyTimepoint T>
struct formatter<T> {
  constexpr auto parse(fmt::format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const T& t, FormatContext& ctx) const
  {
    return fmt::format_to(
	ctx.out(), "{}s",
	std::chrono::duration<double>(t.time_since_epoch()).count());
  }
};
}  // namespace fmt
