// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <iosfwd>
#include <iterator>
#include <fmt/ostream.h>
#include "dout.h"

/// \file dout_fmt.h
///
/// \brief dout macros to format log statements with libfmt
///
/// A set of dout macros taking a format string and its corresponding argument
/// list. Log output is written directly to the underlying std::ostream by
/// fmt::print() rather than exposing the stream for ostream operator
/// chaining.

// work around "warning: value computed is not used" with default dout_prefix
inline void dout_fmt_use_prefix(std::ostream&) {}

#define lsubdout_fmt(cct, sub, v, ...) \
  dout_impl(cct, ceph_subsys_##sub, v) \
  dout_fmt_use_prefix(dout_prefix); \
  fmt::print(*_dout, __VA_ARGS__); \
  *_dout << dendl

#define ldout_fmt(cct, v, ...) \
  dout_impl(cct, dout_subsys, v) \
  dout_fmt_use_prefix(dout_prefix); \
  fmt::print(*_dout, __VA_ARGS__); \
  *_dout << dendl

#define dout_fmt(v, ...) \
  ldout_fmt((dout_context), v, __VA_ARGS__)

#define ldpp_dout_fmt(dpp, v, ...) \
  if (decltype(auto) pdpp = (dpp); pdpp) { /* workaround -Wnonnull-compare for 'this' */ \
    dout_impl(pdpp->get_cct(), ceph::dout::need_dynamic(pdpp->get_subsys()), v) \
    pdpp->gen_prefix(*_dout); \
    fmt::print(*_dout, __VA_ARGS__); \
    *_dout << dendl; \
  }

// This is useful for debugging during development, please do NOT leave calls to it lying around in production:
inline void dout_trace(const std::string_view info, const std::string_view msg_tag = "dout-TRACE", const std::source_location sl = std::source_location::current())
{
 // ...this is an attempt only, as the extension is non-standard-- do NOT count on getting the values back:
#pragma push_macro("dout_subsys")
#pragma push_macro("dout_context")
  
 #define dout_subsys ceph_subsys_rgw
 #define dout_context g_ceph_context

 dout_fmt(0, "{} [{}]: {}", msg_tag, fmt::format("{}#{}", sl.file_name(), sl.function_name()), info);

#pragma pop_macro("dout_subsys")
#pragma pop_macro("dout_context")
} 

inline void dout_trace(const std::source_location sl = std::source_location::current())
{
 dout_trace("", "dout-TRACE", sl);
} 

