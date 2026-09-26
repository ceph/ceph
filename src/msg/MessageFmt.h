// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_MESSAGE_FMT_H
#define CEPH_MESSAGE_FMT_H

#include <sstream>

#include <fmt/core.h> // for FMT_VERSION
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

#include "Message.h"

namespace fmt {
// placed in the fmt namespace due to an ADL bug in g++ < 12
// (https://gcc.gnu.org/bugzilla/show_bug.cgi?id=92944).
// Specifically - gcc pre-12 can't handle two templated specializations of
// the formatter if in two different namespaces.
template <std::derived_from<Message> M>
struct formatter<M> {
  constexpr auto parse(fmt::format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const M& m, FormatContext& ctx) const {
    std::ostringstream oss;
    m.print(oss);
    if (auto ver = m.get_header().version; ver) {
      return fmt::format_to(ctx.out(), "{} v{}", oss.str(), (uint32_t)ver);
    } else {
      return fmt::format_to(ctx.out(), "{}", oss.str());
    }
  }
};
}  // namespace fmt

#endif
