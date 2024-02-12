// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

/**
 * \file fmtlib formatters for some object.h structs
 */
#include <fmt/compile.h>
#include <fmt/format.h>

#include "object.h"


template <>
struct fmt::formatter<snapid_t> {

  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const snapid_t& snp, FormatContext& ctx) const
  {
    if (snp == CEPH_NOSNAP) {
      return fmt::format_to(ctx.out(), "head");
    }
    if (snp == CEPH_SNAPDIR) {
      return fmt::format_to(ctx.out(), "snapdir");
    }
    return fmt::format_to(ctx.out(), FMT_COMPILE("{:x}"), snp.val);
  }
};
