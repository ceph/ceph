// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

/**
 * \file fmtlib formatters for some hobject.h classes
 */
#include <fmt/format.h>
#include <fmt/ranges.h>

#include "common/hobject.h"
#include "msg/msg_fmt.h"

// \todo reimplement
static inline void append_out_escaped(const std::string& in, std::string* out)
{
  for (auto i = in.cbegin(); i != in.cend(); ++i) {
    if (*i == '%' || *i == ':' || *i == '/' || *i < 32 || *i >= 127) {
      char buf[4];
      snprintf(buf, sizeof(buf), "%%%02x", (int)(unsigned char)*i);
      out->append(buf);
    } else {
      out->push_back(*i);
    }
  }
}

template <> struct fmt::formatter<hobject_t> {

  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext> auto format(const hobject_t& ho, FormatContext& ctx)
  {
    if (ho == hobject_t{}) {
      return fmt::format_to(ctx.out(), "MIN");
    }

    if (ho.is_max()) {
      return fmt::format_to(ctx.out(), "MAX");
    }

    std::string v;
    append_out_escaped(ho.nspace, &v);
    v.push_back(':');
    append_out_escaped(ho.get_key(), &v);
    v.push_back(':');
    append_out_escaped(ho.oid.name, &v);

    return fmt::format_to(ctx.out(), "{}:{:08x}:{}:{}", static_cast<uint64_t>(ho.pool),
			  ho.get_bitwise_key_u32(), v, ho.snap);
  }
};
