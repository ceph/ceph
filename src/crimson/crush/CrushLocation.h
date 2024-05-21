// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <iosfwd>
#include <map>
#include <string>
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

#include <seastar/core/seastar.hh>

namespace crimson::crush {

class CrushLocation {
public:
  explicit CrushLocation() {
  }

  seastar::future<> update_from_conf();  ///< refresh from config
  seastar::future<> init_on_startup();
  seastar::future<> update_from_hook();  ///< call hook, if present

  std::multimap<std::string, std::string> get_location() const;

private:
  void _parse(const std::string& s);
  std::multimap<std::string, std::string> loc;
};

std::ostream& operator<<(std::ostream& os, const CrushLocation& loc);
}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::crush::CrushLocation> : fmt::ostream_formatter {};
#endif
