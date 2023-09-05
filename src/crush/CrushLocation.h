// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CRUSH_LOCATION_H
#define CEPH_CRUSH_LOCATION_H

#include <iosfwd>
#include <map>
#include <string>
#if FMT_VERSION >= 90000
#include <fmt/ostream.h>
#endif

#include "common/ceph_mutex.h"
#include "include/common_fwd.h"

namespace ceph::crush {

class CrushLocation {
public:
  explicit CrushLocation(CephContext *c) : cct(c) {
    init_on_startup();
  }

  int update_from_conf();  ///< refresh from config
  int update_from_hook();  ///< call hook, if present
  int init_on_startup();

  std::multimap<std::string,std::string> get_location() const;

private:
  int _parse(const std::string& s);
  CephContext *cct;
  std::multimap<std::string,std::string> loc;
  mutable ceph::mutex lock = ceph::make_mutex("CrushLocation");
};

std::ostream& operator<<(std::ostream& os, const CrushLocation& loc);
}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<ceph::crush::CrushLocation> : fmt::ostream_formatter {};
#endif

#endif
