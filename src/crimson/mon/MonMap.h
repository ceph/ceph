// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>
#include "mon/MonMap.h"

namespace ceph::common {
  class ConfigProxy;
}

namespace ceph::mon {

class MonMap final : public ::MonMap
{
  /// read from encoded monmap file
  seastar::future<> read_monmap(const std::string& monmap);
  /// try to build monmap with different settings, like
  /// mon_host, mon* sections, and mon_dns_srv_name
  seastar::future<> build_monmap(const ConfigProxy& conf);
  /// initialize monmap by resolving given service name
  seastar::future<> init_with_dns_srv(const std::string& name);
public:
  /// build initial monmap with different sources.
  seastar::future<> build_initial(const ConfigProxy& conf);
};

} // namespace ceph::mon
