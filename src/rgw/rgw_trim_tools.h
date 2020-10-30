
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>
#include <optional>
#include <vector>
#include <set>

class RGWCoroutine;
class RGWRados;
class RGWHTTPManager;
namespace rgw { namespace sal {
  class RGWRadosStore;
} }


class RGWTrimTools {
public:
  static RGWCoroutine* get_sip_targets_info_cr(rgw::sal::RGWRadosStore *store,
                                               const std::string& sip_name,
                                               std::optional<std::string> sip_instance,
                                               std::vector<std::string> *min_shard_markers,
                                               std::set<std::string> *sip_targets);
};
