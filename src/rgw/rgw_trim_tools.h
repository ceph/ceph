
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

struct rgw_zone_id;

class RGWTrimSIPMgr {
public:
  virtual ~RGWTrimSIPMgr() {}

  virtual RGWCoroutine *init_cr() = 0;
  virtual RGWCoroutine *get_targets_info_cr(std::vector<std::string> *min_shard_markers,
                                            std::vector<std::string> *min_source_pos,
                                            std::set<std::string> *sip_targets,
                                            std::set<rgw_zone_id> *target_zones) = 0;
  virtual RGWCoroutine *set_min_source_pos_cr(int shard_id, const std::string& pos) = 0;
};


class RGWTrimTools {
public:
  static RGWTrimSIPMgr *get_trim_sip_mgr(rgw::sal::RGWRadosStore *store,
                                         const std::string& sip_name,
                                         std::optional<std::string> sip_instance);
};
