// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <system_error>
#include "include/buffer.h"
#include "common/errno.h"
#include "common/ceph_json.h"
#include "rgw_zone.h"
#include "driver/immutable_config/store.h"
#include "store.h"

namespace rgw::sal {

namespace {

struct DecodedConfig {
  RGWZoneGroup zonegroup;
  RGWZoneParams zone;
  RGWPeriodConfig period_config;

  void decode_json(JSONObj *obj)
  {
    JSONDecoder::decode_json("zonegroup", zonegroup, obj);
    JSONDecoder::decode_json("zone", zone, obj);
    JSONDecoder::decode_json("period_config", period_config, obj);
  }
};

static void parse_config(const DoutPrefixProvider* dpp, const char* filename)
{
  bufferlist bl;
  std::string errmsg;
  int r = bl.read_file(filename, &errmsg);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "failed to read json config file '" << filename
        << "': " << errmsg << dendl;
    throw std::system_error(-r, std::system_category());
  }

  JSONParser p;
  if (!p.parse(bl.c_str(), bl.length())) {
    ldpp_dout(dpp, 0) << "failed to parse json config file" << dendl;
    throw std::system_error(make_error_code(std::errc::invalid_argument));
  }

  DecodedConfig config;
  try {
    decode_json_obj(config, &p);
  } catch (const JSONDecoder::err& e) {
    ldpp_dout(dpp, 0) << "failed to decode JSON input: " << e.what() << dendl;
    throw std::system_error(make_error_code(std::errc::invalid_argument));
  }
}

void sanity_check_config(const DoutPrefixProvider* dpp, DecodedConfig& config)
{
  if (config.zonegroup.id.empty()) {
    config.zonegroup.id = "default";
  }
  if (config.zonegroup.name.empty()) {
    config.zonegroup.name = "default";
  }
  if (config.zonegroup.api_name.empty()) {
    config.zonegroup.api_name = config.zonegroup.name;
  }

  if (config.zone.id.empty()) {
    config.zone.id = "default";
  }
  if (config.zone.name.empty()) {
    config.zone.name = "default";
  }

  // add default placement if it doesn't exist
  rgw_pool pool;
  RGWZonePlacementInfo placement;
  placement.storage_classes.set_storage_class(
      RGW_STORAGE_CLASS_STANDARD, &pool, nullptr);
  config.zone.placement_pools.emplace("default-placement",
                                      std::move(placement));

  std::set<rgw_pool> pools;
  int r = rgw::init_zone_pool_names(dpp, null_yield, pools, config.zone);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "failed to set default zone pool names" << dendl;
    throw std::system_error(-r, std::system_category());
  }

  // verify that config.zonegroup only contains config.zone
  if (config.zonegroup.zones.size() > 1) {
    ldpp_dout(dpp, 0) << "zonegroup cannot contain multiple zones" << dendl;
    throw std::system_error(make_error_code(std::errc::invalid_argument));
  }

  if (config.zonegroup.zones.size() == 1) {
    auto z = config.zonegroup.zones.begin();
    if (z->first != config.zone.id) {
      ldpp_dout(dpp, 0) << "zonegroup contains unknown zone id="
          << z->first << dendl;
      throw std::system_error(make_error_code(std::errc::invalid_argument));
    }
    if (z->second.id != config.zone.id) {
      ldpp_dout(dpp, 0) << "zonegroup contains unknown zone id="
          << z->second.id << dendl;
      throw std::system_error(make_error_code(std::errc::invalid_argument));
    }
    if (z->second.name != config.zone.name) {
      ldpp_dout(dpp, 0) << "zonegroup contains unknown zone name="
          << z->second.name << dendl;
      throw std::system_error(make_error_code(std::errc::invalid_argument));
    }
    if (config.zonegroup.master_zone != config.zone.id) {
      ldpp_dout(dpp, 0) << "zonegroup contains unknown master_zone="
          << config.zonegroup.master_zone << dendl;
      throw std::system_error(make_error_code(std::errc::invalid_argument));
    }
  } else {
    // add the zone to the group
    const bool is_master = true;
    const bool read_only = false;
    std::list<std::string> endpoints;
    std::list<std::string> sync_from;
    std::list<std::string> sync_from_rm;
    rgw::zone_features::set enable_features;
    rgw::zone_features::set disable_features;

    enable_features.insert(rgw::zone_features::supported.begin(),
                           rgw::zone_features::supported.end());

    int r = rgw::add_zone_to_group(dpp, config.zonegroup, config.zone,
                                   &is_master, &read_only, endpoints,
                                   nullptr, nullptr, sync_from, sync_from_rm,
                                   nullptr, std::nullopt,
                                   enable_features, disable_features);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "failed to add zone to zonegroup: "
          << cpp_strerror(r) << dendl;
      throw std::system_error(-r, std::system_category());
    }

    config.zonegroup.enabled_features.insert(rgw::zone_features::enabled.begin(),
                                             rgw::zone_features::enabled.end());
  }

  // insert the default placement target if it doesn't exist
  auto target = RGWZoneGroupPlacementTarget{.name = "default-placement"};
  config.zonegroup.placement_targets.emplace(target.name, target);
  if (config.zonegroup.default_placement.name.empty()) {
    config.zonegroup.default_placement.name = target.name;
  }
}

} // anonymous namespace

auto create_json_config_store(const DoutPrefixProvider* dpp,
                              const std::string& filename)
    -> std::unique_ptr<ConfigStore>
{
  DecodedConfig config;
  parse_config(dpp, filename.c_str());
  sanity_check_config(dpp, config);
  return create_immutable_config_store(dpp, config.zonegroup, config.zone,
                                       config.period_config);
}

} // namespace rgw::sal
