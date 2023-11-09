// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_zone.h"
#include "rgw_realm_watcher.h"
#include "rgw_sal_config.h"
#include "rgw_sync.h"

#include "services/svc_zone.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;
using namespace rgw_zone_defaults;

RGWMetaSyncStatusManager::~RGWMetaSyncStatusManager(){}

#define FIRST_EPOCH 1

struct RGWAccessKey;

/// Generate a random uuid for realm/period/zonegroup/zone ids
static std::string gen_random_uuid()
{
  uuid_d uuid;
  uuid.generate_random();
  return uuid.to_string();
}

void RGWDefaultZoneGroupInfo::dump(Formatter *f) const {
  encode_json("default_zonegroup", default_zonegroup, f);
}

void RGWDefaultZoneGroupInfo::decode_json(JSONObj *obj) {

  JSONDecoder::decode_json("default_zonegroup", default_zonegroup, obj);
  /* backward compatability with region */
  if (default_zonegroup.empty()) {
    JSONDecoder::decode_json("default_region", default_zonegroup, obj);
  }
}

int RGWZoneGroup::create_default(const DoutPrefixProvider *dpp, optional_yield y, bool old_format)
{
  name = default_zonegroup_name;
  api_name = default_zonegroup_name;
  is_master = true;

  RGWZoneGroupPlacementTarget placement_target;
  placement_target.name = "default-placement";
  placement_targets[placement_target.name] = placement_target;
  default_placement.name = "default-placement";

  RGWZoneParams zone_params(default_zone_name);

  int r = zone_params.init(dpp, cct, sysobj_svc, y, false);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "create_default: error initializing zone params: " << cpp_strerror(-r) << dendl;
    return r;
  }

  r = zone_params.create_default(dpp, y);
  if (r < 0 && r != -EEXIST) {
    ldpp_dout(dpp, 0) << "create_default: error in create_default  zone params: " << cpp_strerror(-r) << dendl;
    return r;
  } else if (r == -EEXIST) {
    ldpp_dout(dpp, 10) << "zone_params::create_default() returned -EEXIST, we raced with another default zone_params creation" << dendl;
    zone_params.clear_id();
    r = zone_params.init(dpp, cct, sysobj_svc, y);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "create_default: error in init existing zone params: " << cpp_strerror(-r) << dendl;
      return r;
    }
    ldpp_dout(dpp, 20) << "zone_params::create_default() " << zone_params.get_name() << " id " << zone_params.get_id()
		   << dendl;
  }
  
  RGWZone& default_zone = zones[zone_params.get_id()];
  default_zone.name = zone_params.get_name();
  default_zone.id = zone_params.get_id();
  master_zone = default_zone.id;

  // initialize supported zone features
  default_zone.supported_features.insert(rgw::zone_features::supported.begin(),
                                         rgw::zone_features::supported.end());
  // enable default zonegroup features
  enabled_features.insert(rgw::zone_features::enabled.begin(),
                          rgw::zone_features::enabled.end());
  
  r = create(dpp, y);
  if (r < 0 && r != -EEXIST) {
    ldpp_dout(dpp, 0) << "error storing zone group info: " << cpp_strerror(-r) << dendl;
    return r;
  }

  if (r == -EEXIST) {
    ldpp_dout(dpp, 10) << "create_default() returned -EEXIST, we raced with another zonegroup creation" << dendl;
    id.clear();
    r = init(dpp, cct, sysobj_svc, y);
    if (r < 0) {
      return r;
    }
  }

  if (old_format) {
    name = id;
  }

  post_process_params(dpp, y);

  return 0;
}

int RGWZoneGroup::equals(const string& other_zonegroup) const
{
  if (is_master && other_zonegroup.empty())
    return true;

  return (id  == other_zonegroup);
}

int RGWZoneGroup::add_zone(const DoutPrefixProvider *dpp, 
                           const RGWZoneParams& zone_params, bool *is_master, bool *read_only,
                           const list<string>& endpoints, const string *ptier_type,
                           bool *psync_from_all, list<string>& sync_from, list<string>& sync_from_rm,
                           string *predirect_zone, std::optional<int> bucket_index_max_shards,
                           RGWSyncModulesManager *sync_mgr,
                           const rgw::zone_features::set& enable_features,
                           const rgw::zone_features::set& disable_features,
			   optional_yield y)
{
  auto& zone_id = zone_params.get_id();
  auto& zone_name = zone_params.get_name();

  // check for duplicate zone name on insert
  if (!zones.count(zone_id)) {
    for (const auto& zone : zones) {
      if (zone.second.name == zone_name) {
        ldpp_dout(dpp, 0) << "ERROR: found existing zone name " << zone_name
            << " (" << zone.first << ") in zonegroup " << get_name() << dendl;
        return -EEXIST;
      }
    }
  }

  if (is_master) {
    if (*is_master) {
      if (!master_zone.empty() && master_zone != zone_id) {
        ldpp_dout(dpp, 0) << "NOTICE: overriding master zone: " << master_zone << dendl;
      }
      master_zone = zone_id;
    } else if (master_zone == zone_id) {
      master_zone.clear();
    }
  }

  RGWZone& zone = zones[zone_id];
  zone.name = zone_name;
  zone.id = zone_id;
  if (!endpoints.empty()) {
    zone.endpoints = endpoints;
  }
  if (read_only) {
    zone.read_only = *read_only;
  }
  if (ptier_type) {
    zone.tier_type = *ptier_type;
    if (!sync_mgr->get_module(*ptier_type, nullptr)) {
      ldpp_dout(dpp, 0) << "ERROR: could not found sync module: " << *ptier_type 
                    << ",  valid sync modules: " 
                    << sync_mgr->get_registered_module_names()
                    << dendl;
      return -ENOENT;
    }
  }

  if (psync_from_all) {
    zone.sync_from_all = *psync_from_all;
  }

  if (predirect_zone) {
    zone.redirect_zone = *predirect_zone;
  }

  if (bucket_index_max_shards) {
    zone.bucket_index_max_shards = *bucket_index_max_shards;
  }

  for (auto add : sync_from) {
    zone.sync_from.insert(add);
  }

  for (auto rm : sync_from_rm) {
    zone.sync_from.erase(rm);
  }

  zone.supported_features.insert(enable_features.begin(),
                                 enable_features.end());

  for (const auto& feature : disable_features) {
    if (enabled_features.contains(feature)) {
      lderr(cct) << "ERROR: Cannot disable zone feature \"" << feature
          << "\" until it's been disabled in zonegroup " << name << dendl;
      return -EINVAL;
    }
    auto i = zone.supported_features.find(feature);
    if (i == zone.supported_features.end()) {
      ldout(cct, 1) << "WARNING: zone feature \"" << feature
          << "\" was not enabled in zone " << zone.name << dendl;
      continue;
    }
    zone.supported_features.erase(i);
  }

  post_process_params(dpp, y);

  return update(dpp,y);
}


int RGWZoneGroup::rename_zone(const DoutPrefixProvider *dpp, 
                              const RGWZoneParams& zone_params,
			      optional_yield y)
{
  RGWZone& zone = zones[zone_params.get_id()];
  zone.name = zone_params.get_name();

  return update(dpp, y);
}

void RGWZoneGroup::post_process_params(const DoutPrefixProvider *dpp, optional_yield y)
{
  bool log_data = zones.size() > 1;

  if (master_zone.empty()) {
    auto iter = zones.begin();
    if (iter != zones.end()) {
      master_zone = iter->first;
    }
  }
  
  for (auto& item : zones) {
    RGWZone& zone = item.second;
    zone.log_data = log_data;

    RGWZoneParams zone_params(zone.id, zone.name);
    int ret = zone_params.init(dpp, cct, sysobj_svc, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "WARNING: could not read zone params for zone id=" << zone.id << " name=" << zone.name << dendl;
      continue;
    }

    for (auto& pitem : zone_params.placement_pools) {
      const string& placement_name = pitem.first;
      if (placement_targets.find(placement_name) == placement_targets.end()) {
        RGWZoneGroupPlacementTarget placement_target;
        placement_target.name = placement_name;
        placement_targets[placement_name] = placement_target;
      }
    }
  }

  if (default_placement.empty() && !placement_targets.empty()) {
    default_placement.init(placement_targets.begin()->first, RGW_STORAGE_CLASS_STANDARD);
  }
}

int RGWZoneGroup::remove_zone(const DoutPrefixProvider *dpp, const std::string& zone_id, optional_yield y)
{
  auto iter = zones.find(zone_id);
  if (iter == zones.end()) {
    ldpp_dout(dpp, 0) << "zone id " << zone_id << " is not a part of zonegroup "
        << name << dendl;
    return -ENOENT;
  }

  zones.erase(iter);

  post_process_params(dpp, y);

  return update(dpp, y);
}

void RGWDefaultSystemMetaObjInfo::dump(Formatter *f) const {
  encode_json("default_id", default_id, f);
}

void RGWDefaultSystemMetaObjInfo::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("default_id", default_id, obj);
}

int RGWSystemMetaObj::rename(const DoutPrefixProvider *dpp, const string& new_name, optional_yield y)
{
  string new_id;
  int ret = read_id(dpp, new_name, new_id, y);
  if (!ret) {
    return -EEXIST;
  }
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 0) << "Error read_id " << new_name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  string old_name = name;
  name = new_name;
  ret = update(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "Error storing new obj info " << new_name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  ret = store_name(dpp, true, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "Error storing new name " << new_name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  /* delete old name */
  rgw_pool pool(get_pool(cct));
  string oid = get_names_oid_prefix() + old_name;
  rgw_raw_obj old_name_obj(pool, oid);
  auto sysobj = sysobj_svc->get_obj(old_name_obj);
  ret = sysobj.wop().remove(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "Error delete old obj name  " << old_name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return ret;
}

int RGWSystemMetaObj::read(const DoutPrefixProvider *dpp, optional_yield y)
{
  int ret = read_id(dpp, name, id, y);
  if (ret < 0) {
    return ret;
  }

  return read_info(dpp, id, y);
}

int RGWZoneParams::create_default(const DoutPrefixProvider *dpp, optional_yield y, bool old_format)
{
  name = default_zone_name;

  int r = create(dpp, y);
  if (r < 0) {
    return r;
  }

  if (old_format) {
    name = id;
  }

  return r;
}

const string& RGWZoneParams::get_compression_type(const rgw_placement_rule& placement_rule) const
{
  static const std::string NONE{"none"};
  auto p = placement_pools.find(placement_rule.name);
  if (p == placement_pools.end()) {
    return NONE;
  }
  const auto& type = p->second.get_compression_type(placement_rule.get_storage_class());
  return !type.empty() ? type : NONE;
}

// run an MD5 hash on the zone_id and return the first 32 bits
static uint32_t gen_short_zone_id(const std::string zone_id)
{
  unsigned char md5[CEPH_CRYPTO_MD5_DIGESTSIZE];
  MD5 hash;
  // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  hash.Update((const unsigned char *)zone_id.c_str(), zone_id.size());
  hash.Final(md5);

  uint32_t short_id;
  memcpy((char *)&short_id, md5, sizeof(short_id));
  return std::max(short_id, 1u);
}

int RGWPeriodMap::update(const RGWZoneGroup& zonegroup, CephContext *cct)
{
  if (zonegroup.is_master_zonegroup() && (!master_zonegroup.empty() && zonegroup.get_id() != master_zonegroup)) {
    ldout(cct,0) << "Error updating periodmap, multiple master zonegroups configured "<< dendl;
    ldout(cct,0) << "master zonegroup: " << master_zonegroup << " and  " << zonegroup.get_id() <<dendl;
    return -EINVAL;
  }
  map<string, RGWZoneGroup>::iterator iter = zonegroups.find(zonegroup.get_id());
  if (iter != zonegroups.end()) {
    RGWZoneGroup& old_zonegroup = iter->second;
    if (!old_zonegroup.api_name.empty()) {
      zonegroups_by_api.erase(old_zonegroup.api_name);
    }
  }
  zonegroups[zonegroup.get_id()] = zonegroup;

  if (!zonegroup.api_name.empty()) {
    zonegroups_by_api[zonegroup.api_name] = zonegroup;
  }

  if (zonegroup.is_master_zonegroup()) {
    master_zonegroup = zonegroup.get_id();
  } else if (master_zonegroup == zonegroup.get_id()) {
    master_zonegroup = "";
  }

  for (auto& i : zonegroup.zones) {
    auto& zone = i.second;
    if (short_zone_ids.find(zone.id) != short_zone_ids.end()) {
      continue;
    }
    // calculate the zone's short id
    uint32_t short_id = gen_short_zone_id(zone.id);

    // search for an existing zone with the same short id
    for (auto& s : short_zone_ids) {
      if (s.second == short_id) {
        ldout(cct, 0) << "New zone '" << zone.name << "' (" << zone.id
            << ") generates the same short_zone_id " << short_id
            << " as existing zone id " << s.first << dendl;
        return -EEXIST;
      }
    }

    short_zone_ids[zone.id] = short_id;
  }

  return 0;
}

uint32_t RGWPeriodMap::get_zone_short_id(const string& zone_id) const
{
  auto i = short_zone_ids.find(zone_id);
  if (i == short_zone_ids.end()) {
    return 0;
  }
  return i->second;
}

bool RGWPeriodMap::find_zone_by_name(const string& zone_name,
                                     RGWZoneGroup *zonegroup,
                                     RGWZone *zone) const
{
  for (auto& iter : zonegroups) {
    auto& zg = iter.second;
    for (auto& ziter : zg.zones) {
      auto& z = ziter.second;

      if (z.name == zone_name) {
        *zonegroup = zg;
        *zone = z;
        return true;
      }
    }
  }

  return false;
}

namespace rgw {

int read_realm(const DoutPrefixProvider* dpp, optional_yield y,
               sal::ConfigStore* cfgstore,
               std::string_view realm_id,
               std::string_view realm_name,
               RGWRealm& info,
               std::unique_ptr<sal::RealmWriter>* writer)
{
  if (!realm_id.empty()) {
    return cfgstore->read_realm_by_id(dpp, y, realm_id, info, writer);
  }
  if (!realm_name.empty()) {
    return cfgstore->read_realm_by_name(dpp, y, realm_name, info, writer);
  }
  return cfgstore->read_default_realm(dpp, y, info, writer);
}

int create_realm(const DoutPrefixProvider* dpp, optional_yield y,
                 sal::ConfigStore* cfgstore, bool exclusive,
                 RGWRealm& info,
                 std::unique_ptr<sal::RealmWriter>* writer_out)
{
  if (info.name.empty()) {
    ldpp_dout(dpp, -1) << __func__ << " requires a realm name" << dendl;
    return -EINVAL;
  }
  if (info.id.empty()) {
    info.id = gen_random_uuid();
  }

  // if the realm already has a current_period, just make sure it exists
  std::optional<RGWPeriod> period;
  if (!info.current_period.empty()) {
    period.emplace();
    int r = cfgstore->read_period(dpp, y, info.current_period,
                                  std::nullopt, *period);
    if (r < 0) {
      ldpp_dout(dpp, -1) << __func__ << " failed to read realm's current_period="
          << info.current_period << " with " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  // create the realm
  std::unique_ptr<sal::RealmWriter> writer;
  int r = cfgstore->create_realm(dpp, y, exclusive, info, &writer);
  if (r < 0) {
    return r;
  }

  if (!period) {
    // initialize and exclusive-create the initial period
    period.emplace();
    period->id = gen_random_uuid();
    period->period_map.id = period->id;
    period->epoch = FIRST_EPOCH;
    period->realm_id = info.id;

    r = cfgstore->create_period(dpp, y, true, *period);
    if (r < 0) {
      ldpp_dout(dpp, -1) << __func__ << " failed to create the initial period id="
          << period->id << " for realm " << info.name
          << " with " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  // update the realm's current_period
  r = realm_set_current_period(dpp, y, cfgstore, *writer, info, *period);
  if (r < 0) {
    return r;
  }

  // try to set as default. may race with another create, so pass exclusive=true
  // so we don't override an existing default
  r = set_default_realm(dpp, y, cfgstore, info, true);
  if (r < 0 && r != -EEXIST) {
    ldpp_dout(dpp, 0) << "WARNING: failed to set realm as default: "
        << cpp_strerror(r) << dendl;
  }

  if (writer_out) {
    *writer_out = std::move(writer);
  }
  return 0;
}

int set_default_realm(const DoutPrefixProvider* dpp, optional_yield y,
                      sal::ConfigStore* cfgstore, const RGWRealm& info,
                      bool exclusive)
{
  return cfgstore->write_default_realm_id(dpp, y, exclusive, info.id);
}

int realm_set_current_period(const DoutPrefixProvider* dpp, optional_yield y,
                             sal::ConfigStore* cfgstore,
                             sal::RealmWriter& writer, RGWRealm& realm,
                             const RGWPeriod& period)
{
  // update realm epoch to match the period's
  if (realm.epoch > period.realm_epoch) {
    ldpp_dout(dpp, -1) << __func__ << " with old realm epoch "
        << period.realm_epoch << ", current epoch=" << realm.epoch << dendl;
    return -EINVAL;
  }
  if (realm.epoch == period.realm_epoch && realm.current_period != period.id) {
    ldpp_dout(dpp, -1) << __func__ << " with same realm epoch "
        << period.realm_epoch << ", but different period id "
        << period.id << " != " << realm.current_period << dendl;
    return -EINVAL;
  }

  realm.epoch = period.realm_epoch;
  realm.current_period = period.id;

  // update the realm object
  int r = writer.write(dpp, y, realm);
  if (r < 0) {
    ldpp_dout(dpp, -1) << __func__ << " failed to overwrite realm "
        << realm.name << " with " << cpp_strerror(r) << dendl;
    return r;
  }

  // reflect the zonegroup and period config
  (void) reflect_period(dpp, y, cfgstore, period);
  return 0;
}

int reflect_period(const DoutPrefixProvider* dpp, optional_yield y,
                   sal::ConfigStore* cfgstore, const RGWPeriod& info)
{
  // overwrite the local period config and zonegroup objects
  constexpr bool exclusive = false;

  int r = cfgstore->write_period_config(dpp, y, exclusive, info.realm_id,
                                        info.period_config);
  if (r < 0) {
    ldpp_dout(dpp, -1) << __func__ << " failed to store period config for realm id="
        << info.realm_id << " with " << cpp_strerror(r) << dendl;
    return r;
  }

  for (auto& [zonegroup_id, zonegroup] : info.period_map.zonegroups) {
    r = cfgstore->create_zonegroup(dpp, y, exclusive, zonegroup, nullptr);
    if (r < 0) {
      ldpp_dout(dpp, -1) << __func__ << " failed to store zonegroup id="
          << zonegroup_id << " with " << cpp_strerror(r) << dendl;
      return r;
    }
    if (zonegroup.is_master) {
      // set master as default if no default exists
      constexpr bool exclusive = true;
      r = set_default_zonegroup(dpp, y, cfgstore, zonegroup, exclusive);
      if (r == 0) {
        ldpp_dout(dpp, 1) << "Set the period's master zonegroup "
            << zonegroup.name << " as the default" << dendl;
      }
    }
  }
  return 0;
}

std::string get_staging_period_id(std::string_view realm_id)
{
  return string_cat_reserve(realm_id, ":staging");
}

void fork_period(const DoutPrefixProvider* dpp, RGWPeriod& info)
{
  ldpp_dout(dpp, 20) << __func__ << " realm id=" << info.realm_id
      << " period id=" << info.id << dendl;

  info.predecessor_uuid = std::move(info.id);
  info.id = get_staging_period_id(info.realm_id);
  info.period_map.reset();
  info.realm_epoch++;
}

int update_period(const DoutPrefixProvider* dpp, optional_yield y,
                  sal::ConfigStore* cfgstore, RGWPeriod& info)
{
  // clear zone short ids of removed zones. period_map.update() will add the
  // remaining zones back
  info.period_map.short_zone_ids.clear();

  // list all zonegroups in the realm
  rgw::sal::ListResult<std::string> listing;
  std::array<std::string, 1000> zonegroup_names; // list in pages of 1000
  do {
    int ret = cfgstore->list_zonegroup_names(dpp, y, listing.next,
                                             zonegroup_names, listing);
    if (ret < 0) {
      std::cerr << "failed to list zonegroups: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    for (const auto& name : listing.entries) {
      RGWZoneGroup zg;
      ret = cfgstore->read_zonegroup_by_name(dpp, y, name, zg, nullptr);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "WARNING: failed to read zonegroup "
            << name << ": " << cpp_strerror(-ret) << dendl;
        continue;
      }

      if (zg.realm_id != info.realm_id) {
        ldpp_dout(dpp, 20) << "skipping zonegroup " << zg.get_name()
            << " with realm id " << zg.realm_id
            << ", not on our realm " << info.realm_id << dendl;
        continue;
      }

      if (zg.master_zone.empty()) {
        ldpp_dout(dpp, 0) << "ERROR: zonegroup " << zg.get_name() << " should have a master zone " << dendl;
        return -EINVAL;
      }

      if (zg.zones.find(zg.master_zone) == zg.zones.end()) {
        ldpp_dout(dpp, 0) << "ERROR: zonegroup " << zg.get_name()
                     << " has a non existent master zone "<< dendl;
        return -EINVAL;
      }

      if (zg.is_master_zonegroup()) {
        info.master_zonegroup = zg.get_id();
        info.master_zone = zg.master_zone;
      }

      ret = info.period_map.update(zg, dpp->get_cct());
      if (ret < 0) {
        return ret;
      }
    } // foreach name in listing.entries
  } while (!listing.next.empty());

  // read the realm's current period config
  int ret = cfgstore->read_period_config(dpp, y, info.realm_id,
                                         info.period_config);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 0) << "ERROR: failed to read period config: "
        << cpp_strerror(ret) << dendl;
    return ret;
  }

  return 0;
}

int commit_period(const DoutPrefixProvider* dpp, optional_yield y,
                  sal::ConfigStore* cfgstore, sal::Driver* driver,
                  RGWRealm& realm, sal::RealmWriter& realm_writer,
                  const RGWPeriod& current_period,
                  RGWPeriod& info, std::ostream& error_stream,
                  bool force_if_stale)
{
  auto zone_svc = static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone; // XXX

  ldpp_dout(dpp, 20) << __func__ << " realm " << realm.id
      << " period " << current_period.id << dendl;
  // gateway must be in the master zone to commit
  if (info.master_zone != zone_svc->get_zone_params().id) {
    error_stream << "Cannot commit period on zone "
        << zone_svc->get_zone_params().id << ", it must be sent to "
        "the period's master zone " << info.master_zone << '.' << std::endl;
    return -EINVAL;
  }
  // period predecessor must match current period
  if (info.predecessor_uuid != current_period.id) {
    error_stream << "Period predecessor " << info.predecessor_uuid
        << " does not match current period " << current_period.id
        << ". Use 'period pull' to get the latest period from the master, "
        "reapply your changes, and try again." << std::endl;
    return -EINVAL;
  }
  // realm epoch must be 1 greater than current period
  if (info.realm_epoch != current_period.realm_epoch + 1) {
    error_stream << "Period's realm epoch " << info.realm_epoch
        << " does not come directly after current realm epoch "
        << current_period.realm_epoch << ". Use 'realm pull' to get the "
        "latest realm and period from the master zone, reapply your changes, "
        "and try again." << std::endl;
    return -EINVAL;
  }
  // did the master zone change?
  if (info.master_zone != current_period.master_zone) {
    // store the current metadata sync status in the period
    int r = info.update_sync_status(dpp, driver, current_period,
                                    error_stream, force_if_stale);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "failed to update metadata sync status: "
          << cpp_strerror(-r) << dendl;
      return r;
    }
    // create an object with a new period id
    info.period_map.id = info.id = gen_random_uuid();
    info.epoch = FIRST_EPOCH;

    constexpr bool exclusive = true;
    r = cfgstore->create_period(dpp, y, exclusive, info);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "failed to create new period: " << cpp_strerror(-r) << dendl;
      return r;
    }
    // set as current period
    r = realm_set_current_period(dpp, y, cfgstore, realm_writer, realm, info);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "failed to update realm's current period: "
          << cpp_strerror(-r) << dendl;
      return r;
    }
    ldpp_dout(dpp, 4) << "Promoted to master zone and committed new period "
        << info.id << dendl;
    (void) cfgstore->realm_notify_new_period(dpp, y, info);
    return 0;
  }
  // period must be based on current epoch
  if (info.epoch != current_period.epoch) {
    error_stream << "Period epoch " << info.epoch << " does not match "
        "predecessor epoch " << current_period.epoch << ". Use "
        "'period pull' to get the latest epoch from the master zone, "
        "reapply your changes, and try again." << std::endl;
    return -EINVAL;
  }
  // set period as next epoch
  info.id = current_period.id;
  info.epoch = current_period.epoch + 1;
  info.predecessor_uuid = current_period.predecessor_uuid;
  info.realm_epoch = current_period.realm_epoch;
  // write the period
  constexpr bool exclusive = true;
  int r = cfgstore->create_period(dpp, y, exclusive, info);
  if (r == -EEXIST) {
    // already have this epoch (or a more recent one)
    return 0;
  }
  if (r < 0) {
    ldpp_dout(dpp, 0) << "failed to store period: " << cpp_strerror(r) << dendl;
    return r;
  }
  r = reflect_period(dpp, y, cfgstore, info);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "failed to update local objects: " << cpp_strerror(r) << dendl;
    return r;
  }
  ldpp_dout(dpp, 4) << "Committed new epoch " << info.epoch
      << " for period " << info.id << dendl;
  (void) cfgstore->realm_notify_new_period(dpp, y, info);
  return 0;
}


int read_zonegroup(const DoutPrefixProvider* dpp, optional_yield y,
                   sal::ConfigStore* cfgstore,
                   std::string_view zonegroup_id,
                   std::string_view zonegroup_name,
                   RGWZoneGroup& info,
                   std::unique_ptr<sal::ZoneGroupWriter>* writer)
{
  if (!zonegroup_id.empty()) {
    return cfgstore->read_zonegroup_by_id(dpp, y, zonegroup_id, info, writer);
  }
  if (!zonegroup_name.empty()) {
    return cfgstore->read_zonegroup_by_name(dpp, y, zonegroup_name, info, writer);
  }

  std::string realm_id;
  int r = cfgstore->read_default_realm_id(dpp, y, realm_id);
  if (r == -ENOENT) {
    return cfgstore->read_zonegroup_by_name(dpp, y, default_zonegroup_name,
                                            info, writer);
  }
  if (r < 0) {
    return r;
  }
  return cfgstore->read_default_zonegroup(dpp, y, realm_id, info, writer);
}

int create_zonegroup(const DoutPrefixProvider* dpp, optional_yield y,
                     sal::ConfigStore* cfgstore, bool exclusive,
                     RGWZoneGroup& info)
{
  if (info.name.empty()) {
    ldpp_dout(dpp, -1) << __func__ << " requires a zonegroup name" << dendl;
    return -EINVAL;
  }
  if (info.id.empty()) {
    info.id = gen_random_uuid();
  }

  // insert the default placement target if it doesn't exist
  constexpr std::string_view default_placement_name = "default-placement";

  RGWZoneGroupPlacementTarget placement_target;
  placement_target.name = default_placement_name;

  info.placement_targets.emplace(default_placement_name, placement_target);
  if (info.default_placement.name.empty()) {
    info.default_placement.name = default_placement_name;
  }

  int r = cfgstore->create_zonegroup(dpp, y, exclusive, info, nullptr);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "failed to create zonegroup with "
        << cpp_strerror(r) << dendl;
    return r;
  }

  // try to set as default. may race with another create, so pass exclusive=true
  // so we don't override an existing default
  r = set_default_zonegroup(dpp, y, cfgstore, info, true);
  if (r < 0 && r != -EEXIST) {
    ldpp_dout(dpp, 0) << "WARNING: failed to set zonegroup as default: "
        << cpp_strerror(r) << dendl;
  }

  return 0;
}

static int create_default_zonegroup(const DoutPrefixProvider* dpp,
                                    optional_yield y,
                                    sal::ConfigStore* cfgstore,
                                    bool exclusive,
                                    const RGWZoneParams& default_zone,
                                    RGWZoneGroup& info)
{
  info.name = default_zonegroup_name;
  info.api_name = default_zonegroup_name;
  info.is_master = true;

  // enable all supported features
  info.enabled_features.insert(rgw::zone_features::enabled.begin(),
                               rgw::zone_features::enabled.end());

  // add the zone to the zonegroup
  bool is_master = true;
  std::list<std::string> empty_list;
  rgw::zone_features::set disable_features; // empty
  int r = add_zone_to_group(dpp, info, default_zone, &is_master, nullptr,
                            empty_list, nullptr, nullptr, empty_list,
                            empty_list, nullptr, std::nullopt,
                            info.enabled_features, disable_features);
  if (r < 0) {
    return r;
  }

  // write the zone
  return create_zonegroup(dpp, y, cfgstore, exclusive, info);
}

int set_default_zonegroup(const DoutPrefixProvider* dpp, optional_yield y,
                          sal::ConfigStore* cfgstore, const RGWZoneGroup& info,
                          bool exclusive)
{
  return cfgstore->write_default_zonegroup_id(
      dpp, y, exclusive, info.realm_id, info.id);
}

int remove_zone_from_group(const DoutPrefixProvider* dpp,
                           RGWZoneGroup& zonegroup,
                           const rgw_zone_id& zone_id)
{
  auto z = zonegroup.zones.find(zone_id);
  if (z == zonegroup.zones.end()) {
    return -ENOENT;
  }
  zonegroup.zones.erase(z);

  if (zonegroup.master_zone == zone_id) {
    // choose a new master zone
    auto m = zonegroup.zones.begin();
    if (m != zonegroup.zones.end()) {
      zonegroup.master_zone = m->first;
      ldpp_dout(dpp, 0) << "NOTICE: promoted " << m->second.name
         << " as new master_zone of zonegroup " << zonegroup.name << dendl;
    } else {
      ldpp_dout(dpp, 0) << "NOTICE: removed master_zone of zonegroup "
          << zonegroup.name << dendl;
    }
  }

  const bool log_data = zonegroup.zones.size() > 1;
  for (auto& [id, zone] : zonegroup.zones) {
    zone.log_data = log_data;
  }

  return 0;
}

// try to remove the given zone id from every zonegroup in the cluster
static int remove_zone_from_groups(const DoutPrefixProvider* dpp,
                                   optional_yield y,
                                   sal::ConfigStore* cfgstore,
                                   const rgw_zone_id& zone_id)
{
  std::array<std::string, 128> zonegroup_names;
  sal::ListResult<std::string> listing;
  do {
    int r = cfgstore->list_zonegroup_names(dpp, y, listing.next,
                                           zonegroup_names, listing);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "failed to list zonegroups with "
          << cpp_strerror(r) << dendl;
      return r;
    }

    for (const auto& name : listing.entries) {
      RGWZoneGroup zonegroup;
      std::unique_ptr<sal::ZoneGroupWriter> writer;
      r = cfgstore->read_zonegroup_by_name(dpp, y, name, zonegroup, &writer);
      if (r < 0) {
        ldpp_dout(dpp, 0) << "WARNING: failed to load zonegroup " << name
            << " with " << cpp_strerror(r) << dendl;
        continue;
      }

      r = remove_zone_from_group(dpp, zonegroup, zone_id);
      if (r < 0) {
        continue;
      }

      // write the updated zonegroup
      r = writer->write(dpp, y, zonegroup);
      if (r < 0) {
        ldpp_dout(dpp, 0) << "WARNING: failed to write zonegroup " << name
            << " with " << cpp_strerror(r) << dendl;
        continue;
      }
      ldpp_dout(dpp, 0) << "Removed zone from zonegroup " << name << dendl;
    }
  } while (!listing.next.empty());

  return 0;
}


int read_zone(const DoutPrefixProvider* dpp, optional_yield y,
              sal::ConfigStore* cfgstore,
              std::string_view zone_id,
              std::string_view zone_name,
              RGWZoneParams& info,
              std::unique_ptr<sal::ZoneWriter>* writer)
{
  if (!zone_id.empty()) {
    return cfgstore->read_zone_by_id(dpp, y, zone_id, info, writer);
  }
  if (!zone_name.empty()) {
    return cfgstore->read_zone_by_name(dpp, y, zone_name, info, writer);
  }

  std::string realm_id;
  int r = cfgstore->read_default_realm_id(dpp, y, realm_id);
  if (r == -ENOENT) {
    return cfgstore->read_zone_by_name(dpp, y, default_zone_name, info, writer);
  }
  if (r < 0) {
    return r;
  }
  return cfgstore->read_default_zone(dpp, y, realm_id, info, writer);
}

extern int get_zones_pool_set(const DoutPrefixProvider *dpp, optional_yield y,
                              rgw::sal::ConfigStore* cfgstore,
                              std::string_view my_zone_id,
                              std::set<rgw_pool>& pools);

int create_zone(const DoutPrefixProvider* dpp, optional_yield y,
                sal::ConfigStore* cfgstore, bool exclusive,
                RGWZoneParams& info, std::unique_ptr<sal::ZoneWriter>* writer)
{
  if (info.name.empty()) {
    ldpp_dout(dpp, -1) << __func__ << " requires a zone name" << dendl;
    return -EINVAL;
  }
  if (info.id.empty()) {
    info.id = gen_random_uuid();
  }

  // add default placement with empty pool name
  RGWZonePlacementInfo placement;
  rgw_pool pool;
  placement.storage_classes.set_storage_class(
      RGW_STORAGE_CLASS_STANDARD, &pool, nullptr);
  // don't overwrite if it already exists
  info.placement_pools.emplace("default-placement", std::move(placement));

  // build a set of all pool names used by other zones
  std::set<rgw_pool> pools;
  int r = get_zones_pool_set(dpp, y, cfgstore, info.id, pools);
  if (r < 0) {
    return r;
  }

  // initialize pool names with the zone name prefix
  r = init_zone_pool_names(dpp, y, pools, info);
  if (r < 0) {
    return r;
  }

  r = cfgstore->create_zone(dpp, y, exclusive, info, nullptr);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "failed to create zone with "
        << cpp_strerror(r) << dendl;
    return r;
  }

  // try to set as default. may race with another create, so pass exclusive=true
  // so we don't override an existing default
  r = set_default_zone(dpp, y, cfgstore, info, true);
  if (r < 0 && r != -EEXIST) {
    ldpp_dout(dpp, 0) << "WARNING: failed to set zone as default: "
        << cpp_strerror(r) << dendl;
  }

  return 0;

}

int set_default_zone(const DoutPrefixProvider* dpp, optional_yield y,
                     sal::ConfigStore* cfgstore, const RGWZoneParams& info,
                     bool exclusive)
{
  return cfgstore->write_default_zone_id(
      dpp, y, exclusive, info.realm_id, info.id);
}

int delete_zone(const DoutPrefixProvider* dpp, optional_yield y,
                sal::ConfigStore* cfgstore, const RGWZoneParams& info,
                sal::ZoneWriter& writer)
{
  // remove this zone from any zonegroups that contain it
  int r = remove_zone_from_groups(dpp, y, cfgstore, info.id);
  if (r < 0) {
    return r;
  }

  return writer.remove(dpp, y);
}

auto find_zone_placement(const DoutPrefixProvider* dpp,
                         const RGWZoneParams& info,
                         const rgw_placement_rule& rule)
    -> const RGWZonePlacementInfo*
{
  auto i = info.placement_pools.find(rule.name);
  if (i == info.placement_pools.end()) {
    ldpp_dout(dpp, 0) << "ERROR: This zone does not contain placement rule "
        << rule.name << dendl;
    return nullptr;
  }

  const std::string& storage_class = rule.get_storage_class();
  if (!i->second.storage_class_exists(storage_class)) {
    ldpp_dout(dpp, 5) << "ERROR: The zone placement for rule " << rule.name
        << " does not contain storage class " << storage_class << dendl;
    return nullptr;
  }

  return &i->second;
}

static int read_or_create_default_zone(const DoutPrefixProvider* dpp,
                                       optional_yield y,
                                       sal::ConfigStore* cfgstore,
                                       RGWZoneParams& info)
{
  int r = cfgstore->read_zone_by_name(dpp, y, default_zone_name, info, nullptr);
  if (r == -ENOENT) {
    info.name = default_zone_name;
    constexpr bool exclusive = true;
    r = create_zone(dpp, y, cfgstore, exclusive, info, nullptr);
    if (r == -EEXIST) {
      r = cfgstore->read_zone_by_name(dpp, y, default_zone_name, info, nullptr);
    }
    if (r < 0) {
      ldpp_dout(dpp, 0) << "failed to create default zone: "
          << cpp_strerror(r) << dendl;
      return r;
    }
  }
  return r;
}

static int read_or_create_default_zonegroup(const DoutPrefixProvider* dpp,
                                            optional_yield y,
                                            sal::ConfigStore* cfgstore,
                                            const RGWZoneParams& zone_params,
                                            RGWZoneGroup& info)
{
  int r = cfgstore->read_zonegroup_by_name(dpp, y, default_zonegroup_name,
                                           info, nullptr);
  if (r == -ENOENT) {
    constexpr bool exclusive = true;
    r = create_default_zonegroup(dpp, y, cfgstore, exclusive,
                                 zone_params, info);
    if (r == -EEXIST) {
      r = cfgstore->read_zonegroup_by_name(dpp, y, default_zonegroup_name,
                                           info, nullptr);
    }
    if (r < 0) {
      ldpp_dout(dpp, 0) << "failed to create default zonegroup: "
          << cpp_strerror(r) << dendl;
      return r;
    }
  }
  return r;
}

int SiteConfig::load(const DoutPrefixProvider* dpp, optional_yield y,
                     sal::ConfigStore* cfgstore, bool force_local_zonegroup)
{
  // clear existing configuration
  zone = nullptr;
  zonegroup = nullptr;
  local_zonegroup = std::nullopt;
  period = std::nullopt;
  zone_params = RGWZoneParams{};

  int r = 0;

  // try to load a realm
  realm.emplace();
  std::string realm_name = dpp->get_cct()->_conf->rgw_realm;
  if (!realm_name.empty()) {
    r = cfgstore->read_realm_by_name(dpp, y, realm_name, *realm, nullptr);
  } else {
    r = cfgstore->read_default_realm(dpp, y, *realm, nullptr);
    if (r == -ENOENT) { // no realm
      r = 0;
      realm = std::nullopt;
    }
  }
  if (r < 0) {
    ldpp_dout(dpp, 0) << "failed to load realm: " << cpp_strerror(r) << dendl;
    return r;
  }

  // try to load the local zone params
  std::string zone_name = dpp->get_cct()->_conf->rgw_zone;
  if (!zone_name.empty()) {
    r = cfgstore->read_zone_by_name(dpp, y, zone_name, zone_params, nullptr);
  } else if (realm) {
    // load the realm's default zone
    r = cfgstore->read_default_zone(dpp, y, realm->id, zone_params, nullptr);
  } else {
    // load or create the "default" zone
    r = read_or_create_default_zone(dpp, y, cfgstore, zone_params);
  }
  if (r < 0) {
    ldpp_dout(dpp, 0) << "failed to load zone: " << cpp_strerror(r) << dendl;
    return r;
  }

  if (!realm && !zone_params.realm_id.empty()) {
    realm.emplace();
    r = cfgstore->read_realm_by_id(dpp, y, zone_params.realm_id,
                                   *realm, nullptr);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "failed to load realm: " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  if (realm && !force_local_zonegroup) {
    // try to load the realm's period
    r = load_period_zonegroup(dpp, y, cfgstore, *realm, zone_params.id);
  } else {
    // fall back to a local zonegroup
    r = load_local_zonegroup(dpp, y, cfgstore, zone_params.id);
  }

  return r;
}

std::unique_ptr<SiteConfig> SiteConfig::make_fake() {
  auto fake = std::make_unique<SiteConfig>();
  fake->local_zonegroup.emplace();
  fake->local_zonegroup->zones.emplace(""s, RGWZone{});
  fake->zonegroup = &*fake->local_zonegroup;
  fake->zone = &fake->zonegroup->zones.begin()->second;
  return fake;
}

int SiteConfig::load_period_zonegroup(const DoutPrefixProvider* dpp,
                                      optional_yield y,
                                      sal::ConfigStore* cfgstore,
                                      const RGWRealm& realm,
                                      const rgw_zone_id& zone_id)
{
  // load the realm's current period
  period.emplace();
  int r = cfgstore->read_period(dpp, y, realm.current_period,
                                std::nullopt, *period);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "failed to load current period: "
        << cpp_strerror(r) << dendl;
    return r;
  }

  // find our zone and zonegroup in the period
  for (const auto& zg : period->period_map.zonegroups) {
    auto z = zg.second.zones.find(zone_id);
    if (z != zg.second.zones.end()) {
      zone = &z->second;
      zonegroup = &zg.second;
      return 0;
    }
  }

  ldpp_dout(dpp, 0) << "ERROR: current period " << period->id
      << " does not contain zone id " << zone_id << dendl;

  period = std::nullopt;
  return -ENOENT;
}

int SiteConfig::load_local_zonegroup(const DoutPrefixProvider* dpp,
                                     optional_yield y,
                                     sal::ConfigStore* cfgstore,
                                     const rgw_zone_id& zone_id)
{
  int r = 0;

  // load the zonegroup
  local_zonegroup.emplace();
  std::string zonegroup_name = dpp->get_cct()->_conf->rgw_zonegroup;
  if (!zonegroup_name.empty()) {
    r = cfgstore->read_zonegroup_by_name(dpp, y, zonegroup_name,
                                         *local_zonegroup, nullptr);
  } else {
    r = read_or_create_default_zonegroup(dpp, y, cfgstore, zone_params,
                                         *local_zonegroup);
  }

  if (r < 0) {
    ldpp_dout(dpp, 0) << "failed to load zonegroup: "
        << cpp_strerror(r) << dendl;
  } else {
    // find our zone in the zonegroup
    auto z = local_zonegroup->zones.find(zone_id);
    if (z != local_zonegroup->zones.end()) {
      zone = &z->second;
      zonegroup = &*local_zonegroup;
      return 0;
    }
    ldpp_dout(dpp, 0) << "ERROR: zonegroup " << local_zonegroup->id
        << " does not contain zone id " << zone_id << dendl;
    r = -ENOENT;
  }

  local_zonegroup = std::nullopt;
  return r;
}

} // namespace rgw

static inline int conf_to_uint64(const JSONFormattable& config, const string& key, uint64_t *pval)
{
  string sval;
  if (config.find(key, &sval)) {
    string err;
    uint64_t val = strict_strtoll(sval.c_str(), 10, &err);
    if (!err.empty()) {
      return -EINVAL;
    }
    *pval = val;
  }
  return 0;
}

int RGWZoneGroupPlacementTier::update_params(const JSONFormattable& config)
{
  int r = -1;

  if (config.exists("retain_head_object")) {
    string s = config["retain_head_object"];
    if (s == "true") {
      retain_head_object = true;
    } else {
      retain_head_object = false;
    }
  }

  if (tier_type == "cloud-s3") {
    r = t.s3.update_params(config);
  }

  return r;
}

int RGWZoneGroupPlacementTier::clear_params(const JSONFormattable& config)
{
  if (config.exists("retain_head_object")) {
    retain_head_object = false;
  }

  if (tier_type == "cloud-s3") {
    t.s3.clear_params(config);
  }

  return 0;
}

int RGWZoneGroupPlacementTierS3::update_params(const JSONFormattable& config)
{
  int r = -1;

  if (config.exists("endpoint")) {
    endpoint = config["endpoint"];
  }
  if (config.exists("target_path")) {
    target_path = config["target_path"];
  }
  if (config.exists("region")) {
    region = config["region"];
  }
  if (config.exists("host_style")) {
    string s;
    s = config["host_style"];
    if (s != "virtual") {
      host_style = PathStyle;
    } else {
      host_style = VirtualStyle;
    }
  }
  if (config.exists("target_storage_class")) {
    target_storage_class = config["target_storage_class"];
  }
  if (config.exists("access_key")) {
    key.id = config["access_key"];
  }
  if (config.exists("secret")) {
    key.key = config["secret"];
  }
  if (config.exists("multipart_sync_threshold")) {
    r = conf_to_uint64(config, "multipart_sync_threshold", &multipart_sync_threshold);
    if (r < 0) {
      multipart_sync_threshold = DEFAULT_MULTIPART_SYNC_PART_SIZE;
    }
  }

  if (config.exists("multipart_min_part_size")) {
    r = conf_to_uint64(config, "multipart_min_part_size", &multipart_min_part_size);
    if (r < 0) {
      multipart_min_part_size = DEFAULT_MULTIPART_SYNC_PART_SIZE;
    }
  }

  if (config.exists("acls")) {
    const JSONFormattable& cc = config["acls"];
    if (cc.is_array()) {
      for (auto& c : cc.array()) {
        RGWTierACLMapping m;
        m.init(c);
        if (!m.source_id.empty()) {
          acl_mappings[m.source_id] = m;
        }
      }
    } else {
      RGWTierACLMapping m;
      m.init(cc);
      if (!m.source_id.empty()) {
        acl_mappings[m.source_id] = m;
      }
    }
  }
  return 0;
}

int RGWZoneGroupPlacementTierS3::clear_params(const JSONFormattable& config)
{
  if (config.exists("endpoint")) {
    endpoint.clear();
  }
  if (config.exists("target_path")) {
    target_path.clear();
  }
  if (config.exists("region")) {
    region.clear();
  }
  if (config.exists("host_style")) {
    /* default */
    host_style = PathStyle;
  }
  if (config.exists("target_storage_class")) {
    target_storage_class.clear();
  }
  if (config.exists("access_key")) {
    key.id.clear();
  }
  if (config.exists("secret")) {
    key.key.clear();
  }
  if (config.exists("multipart_sync_threshold")) {
    multipart_sync_threshold = DEFAULT_MULTIPART_SYNC_PART_SIZE;
  }
  if (config.exists("multipart_min_part_size")) {
    multipart_min_part_size = DEFAULT_MULTIPART_SYNC_PART_SIZE;
  }
  if (config.exists("acls")) {
    const JSONFormattable& cc = config["acls"];
    if (cc.is_array()) {
      for (auto& c : cc.array()) {
        RGWTierACLMapping m;
        m.init(c);
        acl_mappings.erase(m.source_id);
      }
    } else {
      RGWTierACLMapping m;
      m.init(cc);
      acl_mappings.erase(m.source_id);
    }
  }
  return 0;
}

void rgw_meta_sync_info::generate_test_instances(list<rgw_meta_sync_info*>& o)
{
  auto info = new rgw_meta_sync_info;
  info->state = rgw_meta_sync_info::StateBuildingFullSyncMaps;
  info->period = "periodid";
  info->realm_epoch = 5;
  o.push_back(info);
  o.push_back(new rgw_meta_sync_info);
}

void rgw_meta_sync_marker::generate_test_instances(list<rgw_meta_sync_marker*>& o)
{
  auto marker = new rgw_meta_sync_marker;
  marker->state = rgw_meta_sync_marker::IncrementalSync;
  marker->marker = "01234";
  marker->realm_epoch = 5;
  o.push_back(marker);
  o.push_back(new rgw_meta_sync_marker);
}

void rgw_meta_sync_status::generate_test_instances(list<rgw_meta_sync_status*>& o)
{
  o.push_back(new rgw_meta_sync_status);
}

void RGWZoneParams::generate_test_instances(list<RGWZoneParams*> &o)
{
  o.push_back(new RGWZoneParams);
  o.push_back(new RGWZoneParams);
}

void RGWPeriodLatestEpochInfo::generate_test_instances(list<RGWPeriodLatestEpochInfo*> &o)
{
  RGWPeriodLatestEpochInfo *z = new RGWPeriodLatestEpochInfo;
  o.push_back(z);
  o.push_back(new RGWPeriodLatestEpochInfo);
}

void RGWZoneGroup::generate_test_instances(list<RGWZoneGroup*>& o)
{
  RGWZoneGroup *r = new RGWZoneGroup;
  o.push_back(r);
  o.push_back(new RGWZoneGroup);
}

void RGWPeriodLatestEpochInfo::dump(Formatter *f) const {
  encode_json("latest_epoch", epoch, f);
}

void RGWPeriodLatestEpochInfo::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("latest_epoch", epoch, obj);
}

void RGWNameToId::dump(Formatter *f) const {
  encode_json("obj_id", obj_id, f);
}

void RGWNameToId::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("obj_id", obj_id, obj);
}

void RGWNameToId::generate_test_instances(list<RGWNameToId*>& o) {
  RGWNameToId *n = new RGWNameToId;
  n->obj_id = "id";
  o.push_back(n);
  o.push_back(new RGWNameToId);
}

