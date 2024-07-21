// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <optional>

#include "common/errno.h"

#include "rgw_zone.h"
#include "rgw_sal.h"
#include "rgw_sal_config.h"
#include "rgw_sync.h"

#include "services/svc_zone.h"


#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

RGWMetaSyncStatusManager::~RGWMetaSyncStatusManager(){}

#define FIRST_EPOCH 1

struct RGWAccessKey;

namespace rgw_zone_defaults {

static std::string default_bucket_index_pool_suffix = "rgw.buckets.index";
static std::string default_storage_extra_pool_suffix = "rgw.buckets.non-ec";
static std::string zone_info_oid_prefix = "zone_info.";

std::string zone_names_oid_prefix = "zone_names.";
std::string region_info_oid_prefix = "region_info.";
std::string zone_group_info_oid_prefix = "zonegroup_info.";
std::string default_region_info_oid = "default.region";
std::string default_zone_group_info_oid = "default.zonegroup";
std::string region_map_oid = "region_map";
std::string default_zonegroup_name = "default";
std::string default_zone_name = "default";
std::string zonegroup_names_oid_prefix = "zonegroups_names.";
std::string RGW_DEFAULT_ZONE_ROOT_POOL = "rgw.root";
std::string RGW_DEFAULT_ZONEGROUP_ROOT_POOL = "rgw.root";
std::string RGW_DEFAULT_PERIOD_ROOT_POOL = "rgw.root";
std::string default_storage_pool_suffix = "rgw.buckets.data";

}

using namespace std;
using namespace rgw_zone_defaults;

void encode_json_plain(const char *name, const RGWAccessKey& val, Formatter *f)
{
  f->open_object_section(name);
  val.dump_plain(f);
  f->close_section();
}

static void decode_zones(map<rgw_zone_id, RGWZone>& zones, JSONObj *o)
{
  RGWZone z;
  z.decode_json(o);
  zones[z.id] = z;
}

static void decode_placement_targets(map<string, RGWZoneGroupPlacementTarget>& targets, JSONObj *o)
{
  RGWZoneGroupPlacementTarget t;
  t.decode_json(o);
  targets[t.name] = t;
}

void RGWZone::generate_test_instances(list<RGWZone*> &o)
{
  RGWZone *z = new RGWZone;
  o.push_back(z);
  o.push_back(new RGWZone);
}

void RGWZone::dump(Formatter *f) const
{
  encode_json("id", id, f);
  encode_json("name", name, f);
  encode_json("endpoints", endpoints, f);
  encode_json("log_meta", log_meta, f);
  encode_json("log_data", log_data, f);
  encode_json("bucket_index_max_shards", bucket_index_max_shards, f);
  encode_json("read_only", read_only, f);
  encode_json("tier_type", tier_type, f);
  encode_json("sync_from_all", sync_from_all, f);
  encode_json("sync_from", sync_from, f);
  encode_json("redirect_zone", redirect_zone, f);
  encode_json("supported_features", supported_features, f);
}

void RGWZone::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("name", name, obj);
  if (id.empty()) {
    id = name;
  }
  JSONDecoder::decode_json("endpoints", endpoints, obj);
  JSONDecoder::decode_json("log_meta", log_meta, obj);
  JSONDecoder::decode_json("log_data", log_data, obj);
  JSONDecoder::decode_json("bucket_index_max_shards", bucket_index_max_shards, obj);
  JSONDecoder::decode_json("read_only", read_only, obj);
  JSONDecoder::decode_json("tier_type", tier_type, obj);
  JSONDecoder::decode_json("sync_from_all", sync_from_all, true, obj);
  JSONDecoder::decode_json("sync_from", sync_from, obj);
  JSONDecoder::decode_json("redirect_zone", redirect_zone, obj);
  JSONDecoder::decode_json("supported_features", supported_features, obj);
}

rgw_pool RGWZoneGroup::get_pool(CephContext *cct_) const
{
  if (cct_->_conf->rgw_zonegroup_root_pool.empty()) {
    return rgw_pool(RGW_DEFAULT_ZONEGROUP_ROOT_POOL);
  }

  return rgw_pool(cct_->_conf->rgw_zonegroup_root_pool);
}

void RGWZoneGroup::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("name", name, obj);
  if (id.empty()) {
    derr << "old format " << dendl;
    JSONDecoder::decode_json("name", name, obj);
    id = name;
  }
  JSONDecoder::decode_json("api_name", api_name, obj);
  JSONDecoder::decode_json("is_master", is_master, obj);
  JSONDecoder::decode_json("endpoints", endpoints, obj);
  JSONDecoder::decode_json("hostnames", hostnames, obj);
  JSONDecoder::decode_json("hostnames_s3website", hostnames_s3website, obj);
  JSONDecoder::decode_json("master_zone", master_zone, obj);
  JSONDecoder::decode_json("zones", zones, decode_zones, obj);
  JSONDecoder::decode_json("placement_targets", placement_targets, decode_placement_targets, obj);
  string pr;
  JSONDecoder::decode_json("default_placement", pr, obj);
  default_placement.from_str(pr);
  JSONDecoder::decode_json("realm_id", realm_id, obj);
  JSONDecoder::decode_json("sync_policy", sync_policy, obj);
  JSONDecoder::decode_json("enabled_features", enabled_features, obj);
}

void RGWZoneParams::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("domain_root", domain_root, obj);
  JSONDecoder::decode_json("control_pool", control_pool, obj);
  JSONDecoder::decode_json("dedup_pool", dedup_pool, obj);
  JSONDecoder::decode_json("gc_pool", gc_pool, obj);
  JSONDecoder::decode_json("lc_pool", lc_pool, obj);
  JSONDecoder::decode_json("log_pool", log_pool, obj);
  JSONDecoder::decode_json("intent_log_pool", intent_log_pool, obj);
  JSONDecoder::decode_json("roles_pool", roles_pool, obj);
  JSONDecoder::decode_json("reshard_pool", reshard_pool, obj);
  JSONDecoder::decode_json("usage_log_pool", usage_log_pool, obj);
  JSONDecoder::decode_json("user_keys_pool", user_keys_pool, obj);
  JSONDecoder::decode_json("user_email_pool", user_email_pool, obj);
  JSONDecoder::decode_json("user_swift_pool", user_swift_pool, obj);
  JSONDecoder::decode_json("user_uid_pool", user_uid_pool, obj);
  JSONDecoder::decode_json("otp_pool", otp_pool, obj);
  JSONDecoder::decode_json("notif_pool", notif_pool, obj);
  JSONDecoder::decode_json("topics_pool", topics_pool, obj);
  JSONDecoder::decode_json("account_pool", account_pool, obj);
  JSONDecoder::decode_json("group_pool", group_pool, obj);
  JSONDecoder::decode_json("system_key", system_key, obj);
  JSONDecoder::decode_json("placement_pools", placement_pools, obj);
  JSONDecoder::decode_json("tier_config", tier_config, obj);
  JSONDecoder::decode_json("realm_id", realm_id, obj);
}

void RGWZoneParams::dump(Formatter *f) const
{
  encode_json("id", id, f);
  encode_json("name", name, f);
  encode_json("domain_root", domain_root, f);
  encode_json("control_pool", control_pool, f);
  encode_json("dedup_pool", dedup_pool, f);
  encode_json("gc_pool", gc_pool, f);
  encode_json("lc_pool", lc_pool, f);
  encode_json("log_pool", log_pool, f);
  encode_json("intent_log_pool", intent_log_pool, f);
  encode_json("usage_log_pool", usage_log_pool, f);
  encode_json("roles_pool", roles_pool, f);
  encode_json("reshard_pool", reshard_pool, f);
  encode_json("user_keys_pool", user_keys_pool, f);
  encode_json("user_email_pool", user_email_pool, f);
  encode_json("user_swift_pool", user_swift_pool, f);
  encode_json("user_uid_pool", user_uid_pool, f);
  encode_json("otp_pool", otp_pool, f);
  encode_json("notif_pool", notif_pool, f);
  encode_json("topics_pool", topics_pool, f);
  encode_json("account_pool", account_pool, f);
  encode_json("group_pool", group_pool, f);
  encode_json_plain("system_key", system_key, f);
  encode_json("placement_pools", placement_pools, f);
  encode_json("tier_config", tier_config, f);
  encode_json("realm_id", realm_id, f);
}

rgw_pool RGWZoneParams::get_pool(CephContext *cct) const
{
  if (cct->_conf->rgw_zone_root_pool.empty()) {
    return rgw_pool(RGW_DEFAULT_ZONE_ROOT_POOL);
  }

  return rgw_pool(cct->_conf->rgw_zone_root_pool);
}

rgw_pool fix_zone_pool_dup(const set<rgw_pool>& pools,
                           const string& default_prefix,
                           const string& default_suffix,
                           const rgw_pool& suggested_pool)
{
  string suggested_name = suggested_pool.to_str();

  string prefix = default_prefix;
  string suffix = default_suffix;

  if (!suggested_pool.empty()) {
    prefix = suggested_name.substr(0, suggested_name.find("."));
    suffix = suggested_name.substr(prefix.length());
  }

  rgw_pool pool(prefix + suffix);
 
  while (pools.count(pool)) {
    pool = prefix + "_" + std::to_string(std::rand()) + suffix;
  }
  return pool;
}

void add_zone_pools(const RGWZoneParams& info,
                    std::set<rgw_pool>& pools)
{
  pools.insert(info.domain_root);
  pools.insert(info.control_pool);
  pools.insert(info.dedup_pool);
  pools.insert(info.gc_pool);
  pools.insert(info.log_pool);
  pools.insert(info.intent_log_pool);
  pools.insert(info.usage_log_pool);
  pools.insert(info.user_keys_pool);
  pools.insert(info.user_email_pool);
  pools.insert(info.user_swift_pool);
  pools.insert(info.user_uid_pool);
  pools.insert(info.otp_pool);
  pools.insert(info.roles_pool);
  pools.insert(info.reshard_pool);
  pools.insert(info.oidc_pool);
  pools.insert(info.notif_pool);
  pools.insert(info.topics_pool);
  pools.insert(info.account_pool);
  pools.insert(info.group_pool);

  for (const auto& [pname, placement] : info.placement_pools) {
    pools.insert(placement.index_pool);
    for (const auto& [sname, sc] : placement.storage_classes.get_all()) {
      if (sc.data_pool) {
        pools.insert(sc.data_pool.get());
      }
    }
    pools.insert(placement.data_extra_pool);
  }
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

int RGWZoneGroup::equals(const string& other_zonegroup) const
{
  if (is_master && other_zonegroup.empty())
    return true;

  return (id  == other_zonegroup);
}

void RGWDefaultSystemMetaObjInfo::dump(Formatter *f) const {
  encode_json("default_id", default_id, f);
}

void RGWDefaultSystemMetaObjInfo::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("default_id", default_id, obj);
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
/// Generate a random uuid for realm/period/zonegroup/zone ids
std::string gen_random_uuid()
{
  uuid_d uuid;
  uuid.generate_random();
  return uuid.to_string();
}

int get_zones_pool_set(const DoutPrefixProvider *dpp,
                       optional_yield y,
                       rgw::sal::ConfigStore* cfgstore,
                       std::string_view my_zone_id,
                       std::set<rgw_pool>& pools)
{
  std::array<std::string, 128> zone_names;
  rgw::sal::ListResult<std::string> listing;
  do {
    int r = cfgstore->list_zone_names(dpp, y, listing.next,
                                      zone_names, listing);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "failed to list zones with " << cpp_strerror(r) << dendl;
      return r;
    }

    for (const auto& name : listing.entries) {
      RGWZoneParams info;
      r = cfgstore->read_zone_by_name(dpp, y, name, info, nullptr);
      if (r < 0) {
        ldpp_dout(dpp, 0) << "failed to load zone " << name
            << " with " << cpp_strerror(r) << dendl;
        return r;
      }
      if (info.get_id() != my_zone_id) {
        add_zone_pools(info, pools);
      }
    }
  } while (!listing.next.empty());

  return 0;
}

}

void RGWPeriodConfig::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("bucket_quota", quota.bucket_quota, obj);
  JSONDecoder::decode_json("user_quota", quota.user_quota, obj);
  JSONDecoder::decode_json("user_ratelimit", user_ratelimit, obj);
  JSONDecoder::decode_json("bucket_ratelimit", bucket_ratelimit, obj);
  JSONDecoder::decode_json("anonymous_ratelimit", anon_ratelimit, obj);
}

void RGWPeriodConfig::dump(Formatter *f) const
{
  encode_json("bucket_quota", quota.bucket_quota, f);
  encode_json("user_quota", quota.user_quota, f);
  encode_json("user_ratelimit", user_ratelimit, f);
  encode_json("bucket_ratelimit", bucket_ratelimit, f);
  encode_json("anonymous_ratelimit", anon_ratelimit, f);
}

void RGWZoneGroup::dump(Formatter *f) const
{
  encode_json("id", id , f);
  encode_json("name", name , f);
  encode_json("api_name", api_name, f);
  encode_json("is_master", is_master, f);
  encode_json("endpoints", endpoints, f);
  encode_json("hostnames", hostnames, f);
  encode_json("hostnames_s3website", hostnames_s3website, f);
  encode_json("master_zone", master_zone, f);
  encode_json_map("zones", zones, f); /* more friendly representation */
  encode_json_map("placement_targets", placement_targets, f); /* more friendly representation */
  encode_json("default_placement", default_placement, f);
  encode_json("realm_id", realm_id, f);
  encode_json("sync_policy", sync_policy, f);
  encode_json("enabled_features", enabled_features, f);
}

void RGWZoneGroupPlacementTarget::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("tags", tags, obj);
  JSONDecoder::decode_json("storage_classes", storage_classes, obj);
  if (storage_classes.empty()) {
    storage_classes.insert(RGW_STORAGE_CLASS_STANDARD);
  }
  JSONDecoder::decode_json("tier_targets", tier_targets, obj);
}

void RGWZonePlacementInfo::dump(Formatter *f) const
{
  encode_json("index_pool", index_pool, f);
  encode_json("storage_classes", storage_classes, f);
  encode_json("data_extra_pool", data_extra_pool, f);
  encode_json("index_type", (uint32_t)index_type, f);
  encode_json("inline_data", inline_data, f);

  /* no real need for backward compatibility of compression_type and data_pool in here,
   * rather not clutter the output */
}

void RGWZonePlacementInfo::generate_test_instances(list<RGWZonePlacementInfo*>& o)
{
  o.push_back(new RGWZonePlacementInfo);
  o.push_back(new RGWZonePlacementInfo);
  o.back()->index_pool = rgw_pool("rgw.buckets.index");
  
  o.back()->data_extra_pool = rgw_pool("rgw.buckets.non-ec");
  o.back()->index_type = rgw::BucketIndexType::Normal;
  o.back()->inline_data = false;
}

void RGWZonePlacementInfo::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("index_pool", index_pool, obj);
  JSONDecoder::decode_json("storage_classes", storage_classes, obj);
  JSONDecoder::decode_json("data_extra_pool", data_extra_pool, obj);
  uint32_t it;
  JSONDecoder::decode_json("index_type", it, obj);
  if(!JSONDecoder::decode_json("inline_data", inline_data, obj)) {
    inline_data = true;
  }
  index_type = (rgw::BucketIndexType)it;

  /* backward compatibility, these are now defined in storage_classes */
  string standard_compression_type;
  string *pcompression = nullptr;
  if (JSONDecoder::decode_json("compression", standard_compression_type, obj)) {
    pcompression = &standard_compression_type;
  }
  rgw_pool standard_data_pool;
  rgw_pool *ppool = nullptr;
  if (JSONDecoder::decode_json("data_pool", standard_data_pool, obj)) {
    ppool = &standard_data_pool;
  }
  if (ppool || pcompression) {
    storage_classes.set_storage_class(RGW_STORAGE_CLASS_STANDARD, ppool, pcompression);
  }
}

void RGWZoneGroupPlacementTarget::dump(Formatter *f) const
{
  encode_json("name", name, f);
  encode_json("tags", tags, f);
  encode_json("storage_classes", storage_classes, f);
  if (!tier_targets.empty()) {
    encode_json("tier_targets", tier_targets, f);
  }
}

void RGWZoneGroupPlacementTier::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("tier_type", tier_type, obj);
  JSONDecoder::decode_json("storage_class", storage_class, obj);
  JSONDecoder::decode_json("retain_head_object", retain_head_object, obj);
  if (is_tier_type_s3()) {
    JSONDecoder::decode_json("s3", t.s3, obj);
  }
  JSONDecoder::decode_json("allow_read_through", allow_read_through, obj);
  JSONDecoder::decode_json("read_through_restore_days", read_through_restore_days, obj);
  JSONDecoder::decode_json("restore_storage_class", restore_storage_class, obj);
  if (is_tier_type_s3_glacier()) {
    JSONDecoder::decode_json("s3-glacier", s3_glacier, obj);
  }
}

void RGWZoneStorageClasses::dump(Formatter *f) const
{
  for (auto& i : m) {
    encode_json(i.first.c_str(), i.second, f);
  }
}

void RGWZoneStorageClasses::generate_test_instances(list<RGWZoneStorageClasses*>& o)
{
  o.push_back(new RGWZoneStorageClasses);
}

void RGWZoneStorageClasses::decode_json(JSONObj *obj)
{
  JSONFormattable f;
  decode_json_obj(f, obj);

  for (auto& field : f.object()) {
    JSONObj *field_obj = obj->find_obj(field.first);
    assert(field_obj);

    decode_json_obj(m[field.first], field_obj);
  }
  standard_class = &m[RGW_STORAGE_CLASS_STANDARD];
}

void RGWZoneGroupTierS3Glacier::dump(Formatter *f) const
{
  encode_json("glacier_restore_days", glacier_restore_days, f);
  string s = (glacier_restore_tier_type == Standard ? "Standard" : "Expedited");
  encode_json("glacier_restore_tier_type", s, f);
}

void RGWZoneGroupTierS3Glacier::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("glacier_restore_days", glacier_restore_days, obj);
  string s;
  JSONDecoder::decode_json("glacier_restore_tier_type", s, obj);
  if (s != "Expedited") {
    glacier_restore_tier_type = Standard;
  } else {
    glacier_restore_tier_type = Expedited;
  }
}

void RGWZoneGroupPlacementTier::dump(Formatter *f) const
{
  encode_json("tier_type", tier_type, f);
  encode_json("storage_class", storage_class, f);
  encode_json("retain_head_object", retain_head_object, f);
  if (is_tier_type_s3()) {
    encode_json("s3", t.s3, f);
  }
  encode_json("allow_read_through", allow_read_through, f);
  encode_json("read_through_restore_days", read_through_restore_days, f);
  encode_json("restore_storage_class", restore_storage_class, f);

  if (is_tier_type_s3_glacier()) {
    encode_json("s3-glacier", s3_glacier, f);
  }
}

void RGWZoneGroupPlacementTierS3::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("endpoint", endpoint, obj);
  JSONDecoder::decode_json("access_key", key.id, obj);
  JSONDecoder::decode_json("secret", key.key, obj);
  JSONDecoder::decode_json("region", region, obj);
  string s;
  JSONDecoder::decode_json("host_style", s, obj);
  if (s != "virtual") {
    host_style = PathStyle;
  } else {
    host_style = VirtualStyle;
  }
  JSONDecoder::decode_json("target_storage_class", target_storage_class, obj);
  JSONDecoder::decode_json("target_path", target_path, obj);
  JSONDecoder::decode_json("acl_mappings", acl_mappings, obj);
  JSONDecoder::decode_json("multipart_sync_threshold", multipart_sync_threshold, obj);
  JSONDecoder::decode_json("multipart_min_part_size", multipart_min_part_size, obj);
}

void RGWZoneStorageClass::dump(Formatter *f) const
{
  if (data_pool) {
    encode_json("data_pool", data_pool.get(), f);
  }
  if (compression_type) {
    encode_json("compression_type", compression_type.get(), f);
  }
}

void RGWZoneStorageClass::generate_test_instances(list<RGWZoneStorageClass*>& o)
{
  o.push_back(new RGWZoneStorageClass);
  o.push_back(new RGWZoneStorageClass);
  o.back()->data_pool = rgw_pool("pool1");
  o.back()->compression_type = "zlib";
}

void RGWZoneStorageClass::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("data_pool", data_pool, obj);
  JSONDecoder::decode_json("compression_type", compression_type, obj);
}

void RGWTierACLMapping::decode_json(JSONObj *obj)
{
  string s;
  JSONDecoder::decode_json("type", s, obj);
  if (s == "email") {
    type = ACL_TYPE_EMAIL_USER;
  } else if (s == "uri") {
    type = ACL_TYPE_GROUP;
  } else {
    type = ACL_TYPE_CANON_USER;
  }

  JSONDecoder::decode_json("source_id", source_id, obj);
  JSONDecoder::decode_json("dest_id", dest_id, obj);
}

void RGWZoneGroupPlacementTierS3::dump(Formatter *f) const
{
  encode_json("endpoint", endpoint, f);
  encode_json("access_key", key.id, f);
  encode_json("secret", key.key, f);
  encode_json("region", region, f);
  string s = (host_style == PathStyle ? "path" : "virtual");
  encode_json("host_style", s, f);
  encode_json("target_storage_class", target_storage_class, f);
  encode_json("target_path", target_path, f);
  encode_json("acl_mappings", acl_mappings, f);
  encode_json("multipart_sync_threshold", multipart_sync_threshold, f);
  encode_json("multipart_min_part_size", multipart_min_part_size, f);
}

void RGWTierACLMapping::dump(Formatter *f) const
{
  string s;
  switch (type) {
    case ACL_TYPE_EMAIL_USER:
      s = "email";
      break;
    case ACL_TYPE_GROUP:
      s = "uri";
      break;
    default:
      s = "id";
      break;
  }
  encode_json("type", s, f);
  encode_json("source_id", source_id, f);
  encode_json("dest_id", dest_id, f);
}

void RGWPeriodMap::dump(Formatter *f) const
{
  encode_json("id", id, f);
  encode_json_map("zonegroups", zonegroups, f);
  encode_json("short_zone_ids", short_zone_ids, f);
}

static void decode_zonegroups(map<string, RGWZoneGroup>& zonegroups, JSONObj *o)
{
  RGWZoneGroup zg;
  zg.decode_json(o);
  zonegroups[zg.get_id()] = zg;
}

void RGWPeriodMap::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("zonegroups", zonegroups, decode_zonegroups, obj);
  /* backward compatability with region */
  if (zonegroups.empty()) {
    JSONDecoder::decode_json("regions", zonegroups, obj);
  }
  /* backward compatability with region */
  if (master_zonegroup.empty()) {
    JSONDecoder::decode_json("master_region", master_zonegroup, obj);
  }
  JSONDecoder::decode_json("short_zone_ids", short_zone_ids, obj);
}

void RGWPeriodMap::decode(bufferlist::const_iterator& bl) {
  DECODE_START(2, bl);
  decode(id, bl);
  decode(zonegroups, bl);
  decode(master_zonegroup, bl);
  if (struct_v >= 2) {
    decode(short_zone_ids, bl);
  }
  DECODE_FINISH(bl);

  zonegroups_by_api.clear();
  for (map<string, RGWZoneGroup>::iterator iter = zonegroups.begin();
       iter != zonegroups.end(); ++iter) {
    RGWZoneGroup& zonegroup = iter->second;
    zonegroups_by_api[zonegroup.api_name] = zonegroup;
    if (zonegroup.is_master_zonegroup()) {
      master_zonegroup = zonegroup.get_id();
    }
  }
}

void RGWPeriodMap::encode(bufferlist& bl) const
{
  ENCODE_START(2, 1, bl);
  encode(id, bl);
  encode(zonegroups, bl);
  encode(master_zonegroup, bl);
  encode(short_zone_ids, bl);
  ENCODE_FINISH(bl);
}

bool RGWPeriodMap::find_zone_by_id(const rgw_zone_id& zone_id,
                                   RGWZoneGroup *zonegroup,
                                   RGWZone *zone) const
{
  for (auto& iter : zonegroups) {
    auto& zg = iter.second;

    auto ziter = zg.zones.find(zone_id);
    if (ziter != zg.zones.end()) {
      *zonegroup = zg;
      *zone = ziter->second;
      return true;
    }
  }

  return false;
}

namespace rgw {

int init_zone_pool_names(const DoutPrefixProvider *dpp, optional_yield y,
                         const std::set<rgw_pool>& pools, RGWZoneParams& info)
{
  info.domain_root = fix_zone_pool_dup(pools, info.name, ".rgw.meta:root", info.domain_root);
  info.control_pool = fix_zone_pool_dup(pools, info.name, ".rgw.control", info.control_pool);
  info.dedup_pool = fix_zone_pool_dup(pools, info.name, ".rgw.dedup", info.dedup_pool);
  info.gc_pool = fix_zone_pool_dup(pools, info.name, ".rgw.log:gc", info.gc_pool);
  info.lc_pool = fix_zone_pool_dup(pools, info.name, ".rgw.log:lc", info.lc_pool);
  info.log_pool = fix_zone_pool_dup(pools, info.name, ".rgw.log", info.log_pool);
  info.intent_log_pool = fix_zone_pool_dup(pools, info.name, ".rgw.log:intent", info.intent_log_pool);
  info.usage_log_pool = fix_zone_pool_dup(pools, info.name, ".rgw.log:usage", info.usage_log_pool);
  info.user_keys_pool = fix_zone_pool_dup(pools, info.name, ".rgw.meta:users.keys", info.user_keys_pool);
  info.user_email_pool = fix_zone_pool_dup(pools, info.name, ".rgw.meta:users.email", info.user_email_pool);
  info.user_swift_pool = fix_zone_pool_dup(pools, info.name, ".rgw.meta:users.swift", info.user_swift_pool);
  info.user_uid_pool = fix_zone_pool_dup(pools, info.name, ".rgw.meta:users.uid", info.user_uid_pool);
  info.roles_pool = fix_zone_pool_dup(pools, info.name, ".rgw.meta:roles", info.roles_pool);
  info.reshard_pool = fix_zone_pool_dup(pools, info.name, ".rgw.log:reshard", info.reshard_pool);
  info.otp_pool = fix_zone_pool_dup(pools, info.name, ".rgw.otp", info.otp_pool);
  info.oidc_pool = fix_zone_pool_dup(pools, info.name, ".rgw.meta:oidc", info.oidc_pool);
  info.notif_pool = fix_zone_pool_dup(pools, info.name, ".rgw.log:notif", info.notif_pool);
  info.topics_pool =
      fix_zone_pool_dup(pools, info.name, ".rgw.meta:topics", info.topics_pool);
  info.account_pool = fix_zone_pool_dup(pools, info.name, ".rgw.meta:accounts", info.account_pool);
  info.group_pool = fix_zone_pool_dup(pools, info.name, ".rgw.meta:groups", info.group_pool);

  for (auto& [pname, placement] : info.placement_pools) {
    placement.index_pool = fix_zone_pool_dup(pools, info.name, "." + default_bucket_index_pool_suffix, placement.index_pool);
    placement.data_extra_pool= fix_zone_pool_dup(pools, info.name, "." + default_storage_extra_pool_suffix, placement.data_extra_pool);
    for (auto& [sname, sc] : placement.storage_classes.get_all()) {
      if (sc.data_pool) {
        sc.data_pool = fix_zone_pool_dup(pools, info.name, "." + default_storage_pool_suffix, *sc.data_pool);
      }
    }
  }

  return 0;
}

std::string get_zonegroup_endpoint(const RGWZoneGroup& info)
{
  if (!info.endpoints.empty()) {
    return info.endpoints.front();
  }
  // use zonegroup's master zone endpoints
  auto z = info.zones.find(info.master_zone);
  if (z != info.zones.end() && !z->second.endpoints.empty()) {
    return z->second.endpoints.front();
  }
  return "";
}

int add_zone_to_group(const DoutPrefixProvider* dpp, RGWZoneGroup& zonegroup,
                      const RGWZoneParams& zone_params,
                      const bool *pis_master, const bool *pread_only,
                      const std::list<std::string>& endpoints,
                      const std::string *ptier_type,
                      const bool *psync_from_all,
                      const std::list<std::string>& sync_from,
                      const std::list<std::string>& sync_from_rm,
                      const std::string *predirect_zone,
                      std::optional<int> bucket_index_max_shards,
                      const rgw::zone_features::set& enable_features,
                      const rgw::zone_features::set& disable_features)
{
  const std::string& zone_id = zone_params.id;
  const std::string& zone_name = zone_params.name;

  if (zone_id.empty()) {
    ldpp_dout(dpp, -1) << __func__ << " requires a zone id" << dendl;
    return -EINVAL;
  }
  if (zone_name.empty()) {
    ldpp_dout(dpp, -1) << __func__ << " requires a zone name" << dendl;
    return -EINVAL;
  }

  // check for duplicate zone name on insert
  if (!zonegroup.zones.count(zone_id)) {
    for (const auto& [id, zone] : zonegroup.zones) {
      if (zone.name == zone_name) {
        ldpp_dout(dpp, 0) << "ERROR: found existing zone name " << zone_name
            << " (" << id << ") in zonegroup " << zonegroup.name << dendl;
        return -EEXIST;
      }
    }
  }

  rgw_zone_id& master_zone = zonegroup.master_zone;
  if (pis_master) {
    if (*pis_master) {
      if (!master_zone.empty() && master_zone != zone_id) {
        ldpp_dout(dpp, 0) << "NOTICE: overriding master zone: "
            << master_zone << dendl;
      }
      master_zone = zone_id;
    } else if (master_zone == zone_id) {
      master_zone.clear();
    }
  } else if (master_zone.empty() && zonegroup.zones.empty()) {
    ldpp_dout(dpp, 0) << "NOTICE: promoted " << zone_name
        << " as new master_zone of zonegroup " << zonegroup.name << dendl;
    master_zone = zone_id;
  }

  // make sure the zone's placement targets are named in the zonegroup
  for (const auto& [name, placement] : zone_params.placement_pools) {
    auto target = RGWZoneGroupPlacementTarget{.name = name};
    zonegroup.placement_targets.emplace(name, std::move(target));
  }

  RGWZone& zone = zonegroup.zones[zone_params.id];
  zone.id = zone_params.id;
  zone.name = zone_params.name;
  if (!endpoints.empty()) {
    zone.endpoints = endpoints;
  }
  if (pread_only) {
    zone.read_only = *pread_only;
  }
  if (ptier_type) {
    zone.tier_type = *ptier_type;
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

  // add/remove sync_from
  for (auto add : sync_from) {
    zone.sync_from.insert(add);
  }

  for (const auto& rm : sync_from_rm) {
    auto i = zone.sync_from.find(rm);
    if (i == zone.sync_from.end()) {
      ldpp_dout(dpp, 1) << "WARNING: zone \"" << rm
          << "\" was not in sync_from" << dendl;
      continue;
    }
    zone.sync_from.erase(i);
  }

  // add/remove supported features
  zone.supported_features.insert(enable_features.begin(),
                                 enable_features.end());

  for (const auto& feature : disable_features) {
    if (zonegroup.enabled_features.contains(feature)) {
      ldpp_dout(dpp, -1) << "ERROR: Cannot disable zone feature \"" << feature
          << "\" until it's been disabled in zonegroup " << zonegroup.name << dendl;
      return -EINVAL;
    }
    auto i = zone.supported_features.find(feature);
    if (i == zone.supported_features.end()) {
      ldpp_dout(dpp, 1) << "WARNING: zone feature \"" << feature
          << "\" was not enabled in zone " << zone.name << dendl;
      continue;
    }
    zone.supported_features.erase(i);
  }

  const bool log_data = zonegroup.zones.size() > 1;
  for (auto& [id, zone] : zonegroup.zones) {
    zone.log_data = log_data;
  }

  return 0;
}

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
                  bool force_if_stale, const rgw::SiteConfig& site)
{
  ldpp_dout(dpp, 20) << __func__ << " realm " << realm.id
      << " period " << current_period.id << dendl;

  // gateway must be in the master zone to commit
  if (info.master_zone != site.get_zone_params().id) {
    error_stream << "Cannot commit period on zone "
        << site.get_zone_params().id << ", it must be sent to "
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

bool all_zonegroups_support(const SiteConfig& site, std::string_view feature)
{
  const auto& period = site.get_period();
  if (!period) {
    // if we're not in a realm, just check the local zonegroup
    return site.get_zonegroup().supports(feature);
  }
  const auto& zgs = period->period_map.zonegroups;
  return std::all_of(zgs.begin(), zgs.end(), [feature] (const auto& pair) {
      return pair.second.supports(feature);
    });
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
    if (r == -ENOENT) {
      if (realm_name.empty()) {
        // rgw_realm was not specified, and we found a default realm that
        // doesn't have a default zone. ignore the realm and try to load the
        // global default zone
        realm = std::nullopt;
        r = read_or_create_default_zone(dpp, y, cfgstore, zone_params);
      } else {
        ldpp_dout(dpp, 0) << "No rgw_zone configured, and the selected realm \""
            << realm->name << "\" does not have a default zone." << dendl;
      }
    }
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
    if (r != -ENOENT) {
      return r;
    }
    ldpp_dout(dpp, 10) << "cannot find current period zonegroup, "
        "using local zonegroup configuration" << dendl;
  }

  // fall back to a local zonegroup
  return load_local_zonegroup(dpp, y, cfgstore, zone_params.id);
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
  } else if (realm) {
    r = cfgstore->read_default_zonegroup(dpp, y, realm->id,
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
  if (config.exists("allow_read_through")) {
    string s = config["allow_read_through"];
    if (s == "true") {
      allow_read_through = true;
    } else {
      allow_read_through = false;
    }
  }
  if (config.exists("read_through_restore_days")) {
    r = conf_to_uint64(config, "read_through_restore_days", &read_through_restore_days);
    if (r < 0) {
      read_through_restore_days = DEFAULT_READ_THROUGH_RESTORE_DAYS;
    }
  }

  if (is_tier_type_s3()) {
    r = t.s3.update_params(config);
  }

  if (config.exists("restore_storage_class")) {
    restore_storage_class = config["restore_storage_class"];
  }

  if (is_tier_type_s3_glacier()) {
    r = s3_glacier.update_params(config);
  }
  return r;
}

int RGWZoneGroupPlacementTier::clear_params(const JSONFormattable& config)
{
  if (config.exists("retain_head_object")) {
    retain_head_object = false;
  }
  if (config.exists("allow_read_through")) {
    allow_read_through = false;
  }
  if (config.exists("read_through_restore_days")) {
    read_through_restore_days = DEFAULT_READ_THROUGH_RESTORE_DAYS;
  }

  if (is_tier_type_s3()) {
    t.s3.clear_params(config);
  }

  if (config.exists("restore_storage_class")) {
    restore_storage_class = RGW_STORAGE_CLASS_STANDARD;
  }

  if (is_tier_type_s3_glacier()) {
    s3_glacier.clear_params(config);
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

int RGWZoneGroupTierS3Glacier::update_params(const JSONFormattable& config)
{
  int r = -1;

  if (config.exists("glacier_restore_days")) {
    r = conf_to_uint64(config, "glacier_restore_days", &glacier_restore_days);
    if (r < 0) {
      glacier_restore_days = DEFAULT_GLACIER_RESTORE_DAYS;
    }
  }
  if (config.exists("glacier_restore_tier_type")) {
    string s;
    s = config["glacier_restore_tier_type"];
    if (s != "Expedited") {
      glacier_restore_tier_type = Standard;
    } else {
      glacier_restore_tier_type = Expedited;
    }
  }
  return 0;
}

int RGWZoneGroupTierS3Glacier::clear_params(const JSONFormattable& config)
{
  if (config.exists("glacier_restore_days")) {
    glacier_restore_days = DEFAULT_GLACIER_RESTORE_DAYS;
  }
  if (config.exists("glacier_restore_tier_type")) {
    /* default */
    glacier_restore_tier_type = Standard;
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

