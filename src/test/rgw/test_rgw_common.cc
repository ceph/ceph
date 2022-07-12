#include "test_rgw_common.h"

void test_rgw_add_placement(RGWZoneGroup *zonegroup, RGWZoneParams *zone_params, const std::string& name, bool is_default)
{
  zonegroup->placement_targets[name] = { name };

  RGWZonePlacementInfo& pinfo = zone_params->placement_pools[name];
  pinfo.index_pool = rgw_pool(name + ".index").to_str();

  rgw_pool data_pool(name + ".data");
  pinfo.storage_classes.set_storage_class(RGW_STORAGE_CLASS_STANDARD, &data_pool, nullptr);
  pinfo.data_extra_pool = rgw_pool(name + ".extra").to_str();

  if (is_default) {
    zonegroup->default_placement = rgw_placement_rule(name, RGW_STORAGE_CLASS_STANDARD);
  }
}

void test_rgw_init_env(RGWZoneGroup *zonegroup, RGWZoneParams *zone_params)
{
  test_rgw_add_placement(zonegroup, zone_params, "default-placement", true);

}

void test_rgw_populate_explicit_placement_bucket(rgw_bucket *b, const char *t, const char *n, const char *dp, const char *ip, const char *m, const char *id)
{
  b->tenant = t;
  b->name = n;
  b->marker = m;
  b->bucket_id = id;
  b->explicit_placement.data_pool = rgw_pool(dp);
  b->explicit_placement.index_pool = rgw_pool(ip);
}

void test_rgw_populate_old_bucket(old_rgw_bucket *b, const char *t, const char *n, const char *dp, const char *ip, const char *m, const char *id)
{
  b->tenant = t;
  b->name = n;
  b->marker = m;
  b->bucket_id = id;
  b->data_pool = dp;
  b->index_pool = ip;
}

std::string test_rgw_get_obj_oid(const rgw_obj& obj)
{
  std::string oid;
  std::string loc;

  get_obj_bucket_and_oid_loc(obj, oid, loc);
  return oid;
}

void test_rgw_init_explicit_placement_bucket(rgw_bucket *bucket, const char *name)
{
  test_rgw_populate_explicit_placement_bucket(bucket, "", name, ".data-pool", ".index-pool", "marker", "bucket-id");
}

void test_rgw_init_old_bucket(old_rgw_bucket *bucket, const char *name)
{
  test_rgw_populate_old_bucket(bucket, "", name, ".data-pool", ".index-pool", "marker", "bucket-id");
}

void test_rgw_populate_bucket(rgw_bucket *b, const char *t, const char *n, const char *m, const char *id)
{
  b->tenant = t;
  b->name = n;
  b->marker = m;
  b->bucket_id = id;
}

void test_rgw_init_bucket(rgw_bucket *bucket, const char *name)
{
  test_rgw_populate_bucket(bucket, "", name, "marker", "bucket-id");
}

rgw_obj test_rgw_create_obj(const rgw_bucket& bucket, const std::string& name, const std::string& instance, const std::string& ns)
{
  rgw_obj obj(bucket, name);
  if (!instance.empty()) {
    obj.key.set_instance(instance);
  }
  if (!ns.empty()) {
    obj.key.ns = ns;
  }
  obj.bucket = bucket;

  return obj;
}


