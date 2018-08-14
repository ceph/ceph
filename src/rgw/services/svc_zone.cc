#include "svc_zone.h"
#include "svc_rados.h"

#include "rgw/rgw_zone.h"

std::map<string, RGWServiceInstance::dependency> RGWSI_Zone::get_deps()
{
  RGWServiceInstance::dependency dep = { .name = "rados",
                                         .conf = "{}" };
  map<string, RGWServiceInstance::dependency> deps;
  deps["rados_dep"] = dep;
  return deps;
}

int RGWSI_Zone::init(const string& conf, std::map<std::string, RGWServiceInstanceRef>& dep_refs)
{
  rados_svc = static_pointer_cast<RGWSI_RADOS>(dep_refs["rados_dep"]);
  assert(rados_svc);
  return 0;
}

RGWZoneParams& RGWSI_Zone::get_zone_params()
{
  return *zone_params;
}

RGWZone& RGWSI_Zone::get_zone()
{
  return *zone_public_config;
}

RGWZoneGroup& RGWSI_Zone::get_zonegroup()
{
  return *zonegroup;
}

int RGWSI_Zone::get_zonegroup(const string& id, RGWZoneGroup& zonegroup)
{
  int ret = 0;
  if (id == get_zonegroup().get_id()) {
    zonegroup = get_zonegroup();
  } else if (!current_period->get_id().empty()) {
    ret = current_period->get_zonegroup(zonegroup, id);
  }
  return ret;
}

RGWRealm& RGWSI_Zone::get_realm()
{
  return *realm;
}

RGWPeriod& RGWSI_Zone::get_current_period()
{
  return *current_period;
}

const string& RGWSI_Zone::get_current_period_id()
{
  return current_period->get_id();
}

bool RGWSI_Zone::has_zonegroup_api(const std::string& api) const
{
  if (!current_period->get_id().empty()) {
    const auto& zonegroups_by_api = current_period->get_map().zonegroups_by_api;
    if (zonegroups_by_api.find(api) != zonegroups_by_api.end())
      return true;
  } else if (zonegroup->api_name == api) {
    return true;
  }
  return false;
}

string RGWSI_Zone::gen_host_id() {
  /* uint64_t needs 16, two '-' separators and a trailing null */
  const string& zone_name = zone_public_config->name;
  const string& zonegroup_name = zonegroup->get_name();
  char charbuf[16 + zone_name.size() + zonegroup_name.size() + 2 + 1];
  snprintf(charbuf, sizeof(charbuf), "%llx-%s-%s", (unsigned long long)rados_svc->instance_id(), zone_name.c_str(), zonegroup_name.c_str());
  return string(charbuf);
}

string RGWSI_Zone::unique_id(uint64_t unique_num)
{
  char buf[32];
  snprintf(buf, sizeof(buf), ".%llu.%llu", (unsigned long long)rados_svc->instance_id(), (unsigned long long)unique_num);
  string s = zone_params->get_id() + buf;
  return s;
}

bool RGWSI_Zone::zone_is_writeable()
{
  return writeable_zone && !get_zone().is_read_only();
}

uint32_t RGWSI_Zone::get_zone_short_id() const
{
  return zone_short_id;
}

const string& RGWSI_Zone::zone_name()
{
  return get_zone_params().get_name();
}
const string& RGWSI_Zone::zone_id()
{
  return get_zone_params().get_id();
}

bool RGWSI_Zone::need_to_log_data() const
{
  return zone_public_config->log_data;
}

bool RGWSI_Zone::is_meta_master() const
{
  if (!zonegroup->is_master_zonegroup()) {
    return false;
  }

  return (zonegroup->master_zone == zone_public_config->id);
}

bool RGWSI_Zone::need_to_log_metadata() const
{
  return is_meta_master() &&
    (zonegroup->zones.size() > 1 || current_period->is_multi_zonegroups_with_zones());
}

bool RGWSI_Zone::can_reshard() const
{
  return current_period->get_id().empty() ||
    (zonegroup->zones.size() == 1 && current_period->is_single_zonegroup());
}

