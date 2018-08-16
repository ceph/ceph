#include "svc_quota.h"
#include "svc_zone.h"

#include "rgw/rgw_zone.h"

int RGWS_Quota::create_instance(const string& conf, RGWServiceInstanceRef *instance)
{
  instance->reset(new RGWSI_Quota(this, cct));
  return 0;
}

std::map<string, RGWServiceInstance::dependency> RGWSI_Quota::get_deps()
{
  RGWServiceInstance::dependency dep = { .name = "zone",
                                         .conf = "{}" };
  map<string, RGWServiceInstance::dependency> deps;
  deps["zone_dep"] = dep;
  return deps;
}

int RGWSI_Quota::init(const string& conf, std::map<std::string, RGWServiceInstanceRef>& dep_refs)
{
  zone_svc = static_pointer_cast<RGWSI_Zone>(dep_refs["zone_dep"]);
  assert(zone_svc);
  return 0;
}

const RGWQuotaInfo& RGWSI_Quota::get_bucket_quota() const
{
  return zone_svc->get_current_period().get_config().bucket_quota;
}

const RGWQuotaInfo& RGWSI_Quota::get_user_quota() const
{
  return zone_svc->get_current_period().get_config().user_quota;
}

