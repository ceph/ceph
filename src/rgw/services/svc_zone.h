#ifndef CEPH_RGW_SERVICES_ZONE_H
#define CEPH_RGW_SERVICES_ZONE_H


#include "rgw/rgw_service.h"


struct RGWZoneGroup;
struct RGWZone;
struct RGWZoneParams;
struct RGWPeriod;

class RGWS_Zone : public RGWService
{
public:
  RGWS_Zone(CephContext *cct) : RGWService(cct, "zone") {}

  int create_instance(JSONFormattable& conf, RGWServiceInstanceRef *instance);
};

class RGWSI_Zone : public RGWServiceInstance
{
  std::unique_ptr<RGWZoneGroup> zonegroup;
  std::unique_ptr<RGWZone> zone_public_config; /* external zone params, e.g., entrypoints, log flags, etc. */  
  std::unique_ptr<RGWZoneParams> zone_params; /* internal zone params, e.g., rados pools */
  std::unique_ptr<RGWPeriod> current_period;
  uint32_t zone_short_id{0};

  std::vector<std::string> svci_deps = { "sys_obj" };
  std::vector<std::string> get_deps() {
    return svci_deps;
  }

  RGWServiceInstanceRef svc_rados;
public:
  RGWSI_Zone(RGWService *svc, CephContext *cct): RGWServiceInstance(svc, cct) {}

  RGWZoneParams& get_zone_params();
  RGWRealm& get_realm();
  RGWZoneGroup& get_zonegroup();
  RGWZone& get_zone();

  const string& zone_name();
  const string& zone_id();
  uint32_t get_zone_short_id() const;

  int get_zonegroup(const string& id, RGWZoneGroup& zonegroup);

  bool zone_is_writeable();
  bool zone_syncs_from(RGWZone& target_zone, RGWZone& source_zone);
  bool get_redirect_zone_endpoint(string *endpoint);
};

#endif
