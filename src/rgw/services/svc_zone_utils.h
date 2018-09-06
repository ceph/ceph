#ifndef CEPH_RGW_SERVICES_ZONEUTILS_H
#define CEPH_RGW_SERVICES_ZONEUTILS_H


#include "rgw/rgw_service.h"


class RGWSI_RADOS;
class RGWSI_Zone;

class RGWS_ZoneUtils : public RGWService
{
public:
  RGWS_ZoneUtils(CephContext *cct) : RGWService(cct, "zone_utils") {}

  int create_instance(const std::string& conf, RGWServiceInstanceRef *instance) override;
};

class RGWSI_ZoneUtils : public RGWServiceInstance
{
  std::shared_ptr<RGWSI_RADOS> rados_svc;
  std::shared_ptr<RGWSI_Zone> zone_svc;

  string trans_id_suffix;

  std::map<std::string, RGWServiceInstance::dependency> get_deps() override;
  int load(const std::string& conf, std::map<std::string, RGWServiceInstanceRef>& dep_refs) override;
  int init() override;

  void init_unique_trans_id_deps();

public:
  RGWSI_ZoneUtils(RGWService *svc, CephContext *cct): RGWServiceInstance(svc, cct) {}

  string gen_host_id();
  string unique_id(uint64_t unique_num);

  string unique_trans_id(const uint64_t unique_num);
};

#endif
