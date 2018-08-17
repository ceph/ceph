#ifndef CEPH_RGW_SERVICES_QUOTA_H
#define CEPH_RGW_SERVICES_QUOTA_H


#include "rgw/rgw_service.h"


class RGWSI_Zone;

class RGWS_Quota : public RGWService
{
public:
  RGWS_Quota(CephContext *cct) : RGWService(cct, "quota") {}

  int create_instance(const std::string& conf, RGWServiceInstanceRef *instance) override;
};

class RGWSI_Quota : public RGWServiceInstance
{
  std::shared_ptr<RGWSI_Zone> zone_svc;

  std::map<std::string, RGWServiceInstance::dependency> get_deps() override;
  int load(const std::string& conf, std::map<std::string, RGWServiceInstanceRef>& dep_refs) override;

public:
  RGWSI_Quota(RGWService *svc, CephContext *cct): RGWServiceInstance(svc, cct) {}

  const RGWQuotaInfo& get_bucket_quota() const;
  const RGWQuotaInfo& get_user_quota() const;
};

#endif
