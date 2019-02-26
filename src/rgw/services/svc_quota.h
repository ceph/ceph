#ifndef CEPH_RGW_SERVICES_QUOTA_H
#define CEPH_RGW_SERVICES_QUOTA_H


#include "rgw/rgw_service.h"


class RGWSI_Quota : public RGWServiceInstance
{
  RGWSI_Zone *zone_svc{nullptr};

public:
  RGWSI_Quota(CephContext *cct): RGWServiceInstance(cct) {}

  void init(RGWSI_Zone *_zone_svc) {
    zone_svc = _zone_svc;
  }

  const RGWQuotaInfo& get_bucket_quota() const;
  const RGWQuotaInfo& get_user_quota() const;
};

#endif
