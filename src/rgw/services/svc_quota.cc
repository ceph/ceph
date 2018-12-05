#include "svc_quota.h"
#include "svc_zone.h"

#include "rgw/rgw_zone.h"

const RGWQuotaInfo& RGWSI_Quota::get_bucket_quota() const
{
  return zone_svc->get_current_period().get_config().bucket_quota;
}

const RGWQuotaInfo& RGWSI_Quota::get_user_quota() const
{
  return zone_svc->get_current_period().get_config().user_quota;
}

