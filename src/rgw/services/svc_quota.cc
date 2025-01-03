// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "svc_quota.h"
#include "svc_zone.h"

#include "rgw_zone.h"

const RGWQuotaInfo& RGWSI_Quota::get_bucket_quota() const
{
  return zone_svc->get_current_period().get_config().quota.bucket_quota;
}

const RGWQuotaInfo& RGWSI_Quota::get_user_quota() const
{
  return zone_svc->get_current_period().get_config().quota.user_quota;
}

