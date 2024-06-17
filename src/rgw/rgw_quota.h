// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "include/utime.h"
#include "common/config_fwd.h"
#include "common/lru_map.h"

#include "rgw/rgw_quota_types.h"
#include "rgw/rgw_user_types.h"
#include "common/async/yield_context.h"
#include "rgw_sal_fwd.h"

struct rgw_bucket;

class RGWQuotaHandler {
public:
  RGWQuotaHandler() {}
  virtual ~RGWQuotaHandler() {
  }
  virtual int check_quota(const DoutPrefixProvider *dpp, const rgw_owner& bucket_owner,
                          const rgw_bucket& bucket, const RGWQuota& quota,
			  uint64_t num_objs, uint64_t size, optional_yield y) = 0;

  virtual void update_stats(const rgw_owner& bucket_owner, rgw_bucket& bucket, int obj_delta, uint64_t added_bytes, uint64_t removed_bytes) = 0;

  static RGWQuotaHandler *generate_handler(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver, bool quota_threads);
  static void free_handler(RGWQuotaHandler *handler);
};

// apply default quotas from configuration
void rgw_apply_default_bucket_quota(RGWQuotaInfo& quota, const ConfigProxy& conf);
void rgw_apply_default_user_quota(RGWQuotaInfo& quota, const ConfigProxy& conf);
void rgw_apply_default_account_quota(RGWQuotaInfo& quota, const ConfigProxy& conf);
