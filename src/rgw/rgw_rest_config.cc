// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "common/ceph_json.h"
#include "common/strtol.h"
#include "rgw_rest.h"
#include "rgw_op.h"
#include "rgw_rados.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_config.h"
#include "rgw_client_io.h"
#include "rgw_sal_rados.h"
#include "common/errno.h"
#include "include/ceph_assert.h"

#include "services/svc_zone.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

void RGWOp_ZoneGroupMap_Get::execute(optional_yield y) {
  op_ret = zonegroup_map.read(this, g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, y);
  if (op_ret < 0) {
    ldpp_dout(this, 5) << "failed to read zone_group map" << dendl;
  }
}

void RGWOp_ZoneGroupMap_Get::send_response() {
  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s);

  if (op_ret < 0)
    return;

  if (old_format) {
    RGWRegionMap region_map;
    region_map.regions = zonegroup_map.zonegroups;
    region_map.master_region = zonegroup_map.master_zonegroup;
    region_map.quota.bucket_quota = zonegroup_map.quota.bucket_quota;
    region_map.quota.user_quota = zonegroup_map.quota.user_quota;
    encode_json("region-map", region_map, s->formatter);
  } else {
    encode_json("zonegroup-map", zonegroup_map, s->formatter);
  }
  flusher.flush();
}

void RGWOp_ZoneConfig_Get::send_response() {
  const RGWZoneParams& zone_params = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->get_zone_params();

  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s);

  if (op_ret < 0)
    return;

  encode_json("zone_params", zone_params, s->formatter);
  flusher.flush();
}

RGWOp* RGWHandler_Config::op_get() {
  bool exists;
  string type = s->info.args.get("type", &exists);

  if (type.compare("zonegroup-map") == 0) {
    return new RGWOp_ZoneGroupMap_Get(false);
  } else if (type.compare("zone") == 0) {
    return new RGWOp_ZoneConfig_Get();
  } else {
    return new RGWOp_ZoneGroupMap_Get(true);
  }
}
