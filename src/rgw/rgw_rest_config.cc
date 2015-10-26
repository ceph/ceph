// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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
#include "common/errno.h"

#define dout_subsys ceph_subsys_rgw

void RGWOp_ZoneGroupMap_Get::execute() {
  http_ret = zonegroup_map.read(g_ceph_context, store);
  if (http_ret < 0) {
    dout(5) << "failed to read zone_group map" << dendl;
  }
}

void RGWOp_ZoneGroupMap_Get::send_response() {
  set_req_state_err(s, http_ret);
  dump_errno(s);
  end_header(s);

  if (http_ret < 0)
    return;

  if (old_format) {
    RGWRegionMap region_map;
    region_map.regions = zonegroup_map.zonegroups;
    region_map.master_region = zonegroup_map.master_zonegroup;
    region_map.bucket_quota = zonegroup_map.bucket_quota;
    region_map.user_quota = zonegroup_map.user_quota;    
    encode_json("region-map", region_map, s->formatter);
  } else {
    encode_json("zonegroup-map", zonegroup_map, s->formatter);
  }
  flusher.flush();
}

void RGWOp_Period_Get::execute() {
  string period_id;
  RESTArgs::get_string(s, "period_id", period_id, &period_id);
  epoch_t epoch = 0;
  RESTArgs::get_uint32(s, "epoch", 0, &epoch);

  period = new RGWPeriod(period_id, epoch);
  http_ret = period->init(g_ceph_context, store);
  if (http_ret < 0) {
    dout(5) << "failed to read period" << dendl;
  }
}

void RGWOp_Period_Get::send_response() {
  set_req_state_err(s, http_ret);
  dump_errno(s);
  end_header(s);

  if (http_ret < 0)
    return;

  encode_json("period", *period, s->formatter);
  flusher.flush();
}

void RGWOp_Period_Post::execute() {
  string period_id;
  RESTArgs::get_string(s, "period_id", period_id, &period_id);
  epoch_t epoch = 0;
  RESTArgs::get_uint32(s, "epoch", 0, &epoch);

  RGWPeriod period(period_id, epoch);
  http_ret = period.init(g_ceph_context, store);
  if (http_ret < 0) {
    dout(5) << "failed to read period" << dendl;
    return;
  }

#define PERIOD_INPUT_MAX_LEN 4096
  bool empty;
  http_ret = rgw_rest_get_json_input(store->ctx(), s, period,
                                     PERIOD_INPUT_MAX_LEN, &empty);
  if (http_ret < 0) {
    dout(5) << "failed to decode period" << dendl;
    return;
  }

  period.store_info(false);
}

RGWOp* RGWHandler_Config::op_get() {
  bool exists;
  string type = s->info.args.get("type", &exists);

  if (type.compare("period") == 0) {
    return new RGWOp_Period_Get;
  }  else if (type.compare("zonegroup-map") == 0) {
    return new RGWOp_ZoneGroupMap_Get(false);
  } else {
    return new RGWOp_ZoneGroupMap_Get(true);
  }
}


RGWOp* RGWHandler_Config::op_post() {
  bool exists;
  string type = s->info.args.get("type", &exists);

  if (type.compare("period") == 0) {
    return new RGWOp_Period_Post;
  }

  return NULL;

}
