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
  http_ret = zone_group_map.read(g_ceph_context, store);
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
  
  encode_json("region-map", zone_group_map, s->formatter);
  flusher.flush(); 
}
