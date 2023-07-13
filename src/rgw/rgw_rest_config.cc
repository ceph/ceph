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
#include "rgw_op.h"
#include "rgw_process_env.h"
#include "rgw_rest_config.h"

using namespace std;

void RGWOp_ZoneConfig_Get::send_response() {
  const RGWZoneParams& zone_params = s->penv.site->get_zone_params();

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

  if (type.compare("zone") == 0) {
    return new RGWOp_ZoneConfig_Get();
  }
  return nullptr;
}
