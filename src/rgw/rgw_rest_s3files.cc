// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_rest_s3files.h"

#include "rgw_auth_s3.h"
#include "rgw_common.h"

int RGWHandler_REST_S3Files::init(
    rgw::sal::Driver* driver, req_state* s, rgw::io::BasicClient* cio) {
  s->dialect = "s3files";
  s->prot_flags = RGW_REST_S3FILES;
  return RGWHandler_REST::init(driver, s, cio);
}

int RGWHandler_REST_S3Files::authorize(
    const DoutPrefixProvider* dpp, optional_yield y) {
  return RGW_Auth_S3::authorize(dpp, driver, auth_registry, s, y);
}

RGWHandler_REST* RGWRESTMgr_S3Files::get_handler(
    rgw::sal::Driver* driver,
    req_state* const s,
    const rgw::auth::StrategyRegistry& auth_registry,
    const std::string& frontend_prefix) {
  return new RGWHandler_REST_S3Files(auth_registry);
}
