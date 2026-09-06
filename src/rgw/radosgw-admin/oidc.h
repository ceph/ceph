// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <string>

#include "radosgw-admin/radosgw-admin.h"

class DoutPrefixProvider;
namespace ceph { class Formatter; }
namespace rgw::sal { class Driver; }

struct rgw_admin_oidc_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string tenant;
  std::string account_id;
  std::string provider_url;
  std::string client_ids_str;
  std::string thumbprints_str;
};

int rgw_admin_oidc(const DoutPrefixProvider* dpp,
                   rgw::sal::Driver* driver,
                   ceph::Formatter* formatter,
                   const rgw_admin_oidc_options& opts);
