// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "radosgw-admin/radosgw-admin.h"

class DoutPrefixProvider;
class RGWObjVersionTracker;
class RGWUser;
class RGWUserAdminOpState;
namespace ceph { class Formatter; }
namespace rgw::sal { class Driver; class User; }

struct rgw_admin_mfa_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string* totp_serial = nullptr;
  std::string* totp_seed = nullptr;
  std::string* totp_seed_type = nullptr;
  std::vector<std::string>* totp_pin = nullptr;
  RGWObjVersionTracker* objv_tracker = nullptr;
  int totp_seconds = 0;
  int totp_window = 0;
};

int rgw_admin_mfa(const DoutPrefixProvider* dpp,
                  rgw::sal::Driver* driver,
                  ceph::Formatter* formatter,
                  RGWUser& ruser,
                  RGWUserAdminOpState& user_op,
                  std::unique_ptr<rgw::sal::User>& user,
                  const rgw_admin_mfa_options& opts);
