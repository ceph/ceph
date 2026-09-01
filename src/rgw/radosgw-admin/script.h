// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <optional>
#include <string>

#include "radosgw-admin/radosgw-admin.h"

class DoutPrefixProvider;
namespace rgw::sal { class Driver; }

struct rgw_admin_script_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
  std::string* tenant = nullptr;
  std::string* infile = nullptr;
  std::optional<std::string>* script_package = nullptr;
  std::optional<std::string>* str_script_ctx = nullptr;
  int allow_compilation = 0;
};

int rgw_admin_script(const DoutPrefixProvider* dpp,
                     rgw::sal::Driver* driver,
                     const rgw_admin_script_options& opts);
