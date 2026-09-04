// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include "radosgw-admin/radosgw-admin.h"

class DoutPrefixProvider;
namespace ceph { class Formatter; }
class RGWStreamFlusher;
class RGWUser;
class RGWUserAdminOpState;
namespace rgw { class SiteConfig; }
namespace rgw::sal { class Driver; class User; class Bucket; class ConfigStore; }

struct rgw_admin_cors_options {
  rgw_admin::OPT command = rgw_admin::OPT::NO_CMD;
};

int rgw_admin_cors(ceph::Formatter* formatter, const rgw_admin_cors_options& opts);
