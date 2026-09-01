// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/cors.h"

#include <iostream>
#include <optional>
#include <string>

#include "common/config.h"
#include "rgw_cors_s3.h"

using namespace rgw_admin;
using namespace std;

int rgw_admin_cors(ceph::Formatter* formatter, const rgw_admin_cors_options& o)
{
  auto& command = o.command;
  int ret = 0;

  if (command == OPT::GLOBAL_CORS_GET) {
    string allow_origins, allow_headers, allow_methods, expose_headers;
    ret = g_conf().get_val("rgw_gcors_allow_origins", &allow_origins);
    if (ret < 0 || allow_origins.empty()) {
      cerr << "ERROR in OPT::GLOBAL_CORS_GET, no rgw_gcors_allow_origins config found or empty, ret=" << ret << std::endl;
      return -EINVAL;
    }
    ret = g_conf().get_val("rgw_gcors_allow_headers", &allow_headers);
    if (ret < 0 || allow_headers.empty()) {
      cerr << "ERROR in OPT::GLOBAL_CORS_GET, no rgw_gcors_allow_headers config found or empty, ret=" << ret << std::endl;
      return -EINVAL;
    }
    ret = g_conf().get_val("rgw_gcors_allow_methods", &allow_methods);
    if (ret < 0 || allow_methods.empty()) {
      cerr << "ERROR in OPT::GLOBAL_CORS_GET, no rgw_gcors_allow_methods config found or empty, ret=" << ret << std::endl;
      return -EINVAL;
    }
    ret = g_conf().get_val("rgw_gcors_expose_headers", &expose_headers);
    std::optional<RGWCORSRule> optional_global_cors;
    if (RGWCORSRule::create_rule(allow_origins.c_str(), allow_headers.c_str(),
                                  expose_headers.c_str(), allow_methods.c_str(),
                                  optional_global_cors) < 0) {
      cerr << "ERROR: couldn't create RGWCORSRule from rgw_gcors_allow_origins="
           << allow_origins << ", rgw_gcors_allow_headers=" << allow_headers
           << ", rgw_gcors_allow_methods=" << allow_methods
           << ", rgw_gcors_expose_headers=" << expose_headers << std::endl;
      return -EINVAL;
    }

    optional_global_cors->dump(formatter);
    formatter->flush(cout);
  }

  return 0;
}
