// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

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

#pragma once

#include <map>
#include <string>
#include <vector>
#include <include/types.h>
#include <include/str_list.h>

#include "rgw_cors.h"

class RGWCORSConfiguration_SWIFT : public RGWCORSConfiguration
{
  public:
    RGWCORSConfiguration_SWIFT() {}
    ~RGWCORSConfiguration_SWIFT() {}
    int create_update(const char *allow_origins, const char *allow_headers, 
                  const char *expose_headers, const char *max_age) {
      std::optional<RGWCORSRule> optional_rule;
      if (RGWCORSRule::create_rule(allow_origins, allow_headers, expose_headers, "*", optional_rule, max_age) < 0) {
        return -EINVAL;
      }
      stack_rule(optional_rule.value());
      return 0;
    }
};
