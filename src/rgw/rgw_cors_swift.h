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
      std::set<std::string> o, h;
      std::list<std::string> e;
      unsigned long a = CORS_MAX_AGE_INVALID;
      uint8_t flags = RGW_CORS_ALL;

      int nr_invalid_names = 0;
      auto add_host = [&nr_invalid_names, &o] (auto host) {
        if (validate_name_string(host) == 0) {
          o.emplace(std::string{host});
        } else {
          nr_invalid_names++;
        }
      };
      for_each_substr(allow_origins, ";,= \t", add_host);
      if (o.empty() || nr_invalid_names > 0) {
        return -EINVAL;
      }

      if (allow_headers) {
        int nr_invalid_headers = 0;
        auto add_header = [&nr_invalid_headers, &h] (auto allow_header) {
          if (validate_name_string(allow_header) == 0) {
            h.emplace(std::string{allow_header});
          } else {
            nr_invalid_headers++;
          }
        };
        for_each_substr(allow_headers, ";,= \t", add_header);
        if (h.empty() || nr_invalid_headers > 0) {
          return -EINVAL;
        }
      }

      if (expose_headers) {
        for_each_substr(expose_headers, ";,= \t",
            [&e] (auto expose_header) {
              e.emplace_back(std::string(expose_header));
            });
      }
      if (max_age) {
        char *end = NULL;
        a = strtoul(max_age, &end, 10);
        if (a == ULONG_MAX)
          a = CORS_MAX_AGE_INVALID;
      }

      RGWCORSRule rule(o, h, e, flags, a);
      stack_rule(rule);
      return 0;
    }
};
