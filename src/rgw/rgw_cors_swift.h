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

#ifndef CEPH_RGW_CORS_SWIFT3_H
#define CEPH_RGW_CORS_SWIFT3_H

#include <map>
#include <string>
#include <vector>
#include <include/types.h>

#include "common/str_util.h"

#include "rgw_cors.h"

class RGWCORSConfiguration_SWIFT : public RGWCORSConfiguration
{
  public:
    RGWCORSConfiguration_SWIFT() {}
    ~RGWCORSConfiguration_SWIFT() {}
  int create_update(std::optional<std::string_view> allow_origins,
		    std::optional<std::string_view> allow_headers,
		    std::optional<std::string_view> expose_headers,
		    std::optional<std::string_view> max_age) {
      std::set<string, std::less<>> o, h, oc;
      std::list<string> e;
      unsigned long a = CORS_MAX_AGE_INVALID;
      uint8_t flags = RGW_CORS_ALL;

      ceph::substr_insert(*allow_origins, std::inserter(oc, oc.end()));
      if (oc.empty())
        return -EINVAL;
      for (auto it = oc.begin(); it != oc.end(); ++it) {
        string host = *it;
        if (validate_name_string(host) != 0)
          return -EINVAL;
        o.insert(o.end(), host);
      }
      if (allow_headers) {
	ceph::substr_insert(*allow_headers, std::inserter(h, h.end()));
        for(auto it = h.begin(); it != h.end(); ++it) {
          string s = (*it);
          if (validate_name_string(s) != 0)
            return -EINVAL;
        }
      }

      if (expose_headers) {
	ceph::substr_insert(*expose_headers, std::back_inserter(e));
      }
      if (max_age) {
        a = ceph::parse<unsigned long>(*max_age).value_or(ULONG_MAX);
        if (a == ULONG_MAX)
          a = CORS_MAX_AGE_INVALID;
      }

      RGWCORSRule rule(o, h, e, flags, a);
      stack_rule(rule);
      return 0;
    }
};
#endif /*CEPH_RGW_CORS_SWIFT3_H*/
