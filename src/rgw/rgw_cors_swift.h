// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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
#include <include/str_list.h>

#include "rgw_cors.h"

using namespace std;

class RGWCORSConfiguration_SWIFT : public RGWCORSConfiguration
{
  public:
    RGWCORSConfiguration_SWIFT() {}
    ~RGWCORSConfiguration_SWIFT() {}
    int create_update(const char *allow_origins, const char *allow_headers, 
                  const char *expose_headers, const char *max_age) {
      set<string> o, h, oc;
      list<string> e;
      unsigned long a = CORS_MAX_AGE_INVALID;
      uint8_t flags = RGW_CORS_ALL;

      string ao = allow_origins;
      get_str_set(ao, oc);
      if (oc.empty())
        return -EINVAL;
      for(set<string>::iterator it = oc.begin(); it != oc.end(); ++it) {
        string host = *it;
        if (validate_name_string(host) != 0)
          return -EINVAL;
        o.insert(o.end(), host);
      }
      if (allow_headers) {
        string ah = allow_headers;
        get_str_set(ah, h);
        for(set<string>::iterator it = h.begin();
            it != h.end(); ++it) {
          string s = (*it);
          if (validate_name_string(s) != 0)
            return -EINVAL;
        }
      }

      if (expose_headers) {
        string eh = expose_headers;
        get_str_list(eh, e);
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
#endif /*CEPH_RGW_CORS_SWIFT3_H*/
