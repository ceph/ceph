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
#include <iostream>
#include <vector>
#include <include/types.h>

#include "rgw_cors.h"

using namespace std;

template <class T>
static inline void char_to_str_list(const char *in, T& out, 
                                    bool is_host_name_list=false){
  const char *start = in, *end = in;
  while(end){
    end = strchr(start, ' ');
    unsigned len = (end > start)?(end - start):strlen(start);
    if(is_host_name_list){
      string host, proto, sin = string(start, len);
      parse_host_name(sin, host, proto);
      out.insert(out.end(), proto+host);
    }else
      out.insert(out.end(), string(start, (size_t)len));
    start = end + 1;
  }
}

class RGWCORSConfiguration_SWIFT : public RGWCORSConfiguration
{
  public:
    RGWCORSConfiguration_SWIFT(){}
    ~RGWCORSConfiguration_SWIFT(){}
    int create_update(const char *allow_origins, const char *allow_headers, 
                  const char *expose_headers, const char *max_age){
      set<string> o, h;
      list<string> e;
      unsigned a = CORS_MAX_AGE_INVALID;
      uint8_t flags = RGW_CORS_ALL;

      char_to_str_list(allow_origins, o, true);
      if(allow_headers)
        char_to_str_list(allow_headers, h);
      if(expose_headers)
        char_to_str_list(expose_headers, e);
      if(max_age){
        char *end = NULL;
        a = strtol(max_age, &end, 10);
        if (a == LONG_MAX)
          a = CORS_MAX_AGE_INVALID;
      }

      RGWCORSRule rule(o, h, e, flags, a);
      stack_rule(rule);
      return 0;
    }
};
#endif /*CEPH_RGW_CORS_SWIFT3_H*/
