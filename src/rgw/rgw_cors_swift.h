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

static inline void remove_proto_www(const char *in, unsigned len, 
                                    const char **out, unsigned *olen){
  const char *o = strstr(in, "www.");
  *out = in;
  if(o && ((o - in) < len))*out = (o + 4);
  else if((o = strstr(in, "://")) && ((o - in) < len))*out = (o + 3);
  *olen = (len - (*out - in));
}

template <class T>
static inline void char_to_str_list(const char *in, T& out, 
                                    void (*process)(const char *, unsigned, 
                                                    const char **, unsigned *) = NULL){
  const char *start = in, *end = in;
  while(end){
    end = strchr(start, ' ');
    unsigned len = end?(end - start):strlen(start);
    if(process)process(start, len, &start, &len);
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

      char_to_str_list(allow_origins, o, remove_proto_www);
      if(allow_headers)char_to_str_list(allow_headers, h);
      if(expose_headers)char_to_str_list(expose_headers, e);
      if(max_age)a = (unsigned)atoi(max_age);

      RGWCORSRule rule(o, h, e, flags, a);
      stack_rule(rule);
      return 0;
    }
};
#endif /*CEPH_RGW_CORS_SWIFT3_H*/
