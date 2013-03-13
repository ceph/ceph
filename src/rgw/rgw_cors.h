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

#ifndef CEPH_RGW_CORS_H
#define CEPH_RGW_CORS_H

#include <map>
#include <string>
#include <iostream>
#include <include/types.h>

#include "common/debug.h"

#define RGW_CORS_GET    0x1
#define RGW_CORS_PUT    0x2
#define RGW_CORS_HEAD   0x4
#define RGW_CORS_POST   0x8
#define RGW_CORS_DELETE 0x10
#define RGW_CORS_ALL    (RGW_CORS_GET   |  \
                         RGW_CORS_PUT   |  \
                         RGW_CORS_HEAD  |  \
                         RGW_CORS_POST  |  \
                         RGW_CORS_DELETE)

#define CORS_MAX_AGE_INVALID ((uint32_t)-1)

class RGWCORSRule
{
protected:
  uint32_t       max_age;
  uint8_t        allowed_methods;
  string         id;
  set<string> allowed_hdrs;
  set<string> allowed_origins;
  list<string> exposable_hdrs;

public:
  RGWCORSRule() : max_age(CORS_MAX_AGE_INVALID),allowed_methods(0) {}
  RGWCORSRule(set<string>& o, set<string>& h, list<string>& e, uint8_t f, unsigned a)
      :max_age(a),
       allowed_methods(f),
       allowed_hdrs(h),
       allowed_origins(o),
       exposable_hdrs(e){}
  virtual ~RGWCORSRule() {}

  string& get_id() { return id; }
  uint32_t get_max_age() { return max_age; }
  uint8_t get_allowed_methods() { return allowed_methods; }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(max_age, bl);
    ::encode(allowed_methods, bl);
    ::encode(id, bl);
    ::encode(allowed_hdrs, bl);
    ::encode(allowed_origins, bl);
    ::encode(exposable_hdrs, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(max_age, bl);
    ::decode(allowed_methods, bl);
    ::decode(id, bl);
    ::decode(allowed_hdrs, bl);
    ::decode(allowed_origins, bl);
    ::decode(exposable_hdrs, bl);
    DECODE_FINISH(bl);
  }
  bool is_origin_present(list<string>& origins);
  void format_exp_headers(string& s);
  void erase_origin_if_present(string& origin, bool *rule_empty);
  void dump_origins(); 
  void dump(Formatter *f) const;
  bool is_header_allowed(const char *hdr, size_t len){
    return ((allowed_hdrs.find("*") != allowed_hdrs.end()) || 
            (allowed_hdrs.find(string(hdr, len)) != allowed_hdrs.end()));
  }
};
WRITE_CLASS_ENCODER(RGWCORSRule)

class RGWCORSConfiguration
{
  protected:
    list<RGWCORSRule> rules;
  public:
    RGWCORSConfiguration(){}
    ~RGWCORSConfiguration(){}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(rules, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(rules, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  list<RGWCORSRule>& get_rules(){
    return rules;
  }
  bool is_empty(){
    return rules.empty();
  }
  void get_origins_list(const char *origin, list<string>& origins);
  RGWCORSRule * host_name_rule(const char *origin);
  void erase_host_name_rule(string& origin);
  void dump();
  void stack_rule(RGWCORSRule& r){
    rules.push_front(r);    
  }
};
WRITE_CLASS_ENCODER(RGWCORSConfiguration)

extern void parse_host_name(string& in, string& host_name, string& proto);
#endif /*CEPH_RGW_CORS_H*/
