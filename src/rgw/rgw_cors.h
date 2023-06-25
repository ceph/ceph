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
#include <include/types.h>

#define RGW_CORS_GET    0x1
#define RGW_CORS_PUT    0x2
#define RGW_CORS_HEAD   0x4
#define RGW_CORS_POST   0x8
#define RGW_CORS_DELETE 0x10
#define RGW_CORS_COPY   0x20
#define RGW_CORS_ALL    (RGW_CORS_GET    |  \
                         RGW_CORS_PUT    |  \
                         RGW_CORS_HEAD   |  \
                         RGW_CORS_POST   |  \
                         RGW_CORS_DELETE |  \
                         RGW_CORS_COPY)

#define CORS_MAX_AGE_INVALID ((uint32_t)-1)

class RGWCORSRule
{
protected:
  uint32_t       max_age;
  uint8_t        allowed_methods;
  std::string         id;
  std::set<std::string> allowed_hdrs; /* If you change this, you need to discard lowercase_allowed_hdrs */
  std::set<std::string> lowercase_allowed_hdrs; /* Not built until needed in RGWCORSRule::is_header_allowed */
  std::set<std::string> allowed_origins;
  std::list<std::string> exposable_hdrs;

public:
  RGWCORSRule() : max_age(CORS_MAX_AGE_INVALID),allowed_methods(0) {}
  RGWCORSRule(std::set<std::string>& o, std::set<std::string>& h,
              std::list<std::string>& e, uint8_t f, uint32_t a)
      :max_age(a),
       allowed_methods(f),
       allowed_hdrs(h),
       allowed_origins(o),
       exposable_hdrs(e) {}
  virtual ~RGWCORSRule() {}

  std::string& get_id() { return id; }
  uint32_t get_max_age() { return max_age; }
  uint8_t get_allowed_methods() { return allowed_methods; }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(max_age, bl);
    encode(allowed_methods, bl);
    encode(id, bl);
    encode(allowed_hdrs, bl);
    encode(allowed_origins, bl);
    encode(exposable_hdrs, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(max_age, bl);
    decode(allowed_methods, bl);
    decode(id, bl);
    decode(allowed_hdrs, bl);
    decode(allowed_origins, bl);
    decode(exposable_hdrs, bl);
    DECODE_FINISH(bl);
  }
  static void generate_test_instances(std::list<RGWCORSRule*>& o);
  bool has_wildcard_origin();
  bool is_origin_present(const char *o);
  void format_exp_headers(std::string& s);
  void erase_origin_if_present(std::string& origin, bool *rule_empty);
  void dump_origins(); 
  void dump(Formatter *f) const;
  bool is_header_allowed(const char *hdr, size_t len);
};
WRITE_CLASS_ENCODER(RGWCORSRule)

class RGWCORSConfiguration
{
  protected:
    std::list<RGWCORSRule> rules;
  public:
    RGWCORSConfiguration() {}
    ~RGWCORSConfiguration() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(rules, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(rules, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  std::list<RGWCORSRule>& get_rules() {
    return rules;
  }
  bool is_empty() {
    return rules.empty();
  }
  void get_origins_list(const char *origin, std::list<std::string>& origins);
  RGWCORSRule * host_name_rule(const char *origin);
  void erase_host_name_rule(std::string& origin);
  void dump();
  void stack_rule(RGWCORSRule& r) {
    rules.push_front(r);    
  }
};
WRITE_CLASS_ENCODER(RGWCORSConfiguration)

static inline int validate_name_string(std::string_view o) {
  if (o.length() == 0)
    return -1;
  if (o.find_first_of("*") != o.find_last_of("*"))
    return -1;
  return 0;
}

static inline uint8_t get_cors_method_flags(const char *req_meth) {
  uint8_t flags = 0;

  if (strcmp(req_meth, "GET") == 0) flags = RGW_CORS_GET;
  else if (strcmp(req_meth, "POST") == 0) flags = RGW_CORS_POST;
  else if (strcmp(req_meth, "PUT") == 0) flags = RGW_CORS_PUT;
  else if (strcmp(req_meth, "DELETE") == 0) flags = RGW_CORS_DELETE;
  else if (strcmp(req_meth, "HEAD") == 0) flags = RGW_CORS_HEAD;

  return flags;
}
