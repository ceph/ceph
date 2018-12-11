// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Yehuda Sadeh <yehuda@redhat.com>
 * Copyright (C) 2015 Robin H. Johnson <robin.johnson@dreamhost.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef RGW_WEBSITE_H
#define RGW_WEBSITE_H

#include <list>
#include <string>

#include "common/ceph_json.h"

#include "rgw_xml.h"

struct RGWRedirectInfo
{
  std::string protocol;
  std::string hostname;
  uint16_t http_redirect_code = 0;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(protocol, bl);
    encode(hostname, bl);
    encode(http_redirect_code, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(protocol, bl);
    decode(hostname, bl);
    decode(http_redirect_code, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWRedirectInfo)


struct RGWBWRedirectInfo
{
  RGWRedirectInfo redirect;
  std::string replace_key_prefix_with;
  std::string replace_key_with;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(redirect, bl);
    encode(replace_key_prefix_with, bl);
    encode(replace_key_with, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(redirect, bl);
    decode(replace_key_prefix_with, bl);
    decode(replace_key_with, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void dump_xml(Formatter *f) const;
  void decode_json(JSONObj *obj);
  void decode_xml(XMLObj *obj);
};
WRITE_CLASS_ENCODER(RGWBWRedirectInfo)

struct RGWBWRoutingRuleCondition
{
  std::string key_prefix_equals;
  uint16_t http_error_code_returned_equals = 0;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(key_prefix_equals, bl);
    encode(http_error_code_returned_equals, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(key_prefix_equals, bl);
    decode(http_error_code_returned_equals, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void dump_xml(Formatter *f) const;
  void decode_json(JSONObj *obj);
  void decode_xml(XMLObj *obj);

  bool check_key_condition(const std::string& key);
  bool check_error_code_condition(const int error_code) {
    return (uint16_t)error_code == http_error_code_returned_equals;
  }
};
WRITE_CLASS_ENCODER(RGWBWRoutingRuleCondition)

struct RGWBWRoutingRule
{
  RGWBWRoutingRuleCondition condition;
  RGWBWRedirectInfo redirect_info;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(condition, bl);
    encode(redirect_info, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(condition, bl);
    decode(redirect_info, bl);
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void dump_xml(Formatter *f) const;
  void decode_json(JSONObj *obj);
  void decode_xml(XMLObj *obj);

  bool check_key_condition(const std::string& key) {
    return condition.check_key_condition(key);
  }
  bool check_error_code_condition(int error_code) {
    return condition.check_error_code_condition(error_code);
  }

  void apply_rule(const std::string& default_protocol,
                  const std::string& default_hostname,
                  const std::string& key,
                  std::string *redirect,
                  int *redirect_code);
};
WRITE_CLASS_ENCODER(RGWBWRoutingRule)

struct RGWBWRoutingRules
{
  std::list<RGWBWRoutingRule> rules;

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
  void dump_xml(Formatter *f) const;
  void decode_json(JSONObj *obj);

  bool check_key_condition(const std::string& key, RGWBWRoutingRule **rule);
  bool check_error_code_condition(int error_code, RGWBWRoutingRule **rule);
  bool check_key_and_error_code_condition(const std::string& key,
                                          const int error_code,
                                          RGWBWRoutingRule **rule);
};
WRITE_CLASS_ENCODER(RGWBWRoutingRules)

struct RGWBucketWebsiteConf
{
  RGWRedirectInfo redirect_all;
  std::string index_doc_suffix;
  std::string error_doc;
  std::string subdir_marker;
  std::string listing_css_doc;
  bool listing_enabled;
  RGWBWRoutingRules routing_rules;

  RGWBucketWebsiteConf()
    : listing_enabled(false) {
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(index_doc_suffix, bl);
    encode(error_doc, bl);
    encode(routing_rules, bl);
    encode(redirect_all, bl);
    encode(subdir_marker, bl);
    encode(listing_css_doc, bl);
    encode(listing_enabled, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(index_doc_suffix, bl);
    decode(error_doc, bl);
    decode(routing_rules, bl);
    decode(redirect_all, bl);
    if (struct_v >= 2) {
      decode(subdir_marker, bl);
      decode(listing_css_doc, bl);
      decode(listing_enabled, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
  void decode_xml(XMLObj *obj);
  void dump_xml(Formatter *f) const;

  bool should_redirect(const std::string& key,
                       const int http_error_code,
                       RGWBWRoutingRule *redirect);

  void get_effective_key(const std::string& key,
                         std::string *effective_key, bool is_file) const;

  const std::string& get_index_doc() const {
    return index_doc_suffix;
  }

  bool is_empty() const {
    return index_doc_suffix.empty() &&
           error_doc.empty() &&
           subdir_marker.empty() &&
           listing_css_doc.empty() &&
           ! listing_enabled;
  }
};
WRITE_CLASS_ENCODER(RGWBucketWebsiteConf)

#endif
