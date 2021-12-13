// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp

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

#include "common/debug.h"

#include "common/ceph_json.h"
#include "common/Formatter.h"

#include "acconfig.h"

#include <errno.h>
#include <string>
#include <list>
#include "include/types.h"
#include "rgw_website.h"
#include "rgw_common.h"
#include "rgw_xml.h"

using namespace std;

bool RGWBWRoutingRuleCondition::check_key_condition(const string& key) {
  return (key.size() >= key_prefix_equals.size() &&
          key.compare(0, key_prefix_equals.size(), key_prefix_equals) == 0);
}


void RGWBWRoutingRule::apply_rule(const string& default_protocol, const string& default_hostname,
                                           const string& key, string *new_url, int *redirect_code)
{
  RGWRedirectInfo& redirect = redirect_info.redirect;

  string protocol = (!redirect.protocol.empty() ? redirect.protocol : default_protocol);
  string hostname = (!redirect.hostname.empty() ? redirect.hostname : default_hostname);

  *new_url = protocol + "://" + hostname + "/";

  if (!redirect_info.replace_key_prefix_with.empty()) {
    *new_url += redirect_info.replace_key_prefix_with;
    if (key.size() > condition.key_prefix_equals.size()) {
      *new_url += key.substr(condition.key_prefix_equals.size());
    }
  } else if (!redirect_info.replace_key_with.empty()) {
    *new_url += redirect_info.replace_key_with;
  } else {
    *new_url += key;
  }

  if(redirect.http_redirect_code > 0) 
	  *redirect_code = redirect.http_redirect_code;
}

bool RGWBWRoutingRules::check_key_and_error_code_condition(const string &key, int error_code, RGWBWRoutingRule **rule)
{
  for (list<RGWBWRoutingRule>::iterator iter = rules.begin(); iter != rules.end(); ++iter) {
    if (iter->check_key_condition(key) && iter->check_error_code_condition(error_code)) {
      *rule = &(*iter);
      return true;
    }
  }
  return false;
}

bool RGWBWRoutingRules::check_key_condition(const string& key, RGWBWRoutingRule **rule)
{
  for (list<RGWBWRoutingRule>::iterator iter = rules.begin(); iter != rules.end(); ++iter) {
    if (iter->check_key_condition(key)) {
      *rule = &(*iter);
      return true;
    }
  }
  return false;
}

bool RGWBWRoutingRules::check_error_code_condition(const int http_error_code, RGWBWRoutingRule **rule)
{
  for (list<RGWBWRoutingRule>::iterator iter = rules.begin(); iter != rules.end(); ++iter) {
    if (iter->check_error_code_condition(http_error_code)) {
      *rule = &(*iter);
      return true;
    }
  }
  return false;
}

bool RGWBucketWebsiteConf::should_redirect(const string& key, const int http_error_code, RGWBWRoutingRule *redirect)
{
  RGWBWRoutingRule *rule;
  if(!redirect_all.hostname.empty()) {
	RGWBWRoutingRule redirect_all_rule;
	redirect_all_rule.redirect_info.redirect = redirect_all;
	redirect_all.http_redirect_code = 301;
	*redirect = redirect_all_rule;
	return true;
  } else if (!routing_rules.check_key_and_error_code_condition(key, http_error_code, &rule)) {
    return false;
  }

  *redirect = *rule;

  return true;
}

bool RGWBucketWebsiteConf::get_effective_key(const string& key, string *effective_key, bool is_file) const
{
  if (index_doc_suffix.empty()) {
    return false;
  }

  if (key.empty()) {
    *effective_key = index_doc_suffix;
  } else if (key[key.size() - 1] == '/') {
    *effective_key = key + index_doc_suffix;
  } else if (! is_file) {
    *effective_key = key + "/" + index_doc_suffix; 
  } else {
    *effective_key = key;
  }

  return true;
}

void RGWRedirectInfo::dump(Formatter *f) const
{
  encode_json("protocol", protocol, f);
  encode_json("hostname", hostname, f);
  encode_json("http_redirect_code", (int)http_redirect_code, f);
}

void RGWRedirectInfo::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("protocol", protocol, obj);
  JSONDecoder::decode_json("hostname", hostname, obj);
  int code;
  JSONDecoder::decode_json("http_redirect_code", code, obj);
  http_redirect_code = code;
}

void RGWBWRedirectInfo::dump(Formatter *f) const
{
  encode_json("redirect", redirect, f);
  encode_json("replace_key_prefix_with", replace_key_prefix_with, f);
  encode_json("replace_key_with", replace_key_with, f);
}

void RGWBWRedirectInfo::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("redirect", redirect, obj);
  JSONDecoder::decode_json("replace_key_prefix_with", replace_key_prefix_with, obj);
  JSONDecoder::decode_json("replace_key_with", replace_key_with, obj);
}

void RGWBWRoutingRuleCondition::dump(Formatter *f) const
{
  encode_json("key_prefix_equals", key_prefix_equals, f);
  encode_json("http_error_code_returned_equals", (int)http_error_code_returned_equals, f);
}

void RGWBWRoutingRuleCondition::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("key_prefix_equals", key_prefix_equals, obj);
  int code;
  JSONDecoder::decode_json("http_error_code_returned_equals", code, obj);
  http_error_code_returned_equals = code;
}

void RGWBWRoutingRule::dump(Formatter *f) const
{
  encode_json("condition", condition, f);
  encode_json("redirect_info", redirect_info, f);
}

void RGWBWRoutingRule::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("condition", condition, obj);
  JSONDecoder::decode_json("redirect_info", redirect_info, obj);
}

void RGWBWRoutingRules::dump(Formatter *f) const
{
  encode_json("rules", rules, f);
}

void RGWBWRoutingRules::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("rules", rules, obj);
}

void RGWBucketWebsiteConf::dump(Formatter *f) const
{
  if (!redirect_all.hostname.empty()) {
    encode_json("redirect_all", redirect_all, f);
  } else {
    encode_json("index_doc_suffix", index_doc_suffix, f);
    encode_json("error_doc", error_doc, f);
    encode_json("routing_rules", routing_rules, f);
  }
}

void RGWBucketWebsiteConf::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("redirect_all", redirect_all, obj);
  JSONDecoder::decode_json("index_doc_suffix", index_doc_suffix, obj);
  JSONDecoder::decode_json("error_doc", error_doc, obj);
  JSONDecoder::decode_json("routing_rules", routing_rules, obj);
}

void RGWBWRedirectInfo::dump_xml(Formatter *f) const
{
  if (!redirect.protocol.empty()) {
    encode_xml("Protocol", redirect.protocol, f);
  }
  if (!redirect.hostname.empty()) {
    encode_xml("HostName", redirect.hostname, f);
  }
  if (redirect.http_redirect_code > 0) {
    encode_xml("HttpRedirectCode", (int)redirect.http_redirect_code, f);
  }
  if (!replace_key_prefix_with.empty()) {
    encode_xml("ReplaceKeyPrefixWith", replace_key_prefix_with, f);
  }
  if (!replace_key_with.empty()) {
    encode_xml("ReplaceKeyWith", replace_key_with, f);
  }
}

#define WEBSITE_HTTP_REDIRECT_CODE_MIN      300
#define WEBSITE_HTTP_REDIRECT_CODE_MAX      400
void RGWBWRedirectInfo::decode_xml(XMLObj *obj) {
  RGWXMLDecoder::decode_xml("Protocol", redirect.protocol, obj);
  RGWXMLDecoder::decode_xml("HostName", redirect.hostname, obj);
  int code = 0;
  bool has_http_redirect_code = RGWXMLDecoder::decode_xml("HttpRedirectCode", code, obj);
  if (has_http_redirect_code &&
      !(code > WEBSITE_HTTP_REDIRECT_CODE_MIN &&
        code < WEBSITE_HTTP_REDIRECT_CODE_MAX)) {
    throw RGWXMLDecoder::err("The provided HTTP redirect code is not valid. Valid codes are 3XX except 300.");
  }
  redirect.http_redirect_code = code;
  bool has_replace_key_prefix_with = RGWXMLDecoder::decode_xml("ReplaceKeyPrefixWith", replace_key_prefix_with, obj);
  bool has_replace_key_with = RGWXMLDecoder::decode_xml("ReplaceKeyWith", replace_key_with, obj);
  if (has_replace_key_prefix_with && has_replace_key_with) {
    throw RGWXMLDecoder::err("You can only define ReplaceKeyPrefix or ReplaceKey but not both.");
  }
}

void RGWBWRoutingRuleCondition::dump_xml(Formatter *f) const
{
  if (!key_prefix_equals.empty()) {
    encode_xml("KeyPrefixEquals", key_prefix_equals, f);
  }
  if (http_error_code_returned_equals > 0) {
    encode_xml("HttpErrorCodeReturnedEquals", (int)http_error_code_returned_equals, f);
  }
}

#define WEBSITE_HTTP_ERROR_CODE_RETURNED_EQUALS_MIN      400
#define WEBSITE_HTTP_ERROR_CODE_RETURNED_EQUALS_MAX      600
void RGWBWRoutingRuleCondition::decode_xml(XMLObj *obj) {
  RGWXMLDecoder::decode_xml("KeyPrefixEquals", key_prefix_equals, obj);
  int code = 0;
  bool has_http_error_code_returned_equals = RGWXMLDecoder::decode_xml("HttpErrorCodeReturnedEquals", code, obj);
  if (has_http_error_code_returned_equals &&
      !(code >= WEBSITE_HTTP_ERROR_CODE_RETURNED_EQUALS_MIN &&
        code < WEBSITE_HTTP_ERROR_CODE_RETURNED_EQUALS_MAX)) {
    throw RGWXMLDecoder::err("The provided HTTP redirect code is not valid. Valid codes are 4XX or 5XX.");
  }
  http_error_code_returned_equals = code;
}

void RGWBWRoutingRule::dump_xml(Formatter *f) const
{
  encode_xml("Condition", condition, f);
  encode_xml("Redirect", redirect_info, f);
}

void RGWBWRoutingRule::decode_xml(XMLObj *obj) {
  RGWXMLDecoder::decode_xml("Condition", condition, obj);
  RGWXMLDecoder::decode_xml("Redirect", redirect_info, obj);
}

static void encode_xml(const char *name, const std::list<RGWBWRoutingRule>& l, ceph::Formatter *f)
{
  do_encode_xml("RoutingRules", l, "RoutingRule", f);
}

void RGWBucketWebsiteConf::dump_xml(Formatter *f) const
{
  if (!redirect_all.hostname.empty()) {
    f->open_object_section("RedirectAllRequestsTo");
    encode_xml("HostName", redirect_all.hostname, f);
    if (!redirect_all.protocol.empty()) {
      encode_xml("Protocol", redirect_all.protocol, f);
    }
    f->close_section();
  }
  if (!index_doc_suffix.empty()) {
    f->open_object_section("IndexDocument");
    encode_xml("Suffix", index_doc_suffix, f);
    f->close_section();
  }
  if (!error_doc.empty()) {
    f->open_object_section("ErrorDocument");
    encode_xml("Key", error_doc, f);
    f->close_section();
  }
  if (!routing_rules.rules.empty()) {
    encode_xml("RoutingRules", routing_rules.rules, f);
  }
}

void decode_xml_obj(list<RGWBWRoutingRule>& l, XMLObj *obj)
{
  do_decode_xml_obj(l, "RoutingRule", obj);
}

void RGWBucketWebsiteConf::decode_xml(XMLObj *obj) {
  XMLObj *o = obj->find_first("RedirectAllRequestsTo");
  if (o) {
    is_redirect_all = true;
    RGWXMLDecoder::decode_xml("HostName", redirect_all.hostname, o, true);
    RGWXMLDecoder::decode_xml("Protocol", redirect_all.protocol, o);
  } else {
    o = obj->find_first("IndexDocument");
    if (o) {
      is_set_index_doc = true;
      RGWXMLDecoder::decode_xml("Suffix", index_doc_suffix, o);
    }
    o = obj->find_first("ErrorDocument");
    if (o) {
      RGWXMLDecoder::decode_xml("Key", error_doc, o);
    }
    RGWXMLDecoder::decode_xml("RoutingRules", routing_rules.rules, obj);
  }
}
