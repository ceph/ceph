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

#include "rgw_common.h"
#include "rgw_xml.h"

#include "common/Formatter.h"

#define dout_subsys ceph_subsys_rgw

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
  bool have_protocol = RGWXMLDecoder::decode_xml("Protocol", redirect.protocol, obj);
  if (have_protocol && redirect.protocol != "http" && redirect.protocol != "https") {
    throw RGWXMLDecoder::err("Invalid protocol, protocol can be http or https. If not defined the protocol will be selected automatically.");
  }
  bool have_host_name = RGWXMLDecoder::decode_xml("HostName", redirect.hostname, obj);
  if (have_host_name && redirect.hostname.empty()) {
    throw RGWXMLDecoder::err("The HostName cannot be blank. To redirect within the same domain, the element should not be present.");
  }
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
  bool have_key_prefix_equals = RGWXMLDecoder::decode_xml("KeyPrefixEquals", key_prefix_equals, obj);
  int code = 0;
  bool has_http_error_code_returned_equals = RGWXMLDecoder::decode_xml("HttpErrorCodeReturnedEquals", code, obj);
  if (has_http_error_code_returned_equals &&
      !(code >= WEBSITE_HTTP_ERROR_CODE_RETURNED_EQUALS_MIN &&
        code < WEBSITE_HTTP_ERROR_CODE_RETURNED_EQUALS_MAX)) {
    throw RGWXMLDecoder::err("The provided HTTP redirect code is not valid. Valid codes are 4XX or 5XX.");
  }
  if (!have_key_prefix_equals && !has_http_error_code_returned_equals) {
    throw RGWXMLDecoder::err("Condition cannot be empty. To redirect all requests without a condition, the condition element should not be present.");
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

