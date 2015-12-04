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

void RGWBWRedirectInfo::decode_xml(XMLObj *obj) {
  RGWXMLDecoder::decode_xml("Protocol", redirect.protocol, obj);
  RGWXMLDecoder::decode_xml("HostName", redirect.hostname, obj);
  int code = 0;
  RGWXMLDecoder::decode_xml("HttpRedirectCode", code, obj);
  redirect.http_redirect_code = code;
  RGWXMLDecoder::decode_xml("ReplaceKeyPrefixWith", replace_key_prefix_with, obj);
  RGWXMLDecoder::decode_xml("ReplaceKeyWith", replace_key_with, obj);
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

void RGWBWRoutingRuleCondition::decode_xml(XMLObj *obj) {
  RGWXMLDecoder::decode_xml("KeyPrefixEquals", key_prefix_equals, obj);
  int code = 0;
  RGWXMLDecoder::decode_xml("HttpErrorCodeReturnedEquals", code, obj);
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
    RGWXMLDecoder::decode_xml("HostName", redirect_all.hostname, o, true);
    RGWXMLDecoder::decode_xml("Protocol", redirect_all.protocol, o);
  } else {
    o = obj->find_first("IndexDocument");
    if (o) {
      RGWXMLDecoder::decode_xml("Suffix", index_doc_suffix, o);
    }
    o = obj->find_first("ErrorDocument");
    if (o) {
      RGWXMLDecoder::decode_xml("Key", error_doc, o);
    }
    RGWXMLDecoder::decode_xml("RoutingRules", routing_rules.rules, obj);
  }
}

