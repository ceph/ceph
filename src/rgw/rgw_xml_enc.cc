// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_common.h"
#include "rgw_xml.h"

#include "common/Formatter.h"

void RGWBWRedirectInfo::dump_xml(Formatter *f) const
{
  if (!redirect.protocol.empty()) {
    encode_xml("Protocol", redirect.protocol, f);
  }
  if (!redirect.hostname.empty()) {
    encode_xml("Hostname", redirect.hostname, f);
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

void RGWBWRoutingRuleCondition::dump_xml(Formatter *f) const
{
  if (!key_prefix_equals.empty()) {
    encode_xml("KeyPrefixEquals", key_prefix_equals, f);
  }
  if (http_error_code_returned_equals > 0) {
    encode_xml("HttpErrorCodeReturnedEquals", (int)http_error_code_returned_equals, f);
  }
}

void RGWBWRoutingRule::dump_xml(Formatter *f) const
{
  encode_xml("Condition", condition, f);
  encode_xml("Redirect", redirect_info, f);
}

static void encode_xml(const char *name, const std::list<RGWBWRoutingRule>& l, ceph::Formatter *f)
{
  do_encode_xml("RoutingRules", l, "RoutingRule", f);
}

void RGWBucketWebsiteConf::dump_xml(Formatter *f) const
{
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

