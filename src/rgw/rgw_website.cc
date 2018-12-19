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

#include "common/debug.h"
#include "common/ceph_json.h"

#include "acconfig.h"

#include <errno.h>
#include <string>
#include <list>
#include "include/types.h"
#include "rgw_website.h"



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
    *new_url += key.substr(condition.key_prefix_equals.size());
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

void RGWBucketWebsiteConf::get_effective_key(const string& key, string *effective_key, bool is_file) const
{

  if (key.empty()) {
    *effective_key = index_doc_suffix;
  } else if (key[key.size() - 1] == '/') {
    *effective_key = key + index_doc_suffix;
  } else if (! is_file) {
    *effective_key = key + "/" + index_doc_suffix; 
  } else {
    *effective_key = key;
  }
}
