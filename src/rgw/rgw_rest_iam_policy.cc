// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_rest_iam_policy.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "include/types.h"
#include "rgw_common.h"
#include "rgw_customer_managed_policy.h"
#include "rgw_process_env.h"
#include "rgw_rest.h"
#include "rgw_rest_iam.h"
#include <regex>

#define dout_subsys ceph_subsys_rgw

RGWRestIAMPolicy ::RGWRestIAMPolicy(uint64_t action, uint32_t perm)
    : action(action), perm(perm){ }

int RGWRestIAMPolicy::get_params() {
  ldpp_dout(this, 0) << "RAJA RGWRestIAMPolicy::get_params" << dendl;
  return 0;
}

int RGWRestIAMPolicy::check_caps(const RGWUserCaps &caps) {
  ldpp_dout(this, 5) << "RAJA RGWRestIAMPolicy::check_caps " << dendl;
  return caps.check_cap("user-policy", perm);
}

void RGWRestIAMPolicy::send_response() {
  ldpp_dout(this, 5) << "RAJA RGWRestIAMPolicy::send_response " << dendl;
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s);
}

int RGWRestIAMPolicy::init_processing(optional_yield y) {
  ldpp_dout(this, 5) << "RAJA RGWRestIAMPolicy::init_processing " << dendl;
  int r = get_params();
  if (r < 0)
  {
    return r;
  }

  return r;
}

int RGWRestIAMPolicy::verify_permission(optional_yield y) {
  ldpp_dout(this, 5) << "RAJA RGWRestIAMPolicy::verify_permission " << dendl;
  return 0;
}

/*
  PolicyName : string, required
  PolicyDocument : string, required
  Desc : string
  Version : string
  Tags : string
*/
int RGWPutIAMPolicy::get_params(){
  ldpp_dout(this, 0) << "RAJA RGWPutIAMPolicy::get_params" << dendl;

  policy_name = s->info.args.get("PolicyName");
  if (!validate_iam_policy_name(policy_name, s->err.message)) {
    return -EINVAL;
  }

  policy = s->info.args.get("PolicyDocument");
  if (policy.empty()) {
    s->err.message = "Missing required element PolicyDocument";
    return -EINVAL;
  }
  
  return 0;
}
static int parse_tags(const DoutPrefixProvider *dpp,
                      const std::map<std::string, std::string>& params,
                      std::multimap<std::string, std::string> & tags,
                      std::string &message){
  vector<string> keys, vals;
  const regex pattern_key("Tags.member.([0-9]+).Key");
  const regex pattern_value("Tags.member.([0-9]+).Value");
  for (const auto &v : params)
  {
    string key_index = "", value_index = "";
    for (sregex_iterator it = sregex_iterator(
             v.first.begin(), v.first.end(), pattern_key);
         it != sregex_iterator(); it++)
    {
      smatch match;
      match = *it;
      key_index = match.str(1);
      ldpp_dout(dpp, 20) << "Key index: " << match.str(1) << dendl;
      if (!key_index.empty())
      {
        int index = stoi(key_index);
        auto pos = keys.begin() + (index - 1);
        keys.insert(pos, v.second);
      }
    }
    for (sregex_iterator it = sregex_iterator(
             v.first.begin(), v.first.end(), pattern_value);
         it != sregex_iterator(); it++)
    {
      smatch match;
      match = *it;
      value_index = match.str(1);
      ldpp_dout(dpp, 20) << "Value index: " << match.str(1) << dendl;
      if (!value_index.empty())
      {
        int index = stoi(value_index);
        auto pos = vals.begin() + (index - 1);
        vals.insert(pos, v.second);
      }
    }
  }
  if (keys.size() != vals.size())
  {
    message = "Tags array found mismatched Keys/Values";
    return -EINVAL;
  }
  for (size_t i = 0; i < keys.size(); i++)
  {
    tags.emplace(keys[i], vals[i]);
    ldpp_dout(dpp, 4) << "Tag Key: " << keys[i] << " Tag Value is: " << vals[i] << dendl;
  }
  return 0;
}

static rgw::ARN make_policy_arn(const std::string &path,
                              const std::string &name,
                              const std::string &account)
{
  return {string_cat_reserve(path, name), "policy", account, true};
}

int check_policy_limit(const DoutPrefixProvider *dpp, optional_yield y,
                       rgw::sal::Driver *driver, std::string_view account_id,
                       std::string &err)
{
  RGWAccountInfo account;
  rgw::sal::Attrs attrs;     // unused
  RGWObjVersionTracker objv; // unused
  int r = driver->load_account_by_id(dpp, y, account_id, account, attrs, objv);
  if(r < 0) {
    ldpp_dout(dpp, 4) << "failed to load iam account "
                      << account_id << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  if (account.max_roles < 0) { // max_roles < 0 means unlimited
    return 0;
  }

  uint32_t count = 0;
  r = driver->count_account_roles(dpp, y, account_id, count);
  if (r < 0) {
    ldpp_dout(dpp, 4) << "failed to count roles for iam account "
                      << account_id << ": " << cpp_strerror(r) << dendl;
    return r;
  }
  if(std::cmp_greater_equal(count, account.max_roles)) {
    err = fmt::format("Role limit {} exceeded", account.max_roles);
    return -ERR_LIMIT_EXCEEDED;
  }

  return 0;
}

int RGWPutIAMPolicy::init_processing(optional_yield y)
{
  ldpp_dout(this, 5) << "RAJA RGWRestIAMPolicy::init_processing " << dendl;

  policy_name = s->info.args.get("PolicyName");
  if(!validate_iam_policy_name(policy_name, s->err.message)) {
    return -ENAVAIL;
  }

  policy_path = s->info.args.get("Path");
  if(policy_path.empty()) {
    policy_path = "/";
  } else if (!validate_iam_path(policy_path, s->err.message)) {
    return -ENAVAIL;
  }

  description = s->info.args.get("Description");
  if (description.size() > 1000) {
    s->err.message = "Description exceeds maximum length of 1000 characters.";
    return -EINVAL;
  }

  policy_document = s->info.args.get("PolicyDocument");
  if (policy_document.empty()) {
    s->err.message = "Missing required element PolicyDocument";
    return -EINVAL;
  }

  try {
    const rgw::IAM::Policy policy(
        s->cct, nullptr, policy_document,
        false);
  }
  catch (rgw::IAM::PolicyParseException &e) {
    ldpp_dout(this, 5) << "failed to parse policy '" << policy_document << "' with: " << e.what() << dendl;
    s->err.message = e.what();
    return -ERR_MALFORMED_DOC;
  }

  int ret = parse_tags(this, s->info.args.get_params(), tags, s->err.message);
  if(ret < 0) {
    return ret;
  }

  if (tags.size() > 50) {
    s->err.message = "Tags count cannot exceed 50";
    return -ERR_LIMIT_EXCEEDED;
  }
  if (const auto *id = std::get_if<rgw_account_id>(&s->owner.id); id){
    account_id = *id;
    resource = make_policy_arn(policy_path, policy_name, *id);
    //TODO limit check
    ret = check_policy_limit(this, y, driver, account_id, s->err.message);
    if(ret < 0) {
      return ret;
    }
  } else {
      resource = make_policy_arn(policy_path, policy_name, s->user->get_tenant());
  }
  return 0;
}
void RGWPutIAMPolicy::execute(optional_yield y) {
  ldpp_dout(this, 5) << "RAJA RGWPutIAMPolicy::execute " << dendl;
  std::string user_tenant = s->user->get_tenant();
}

RGWPutIAMPolicy::RGWPutIAMPolicy(const ceph::bufferlist &post_body)
    : RGWRestIAMPolicy(rgw::IAM::iamCreatePolicy, RGW_CAP_WRITE),
      post_body(post_body){ }

int RGWPutIAMPolicy::forward_to_master(optional_yield y, const rgw::SiteConfig &site) {
  ldpp_dout(this, 5) << "RAJA RGWPutIAMPolicy::forward_to_master " << dendl;
  return 0;
}


