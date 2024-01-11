// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <errno.h>
#include <regex>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"

#include "include/types.h"
#include "rgw_string.h"

#include "rgw_common.h"
#include "rgw_op.h"
#include "rgw_process_env.h"
#include "rgw_rest.h"
#include "rgw_rest_iam.h"
#include "rgw_rest_user_policy.h"
#include "rgw_sal.h"
#include "services/svc_zone.h"

#define dout_subsys ceph_subsys_rgw

RGWRestUserPolicy::RGWRestUserPolicy(uint64_t action, uint32_t perm)
  : action(action), perm(perm)
{
}

void RGWRestUserPolicy::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s);
}

int RGWRestUserPolicy::get_params()
{
  user_name = s->info.args.get("UserName");
  if (!validate_iam_user_name(user_name, s->err.message)) {
    return -EINVAL;
  }
  return 0;
}

int RGWRestUserPolicy::init_processing(optional_yield y)
{
  int r = get_params();
  if (r < 0) {
    return r;
  }

  if (const auto* id = std::get_if<rgw_account_id>(&s->owner.id); id) {
    account_id = *id;

    // look up account user by UserName
    const std::string& tenant = s->auth.identity->get_tenant();
    r = driver->load_account_user_by_name(this, y, account_id,
                                          tenant, user_name, &user);

    if (r == -ENOENT) {
      s->err.message = "No such UserName in the account";
      return -ERR_NO_SUCH_ENTITY;
    }
    if (r >= 0) {
      // user ARN includes account id, path, and display name
      const RGWUserInfo& info = user->get_info();
      const std::string resource = string_cat_reserve(info.path, info.display_name);
      user_arn = rgw::ARN{resource, "user", account_id, true};
    }
  } else {
    // interpret UserName as a uid with optional tenant
    const auto uid = rgw_user{user_name};
    // user ARN includes tenant and user id
    user_arn = rgw::ARN{uid.id, "user", uid.tenant};

    user = driver->get_user(uid);
    r = user->load_user(this, y);
    if (r == -ENOENT) {
      s->err.message = "No such UserName in the tenant";
      return -ERR_NO_SUCH_ENTITY;
    }
  }

  return r;
}

int RGWRestUserPolicy::check_caps(const RGWUserCaps& caps)
{
  return caps.check_cap("user-policy", perm);
}

int RGWRestUserPolicy::verify_permission(optional_yield y)
{
  if (s->auth.identity->is_anonymous()) {
    return -EACCES;
  }

  // admin caps are required for non-account users
  if (check_caps(s->user->get_caps()) == 0) {
    return 0;
  }

  if (! verify_user_permission(this, s, user_arn, action)) {
    return -EACCES;
  }
  return 0;
}


RGWPutUserPolicy::RGWPutUserPolicy()
  : RGWRestUserPolicy(rgw::IAM::iamPutUserPolicy, RGW_CAP_WRITE)
{
}

int RGWPutUserPolicy::get_params()
{
  policy_name = s->info.args.get("PolicyName");
  if (!validate_iam_policy_name(policy_name, s->err.message)) {
    return -EINVAL;
  }

  policy = s->info.args.get("PolicyDocument");
  if (policy.empty()) {
    s->err.message = "Missing required element PolicyDocument";
    return -EINVAL;
  }

  return RGWRestUserPolicy::get_params();
}

void RGWPutUserPolicy::execute(optional_yield y)
{
  bufferlist bl = bufferlist::static_from_string(policy);

  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         nullptr, nullptr, s->info, y);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "ERROR: forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  try {
    const rgw::IAM::Policy p(
      s->cct, s->user->get_tenant(), bl,
      s->cct->_conf.get_val<bool>("rgw_policy_reject_invalid_principals"));
    std::map<std::string, std::string> policies;
    if (auto it = user->get_attrs().find(RGW_ATTR_USER_POLICY); it != user->get_attrs().end()) {
      decode(policies, it->second);
    }
    bufferlist in_bl;
    policies[policy_name] = policy;
    constexpr unsigned int USER_POLICIES_MAX_NUM = 100;
    const unsigned int max_num = s->cct->_conf->rgw_user_policies_max_num < 0 ?
      USER_POLICIES_MAX_NUM : s->cct->_conf->rgw_user_policies_max_num;
    if (policies.size() > max_num) {
      ldpp_dout(this, 4) << "IAM user policies has reached the num config: "
                         << max_num << ", cant add another" << dendl;
      op_ret = -ERR_LIMIT_EXCEEDED;
      s->err.message =
          "The number of IAM user policies should not exceed allowed limit "
          "of " +
          std::to_string(max_num) + " policies.";
      return;
    }
    encode(policies, in_bl);
    user->get_attrs()[RGW_ATTR_USER_POLICY] = in_bl;

    op_ret = user->store_user(s, s->yield, false);
    if (op_ret < 0) {
      op_ret = -ERR_INTERNAL_ERROR;
    }
  } catch (buffer::error& err) {
    ldpp_dout(this, 0) << "ERROR: failed to decode user policies" << dendl;
    op_ret = -EIO;
  } catch (rgw::IAM::PolicyParseException& e) {
    ldpp_dout(this, 5) << "failed to parse policy: " << e.what() << dendl;
    s->err.message = e.what();
    op_ret = -ERR_MALFORMED_DOC;
  }

  if (op_ret == 0) {
    s->formatter->open_object_section_in_ns("PutUserPolicyResponse", RGW_REST_IAM_XMLNS);
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}


RGWGetUserPolicy::RGWGetUserPolicy()
  : RGWRestUserPolicy(rgw::IAM::iamGetUserPolicy, RGW_CAP_READ)
{
}

int RGWGetUserPolicy::get_params()
{
  policy_name = s->info.args.get("PolicyName");
  if (!validate_iam_policy_name(policy_name, s->err.message)) {
    return -EINVAL;
  }

  return RGWRestUserPolicy::get_params();
}

void RGWGetUserPolicy::execute(optional_yield y)
{
  std::map<std::string, std::string> policies;
  if (auto it = user->get_attrs().find(RGW_ATTR_USER_POLICY); it != user->get_attrs().end()) {
    try {
      decode(policies, it->second);
    } catch (buffer::error& err) {
      ldpp_dout(this, 0) << "ERROR: failed to decode user policies" << dendl;
      op_ret = -EIO;
      return;
    }
  }

  auto policy = policies.find(policy_name);
  if (policy == policies.end()) {
    s->err.message = "No such PolicyName on the user";
    op_ret = -ERR_NO_SUCH_ENTITY;
    return;
  }

  s->formatter->open_object_section_in_ns("GetUserPolicyResponse", RGW_REST_IAM_XMLNS);
  s->formatter->open_object_section("ResponseMetadata");
  s->formatter->dump_string("RequestId", s->trans_id);
  s->formatter->close_section();
  s->formatter->open_object_section("GetUserPolicyResult");
  encode_json("PolicyName", policy_name , s->formatter);
  encode_json("UserName", user_name, s->formatter);
  encode_json("PolicyDocument", policy->second, s->formatter);
  s->formatter->close_section();
  s->formatter->close_section();
}


RGWListUserPolicies::RGWListUserPolicies()
  : RGWRestUserPolicy(rgw::IAM::iamListUserPolicies, RGW_CAP_READ)
{
}

void RGWListUserPolicies::execute(optional_yield y)
{
  std::map<std::string, std::string> policies;
  if (auto it = user->get_attrs().find(RGW_ATTR_USER_POLICY); it != user->get_attrs().end()) {
    try {
      decode(policies, it->second);
    } catch (buffer::error& err) {
      ldpp_dout(this, 0) << "ERROR: failed to decode user policies" << dendl;
      op_ret = -EIO;
      return;
    }
  }

  s->formatter->open_object_section_in_ns("ListUserPoliciesResponse", RGW_REST_IAM_XMLNS);
  s->formatter->open_object_section("ResponseMetadata");
  s->formatter->dump_string("RequestId", s->trans_id);
  s->formatter->close_section();
  s->formatter->open_object_section("ListUserPoliciesResult");
  s->formatter->open_array_section("PolicyNames");
  for (const auto& p : policies) {
    s->formatter->dump_string("member", p.first);
  }
  s->formatter->close_section(); // PolicyNames
  s->formatter->close_section(); // ListUserPoliciesResult
  s->formatter->close_section(); // ListUserPoliciesResponse
}


RGWDeleteUserPolicy::RGWDeleteUserPolicy()
  : RGWRestUserPolicy(rgw::IAM::iamDeleteUserPolicy, RGW_CAP_WRITE)
{
}

int RGWDeleteUserPolicy::get_params()
{
  policy_name = s->info.args.get("PolicyName");
  if (!validate_iam_policy_name(policy_name, s->err.message)) {
    return -EINVAL;
  }

  return RGWRestUserPolicy::get_params();
}

void RGWDeleteUserPolicy::execute(optional_yield y)
{
  op_ret = rgw_forward_request_to_master(this, *s->penv.site, s->user->get_id(),
                                         nullptr, nullptr, s->info, y);
  if (op_ret < 0) {
    // a policy might've been uploaded to this site when there was no sync
    // req. in earlier releases, proceed deletion
    if (op_ret != -ENOENT) {
      ldpp_dout(this, 5) << "forward_request_to_master returned ret=" << op_ret << dendl;
      return;
    }
    ldpp_dout(this, 0) << "ERROR: forward_request_to_master returned ret=" << op_ret << dendl;
  }

  std::map<std::string, std::string> policies;
  if (auto it = user->get_attrs().find(RGW_ATTR_USER_POLICY); it != user->get_attrs().end()) {
    bufferlist out_bl = it->second;
    try {
      decode(policies, out_bl);
    } catch (buffer::error& err) {
      ldpp_dout(this, 0) << "ERROR: failed to decode user policies" << dendl;
      op_ret = -EIO;
      return;
    }
  }

  auto policy = policies.find(policy_name);
  if (policy == policies.end()) {
    s->err.message = "No such PolicyName on the user";
    op_ret = -ERR_NO_SUCH_ENTITY;
    return;
  }

  bufferlist in_bl;
  policies.erase(policy);
  encode(policies, in_bl);
  user->get_attrs()[RGW_ATTR_USER_POLICY] = in_bl;

  op_ret = user->store_user(s, s->yield, false);
  if (op_ret < 0) {
    op_ret = -ERR_INTERNAL_ERROR;
    return;
  }

  s->formatter->open_object_section_in_ns("DeleteUserPoliciesResponse", RGW_REST_IAM_XMLNS);
  s->formatter->open_object_section("ResponseMetadata");
  s->formatter->dump_string("RequestId", s->trans_id);
  s->formatter->close_section();
  s->formatter->close_section();
}
