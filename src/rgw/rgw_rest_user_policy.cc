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
#include "rgw_rest.h"
#include "rgw_rest_user_policy.h"
#include "services/svc_zone.h"

#define dout_subsys ceph_subsys_rgw

using rgw::IAM::Policy;

void RGWRestUserPolicy::dump(Formatter *f) const
{
  encode_json("Policyname", policy_name , f);
  encode_json("Username", user_name , f);
  encode_json("Policydocument", policy, f);
}

void RGWRestUserPolicy::send_response(const Span& parent_span)
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s);
}

int RGWRestUserPolicy::verify_permission(const Span& parent_span)
{
  if (s->auth.identity->is_anonymous()) {
    return -EACCES;
  }

  if(int ret = check_caps(s->user->get_caps()); ret == 0) {
    return ret;
  }

  uint64_t op = get_op();
  string user_name = s->info.args.get("UserName");
  rgw_user user_id(user_name);
  if (! verify_user_permission(this, s, rgw::ARN(rgw::ARN(user_id.id,
                                                "user",
                                                 user_id.tenant)), op)) {
    return -EACCES;
  }
  return 0;
}

bool RGWRestUserPolicy::validate_input()
{
  if (policy_name.length() > MAX_POLICY_NAME_LEN) {
    ldout(s->cct, 0) << "ERROR: Invalid policy name length " << dendl;
    return false;
  }

  std::regex regex_policy_name("[A-Za-z0-9:=,.@-]+");
  if (! std::regex_match(policy_name, regex_policy_name)) {
    ldout(s->cct, 0) << "ERROR: Invalid chars in policy name " << dendl;
    return false;
  }

  return true;
}

int RGWUserPolicyRead::check_caps(const RGWUserCaps& caps)
{
    return caps.check_cap("user-policy", RGW_CAP_READ);
}

int RGWUserPolicyWrite::check_caps(const RGWUserCaps& caps)
{
    return caps.check_cap("user-policy", RGW_CAP_WRITE);
}

uint64_t RGWPutUserPolicy::get_op()
{
  return rgw::IAM::iamPutUserPolicy;
}

int RGWPutUserPolicy::get_params()
{
  policy_name = url_decode(s->info.args.get("PolicyName"), true);
  user_name = url_decode(s->info.args.get("UserName"), true);
  policy = url_decode(s->info.args.get("PolicyDocument"), true);

  if (policy_name.empty() || user_name.empty() || policy.empty()) {
    ldout(s->cct, 20) << "ERROR: one of policy name, user name or policy document is empty"
    << dendl;
    return -EINVAL;
  }

  if (! validate_input()) {
    return -EINVAL;
  }

  return 0;
}

void RGWPutUserPolicy::execute(const Span& parent_span)
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  bufferlist bl = bufferlist::static_from_string(policy);

  RGWUserInfo info;
  rgw_user user_id(user_name);
  op_ret = store->ctl()->user->get_info_by_uid(user_id, &info, s->yield);
  if (op_ret < 0) {
    op_ret = -ERR_NO_SUCH_ENTITY;
    return;
  }

  map<string, bufferlist> uattrs;
  op_ret = store->ctl()->user->get_attrs_by_uid(user_id, &uattrs, s->yield);
  if (op_ret == -ENOENT) {
    op_ret = -ERR_NO_SUCH_ENTITY;
    return;
  }

  ceph::bufferlist in_data;
  op_ret = store->forward_request_to_master(s->user, nullptr, in_data, nullptr, s->info);
  if (op_ret < 0) {
    ldpp_dout(this, 0) << "ERROR: forward_request_to_master returned ret=" << op_ret << dendl;
    return;
  }

  try {
    const Policy p(s->cct, s->user->get_tenant(), bl);
    map<string, string> policies;
    if (auto it = uattrs.find(RGW_ATTR_USER_POLICY); it != uattrs.end()) {
      bufferlist out_bl = uattrs[RGW_ATTR_USER_POLICY];
      decode(policies, out_bl);
    }
    bufferlist in_bl;
    policies[policy_name] = policy;
    encode(policies, in_bl);
    uattrs[RGW_ATTR_USER_POLICY] = in_bl;

    RGWObjVersionTracker objv_tracker;
    op_ret = store->ctl()->user->store_info(info, s->yield,
                                         RGWUserCtl::PutParams()
                                         .set_objv_tracker(&objv_tracker)
                                         .set_attrs(&uattrs));
    if (op_ret < 0) {
      op_ret = -ERR_INTERNAL_ERROR;
    }
  } catch (rgw::IAM::PolicyParseException& e) {
    ldout(s->cct, 20) << "failed to parse policy: " << e.what() << dendl;
    op_ret = -ERR_MALFORMED_DOC;
  }

  if (op_ret == 0) {
    s->formatter->open_object_section("PutUserPolicyResponse");
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

uint64_t RGWGetUserPolicy::get_op()
{
  return rgw::IAM::iamGetUserPolicy;
}

int RGWGetUserPolicy::get_params()
{
  policy_name = s->info.args.get("PolicyName");
  user_name = s->info.args.get("UserName");

  if (policy_name.empty() || user_name.empty()) {
    ldout(s->cct, 20) << "ERROR: one of policy name or user name is empty"
    << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWGetUserPolicy::execute(const Span& parent_span)
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  rgw_user user_id(user_name);
  map<string, bufferlist> uattrs;
  op_ret = store->ctl()->user->get_attrs_by_uid(user_id, &uattrs, s->yield);
  if (op_ret == -ENOENT) {
    ldout(s->cct, 0) << "ERROR: attrs not found for user" << user_name << dendl;
    op_ret = -ERR_NO_SUCH_ENTITY;
    return;
  }

  if (op_ret == 0) {
    s->formatter->open_object_section("GetUserPolicyResponse");
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->open_object_section("GetUserPolicyResult");
    map<string, string> policies;
    if (auto it = uattrs.find(RGW_ATTR_USER_POLICY); it != uattrs.end()) {
      bufferlist bl = uattrs[RGW_ATTR_USER_POLICY];
      decode(policies, bl);
      if (auto it = policies.find(policy_name); it != policies.end()) {
        policy = policies[policy_name];
        dump(s->formatter);
      } else {
        ldout(s->cct, 0) << "ERROR: policy not found" << policy << dendl;
        op_ret = -ERR_NO_SUCH_ENTITY;
        return;
      }
    } else {
      ldout(s->cct, 0) << "ERROR: RGW_ATTR_USER_POLICY not found" << dendl;
      op_ret = -ERR_NO_SUCH_ENTITY;
      return;
    }
    s->formatter->close_section();
    s->formatter->close_section();
  }
  if (op_ret < 0) {
    op_ret = -ERR_INTERNAL_ERROR;
  }
}

uint64_t RGWListUserPolicies::get_op()
{
  return rgw::IAM::iamListUserPolicies;
}

int RGWListUserPolicies::get_params()
{
  user_name = s->info.args.get("UserName");

  if (user_name.empty()) {
    ldout(s->cct, 20) << "ERROR: user name is empty" << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWListUserPolicies::execute(const Span& parent_span)
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  rgw_user user_id(user_name);
  map<string, bufferlist> uattrs;
  op_ret = store->ctl()->user->get_attrs_by_uid(user_id, &uattrs, s->yield);
  if (op_ret == -ENOENT) {
    ldout(s->cct, 0) << "ERROR: attrs not found for user" << user_name << dendl;
    op_ret = -ERR_NO_SUCH_ENTITY;
    return;
  }

  if (op_ret == 0) {
    map<string, string> policies;
    if (auto it = uattrs.find(RGW_ATTR_USER_POLICY); it != uattrs.end()) {
      s->formatter->open_object_section("ListUserPoliciesResponse");
      s->formatter->open_object_section("ResponseMetadata");
      s->formatter->dump_string("RequestId", s->trans_id);
      s->formatter->close_section();
      s->formatter->open_object_section("ListUserPoliciesResult");
      bufferlist bl = uattrs[RGW_ATTR_USER_POLICY];
      decode(policies, bl);
      for (const auto& p : policies) {
        s->formatter->open_object_section("PolicyNames");
        s->formatter->dump_string("member", p.first);
        s->formatter->close_section();
      }
      s->formatter->close_section();
      s->formatter->close_section();
    } else {
      ldout(s->cct, 0) << "ERROR: RGW_ATTR_USER_POLICY not found" << dendl;
      op_ret = -ERR_NO_SUCH_ENTITY;
      return;
    }
  }
  if (op_ret < 0) {
    op_ret = -ERR_INTERNAL_ERROR;
  }
}

uint64_t RGWDeleteUserPolicy::get_op()
{
  return rgw::IAM::iamDeleteUserPolicy;
}

int RGWDeleteUserPolicy::get_params()
{
  policy_name = s->info.args.get("PolicyName");
  user_name = s->info.args.get("UserName");

  if (policy_name.empty() || user_name.empty()) {
    ldout(s->cct, 20) << "ERROR: One of policy name or user name is empty"<< dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWDeleteUserPolicy::execute(const Span& parent_span)
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  RGWUserInfo info;
  map<string, bufferlist> uattrs;
  rgw_user user_id(user_name);
  op_ret = store->ctl()->user->get_info_by_uid(user_id, &info, s->yield,
                                            RGWUserCtl::GetParams()
                                            .set_attrs(&uattrs));
  if (op_ret < 0) {
    op_ret = -ERR_NO_SUCH_ENTITY;
    return;
  }

  ceph::bufferlist in_data;
  op_ret = store->forward_request_to_master(s->user, nullptr, in_data, nullptr, s->info);
  if (op_ret < 0) {
    // a policy might've been uploaded to this site when there was no sync
    // req. in earlier releases, proceed deletion
    if (op_ret != -ENOENT) {
      ldpp_dout(this, 5) << "forward_request_to_master returned ret=" << op_ret << dendl;
      return;
    }
    ldpp_dout(this, 0) << "ERROR: forward_request_to_master returned ret=" << op_ret << dendl;
  }

  map<string, string> policies;
  if (auto it = uattrs.find(RGW_ATTR_USER_POLICY); it != uattrs.end()) {
    bufferlist out_bl = uattrs[RGW_ATTR_USER_POLICY];
    decode(policies, out_bl);

    if (auto p = policies.find(policy_name); p != policies.end()) {
      bufferlist in_bl;
      policies.erase(p);
      encode(policies, in_bl);
      uattrs[RGW_ATTR_USER_POLICY] = in_bl;

      RGWObjVersionTracker objv_tracker;
      op_ret = store->ctl()->user->store_info(info, s->yield,
                                           RGWUserCtl::PutParams()
                                           .set_old_info(&info)
                                           .set_objv_tracker(&objv_tracker)
                                           .set_attrs(&uattrs));
      if (op_ret < 0) {
        op_ret = -ERR_INTERNAL_ERROR;
      }
      if (op_ret == 0) {
        s->formatter->open_object_section("DeleteUserPoliciesResponse");
        s->formatter->open_object_section("ResponseMetadata");
        s->formatter->dump_string("RequestId", s->trans_id);
        s->formatter->close_section();
        s->formatter->close_section();
      }
    } else {
      op_ret = -ERR_NO_SUCH_ENTITY;
      return;
    }
  } else {
    op_ret = -ERR_NO_SUCH_ENTITY;
    return;
  }
}
