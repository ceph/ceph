// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <errno.h>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"

#include "include/types.h"
#include "rgw_string.h"

#include "rgw_common.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_role.h"
#include "rgw_rest_role.h"

#define dout_subsys ceph_subsys_rgw

int RGWRestRole::verify_permission()
{
  if (s->auth.identity->is_anonymous()) {
    return -EACCES;
  }

  string role_name = s->info.args.get("RoleName");
  RGWRole role(s->cct, store->getRados()->pctl, role_name, s->user->get_tenant());
  if (op_ret = role.get(); op_ret < 0) {
    if (op_ret == -ENOENT) {
      op_ret = -ERR_NO_ROLE_FOUND;
    }
    return op_ret;
  }

  if (int ret = check_caps(s->user->get_caps()); ret == 0) {
    _role = std::move(role);
    return ret;
  }

  string resource_name = role.get_path() + role_name;
  uint64_t op = get_op();
  if (!verify_user_permission(this,
                              s,
                              rgw::ARN(resource_name,
                                            "role",
                                             s->user->get_tenant(), true),
                                             op)) {
    return -EACCES;
  }

  _role = std::move(role);

  return 0;
}

void RGWRestRole::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s, this);
}

int RGWRoleRead::check_caps(const RGWUserCaps& caps)
{
    return caps.check_cap("roles", RGW_CAP_READ);
}

int RGWRoleWrite::check_caps(const RGWUserCaps& caps)
{
    return caps.check_cap("roles", RGW_CAP_WRITE);
}

int RGWCreateRole::verify_permission()
{
  if (s->auth.identity->is_anonymous()) {
    return -EACCES;
  }

  if (int ret = check_caps(s->user->get_caps()); ret == 0) {
    return ret;
  }

  string role_name = s->info.args.get("RoleName");
  string role_path = s->info.args.get("Path");

  string resource_name = role_path + role_name;
  if (!verify_user_permission(this,
                              s,
                              rgw::ARN(resource_name,
                                            "role",
                                             s->user->get_tenant(), true),
                                             get_op())) {
    return -EACCES;
  }
  return 0;
}

int RGWCreateRole::get_params()
{
  role_name = s->info.args.get("RoleName");
  role_path = s->info.args.get("Path");
  trust_policy = s->info.args.get("AssumeRolePolicyDocument");
  max_session_duration = s->info.args.get("MaxSessionDuration");

  if (role_name.empty() || trust_policy.empty()) {
    ldout(s->cct, 20) << "ERROR: one of role name or assume role policy document is empty"
    << dendl;
    return -EINVAL;
  }

  bufferlist bl = bufferlist::static_from_string(trust_policy);
  try {
    const rgw::IAM::Policy p(s->cct, s->user->get_tenant(), bl);
  }
  catch (rgw::IAM::PolicyParseException& e) {
    ldout(s->cct, 20) << "failed to parse policy: " << e.what() << dendl;
    return -ERR_MALFORMED_DOC;
  }

  return 0;
}

void RGWCreateRole::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  RGWRole role(s->cct, store->getRados()->pctl, role_name, role_path, trust_policy,
                s->user->get_tenant(), max_session_duration);
  op_ret = role.create(true);

  if (op_ret == -EEXIST) {
    op_ret = -ERR_ROLE_EXISTS;
  }

  if (op_ret == 0) {
    s->formatter->open_object_section("CreateRoleResponse");
    s->formatter->open_object_section("CreateRoleResult");
    s->formatter->open_object_section("Role");
    role.dump(s->formatter);
    s->formatter->close_section();
    s->formatter->close_section();
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

int RGWDeleteRole::get_params()
{
  role_name = s->info.args.get("RoleName");

  if (role_name.empty()) {
    ldout(s->cct, 20) << "ERROR: Role name is empty"<< dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWDeleteRole::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  op_ret = _role.delete_obj();

  if (op_ret == -ENOENT) {
    op_ret = -ERR_NO_ROLE_FOUND;
  }

  s->formatter->open_object_section("DeleteRoleResponse");
  s->formatter->open_object_section("ResponseMetadata");
  s->formatter->dump_string("RequestId", s->trans_id);
  s->formatter->close_section();
  s->formatter->close_section();
}

int RGWGetRole::verify_permission()
{
  return 0;
}

int RGWGetRole::_verify_permission(const RGWRole& role)
{
  if (s->auth.identity->is_anonymous()) {
    return -EACCES;
  }

  if (int ret = check_caps(s->user->get_caps()); ret == 0) {
    return ret;
  }

  string resource_name = role.get_path() + role.get_name();
  if (!verify_user_permission(this,
                              s,
                              rgw::ARN(resource_name,
                                            "role",
                                             s->user->get_tenant(), true),
                                             get_op())) {
    return -EACCES;
  }
  return 0;
}

int RGWGetRole::get_params()
{
  role_name = s->info.args.get("RoleName");

  if (role_name.empty()) {
    ldout(s->cct, 20) << "ERROR: Role name is empty"<< dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWGetRole::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  RGWRole role(s->cct, store->getRados()->pctl, role_name, s->user->get_tenant());
  op_ret = role.get();

  if (op_ret == -ENOENT) {
    op_ret = -ERR_NO_ROLE_FOUND;
    return;
  }

  op_ret = _verify_permission(role);

  if (op_ret == 0) {
    s->formatter->open_object_section("GetRoleResponse");
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->open_object_section("GetRoleResult");
    s->formatter->open_object_section("Role");
    role.dump(s->formatter);
    s->formatter->close_section();
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

int RGWModifyRole::get_params()
{
  role_name = s->info.args.get("RoleName");
  trust_policy = s->info.args.get("PolicyDocument");

  if (role_name.empty() || trust_policy.empty()) {
    ldout(s->cct, 20) << "ERROR: One of role name or trust policy is empty"<< dendl;
    return -EINVAL;
  }
  JSONParser p;
  if (!p.parse(trust_policy.c_str(), trust_policy.length())) {
    ldout(s->cct, 20) << "ERROR: failed to parse assume role policy doc" << dendl;
    return -ERR_MALFORMED_DOC;
  }

  return 0;
}

void RGWModifyRole::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  _role.update_trust_policy(trust_policy);
  op_ret = _role.update();

  s->formatter->open_object_section("UpdateAssumeRolePolicyResponse");
  s->formatter->open_object_section("ResponseMetadata");
  s->formatter->dump_string("RequestId", s->trans_id);
  s->formatter->close_section();
  s->formatter->close_section();
}

int RGWListRoles::verify_permission()
{
  if (s->auth.identity->is_anonymous()) {
    return -EACCES;
  }

  if (int ret = check_caps(s->user->get_caps()); ret == 0) {
    return ret;
  }

  if (!verify_user_permission(this, 
                              s,
                              rgw::ARN(),
                              get_op())) {
    return -EACCES;
  }

  return 0;
}

int RGWListRoles::get_params()
{
  path_prefix = s->info.args.get("PathPrefix");

  return 0;
}

void RGWListRoles::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  vector<RGWRole> result;
  op_ret = RGWRole::get_roles_by_path_prefix(store->getRados(), s->cct, path_prefix, s->user->get_tenant(), result);

  if (op_ret == 0) {
    s->formatter->open_array_section("ListRolesResponse");
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->open_array_section("ListRolesResult");
    s->formatter->open_object_section("Roles");
    for (const auto& it : result) {
      s->formatter->open_object_section("member");
      it.dump(s->formatter);
      s->formatter->close_section();
    }
    s->formatter->close_section();
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

int RGWPutRolePolicy::get_params()
{
  role_name = s->info.args.get("RoleName");
  policy_name = s->info.args.get("PolicyName");
  perm_policy = s->info.args.get("PolicyDocument");

  if (role_name.empty() || policy_name.empty() || perm_policy.empty()) {
    ldout(s->cct, 20) << "ERROR: One of role name, policy name or perm policy is empty"<< dendl;
    return -EINVAL;
  }
  bufferlist bl = bufferlist::static_from_string(perm_policy);
  try {
    const rgw::IAM::Policy p(s->cct, s->user->get_tenant(), bl);
  }
  catch (rgw::IAM::PolicyParseException& e) {
    ldout(s->cct, 20) << "failed to parse policy: " << e.what() << dendl;
    return -ERR_MALFORMED_DOC;
  }
  return 0;
}

void RGWPutRolePolicy::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  _role.set_perm_policy(policy_name, perm_policy);
  op_ret = _role.update();

  if (op_ret == 0) {
    s->formatter->open_object_section("PutRolePolicyResponse");
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

int RGWGetRolePolicy::get_params()
{
  role_name = s->info.args.get("RoleName");
  policy_name = s->info.args.get("PolicyName");

  if (role_name.empty() || policy_name.empty()) {
    ldout(s->cct, 20) << "ERROR: One of role name or policy name is empty"<< dendl;
    return -EINVAL;
  }
  return 0;
}

void RGWGetRolePolicy::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  string perm_policy;
  op_ret = _role.get_role_policy(policy_name, perm_policy);
  if (op_ret == -ENOENT) {
    op_ret = -ERR_NO_SUCH_ENTITY;
  }

  if (op_ret == 0) {
    s->formatter->open_object_section("GetRolePolicyResponse");
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->open_object_section("GetRolePolicyResult");
    s->formatter->dump_string("PolicyName", policy_name);
    s->formatter->dump_string("RoleName", role_name);
    s->formatter->dump_string("PolicyDocument", perm_policy);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

int RGWListRolePolicies::get_params()
{
  role_name = s->info.args.get("RoleName");

  if (role_name.empty()) {
    ldout(s->cct, 20) << "ERROR: Role name is empty"<< dendl;
    return -EINVAL;
  }
  return 0;
}

void RGWListRolePolicies::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  std::vector<string> policy_names = _role.get_role_policy_names();
  s->formatter->open_object_section("ListRolePoliciesResponse");
  s->formatter->open_object_section("ResponseMetadata");
  s->formatter->dump_string("RequestId", s->trans_id);
  s->formatter->close_section();
  s->formatter->open_object_section("ListRolePoliciesResult");
  s->formatter->open_array_section("PolicyNames");
  for (const auto& it : policy_names) {
    s->formatter->dump_string("member", it);
  }
  s->formatter->close_section();
  s->formatter->close_section();
  s->formatter->close_section();
}

int RGWDeleteRolePolicy::get_params()
{
  role_name = s->info.args.get("RoleName");
  policy_name = s->info.args.get("PolicyName");

  if (role_name.empty() || policy_name.empty()) {
    ldout(s->cct, 20) << "ERROR: One of role name or policy name is empty"<< dendl;
    return -EINVAL;
  }
  return 0;
}

void RGWDeleteRolePolicy::execute()
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  op_ret = _role.delete_policy(policy_name);
  if (op_ret == -ENOENT) {
    op_ret = -ERR_NO_ROLE_FOUND;
  }

  if (op_ret == 0) {
    op_ret = _role.update();
  }

  s->formatter->open_object_section("DeleteRolePoliciesResponse");
  s->formatter->open_object_section("ResponseMetadata");
  s->formatter->dump_string("RequestId", s->trans_id);
  s->formatter->close_section();
  s->formatter->close_section();
}
