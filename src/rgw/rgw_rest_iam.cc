// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <regex>
#include <boost/tokenizer.hpp>

#include "rgw_auth_s3.h"
#include "rgw_rest_iam.h"

#include "rgw_rest_role.h"
#include "rgw_rest_user_policy.h"
#include "rgw_rest_oidc_provider.h"
#include "rgw_rest_iam_group.h"
#include "rgw_rest_iam_user.h"
#include "rgw_rest_conn.h"
#include "driver/rados/rgw_zone.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

using op_generator = RGWOp*(*)(const bufferlist&);
static const std::unordered_map<std::string_view, op_generator> op_generators = {
  {"CreateRole", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWCreateRole(bl_post_body);}},
  {"DeleteRole", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWDeleteRole(bl_post_body);}},
  {"GetRole", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWGetRole;}},
  {"UpdateAssumeRolePolicy", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWModifyRoleTrustPolicy(bl_post_body);}},
  {"ListRoles", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWListRoles;}},
  {"PutRolePolicy", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWPutRolePolicy(bl_post_body);}},
  {"GetRolePolicy", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWGetRolePolicy;}},
  {"ListRolePolicies", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWListRolePolicies;}},
  {"DeleteRolePolicy", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWDeleteRolePolicy(bl_post_body);}},
  {"AttachRolePolicy", make_iam_attach_role_policy_op},
  {"DetachRolePolicy", make_iam_detach_role_policy_op},
  {"ListAttachedRolePolicies", make_iam_list_attached_role_policies_op},
  {"PutUserPolicy", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWPutUserPolicy(bl_post_body);}},
  {"GetUserPolicy", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWGetUserPolicy;}},
  {"ListUserPolicies", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWListUserPolicies;}},
  {"DeleteUserPolicy", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWDeleteUserPolicy(bl_post_body);}},
  {"AttachUserPolicy", make_iam_attach_user_policy_op},
  {"DetachUserPolicy", make_iam_detach_user_policy_op},
  {"ListAttachedUserPolicies", make_iam_list_attached_user_policies_op},
  {"CreateOpenIDConnectProvider", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWCreateOIDCProvider;}},
  {"ListOpenIDConnectProviders", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWListOIDCProviders;}},
  {"GetOpenIDConnectProvider", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWGetOIDCProvider;}},
  {"DeleteOpenIDConnectProvider", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWDeleteOIDCProvider;}},
  {"TagRole", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWTagRole(bl_post_body);}},
  {"ListRoleTags", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWListRoleTags;}},
  {"UntagRole", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWUntagRole(bl_post_body);}},
  {"UpdateRole", [](const bufferlist& bl_post_body) -> RGWOp* {return new RGWUpdateRole(bl_post_body);}},
  {"CreateUser", make_iam_create_user_op},
  {"GetUser", make_iam_get_user_op},
  {"UpdateUser", make_iam_update_user_op},
  {"DeleteUser", make_iam_delete_user_op},
  {"ListUsers", make_iam_list_users_op},
  {"CreateAccessKey", make_iam_create_access_key_op},
  {"UpdateAccessKey", make_iam_update_access_key_op},
  {"DeleteAccessKey", make_iam_delete_access_key_op},
  {"ListAccessKeys", make_iam_list_access_keys_op},
  {"CreateGroup", make_iam_create_group_op},
  {"GetGroup", make_iam_get_group_op},
  {"UpdateGroup", make_iam_update_group_op},
  {"DeleteGroup", make_iam_delete_group_op},
  {"ListGroups", make_iam_list_groups_op},
  {"AddUserToGroup", make_iam_add_user_to_group_op},
  {"RemoveUserFromGroup", make_iam_remove_user_from_group_op},
  {"ListGroupsForUser", make_iam_list_groups_for_user_op},
  {"PutGroupPolicy", make_iam_put_group_policy_op},
  {"GetGroupPolicy", make_iam_get_group_policy_op},
  {"ListGroupPolicies", make_iam_list_group_policies_op},
  {"DeleteGroupPolicy", make_iam_delete_group_policy_op},
  {"AttachGroupPolicy", make_iam_attach_group_policy_op},
  {"DetachGroupPolicy", make_iam_detach_group_policy_op},
  {"ListAttachedGroupPolicies", make_iam_list_attached_group_policies_op},
};

bool RGWHandler_REST_IAM::action_exists(const req_state* s) 
{
  if (s->info.args.exists("Action")) {
    const std::string action_name = s->info.args.get("Action");
    return op_generators.contains(action_name);
  }
  return false;
}

RGWOp *RGWHandler_REST_IAM::op_post()
{
  if (s->info.args.exists("Action")) {
    const std::string action_name = s->info.args.get("Action");
    const auto action_it = op_generators.find(action_name);
    if (action_it != op_generators.end()) {
      return action_it->second(bl_post_body);
    }
    ldpp_dout(s, 10) << "unknown action '" << action_name << "' for IAM handler" << dendl;
  } else {
    ldpp_dout(s, 10) << "missing action argument in IAM handler" << dendl;
  }
  return nullptr;
}

int RGWHandler_REST_IAM::init(rgw::sal::Driver* driver,
                              req_state *s,
                              rgw::io::BasicClient *cio)
{
  s->dialect = "iam";
  s->prot_flags = RGW_REST_IAM;

  return RGWHandler_REST::init(driver, s, cio);
}

int RGWHandler_REST_IAM::authorize(const DoutPrefixProvider* dpp, optional_yield y)
{
  return RGW_Auth_S3::authorize(dpp, driver, auth_registry, s, y);
}

RGWHandler_REST*
RGWRESTMgr_IAM::get_handler(rgw::sal::Driver* driver,
			    req_state* const s,
			    const rgw::auth::StrategyRegistry& auth_registry,
			    const std::string& frontend_prefix)
{
  bufferlist bl;
  return new RGWHandler_REST_IAM(auth_registry, bl);
}

static constexpr size_t MAX_POLICY_NAME_LEN = 128;

bool validate_iam_policy_name(const std::string& name, std::string& err)
{
  if (name.empty()) {
    err = "Missing required element PolicyName";
    return false;
  }

  if (name.size() > MAX_POLICY_NAME_LEN) {
    err = "PolicyName too long";
    return false;
  }

  std::regex regex_policy_name("[A-Za-z0-9:=,.@-]+");
  if (! std::regex_match(name, regex_policy_name)) {
    err = "PolicyName contains invalid characters";
    return false;
  }

  return true;
}

bool validate_iam_policy_arn(const std::string& arn, std::string& err)
{
  if (arn.empty()) {
    err = "Missing required element PolicyArn";
    return false;
  }

  if (arn.size() > 2048) {
    err = "PolicyArn must be at most 2048 characters long";
    return false;
  }

  if (arn.size() < 20) {
    err = "PolicyArn must be at least 20 characters long";
    return false;
  }

  return true;
}

static constexpr size_t MAX_USER_NAME_LEN = 64;

bool validate_iam_user_name(const std::string& name, std::string& err)
{
  if (name.empty()) {
    err = "Missing required element UserName";
    return false;
  }
  if (name.size() > MAX_USER_NAME_LEN) {
    err = "UserName too long";
    return false;
  }
  const std::regex pattern("[\\w+=,.@-]+");
  if (!std::regex_match(name, pattern)) {
    err = "UserName contains invalid characters";
    return false;
  }
  return true;
}

bool validate_iam_role_name(const std::string& name, std::string& err)
{
  if (name.empty()) {
    err = "Missing required element RoleName";
    return false;
  }
  if (name.size() > rgw::sal::RGWRole::MAX_ROLE_NAME_LEN) {
    err = "RoleName too long";
    return false;
  }
  const std::regex pattern("[\\w+=,.@-]+");
  if (!std::regex_match(name, pattern)) {
    err = "RoleName contains invalid characters";
    return false;
  }
  return true;
}

static constexpr size_t MAX_GROUP_NAME_LEN = 128;

bool validate_iam_group_name(const std::string& name, std::string& err)
{
  if (name.empty()) {
    err = "Missing required element GroupName";
    return false;
  }
  if (name.size() > MAX_GROUP_NAME_LEN) {
    err = "GroupName too long";
    return false;
  }
  const std::regex pattern("[\\w+=,.@-]+");
  if (!std::regex_match(name, pattern)) {
    err = "GroupName contains invalid characters";
    return false;
  }
  return true;
}

static constexpr size_t MAX_PATH_LEN = 512;

bool validate_iam_path(const std::string& path, std::string& err)
{
  if (path.size() > MAX_PATH_LEN) {
    err = "Path too long";
    return false;
  }
  const std::regex pattern("(/[!-~]+/)|(/)");
  if (!std::regex_match(path, pattern)) {
    err = "Path contains invalid characters";
    return false;
  }
  return true;
}

std::string iam_user_arn(const RGWUserInfo& info)
{
  if (info.type == TYPE_ROOT) {
    return fmt::format("arn:aws:iam::{}:root", info.account_id);
  }
  std::string_view acct = !info.account_id.empty()
      ? info.account_id : info.user_id.tenant;
  std::string_view path = info.path;
  if (path.empty()) {
    path = "/";
  }
  return fmt::format("arn:aws:iam::{}:user{}{}",
                     acct, path, info.display_name);
}

std::string iam_group_arn(const RGWGroupInfo& info)
{
  std::string_view path = info.path;
  if (path.empty()) {
    path = "/";
  }
  return fmt::format("arn:aws:iam::{}:group{}{}",
                     info.account_id, path, info.name);
}

int forward_iam_request_to_master(const DoutPrefixProvider* dpp,
                                  const rgw::SiteConfig& site,
                                  const RGWUserInfo& user,
                                  bufferlist& indata,
                                  RGWXMLDecoder::XMLParser& parser,
                                  req_info& req, optional_yield y)
{
  const auto& period = site.get_period();
  if (!period) {
    return 0; // not multisite
  }
  if (site.is_meta_master()) {
    return 0; // don't need to forward metadata requests
  }
  const auto& pmap = period->period_map;
  auto zg = pmap.zonegroups.find(pmap.master_zonegroup);
  if (zg == pmap.zonegroups.end()) {
    return -EINVAL;
  }
  auto z = zg->second.zones.find(zg->second.master_zone);
  if (z == zg->second.zones.end()) {
    return -EINVAL;
  }

  RGWAccessKey creds;
  if (auto i = user.access_keys.begin(); i != user.access_keys.end()) {
    creds.id = i->first;
    creds.key = i->second.key;
  }

  // use the master zone's endpoints
  auto conn = RGWRESTConn{dpp->get_cct(), z->second.id, z->second.endpoints,
                          std::move(creds), zg->second.id, zg->second.api_name};
  bufferlist outdata;
  constexpr size_t max_response_size = 128 * 1024; // we expect a very small response
  int ret = conn.forward_iam_request(dpp, req, nullptr, max_response_size,
                                     &indata, &outdata, y);
  if (ret < 0) {
    return ret;
  }

  std::string r = rgw_bl_str(outdata);
  boost::replace_all(r, "&quot;", "\"");

  if (!parser.parse(r.c_str(), r.length(), 1)) {
    ldpp_dout(dpp, 0) << "ERROR: failed to parse response from master zonegroup" << dendl;
    return -EIO;
  }
  return 0;
}
