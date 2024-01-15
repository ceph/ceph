// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <algorithm>
#include <errno.h>
#include <iterator>
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
#include "rgw_rest_role.h"
#include "rgw_role.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

int RGWRestRole::verify_permission(optional_yield y)
{
  if (verify_user_permission(this, s, resource, action)) {
    return 0;
  }

  return RGWRESTOp::verify_permission(y);
}

int RGWRestRole::check_caps(const RGWUserCaps& caps)
{
  return caps.check_cap("roles", perm);
}

static int parse_tags(const DoutPrefixProvider* dpp,
                      const std::map<std::string, std::string>& params,
                      std::multimap<std::string, std::string>& tags,
                      std::string& message)
{
  vector<string> keys, vals;
  const regex pattern_key("Tags.member.([0-9]+).Key");
  const regex pattern_value("Tags.member.([0-9]+).Value");
  for (const auto& v : params) {
    string key_index="", value_index="";
    for(sregex_iterator it = sregex_iterator(
        v.first.begin(), v.first.end(), pattern_key);
        it != sregex_iterator(); it++) {
        smatch match;
        match = *it;
        key_index = match.str(1);
        ldpp_dout(dpp, 20) << "Key index: " << match.str(1) << dendl;
        if (!key_index.empty()) {
          int index = stoi(key_index);
          auto pos = keys.begin() + (index-1);
          keys.insert(pos, v.second);
        }
    }
    for(sregex_iterator it = sregex_iterator(
        v.first.begin(), v.first.end(), pattern_value);
        it != sregex_iterator(); it++) {
        smatch match;
        match = *it;
        value_index = match.str(1);
        ldpp_dout(dpp, 20) << "Value index: " << match.str(1) << dendl;
        if (!value_index.empty()) {
          int index = stoi(value_index);
          auto pos = vals.begin() + (index-1);
          vals.insert(pos, v.second);
        }
    }
  }
  if (keys.size() != vals.size()) {
    message = "Tags array found mismatched Keys/Values";
    return -EINVAL;
  }
  for (size_t i = 0; i < keys.size(); i++) {
    tags.emplace(keys[i], vals[i]);
    ldpp_dout(dpp, 4) << "Tag Key: " << keys[i] << " Tag Value is: " << vals[i] << dendl;
  }
  return 0;
}

static rgw::ARN make_role_arn(const std::string& path,
                              const std::string& name,
                              const std::string& account)
{
  return {string_cat_reserve(path, name), "role", account, true};
}

static int load_role(const DoutPrefixProvider* dpp, optional_yield y,
                     rgw::sal::Driver* driver, const rgw_owner& owner,
                     rgw_account_id& account_id, const std::string& tenant,
                     const std::string& name,
                     std::unique_ptr<rgw::sal::RGWRole>& role,
                     rgw::ARN& resource, std::string& message)
{
  if (const auto* id = std::get_if<rgw_account_id>(&owner); id) {
    account_id = *id;

    // look up account role by RoleName
    int r = driver->load_account_role_by_name(dpp, y, account_id, name, &role);
    if (r == -ENOENT) {
      message = "No such RoleName in the account";
      return -ERR_NO_ROLE_FOUND;
    }
    if (r >= 0) {
      resource = make_role_arn(role->get_path(), role->get_name(), account_id);
    }
    return r;
  }

  role = driver->get_role(name, tenant);
  const int r = role->get(dpp, y);
  if (r == -ENOENT) {
    message = "No such RoleName in the tenant";
    return -ERR_NO_ROLE_FOUND;
  }
  if (r >= 0) {
    resource = make_role_arn(role->get_path(),
                             role->get_name(),
                             role->get_tenant());
  }
  return r;
}


int RGWCreateRole::init_processing(optional_yield y)
{
  role_name = s->info.args.get("RoleName");
  if (!validate_iam_role_name(role_name, s->err.message)) {
    return -EINVAL;
  }

  role_path = s->info.args.get("Path");
  if (role_path.empty()) {
    role_path = "/";
  } else if (!validate_iam_path(role_path, s->err.message)) {
    return -EINVAL;
  }

  trust_policy = s->info.args.get("AssumeRolePolicyDocument");
  max_session_duration = s->info.args.get("MaxSessionDuration");

  if (trust_policy.empty()) {
    s->err.message = "Missing required element AssumeRolePolicyDocument";
    return -EINVAL;
  }
  bufferlist bl = bufferlist::static_from_string(trust_policy);
  try {
    const rgw::IAM::Policy p(
      s->cct, s->user->get_tenant(), bl,
      s->cct->_conf.get_val<bool>("rgw_policy_reject_invalid_principals"));
  }
  catch (rgw::IAM::PolicyParseException& e) {
    ldpp_dout(this, 5) << "failed to parse policy: " << e.what() << dendl;
    s->err.message = e.what();
    return -ERR_MALFORMED_DOC;
  }

  int ret = parse_tags(this, s->info.args.get_params(), tags, s->err.message);
  if (ret < 0) {
    return ret;
  }

  if (tags.size() > 50) {
    s->err.message = "Tags count cannot exceed 50";
    return -ERR_LIMIT_EXCEEDED;
  }


  if (const auto* id = std::get_if<rgw_account_id>(&s->owner.id); id) {
    resource = make_role_arn(role_path, role_name, *id);
  } else {
    resource = make_role_arn(role_path, role_name, s->user->get_tenant());
  }
  return 0;
}

void RGWCreateRole::execute(optional_yield y)
{
  std::string user_tenant = s->user->get_tenant();
  std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name,
							    user_tenant,
							    role_path,
							    trust_policy,
							    max_session_duration,
	                tags);
  if (!user_tenant.empty() && role->get_tenant() != user_tenant) {
    ldpp_dout(this, 20) << "ERROR: the tenant provided in the role name does not match with the tenant of the user creating the role"
    << dendl;
    op_ret = -EINVAL;
    return;
  }

  std::string role_id;

  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    RGWXMLDecoder::XMLParser parser;
    if (!parser.init()) {
      ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
      op_ret = -EINVAL;
      return;
    }

    bufferlist data;
    s->info.args.remove("RoleName");
    s->info.args.remove("Path");
    s->info.args.remove("AssumeRolePolicyDocument");
    s->info.args.remove("MaxSessionDuration");
    s->info.args.remove("Action");
    s->info.args.remove("Version");
    auto& val_map = s->info.args.get_params();
    for (auto it = val_map.begin(); it!= val_map.end(); it++) {
      if (it->first.find("Tags.member.") == 0) {
        val_map.erase(it);
      }
    }

    op_ret = forward_iam_request_to_master(this, site, s->user->get_info(),
                                           bl_post_body, parser, s->info, y);
    if (op_ret < 0) {
      ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << op_ret << dendl;
      return;
    }

    XMLObj* create_role_resp_obj = parser.find_first("CreateRoleResponse");;
    if (!create_role_resp_obj) {
      ldpp_dout(this, 5) << "ERROR: unexpected xml: CreateRoleResponse" << dendl;
      op_ret = -EINVAL;
      return;
    }

    XMLObj* create_role_res_obj = create_role_resp_obj->find_first("CreateRoleResult");
    if (!create_role_res_obj) {
      ldpp_dout(this, 5) << "ERROR: unexpected xml: CreateRoleResult" << dendl;
      op_ret = -EINVAL;
      return;
    }

    XMLObj* role_obj = nullptr;
    if (create_role_res_obj) {
      role_obj = create_role_res_obj->find_first("Role");
    }
    if (!role_obj) {
      ldpp_dout(this, 5) << "ERROR: unexpected xml: Role" << dendl;
      op_ret = -EINVAL;
      return;
    }

    try {
      if (role_obj) {
        RGWXMLDecoder::decode_xml("RoleId", role_id, role_obj, true);
      }
    } catch (RGWXMLDecoder::err& err) {
      ldpp_dout(this, 5) << "ERROR: unexpected xml: RoleId" << dendl;
      op_ret = -EINVAL;
      return;
    }
    ldpp_dout(this, 0) << "role_id decoded from master zonegroup response is" << role_id << dendl;
  }

  op_ret = role->create(s, true, role_id, y);
  if (op_ret == -EEXIST) {
    op_ret = -ERR_ROLE_EXISTS;
    return;
  }

  if (op_ret == 0) {
    s->formatter->open_object_section("CreateRoleResponse");
    s->formatter->open_object_section("CreateRoleResult");
    s->formatter->open_object_section("Role");
    role->dump(s->formatter);
    s->formatter->close_section();
    s->formatter->close_section();
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

int RGWDeleteRole::init_processing(optional_yield y)
{
  role_name = s->info.args.get("RoleName");
  if (!validate_iam_role_name(role_name, s->err.message)) {
    return -EINVAL;
  }

  return load_role(this, y, driver, s->owner.id, account_id,
                   s->user->get_tenant(), role_name, role, resource,
                   s->err.message);
}

void RGWDeleteRole::execute(optional_yield y)
{
  bool is_master = true;

  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    is_master = false;
    RGWXMLDecoder::XMLParser parser;
    if (!parser.init()) {
      ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
      op_ret = -EINVAL;
    }

    bufferlist data;
    s->info.args.remove("RoleName");
    s->info.args.remove("Action");
    s->info.args.remove("Version");

    op_ret = forward_iam_request_to_master(this, site, s->user->get_info(),
                                           bl_post_body, parser, s->info, y);
    if (op_ret < 0) {
      ldpp_dout(this, 0) << "forward_iam_request_to_master returned ret=" << op_ret << dendl;
      return;
    }
  }

  op_ret = role->delete_obj(s, y);

  if (op_ret == -ENOENT) {
    //Role has been deleted since metadata from master has synced up
    if (!is_master) {
      op_ret = 0;
    } else {
      op_ret = -ERR_NO_ROLE_FOUND;
    }
    return;
  }
  if (!op_ret) {
    s->formatter->open_object_section("DeleteRoleResponse");
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

int RGWGetRole::init_processing(optional_yield y)
{
  role_name = s->info.args.get("RoleName");
  if (!validate_iam_role_name(role_name, s->err.message)) {
    return -EINVAL;
  }

  return load_role(this, y, driver, s->owner.id, account_id,
                   s->user->get_tenant(), role_name, role, resource,
                   s->err.message);
}

void RGWGetRole::execute(optional_yield y)
{
  s->formatter->open_object_section("GetRoleResponse");
  s->formatter->open_object_section("ResponseMetadata");
  s->formatter->dump_string("RequestId", s->trans_id);
  s->formatter->close_section();
  s->formatter->open_object_section("GetRoleResult");
  s->formatter->open_object_section("Role");
  role->dump(s->formatter);
  s->formatter->close_section();
  s->formatter->close_section();
  s->formatter->close_section();
}

int RGWModifyRoleTrustPolicy::init_processing(optional_yield y)
{
  role_name = s->info.args.get("RoleName");
  if (!validate_iam_role_name(role_name, s->err.message)) {
    return -EINVAL;
  }

  trust_policy = s->info.args.get("PolicyDocument");
  if (trust_policy.empty()) {
    s->err.message = "Missing required element PolicyDocument";
    return -EINVAL;
  }

  JSONParser p;
  if (!p.parse(trust_policy.c_str(), trust_policy.length())) {
    ldpp_dout(this, 20) << "ERROR: failed to parse assume role policy doc" << dendl;
    return -ERR_MALFORMED_DOC;
  }

  return load_role(this, y, driver, s->owner.id, account_id,
                   s->user->get_tenant(), role_name, role, resource,
                   s->err.message);
}

void RGWModifyRoleTrustPolicy::execute(optional_yield y)
{
  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    RGWXMLDecoder::XMLParser parser;
    if (!parser.init()) {
      ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
      op_ret = -EINVAL;
      return;
    }

    bufferlist data;
    s->info.args.remove("RoleName");
    s->info.args.remove("PolicyDocument");
    s->info.args.remove("Action");
    s->info.args.remove("Version");

    op_ret = forward_iam_request_to_master(this, site, s->user->get_info(),
                                           bl_post_body, parser, s->info, y);
    if (op_ret < 0) {
      ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << op_ret << dendl;
      return;
    }
  }

  role->update_trust_policy(trust_policy);
  op_ret = role->update(this, y);

  s->formatter->open_object_section("UpdateAssumeRolePolicyResponse");
  s->formatter->open_object_section("ResponseMetadata");
  s->formatter->dump_string("RequestId", s->trans_id);
  s->formatter->close_section();
  s->formatter->close_section();
}

int RGWListRoles::init_processing(optional_yield y)
{
  path_prefix = s->info.args.get("PathPrefix");

  return 0;
}

void RGWListRoles::execute(optional_yield y)
{
  // TODO: list_account_roles() for account owner
  vector<std::unique_ptr<rgw::sal::RGWRole>> result;
  op_ret = driver->get_roles(s, y, path_prefix, s->user->get_tenant(), result);

  if (op_ret == 0) {
    s->formatter->open_array_section("ListRolesResponse");
    s->formatter->open_array_section("ListRolesResult");
    s->formatter->open_object_section("Roles");
    for (const auto& it : result) {
      s->formatter->open_object_section("member");
      it->dump(s->formatter);
      s->formatter->close_section();
    }
    s->formatter->close_section();
    s->formatter->close_section();
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

int RGWPutRolePolicy::init_processing(optional_yield y)
{
  role_name = s->info.args.get("RoleName");
  if (!validate_iam_role_name(role_name, s->err.message)) {
    return -EINVAL;
  }

  policy_name = s->info.args.get("PolicyName");
  perm_policy = s->info.args.get("PolicyDocument");

  if (policy_name.empty()) {
    s->err.message = "Missing required element PolicyName";
    return -EINVAL;
  }
  if (perm_policy.empty()) {
    s->err.message = "Missing required element PolicyDocument";
    return -EINVAL;
  }
  bufferlist bl = bufferlist::static_from_string(perm_policy);
  try {
    const rgw::IAM::Policy p(
      s->cct, s->user->get_tenant(), bl,
      s->cct->_conf.get_val<bool>("rgw_policy_reject_invalid_principals"));
  }
  catch (rgw::IAM::PolicyParseException& e) {
    ldpp_dout(this, 20) << "failed to parse policy: " << e.what() << dendl;
    s->err.message = e.what();
    return -ERR_MALFORMED_DOC;
  }

  return load_role(this, y, driver, s->owner.id, account_id,
                   s->user->get_tenant(), role_name, role, resource,
                   s->err.message);
}

void RGWPutRolePolicy::execute(optional_yield y)
{
  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    RGWXMLDecoder::XMLParser parser;
    if (!parser.init()) {
      ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
      op_ret = -EINVAL;
      return;
    }

    bufferlist data;
    s->info.args.remove("RoleName");
    s->info.args.remove("PolicyName");
    s->info.args.remove("PolicyDocument");
    s->info.args.remove("Action");
    s->info.args.remove("Version");

    op_ret = forward_iam_request_to_master(this, site, s->user->get_info(),
                                           bl_post_body, parser, s->info, y);
    if (op_ret < 0) {
      ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << op_ret << dendl;
      return;
    }
  }

  role->set_perm_policy(policy_name, perm_policy);
  op_ret = role->update(this, y);

  if (op_ret == 0) {
    s->formatter->open_object_section("PutRolePolicyResponse");
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

int RGWGetRolePolicy::init_processing(optional_yield y)
{
  role_name = s->info.args.get("RoleName");
  if (!validate_iam_role_name(role_name, s->err.message)) {
    return -EINVAL;
  }

  policy_name = s->info.args.get("PolicyName");
  if (policy_name.empty()) {
    s->err.message = "Missing required element PolicyName";
    return -EINVAL;
  }

  return load_role(this, y, driver, s->owner.id, account_id,
                   s->user->get_tenant(), role_name, role, resource,
                   s->err.message);
}

void RGWGetRolePolicy::execute(optional_yield y)
{
  string perm_policy;
  op_ret = role->get_role_policy(this, policy_name, perm_policy);
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

int RGWListRolePolicies::init_processing(optional_yield y)
{
  role_name = s->info.args.get("RoleName");
  if (!validate_iam_role_name(role_name, s->err.message)) {
    return -EINVAL;
  }

  return load_role(this, y, driver, s->owner.id, account_id,
                   s->user->get_tenant(), role_name, role, resource,
                   s->err.message);
}

void RGWListRolePolicies::execute(optional_yield y)
{
  std::vector<string> policy_names = role->get_role_policy_names();
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

int RGWDeleteRolePolicy::init_processing(optional_yield y)
{
  role_name = s->info.args.get("RoleName");
  if (!validate_iam_role_name(role_name, s->err.message)) {
    return -EINVAL;
  }

  policy_name = s->info.args.get("PolicyName");
  if (policy_name.empty()) {
    s->err.message = "Missing required element PolicyName";
    return -EINVAL;
  }

  return load_role(this, y, driver, s->owner.id, account_id,
                   s->user->get_tenant(), role_name, role, resource,
                   s->err.message);
}

void RGWDeleteRolePolicy::execute(optional_yield y)
{
  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    RGWXMLDecoder::XMLParser parser;
    if (!parser.init()) {
      ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
      op_ret = -EINVAL;
      return;
    }

    bufferlist data;
    s->info.args.remove("RoleName");
    s->info.args.remove("PolicyName");
    s->info.args.remove("Action");
    s->info.args.remove("Version");

    op_ret = forward_iam_request_to_master(this, site, s->user->get_info(),
                                           bl_post_body, parser, s->info, y);
    if (op_ret < 0) {
      ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << op_ret << dendl;
      return;
    }
  }

  op_ret = role->delete_policy(this, policy_name);
  if (op_ret == -ENOENT) {
    op_ret = -ERR_NO_ROLE_FOUND;
    return;
  }

  if (op_ret == 0) {
    op_ret = role->update(this, y);
  }

  s->formatter->open_object_section("DeleteRolePoliciesResponse");
  s->formatter->open_object_section("ResponseMetadata");
  s->formatter->dump_string("RequestId", s->trans_id);
  s->formatter->close_section();
  s->formatter->close_section();
}

int RGWTagRole::init_processing(optional_yield y)
{
  role_name = s->info.args.get("RoleName");
  if (!validate_iam_role_name(role_name, s->err.message)) {
    return -EINVAL;
  }

  int r = parse_tags(this, s->info.args.get_params(), tags, s->err.message);
  if (r < 0) {
    return r;
  }

  return load_role(this, y, driver, s->owner.id, account_id,
                   s->user->get_tenant(), role_name, role, resource,
                   s->err.message);
}

void RGWTagRole::execute(optional_yield y)
{
  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    RGWXMLDecoder::XMLParser parser;
    if (!parser.init()) {
      ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
      op_ret = -EINVAL;
      return;
    }

    bufferlist data;
    s->info.args.remove("RoleName");
    s->info.args.remove("Action");
    s->info.args.remove("Version");
    auto& val_map = s->info.args.get_params();
    for (auto it = val_map.begin(); it!= val_map.end(); it++) {
      if (it->first.find("Tags.member.") == 0) {
        val_map.erase(it);
      }
    }

    op_ret = forward_iam_request_to_master(this, site, s->user->get_info(),
                                           bl_post_body, parser, s->info, y);
    if (op_ret < 0) {
      ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << op_ret << dendl;
      return;
    }
  }

  op_ret = role->set_tags(this, tags);
  if (op_ret == 0) {
    op_ret = role->update(this, y);
  }

  if (op_ret == 0) {
    s->formatter->open_object_section("TagRoleResponse");
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

int RGWListRoleTags::init_processing(optional_yield y)
{
  role_name = s->info.args.get("RoleName");
  if (!validate_iam_role_name(role_name, s->err.message)) {
    return -EINVAL;
  }

  return load_role(this, y, driver, s->owner.id, account_id,
                   s->user->get_tenant(), role_name, role, resource,
                   s->err.message);
}

void RGWListRoleTags::execute(optional_yield y)
{
  boost::optional<multimap<string,string>> tag_map = role->get_tags();
  s->formatter->open_object_section("ListRoleTagsResponse");
  s->formatter->open_object_section("ListRoleTagsResult");
  if (tag_map) {
    s->formatter->open_array_section("Tags");
    for (const auto& it : tag_map.get()) {
      s->formatter->open_object_section("Key");
      encode_json("Key", it.first, s->formatter);
      s->formatter->close_section();
      s->formatter->open_object_section("Value");
      encode_json("Value", it.second, s->formatter);
      s->formatter->close_section();
    }
    s->formatter->close_section();
  }
  s->formatter->close_section();
  s->formatter->open_object_section("ResponseMetadata");
  s->formatter->dump_string("RequestId", s->trans_id);
  s->formatter->close_section();
  s->formatter->close_section();
}

int RGWUntagRole::init_processing(optional_yield y)
{
  role_name = s->info.args.get("RoleName");
  if (!validate_iam_role_name(role_name, s->err.message)) {
    return -EINVAL;
  }

  const auto& params = s->info.args.get_params();
  const std::string prefix = "TagKeys.member.";
  if (auto l = params.lower_bound(prefix); l != params.end()) {
    // copy matching values into untag vector
    std::transform(l, params.upper_bound(prefix), std::back_inserter(untag),
        [] (const std::pair<const std::string, std::string>& p) {
          return p.second;
        });
  }

  return load_role(this, y, driver, s->owner.id, account_id,
                   s->user->get_tenant(), role_name, role, resource,
                   s->err.message);
}

void RGWUntagRole::execute(optional_yield y)
{
  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    RGWXMLDecoder::XMLParser parser;
    if (!parser.init()) {
      ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
      op_ret = -EINVAL;
      return;
    }

    bufferlist data;
    s->info.args.remove("RoleName");
    s->info.args.remove("Action");
    s->info.args.remove("Version");
    auto& params = s->info.args.get_params();
    if (auto l = params.lower_bound("TagKeys.member."); l != params.end()) {
      params.erase(l, params.upper_bound("TagKeys.member."));
    }

    op_ret = forward_iam_request_to_master(this, site, s->user->get_info(),
                                           bl_post_body, parser, s->info, y);
    if (op_ret < 0) {
      ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << op_ret << dendl;
      return;
    }
  }

  role->erase_tags(untag);
  op_ret = role->update(this, y);

  if (op_ret == 0) {
    s->formatter->open_object_section("UntagRoleResponse");
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

int RGWUpdateRole::init_processing(optional_yield y)
{
  role_name = s->info.args.get("RoleName");
  if (!validate_iam_role_name(role_name, s->err.message)) {
    return -EINVAL;
  }

  max_session_duration = s->info.args.get("MaxSessionDuration");

  return load_role(this, y, driver, s->owner.id, account_id,
                   s->user->get_tenant(), role_name, role, resource,
                   s->err.message);
}

void RGWUpdateRole::execute(optional_yield y)
{
  const rgw::SiteConfig& site = *s->penv.site;
  if (!site.is_meta_master()) {
    RGWXMLDecoder::XMLParser parser;
    if (!parser.init()) {
      ldpp_dout(this, 0) << "ERROR: failed to initialize xml parser" << dendl;
      op_ret = -EINVAL;
      return;
    }

    bufferlist data;
    s->info.args.remove("RoleName");
    s->info.args.remove("MaxSessionDuration");
    s->info.args.remove("Action");
    s->info.args.remove("Version");

    op_ret = forward_iam_request_to_master(this, site, s->user->get_info(),
                                           bl_post_body, parser, s->info, y);
    if (op_ret < 0) {
      ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << op_ret << dendl;
      return;
    }
  }

  role->update_max_session_duration(max_session_duration);
  if (!role->validate_max_session_duration(this)) {
    op_ret = -EINVAL;
    return;
  }

  op_ret = role->update(this, y);

  s->formatter->open_object_section("UpdateRoleResponse");
  s->formatter->open_object_section("UpdateRoleResult");
  s->formatter->open_object_section("ResponseMetadata");
  s->formatter->dump_string("RequestId", s->trans_id);
  s->formatter->close_section();
  s->formatter->close_section();
}
