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
#include "rgw_rest_conn.h"
#include "rgw_rest_role.h"
#include "rgw_role.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

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

int RGWRestRole::verify_permission(optional_yield y)
{
  if (s->auth.identity->is_anonymous()) {
    return -EACCES;
  }
  
  string role_name = s->info.args.get("RoleName");
  if (int ret = check_caps(s->user->get_caps()); ret == 0) {
    return ret;
  }

  string resource_name = _role->get_path() + role_name;
  uint64_t op = get_op();
  if (!verify_user_permission(this,
                              s,
                              rgw::ARN(resource_name,
                                            "role",
                                             s->user->get_tenant(), true),
                                             op)) {
    return -EACCES;
  }

  return 0;
}

int RGWRestRole::init_processing(optional_yield y)
{
  string role_name = s->info.args.get("RoleName");
  std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name,
                                                             s->user->get_tenant());
  if (int ret = role->get(s, y); ret < 0) {
    if (ret == -ENOENT) {
      return -ERR_NO_ROLE_FOUND;
    }
    return ret;
  }
  _role = std::move(role);
  return 0;
}

int RGWRestRole::parse_tags()
{
  vector<string> keys, vals;
  auto val_map = s->info.args.get_params();
  const regex pattern_key("Tags.member.([0-9]+).Key");
  const regex pattern_value("Tags.member.([0-9]+).Value");
  for (auto& v : val_map) {
    string key_index="", value_index="";
    for(sregex_iterator it = sregex_iterator(
        v.first.begin(), v.first.end(), pattern_key);
        it != sregex_iterator(); it++) {
        smatch match;
        match = *it;
        key_index = match.str(1);
        ldout(s->cct, 20) << "Key index: " << match.str(1) << dendl;
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
        ldout(s->cct, 20) << "Value index: " << match.str(1) << dendl;
        if (!value_index.empty()) {
          int index = stoi(value_index);
          auto pos = vals.begin() + (index-1);
          vals.insert(pos, v.second);
        }
    }
  }
  if (keys.size() != vals.size()) {
    ldout(s->cct, 0) << "No. of keys doesn't match with no. of values in tags" << dendl;
    return -EINVAL;
  }
  for (size_t i = 0; i < keys.size(); i++) {
    tags.emplace(keys[i], vals[i]);
    ldout(s->cct, 0) << "Tag Key: " << keys[i] << " Tag Value is: " << vals[i] << dendl;
  }
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

int RGWCreateRole::verify_permission(optional_yield y)
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

int RGWCreateRole::init_processing(optional_yield y)
{
  return 0; // avoid calling RGWRestRole::init_processing()
}

int RGWCreateRole::get_params()
{
  role_name = s->info.args.get("RoleName");
  role_path = s->info.args.get("Path");
  trust_policy = s->info.args.get("AssumeRolePolicyDocument");
  max_session_duration = s->info.args.get("MaxSessionDuration");

  if (role_name.empty() || trust_policy.empty()) {
    ldpp_dout(this, 20) << "ERROR: one of role name or assume role policy document is empty"
    << dendl;
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

  int ret = parse_tags();
  if (ret < 0) {
    return ret;
  }

  if (tags.size() > 50) {
    ldout(s->cct, 0) << "No. tags is greater than 50" << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWCreateRole::execute(optional_yield y)
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
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
        RGWXMLDecoder::decode_xml("CreateDate", role->get_info().creation_date, role_obj);
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

int RGWDeleteRole::get_params()
{
  role_name = s->info.args.get("RoleName");

  if (role_name.empty()) {
    ldpp_dout(this, 20) << "ERROR: Role name is empty"<< dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWDeleteRole::execute(optional_yield y)
{
  bool is_master = true;
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

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

  op_ret = _role->delete_obj(s, y);

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

int RGWGetRole::verify_permission(optional_yield y)
{
  return 0;
}

int RGWGetRole::_verify_permission(const rgw::sal::RGWRole* role)
{
  if (s->auth.identity->is_anonymous()) {
    return -EACCES;
  }

  if (int ret = check_caps(s->user->get_caps()); ret == 0) {
    return ret;
  }

  string resource_name = role->get_path() + role->get_name();
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

int RGWGetRole::init_processing(optional_yield y)
{
  return 0; // avoid calling RGWRestRole::init_processing()
}

int RGWGetRole::get_params()
{
  role_name = s->info.args.get("RoleName");

  if (role_name.empty()) {
    ldpp_dout(this, 20) << "ERROR: Role name is empty"<< dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWGetRole::execute(optional_yield y)
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
  std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name,
							    s->user->get_tenant());
  op_ret = role->get(s, y);

  if (op_ret == -ENOENT) {
    op_ret = -ERR_NO_ROLE_FOUND;
    return;
  }

  op_ret = _verify_permission(role.get());

  if (op_ret == 0) {
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
}

int RGWModifyRoleTrustPolicy::get_params()
{
  role_name = s->info.args.get("RoleName");
  trust_policy = s->info.args.get("PolicyDocument");

  if (role_name.empty() || trust_policy.empty()) {
    ldpp_dout(this, 20) << "ERROR: One of role name or trust policy is empty"<< dendl;
    return -EINVAL;
  }
  JSONParser p;
  if (!p.parse(trust_policy.c_str(), trust_policy.length())) {
    ldpp_dout(this, 20) << "ERROR: failed to parse assume role policy doc" << dendl;
    return -ERR_MALFORMED_DOC;
  }

  return 0;
}

void RGWModifyRoleTrustPolicy::execute(optional_yield y)
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

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

  _role->update_trust_policy(trust_policy);
  op_ret = _role->update(this, y);

  s->formatter->open_object_section("UpdateAssumeRolePolicyResponse");
  s->formatter->open_object_section("ResponseMetadata");
  s->formatter->dump_string("RequestId", s->trans_id);
  s->formatter->close_section();
  s->formatter->close_section();
}

int RGWListRoles::verify_permission(optional_yield y)
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

int RGWListRoles::init_processing(optional_yield y)
{
  return 0; // avoid calling RGWRestRole::init_processing()
}

int RGWListRoles::get_params()
{
  path_prefix = s->info.args.get("PathPrefix");

  return 0;
}

void RGWListRoles::execute(optional_yield y)
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }
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

int RGWPutRolePolicy::get_params()
{
  role_name = s->info.args.get("RoleName");
  policy_name = s->info.args.get("PolicyName");
  perm_policy = s->info.args.get("PolicyDocument");

  if (role_name.empty() || policy_name.empty() || perm_policy.empty()) {
    ldpp_dout(this, 20) << "ERROR: One of role name, policy name or perm policy is empty"<< dendl;
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
  return 0;
}

void RGWPutRolePolicy::execute(optional_yield y)
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

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

  _role->set_perm_policy(policy_name, perm_policy);
  op_ret = _role->update(this, y);

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
    ldpp_dout(this, 20) << "ERROR: One of role name or policy name is empty"<< dendl;
    return -EINVAL;
  }
  return 0;
}

void RGWGetRolePolicy::execute(optional_yield y)
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  string perm_policy;
  op_ret = _role->get_role_policy(this, policy_name, perm_policy);
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
    ldpp_dout(this, 20) << "ERROR: Role name is empty"<< dendl;
    return -EINVAL;
  }
  return 0;
}

void RGWListRolePolicies::execute(optional_yield y)
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  std::vector<string> policy_names = _role->get_role_policy_names();
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
    ldpp_dout(this, 20) << "ERROR: One of role name or policy name is empty"<< dendl;
    return -EINVAL;
  }
  return 0;
}

void RGWDeleteRolePolicy::execute(optional_yield y)
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

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

  op_ret = _role->delete_policy(this, policy_name);
  if (op_ret == -ENOENT) {
    op_ret = -ERR_NO_ROLE_FOUND;
    return;
  }

  if (op_ret == 0) {
    op_ret = _role->update(this, y);
  }

  s->formatter->open_object_section("DeleteRolePolicyResponse");
  s->formatter->open_object_section("ResponseMetadata");
  s->formatter->dump_string("RequestId", s->trans_id);
  s->formatter->close_section();
  s->formatter->close_section();
}

int RGWTagRole::get_params()
{
  role_name = s->info.args.get("RoleName");

  if (role_name.empty()) {
    ldout(s->cct, 0) << "ERROR: Role name is empty" << dendl;
    return -EINVAL;
  }
  int ret = parse_tags();
  if (ret < 0) {
    return ret;
  }

  return 0;
}

void RGWTagRole::execute(optional_yield y)
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

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

  op_ret = _role->set_tags(this, tags);
  if (op_ret == 0) {
    op_ret = _role->update(this, y);
  }

  if (op_ret == 0) {
    s->formatter->open_object_section("TagRoleResponse");
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

int RGWListRoleTags::get_params()
{
  role_name = s->info.args.get("RoleName");

  if (role_name.empty()) {
    ldout(s->cct, 0) << "ERROR: Role name is empty" << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWListRoleTags::execute(optional_yield y)
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

  boost::optional<multimap<string,string>> tag_map = _role->get_tags();
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

int RGWUntagRole::get_params()
{
  role_name = s->info.args.get("RoleName");

  if (role_name.empty()) {
    ldout(s->cct, 0) << "ERROR: Role name is empty" << dendl;
    return -EINVAL;
  }

  auto val_map = s->info.args.get_params();
  for (auto& it : val_map) {
    if (it.first.find("TagKeys.member.") != string::npos) {
        tagKeys.emplace_back(it.second);
    }
  }
  return 0;
}

void RGWUntagRole::execute(optional_yield y)
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

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
    std::vector<std::multimap<std::string, std::string>::iterator> iters;
    for (auto it = val_map.begin(); it!= val_map.end(); it++) {
      if (it->first.find("Tags.member.") == 0) {
        iters.emplace_back(it);
      }
    }

    for (auto& it : iters) {
      val_map.erase(it);
    }
    op_ret = forward_iam_request_to_master(this, site, s->user->get_info(),
                                           bl_post_body, parser, s->info, y);
    if (op_ret < 0) {
      ldpp_dout(this, 20) << "ERROR: forward_iam_request_to_master failed with error code: " << op_ret << dendl;
      return;
    }
  }

  _role->erase_tags(tagKeys);
  op_ret = _role->update(this, y);

  if (op_ret == 0) {
    s->formatter->open_object_section("UntagRoleResponse");
    s->formatter->open_object_section("ResponseMetadata");
    s->formatter->dump_string("RequestId", s->trans_id);
    s->formatter->close_section();
    s->formatter->close_section();
  }
}

int RGWUpdateRole::get_params()
{
  role_name = s->info.args.get("RoleName");
  max_session_duration = s->info.args.get("MaxSessionDuration");

  if (role_name.empty()) {
    ldpp_dout(this, 20) << "ERROR: Role name is empty"<< dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWUpdateRole::execute(optional_yield y)
{
  op_ret = get_params();
  if (op_ret < 0) {
    return;
  }

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

  _role->update_max_session_duration(max_session_duration);
  if (!_role->validate_max_session_duration(this)) {
    op_ret = -EINVAL;
    return;
  }

  op_ret = _role->update(this, y);

  s->formatter->open_object_section("UpdateRoleResponse");
  s->formatter->open_object_section("UpdateRoleResult");
  s->formatter->open_object_section("ResponseMetadata");
  s->formatter->dump_string("RequestId", s->trans_id);
  s->formatter->close_section();
  s->formatter->close_section();
}
