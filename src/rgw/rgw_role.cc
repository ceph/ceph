// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <errno.h>
#include <ctime>
#include <regex>
#include <boost/algorithm/string/replace.hpp>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "common/ceph_time.h"
#include "rgw_rados.h"
#include "rgw_zone.h"

#include "include/types.h"
#include "rgw_string.h"

#include "rgw_common.h"
#include "rgw_metadata.h"
#include "rgw_metadata_lister.h"
#include "rgw_tools.h"
#include "rgw_role.h"

#include "services/svc_zone.h"
#include "services/svc_sys_obj.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

void RGWRoleInfo::dump(Formatter *f) const
{
  encode_json("RoleId", id , f);
  std::string role_name;
  if (tenant.empty()) {
    role_name = name;
  } else {
    role_name = tenant + '$' + name;
  }
  encode_json("RoleName", role_name , f);
  encode_json("Path", path, f);
  encode_json("Arn", arn, f);
  encode_json("CreateDate", creation_date, f);
  encode_json("Description", description, f);
  encode_json("MaxSessionDuration", max_session_duration, f);
  encode_json("AssumeRolePolicyDocument", trust_policy, f);
  encode_json("AccountId", account_id, f);
  if (!perm_policy_map.empty()) {
    f->open_array_section("PermissionPolicies");
    for (const auto& it : perm_policy_map) {
      f->open_object_section("Policy");
      encode_json("PolicyName", it.first, f);
      encode_json("PolicyValue", it.second, f);
      f->close_section();
    }
    f->close_section();
  }
  if (!managed_policies.arns.empty()) {
    f->open_array_section("ManagedPermissionPolicies");
    for (const auto& arn : managed_policies.arns) {
      encode_json("PolicyArn", arn, f);
    }
    f->close_section();
  }
  if (!tags.empty()) {
    f->open_array_section("Tags");
    for (const auto& it : tags) {
      f->open_object_section("Tag");
      encode_json("Key", it.first, f);
      encode_json("Value", it.second, f);
      f->close_section();
    }
    f->close_section();
  }
}

void RGWRoleInfo::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("RoleId", id, obj);
  JSONDecoder::decode_json("RoleName", name, obj);
  JSONDecoder::decode_json("Path", path, obj);
  JSONDecoder::decode_json("Arn", arn, obj);
  JSONDecoder::decode_json("CreateDate", creation_date, obj);
  JSONDecoder::decode_json("Description", description, obj);
  JSONDecoder::decode_json("MaxSessionDuration", max_session_duration, obj);
  JSONDecoder::decode_json("AssumeRolePolicyDocument", trust_policy, obj);
  JSONDecoder::decode_json("AccountId", account_id, obj);

  auto tags_iter = obj->find_first("Tags");
  if (!tags_iter.end()) {
    JSONObj* tags_json = *tags_iter;
    auto iter = tags_json->find_first();

    for (; !iter.end(); ++iter) {
      std::string key, val;
      JSONDecoder::decode_json("Key", key, *iter);
      JSONDecoder::decode_json("Value", val, *iter);
      this->tags.emplace(key, val);
    }
  }

  if (auto perm_policy_iter = obj->find_first("PermissionPolicies");
      !perm_policy_iter.end()) {
    JSONObj* perm_policies = *perm_policy_iter;
    auto iter = perm_policies->find_first();

    for (; !iter.end(); ++iter) {
      std::string policy_name, policy_val;
      JSONDecoder::decode_json("PolicyName", policy_name, *iter);
      JSONDecoder::decode_json("PolicyValue", policy_val, *iter);
      this->perm_policy_map.emplace(policy_name, policy_val);
    }
  }

  if (auto p = obj->find_first("ManagedPermissionPolicies"); !p.end()) {
    for (auto iter = (*p)->find_first(); !iter.end(); ++iter) {
      std::string arn = (*iter)->get_data();
      this->managed_policies.arns.insert(std::move(arn));
    }
  }

  if (auto pos = name.find('$'); pos != std::string::npos) {
    tenant = name.substr(0, pos);
    name = name.substr(pos+1);
  }
}

namespace rgw::sal {

const string RGWRole::role_arn_prefix = "arn:aws:iam::";

RGWRole::RGWRole(std::string name,
              std::string tenant,
              rgw_account_id account_id,
              std::string path,
              std::string trust_policy,
              std::string description,
              std::string max_session_duration_str,
              std::multimap<std::string,std::string> tags)
{
  info.name = std::move(name);
  info.account_id = std::move(account_id);
  info.path = std::move(path);
  info.trust_policy = std::move(trust_policy);
  info.tenant = std::move(tenant);
  info.tags = std::move(tags);
  if (this->info.path.empty())
    this->info.path = "/";
  extract_name_tenant(this->info.name);
  info.description = std::move(description);
  if (max_session_duration_str.empty()) {
    info.max_session_duration = SESSION_DURATION_MIN;
  } else {
    info.max_session_duration = std::stoull(max_session_duration_str);
  }
  info.mtime = real_time();
}

RGWRole::RGWRole(std::string id)
{
  info.id = std::move(id);
}

bool RGWRole::validate_max_session_duration(const DoutPrefixProvider* dpp)
{
  if (info.max_session_duration < SESSION_DURATION_MIN ||
          info.max_session_duration > SESSION_DURATION_MAX) {
    ldpp_dout(dpp, 0) << "ERROR: Invalid session duration, should be between 3600 and 43200 seconds " << dendl;
    return false;
  }
  return true;
}

bool RGWRole::validate_input(const DoutPrefixProvider* dpp)
{
  if (info.name.length() > MAX_ROLE_NAME_LEN) {
    ldpp_dout(dpp, 0) << "ERROR: Invalid name length " << dendl;
    return false;
  }

  if (info.path.length() > MAX_PATH_NAME_LEN) {
    ldpp_dout(dpp, 0) << "ERROR: Invalid path length " << dendl;
    return false;
  }

  std::regex regex_name("[A-Za-z0-9:=,.@-]+");
  if (! std::regex_match(info.name, regex_name)) {
    ldpp_dout(dpp, 0) << "ERROR: Invalid chars in name " << dendl;
    return false;
  }

  std::regex regex_path("(/[!-~]+/)|(/)");
  if (! std::regex_match(info.path,regex_path)) {
    ldpp_dout(dpp, 0) << "ERROR: Invalid chars in path " << dendl;
    return false;
  }

  if (!validate_max_session_duration(dpp)) {
    return false;
  }
  return true;
}

void RGWRole::extract_name_tenant(const std::string& str) {
  if (auto pos = str.find('$');
      pos != std::string::npos) {
    info.tenant = str.substr(0, pos);
    info.name = str.substr(pos+1);
  }
}

int RGWRole::create(const DoutPrefixProvider *dpp, const std::string& role_id, optional_yield y)
{
  if (! validate_input(dpp)) {
    return -EINVAL;
  }

  if (!role_id.empty()) {
    info.id = role_id;
  }

  if (info.id.empty()) {
    /* create unique id */
    uuid_d new_uuid;
    char uuid_str[37];
    new_uuid.generate_random();
    new_uuid.print(uuid_str);
    info.id = uuid_str;
  }

  //arn
  std::string_view account = !info.account_id.empty() ? info.account_id : info.tenant;
  info.arn = string_cat_reserve(role_arn_prefix, account, ":role", info.path, info.name);

  if (info.creation_date.empty()) {
    // Creation time
    real_clock::time_point t = real_clock::now();

    struct timeval tv;
    real_clock::to_timeval(t, tv);

    char buf[30];
    struct tm result;
    gmtime_r(&tv.tv_sec, &result);
    strftime(buf,30,"%Y-%m-%dT%H:%M:%S", &result);
    sprintf(buf + strlen(buf),".%03dZ",(int)tv.tv_usec/1000);
    info.creation_date.assign(buf, strlen(buf));
  }

  constexpr bool exclusive = true;
  return store_info(dpp, exclusive, y);
}

void RGWRole::set_perm_policy(const string& policy_name, const string& perm_policy)
{
  info.perm_policy_map[policy_name] = perm_policy;
}

vector<string> RGWRole::get_role_policy_names()
{
  vector<string> policy_names;
  for (const auto& it : info.perm_policy_map)
  {
    policy_names.push_back(std::move(it.first));
  }

  return policy_names;
}

int RGWRole::get_role_policy(const DoutPrefixProvider* dpp, const string& policy_name, string& perm_policy)
{
  const auto it = info.perm_policy_map.find(policy_name);
  if (it == info.perm_policy_map.end()) {
    ldpp_dout(dpp, 0) << "ERROR: Policy name: " << policy_name << " not found" << dendl;
    return -ENOENT;
  } else {
    perm_policy = it->second;
  }
  return 0;
}

int RGWRole::delete_policy(const DoutPrefixProvider* dpp, const string& policy_name)
{
  const auto& it = info.perm_policy_map.find(policy_name);
  if (it == info.perm_policy_map.end()) {
    ldpp_dout(dpp, 0) << "ERROR: Policy name: " << policy_name << " not found" << dendl;
    return -ENOENT;
  } else {
    info.perm_policy_map.erase(it);
  }
  return 0;
}

void RGWRole::update_trust_policy(string& trust_policy)
{
  this->info.trust_policy = trust_policy;
}

int RGWRole::set_tags(const DoutPrefixProvider* dpp, const multimap<string,string>& tags_map)
{
  for (auto& it : tags_map) {
    this->info.tags.emplace(it.first, it.second);
  }
  if (this->info.tags.size() > 50) {
    ldpp_dout(dpp, 0) << "No. of tags is greater than 50" << dendl;
    return -EINVAL;
  }
  return 0;
}

boost::optional<multimap<string,string>> RGWRole::get_tags()
{
  if(this->info.tags.empty()) {
    return boost::none;
  }
  return this->info.tags;
}

void RGWRole::erase_tags(const vector<string>& tagKeys)
{
  for (auto& it : tagKeys) {
    this->info.tags.erase(it);
  }
}

void RGWRole::update_max_session_duration(const std::string& max_session_duration_str)
{
  if (max_session_duration_str.empty()) {
    info.max_session_duration = SESSION_DURATION_MIN;
  } else {
    info.max_session_duration = std::stoull(max_session_duration_str);
  }
}

} // namespace rgw::sal
