// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <errno.h>
#include <ctime>
#include <regex>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "common/ceph_time.h"
#include "rgw_rados.h"
#include "rgw_zone.h"

#include "include/types.h"
#include "rgw_string.h"

#include "rgw_common.h"
#include "rgw_tools.h"
#include "rgw_role.h"

#include "services/svc_zone.h"
#include "services/svc_sys_obj.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw { namespace sal {

const string RGWRole::role_name_oid_prefix = "role_names.";
const string RGWRole::role_oid_prefix = "roles.";
const string RGWRole::role_path_oid_prefix = "role_paths.";
const string RGWRole::role_arn_prefix = "arn:aws:iam::";

int RGWRole::get(const DoutPrefixProvider *dpp, optional_yield y)
{
  int ret = read_name(dpp, y);
  if (ret < 0) {
    return ret;
  }

  ret = read_info(dpp, y);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWRole::get_by_id(const DoutPrefixProvider *dpp, optional_yield y)
{
  int ret = read_info(dpp, y);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWRole::update(const DoutPrefixProvider *dpp, optional_yield y)
{
  int ret = store_info(dpp, false, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR:  storing info in Role pool: "
                  << id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return 0;
}

void RGWRole::set_perm_policy(const string& policy_name, const string& perm_policy)
{
  perm_policy_map[policy_name] = perm_policy;
}

vector<string> RGWRole::get_role_policy_names()
{
  vector<string> policy_names;
  for (const auto& it : perm_policy_map)
  {
    policy_names.push_back(std::move(it.first));
  }

  return policy_names;
}

int RGWRole::get_role_policy(const DoutPrefixProvider* dpp, const string& policy_name, string& perm_policy)
{
  const auto it = perm_policy_map.find(policy_name);
  if (it == perm_policy_map.end()) {
    ldpp_dout(dpp, 0) << "ERROR: Policy name: " << policy_name << " not found" << dendl;
    return -ENOENT;
  } else {
    perm_policy = it->second;
  }
  return 0;
}

int RGWRole::delete_policy(const DoutPrefixProvider* dpp, const string& policy_name)
{
  const auto& it = perm_policy_map.find(policy_name);
  if (it == perm_policy_map.end()) {
    ldpp_dout(dpp, 0) << "ERROR: Policy name: " << policy_name << " not found" << dendl;
    return -ENOENT;
  } else {
    perm_policy_map.erase(it);
  }
    return 0;
}

void RGWRole::dump(Formatter *f) const
{
  encode_json("RoleId", id , f);
  encode_json("RoleName", name , f);
  encode_json("Path", path, f);
  encode_json("Arn", arn, f);
  encode_json("CreateDate", creation_date, f);
  encode_json("MaxSessionDuration", max_session_duration, f);
  encode_json("AssumeRolePolicyDocument", trust_policy, f);
  if (!tags.empty()) {
    f->open_array_section("Tags");
    for (const auto& it : tags) {
      f->open_object_section("Key");
      encode_json("Key", it.first, f);
      f->close_section();
      f->open_object_section("Value");
      encode_json("Value", it.second, f);
      f->close_section();
    }
    f->close_section();
  }
}

void RGWRole::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("path", path, obj);
  JSONDecoder::decode_json("arn", arn, obj);
  JSONDecoder::decode_json("create_date", creation_date, obj);
  JSONDecoder::decode_json("max_session_duration", max_session_duration, obj);
  JSONDecoder::decode_json("assume_role_policy_document", trust_policy, obj);
}

bool RGWRole::validate_input(const DoutPrefixProvider* dpp)
{
  if (name.length() > MAX_ROLE_NAME_LEN) {
    ldpp_dout(dpp, 0) << "ERROR: Invalid name length " << dendl;
    return false;
  }

  if (path.length() > MAX_PATH_NAME_LEN) {
    ldpp_dout(dpp, 0) << "ERROR: Invalid path length " << dendl;
    return false;
  }

  std::regex regex_name("[A-Za-z0-9:=,.@-]+");
  if (! std::regex_match(name, regex_name)) {
    ldpp_dout(dpp, 0) << "ERROR: Invalid chars in name " << dendl;
    return false;
  }

  std::regex regex_path("(/[!-~]+/)|(/)");
  if (! std::regex_match(path,regex_path)) {
    ldpp_dout(dpp, 0) << "ERROR: Invalid chars in path " << dendl;
    return false;
  }

  if (max_session_duration < SESSION_DURATION_MIN ||
          max_session_duration > SESSION_DURATION_MAX) {
    ldpp_dout(dpp, 0) << "ERROR: Invalid session duration, should be between 3600 and 43200 seconds " << dendl;
    return false;
  }
  return true;
}

void RGWRole::extract_name_tenant(const std::string& str) {
  if (auto pos = str.find('$');
      pos != std::string::npos) {
    tenant = str.substr(0, pos);
    name = str.substr(pos+1);
  }
}

void RGWRole::update_trust_policy(string& trust_policy)
{
  this->trust_policy = trust_policy;
}

int RGWRole::set_tags(const DoutPrefixProvider* dpp, const multimap<string,string>& tags_map)
{
  for (auto& it : tags_map) {
    this->tags.emplace(it.first, it.second);
  }
  if (this->tags.size() > 50) {
    ldpp_dout(dpp, 0) << "No. of tags is greater than 50" << dendl;
    return -EINVAL;
  }
  return 0;
}

boost::optional<multimap<string,string>> RGWRole::get_tags()
{
  if(this->tags.empty()) {
    return boost::none;
  }
  return this->tags;
}

void RGWRole::erase_tags(const vector<string>& tagKeys)
{
  for (auto& it : tagKeys) {
    this->tags.erase(it);
  }
}

const string& RGWRole::get_names_oid_prefix()
{
  return role_name_oid_prefix;
}

const string& RGWRole::get_info_oid_prefix()
{
  return role_oid_prefix;
}

const string& RGWRole::get_path_oid_prefix()
{
  return role_path_oid_prefix;
}

} } // namespace rgw::sal
