// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_common.h"
#include "rgw_log.h"

#include <string>
#include <map>

#include "common/str_util.h"
#include "rgw_crypt_sanitize.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

void RGWEnv::init(CephContext *cct)
{
  conf.init(cct);
}

void RGWEnv::set(std::string name, std::string val)
{
  env_map[std::move(name)] = std::move(val);
}

void RGWEnv::init(CephContext *cct, char **envp)
{
  const char *p;

  env_map.clear();

  for (int i=0; (p = envp[i]); ++i) {
    string s(p);
    int pos = s.find('=');
    if (pos <= 0) // should never be 0
      continue;
    string name = s.substr(0, pos);
    string val = s.substr(pos + 1);
    env_map[name] = val;
  }

  init(cct);
}

std::optional<std::string_view> rgw_conf_get(const RGWConfMap& conf_map,
					     std::string_view name)
{
  auto iter = conf_map.find(name);
  if (iter == conf_map.end())
    return std::nullopt;

  return iter->second;
}

std::optional<std::string_view> RGWEnv::get(std::string_view name) const
{
  return rgw_conf_get(env_map, name);
}

std::optional<int> rgw_conf_get_int(const RGWConfMap& conf_map,
				    std::string_view name)
{
  auto iter = conf_map.find(name);
  if (iter == conf_map.end())
    return std::nullopt;

  return ceph::parse<int>(iter->second);
}

std::optional<int> RGWEnv::get_int(std::string_view name) const
{
  return rgw_conf_get_int(env_map, name);
}

std::optional<bool> rgw_conf_get_bool(const RGWConfMap& conf_map,
				      std::string_view name)
{
  auto iter = conf_map.find(name);
  if (iter == conf_map.end())
    return std::nullopt;

  return rgw_str_to_bool(iter->second);
}

std::optional<bool> RGWEnv::get_bool(std::string_view name)
{
  return rgw_conf_get_bool(env_map, name);
}

std::optional<std::size_t> RGWEnv::get_size(std::string_view name) const
{
  const auto iter = env_map.find(name);
  if (iter == env_map.end())
    return std::nullopt;

  return ceph::parse<size_t>(iter->second);
}

bool RGWEnv::exists(std::string_view name) const
{
  return env_map.find(name) != env_map.end();
}

bool RGWEnv::exists_prefix(std::string_view prefix) const
{
  if (env_map.empty() || prefix.empty())
    return false;

  const auto iter = env_map.lower_bound(prefix);
  if (iter == env_map.end())
    return false;

  return iter->first.compare(prefix) == 0;
}

void RGWEnv::remove(std::string_view name)
{
  auto iter = env_map.find(name);
  if (iter != env_map.end())
    env_map.erase(iter);
}

void RGWConf::init(CephContext *cct)
{
  enable_ops_log = cct->_conf->rgw_enable_ops_log;
  enable_usage_log = cct->_conf->rgw_enable_usage_log;

  defer_to_bucket_acls = 0;  // default
  if (cct->_conf->rgw_defer_to_bucket_acls == "recurse") {
    defer_to_bucket_acls = RGW_DEFER_TO_BUCKET_ACLS_RECURSE;
  } else if (cct->_conf->rgw_defer_to_bucket_acls == "full_control") {
    defer_to_bucket_acls = RGW_DEFER_TO_BUCKET_ACLS_FULL_CONTROL;
  }
}
