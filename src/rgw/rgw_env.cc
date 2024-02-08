// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_common.h"
#include "rgw_log.h"

#include <string>
#include <map>
#include "include/ceph_assert.h"
#include "rgw_crypt_sanitize.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

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

const char *rgw_conf_get(const map<string, string, ltstr_nocase>& conf_map, const char *name, const char *def_val)
{
  auto iter = conf_map.find(name);
  if (iter == conf_map.end())
    return def_val;

  return iter->second.c_str();
}

boost::optional<const std::string&> rgw_conf_get_optional(const map<string, string, ltstr_nocase>& conf_map, const std::string& name)
{
  auto iter = conf_map.find(name);
  if (iter == conf_map.end())
    return boost::none;

  return boost::optional<const std::string&>(iter->second);
}

const char *RGWEnv::get(const char *name, const char *def_val) const
{
  return rgw_conf_get(env_map, name, def_val);
}

boost::optional<const std::string&>
RGWEnv::get_optional(const std::string& name) const
{
  return rgw_conf_get_optional(env_map, name);
}

int rgw_conf_get_int(const map<string, string, ltstr_nocase>& conf_map, const char *name, int def_val)
{
  auto iter = conf_map.find(name);
  if (iter == conf_map.end())
    return def_val;

  const char *s = iter->second.c_str();
  return atoi(s);  
}

int RGWEnv::get_int(const char *name, int def_val) const
{
  return rgw_conf_get_int(env_map, name, def_val);
}

bool rgw_conf_get_bool(const map<string, string, ltstr_nocase>& conf_map, const char *name, bool def_val)
{
  auto iter = conf_map.find(name);
  if (iter == conf_map.end())
    return def_val;

  const char *s = iter->second.c_str();
  return rgw_str_to_bool(s, def_val);
}

bool RGWEnv::get_bool(const char *name, bool def_val)
{
  return rgw_conf_get_bool(env_map, name, def_val);
}

size_t RGWEnv::get_size(const char *name, size_t def_val) const
{
  const auto iter = env_map.find(name);
  if (iter == env_map.end())
    return def_val;

  size_t sz;
  try{
    sz = stoull(iter->second);
  } catch(...){
    /* it is very unlikely that we'll ever encounter out_of_range, but let's
       return the default either way */
    sz = def_val;
  }

  return sz;
}

bool RGWEnv::exists(const char *name) const
{
  return env_map.find(name)!= env_map.end();
}

bool RGWEnv::exists_prefix(const char *prefix) const
{
  if (env_map.empty() || prefix == NULL)
    return false;

  const auto iter = env_map.lower_bound(prefix);
  if (iter == env_map.end())
    return false;

  return (strncmp(iter->first.c_str(), prefix, strlen(prefix)) == 0);
}

void RGWEnv::remove(const char *name)
{
  map<string, string, ltstr_nocase>::iterator iter = env_map.find(name);
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
