#include "rgw_common.h"
#include "rgw_log.h"

#include <string>
#include <map>

#define dout_subsys ceph_subsys_rgw

RGWEnv::RGWEnv()
{
  conf = new RGWConf;
}

RGWEnv::~RGWEnv()
{
  delete conf;
}

void RGWEnv::init(CephContext *cct)
{
  conf->init(cct, this);
}

void RGWEnv::set(const char *name, const char *val)
{
  if (!val)
    val = "";
  env_map[name] = val;

  dout(0) << "RGWEnv::set(): " << name << ": " << val << dendl;
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

const char *RGWEnv::get(const char *name, const char *def_val)
{
  map<string, string, ltstr_nocase>::iterator iter = env_map.find(name);
  if (iter == env_map.end())
    return def_val;

  return iter->second.c_str();
}

int RGWEnv::get_int(const char *name, int def_val)
{
  map<string, string, ltstr_nocase>::iterator iter = env_map.find(name);
  if (iter == env_map.end())
    return def_val;

  const char *s = iter->second.c_str();
  return atoi(s);  
}

bool RGWEnv::get_bool(const char *name, bool def_val)
{
  map<string, string, ltstr_nocase>::iterator iter = env_map.find(name);
  if (iter == env_map.end())
    return def_val;

  const char *s = iter->second.c_str();
  return rgw_str_to_bool(s, def_val);
}

size_t RGWEnv::get_size(const char *name, size_t def_val)
{
  map<string, string, ltstr_nocase>::iterator iter = env_map.find(name);
  if (iter == env_map.end())
    return def_val;

  const char *s = iter->second.c_str();
  return atoll(s);  
}

bool RGWEnv::exists(const char *name)
{
  map<string, string, ltstr_nocase>::iterator iter = env_map.find(name);
  return (iter != env_map.end());
}

bool RGWEnv::exists_prefix(const char *prefix)
{
  if (env_map.empty() || prefix == NULL)
    return false;

  map<string, string, ltstr_nocase>::iterator iter = env_map.lower_bound(prefix);
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

void RGWConf::init(CephContext *cct, RGWEnv *env)
{
  enable_ops_log = cct->_conf->rgw_enable_ops_log;
  enable_usage_log = cct->_conf->rgw_enable_usage_log;
}
