#include "rgw_common.h"
#include "rgw_log.h"

#include <string>
#include <map>

#undef DOUT_CONDVAR
#define DOUT_CONDVAR(cct, x) cct->_conf->rgw_log

RGWEnv::RGWEnv()
{
  conf = new RGWConf;
}

RGWEnv::~RGWEnv()
{
  delete conf;
}

void RGWEnv::init(char **envp)
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

  conf->init(this);
}

const char *RGWEnv::get(const char *name, const char *def_val)
{
  map<string, string>::iterator iter = env_map.find(name);
  if (iter == env_map.end())
    return def_val;

  return iter->second.c_str();
}

int RGWEnv::get_int(const char *name, int def_val)
{
  map<string, string>::iterator iter = env_map.find(name);
  if (iter == env_map.end())
    return def_val;

  const char *s = iter->second.c_str();
  return atoi(s);  
}

bool RGWEnv::get_bool(const char *name, bool def_val)
{
  map<string, string>::iterator iter = env_map.find(name);
  if (iter == env_map.end())
    return def_val;

  const char *s = iter->second.c_str();
  return rgw_str_to_bool(s, def_val);
}

size_t RGWEnv::get_size(const char *name, size_t def_val)
{
  map<string, string>::iterator iter = env_map.find(name);
  if (iter == env_map.end())
    return def_val;

  const char *s = iter->second.c_str();
  return atoll(s);  
}

void RGWConf::init(RGWEnv *env)
{
  should_log = env->get_bool("RGW_SHOULD_LOG", RGW_SHOULD_LOG_DEFAULT);
}
