#include "rgw_common.h"
#include "rgw_log.h"

#include <string>
#include <map>

RGWEnv rgw_env;

RGWEnv::~RGWEnv()
{
  delete conf;
}

void RGWEnv::reinit(char **envp)
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

  if (!conf)
    conf = new RGWConf;

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
  max_cache_lru = env->get_size("RGW_MAX_CACHE_LRU", 10000);
  log_level = env->get_int("RGW_LOG_LEVEL", g_conf->rgw_log);
  should_log = env->get_int("RGW_SHOULD_LOG", RGW_SHOULD_LOG_DEFAULT);
}
