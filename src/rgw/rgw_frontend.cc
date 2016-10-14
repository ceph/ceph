// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <signal.h>

#include "rgw_frontend.h"
#include "include/str_list.h"

#include "include/assert.h"


#define dout_subsys ceph_subsys_rgw

int RGWFrontendConfig::parse_config(const string& config,
				    map<string, string>& config_map)
{
  list<string> config_list;
  get_str_list(config, " ", config_list);

  list<string>::iterator iter;
  for (iter = config_list.begin(); iter != config_list.end(); ++iter) {
    string& entry = *iter;
    string key;
    string val;

    if (framework.empty()) {
      framework = entry;
      dout(0) << "framework: " << framework << dendl;
      continue;
    }

    ssize_t pos = entry.find('=');
    if (pos < 0) {
      dout(0) << "framework conf key: " << entry << dendl;
      config_map[entry] = "";
      continue;
    }

    int ret = parse_key_value(entry, key, val);
    if (ret < 0) {
      cerr << "ERROR: can't parse " << entry << std::endl;
      return ret;
    }

    dout(0) << "framework conf key: " << key << ", val: " << val << dendl;
    config_map[key] = val;
  }

  return 0;
}

bool RGWFrontendConfig::get_val(const string& key, const string& def_val,
				string *out)
{
 map<string, string>::iterator iter = config_map.find(key);
 if (iter == config_map.end()) {
   *out = def_val;
   return false;
 }

 *out = iter->second;
 return true;
}

bool RGWFrontendConfig::get_val(const string& key, int def_val, int *out)
{
  string str;
  bool found = get_val(key, "", &str);
  if (!found) {
    *out = def_val;
    return false;
  }
  string err;
  *out = strict_strtol(str.c_str(), 10, &err);
  if (!err.empty()) {
    cerr << "error parsing int: " << str << ": " << err << std::endl;
    return -EINVAL;
  }
  return 0;
}

void RGWProcessFrontend::stop()
{
  pprocess->close_fd();
  thread->kill(SIGUSR1);
}
