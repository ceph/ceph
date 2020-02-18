// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <signal.h>

#include "rgw_frontend.h"
#include "include/str_list.h"

#include "include/ceph_assert.h"


#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

int RGWFrontendConfig::parse_config(const string& config,
				    std::multimap<string, string>& config_map)
{
  for (auto& entry : get_str_vec(config, " ")) {
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
      config_map.emplace(std::move(entry), "");
      continue;
    }

    int ret = parse_key_value(entry, key, val);
    if (ret < 0) {
      cerr << "ERROR: can't parse " << entry << std::endl;
      return ret;
    }

    dout(0) << "framework conf key: " << key << ", val: " << val << dendl;
    config_map.emplace(std::move(key), std::move(val));
  }

  return 0;
}

void RGWFrontendConfig::set_default_config(RGWFrontendConfig& def_conf)
{
  const auto& def_conf_map = def_conf.get_config_map();

  for (auto& entry : def_conf_map) {
    if (config_map.find(entry.first) == config_map.end()) {
      config_map.emplace(entry.first, entry.second);
    }
  }
}

std::optional<string> RGWFrontendConfig::get_val(const std::string& key)
{
 auto iter = config_map.find(key);
 if (iter == config_map.end()) {
   return std::nullopt;
 }

 return iter->second;
}

bool RGWFrontendConfig::get_val(const string& key, const string& def_val,
				string *out)
{
 auto iter = config_map.find(key);
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
