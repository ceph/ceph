// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once
#include "rgw_string.h"
#include <string>
#include <map>

class RGWEnv;
class CephContext;

class RGWConf {
  friend class RGWEnv;
  int enable_ops_log;
  int enable_usage_log;
  uint8_t defer_to_bucket_acls;
  void init(CephContext *cct);
public:
  RGWConf()
    : enable_ops_log(1),
      enable_usage_log(1),
      defer_to_bucket_acls(0) {
  }
};

class RGWEnv {
  std::map<std::string, std::string, ltstr_nocase> env_map;
  RGWConf conf;
public:
  void init(CephContext *cct);
  void init(CephContext *cct, char **envp);
  void set(std::string name, std::string val);
  const char *get(const char *name, const char *def_val = nullptr) const;
  int get_int(const char *name, int def_val = 0) const;
  bool get_bool(const char *name, bool def_val = 0);
  size_t get_size(const char *name, size_t def_val = 0) const;
  bool exists(const char *name) const;
  bool exists_prefix(const char *prefix) const;
  void remove(const char *name);
  const std::map<std::string, std::string, ltstr_nocase>& get_map() const { return env_map; }
  int get_enable_ops_log() const {
    return conf.enable_ops_log;
  }

  int get_enable_usage_log() const {
    return conf.enable_usage_log;
  }

  int get_defer_to_bucket_acls() const {
    return conf.defer_to_bucket_acls;
  }
};

