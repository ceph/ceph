// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <vector>

#include "AuthAuthorizeHandler.h"
#include "AuthMethodList.h"
#include "common/ceph_mutex.h"
#include "common/config_proxy.h"
#include "common/config_cacher.h"

class CephContext;

class AuthRegistry : public md_config_obs_t {
  CephContext *cct;
  ceph::mutex lock = ceph::make_mutex("AuthRegistry::lock");

  std::map<int,AuthAuthorizeHandler*> authorize_handlers;

  bool _no_keyring_disabled_cephx = false;

  std::vector<uint32_t> cluster_methods; // CEPH_AUTH_*
  std::vector<uint32_t> service_methods; // CEPH_AUTH_*
  std::vector<uint32_t> client_methods;  // CEPH_AUTH_*

  void _parse_method_list(const string& str, std::vector<uint32_t> *v);
  void _refresh_config();

public:
  AuthRegistry(CephContext *cct) : cct(cct) {
  }
  ~AuthRegistry();

  void refresh_config() {
    std::scoped_lock l(lock);
    _refresh_config();
  }

  void get_supported_methods(int peer_type, std::vector<uint32_t> *v);
  bool is_supported_method(int peer_type, int method);
  bool any_supported_methods(int peer_type);

  AuthAuthorizeHandler *get_handler(int peer_type, int method);

  const char** get_tracked_conf_keys() const override;
  void handle_conf_change(const ConfigProxy& conf,
                          const std::set<std::string>& changed) override;

  bool no_keyring_disabled_cephx() {
    std::scoped_lock l(lock);
    return _no_keyring_disabled_cephx;
  }
};
