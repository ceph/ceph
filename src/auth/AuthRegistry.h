// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <vector>

#include "AuthAuthorizeHandler.h"
#include "AuthMethodList.h"
#include "common/ceph_mutex.h"
#include "common/ceph_context.h"
#include "common/config_cacher.h"

class AuthRegistry : public md_config_obs_t {
  CephContext *cct;
  mutable ceph::mutex lock = ceph::make_mutex("AuthRegistry::lock");

  std::map<int,AuthAuthorizeHandler*> authorize_handlers;

  bool _no_keyring_disabled_cephx = false;

  // CEPH_AUTH_*
  std::vector<uint32_t> cluster_methods;
  std::vector<uint32_t> service_methods;
  std::vector<uint32_t> client_methods;

  // CEPH_CON_MODE_*
  std::vector<uint32_t> mon_cluster_modes;
  std::vector<uint32_t> mon_service_modes;
  std::vector<uint32_t> mon_client_modes;
  std::vector<uint32_t> cluster_modes;
  std::vector<uint32_t> service_modes;
  std::vector<uint32_t> client_modes;

  void _parse_method_list(const std::string& str, std::vector<uint32_t> *v);
  void _parse_mode_list(const std::string& str, std::vector<uint32_t> *v);
  void _refresh_config();

public:
  AuthRegistry(CephContext *cct);
  ~AuthRegistry();

  void refresh_config() {
    std::scoped_lock l(lock);
    _refresh_config();
  }

  void get_supported_methods(int peer_type,
			     std::vector<uint32_t> *methods,
			     std::vector<uint32_t> *modes=nullptr) const;
  bool is_supported_method(int peer_type, int method) const;
  bool any_supported_methods(int peer_type) const;

  void get_supported_modes(int peer_type,
			   uint32_t auth_method,
			   std::vector<uint32_t> *modes) const;

  uint32_t pick_mode(int peer_type,
		     uint32_t auth_method,
		     const std::vector<uint32_t>& preferred_modes);

  static bool is_secure_method(uint32_t method) {
    return (method == CEPH_AUTH_CEPHX);
  }

  static bool is_secure_mode(uint32_t mode) {
    return (mode == CEPH_CON_MODE_SECURE);
  }

  AuthAuthorizeHandler *get_handler(int peer_type, int method);

  std::vector<std::string> get_tracked_keys() const noexcept override;
  void handle_conf_change(const ConfigProxy& conf,
                          const std::set<std::string>& changed) override;

  bool no_keyring_disabled_cephx() {
    std::scoped_lock l(lock);
    return _no_keyring_disabled_cephx;
  }
};
