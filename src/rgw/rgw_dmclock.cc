// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "common/ceph_context.h"
#include "common/config.h"
#include "rgw_dmclock.h"

namespace rgw::dmclock {

ClientConfig::ClientConfig(CephContext *cct)
{
  update(cct->_conf);
}

ClientInfo* ClientConfig::operator()(client_id client)
{
  return &clients[static_cast<size_t>(client)];
}

const char** ClientConfig::get_tracked_conf_keys() const
{
  static const char* keys[] = {
    "rgw_dmclock_admin_res",
    "rgw_dmclock_admin_wgt",
    "rgw_dmclock_admin_lim",
    "rgw_dmclock_auth_res",
    "rgw_dmclock_auth_wgt",
    "rgw_dmclock_auth_lim",
    "rgw_dmclock_data_res",
    "rgw_dmclock_data_wgt",
    "rgw_dmclock_data_lim",
    "rgw_dmclock_metadata_res",
    "rgw_dmclock_metadata_wgt",
    "rgw_dmclock_metadata_lim",
    nullptr
  };
  return keys;
}

void ClientConfig::update(const md_config_t *conf)
{
  clients.clear();
  static_assert(0 == static_cast<int>(client_id::admin));
  clients.emplace_back(conf->get_val<double>("rgw_dmclock_admin_res"),
                       conf->get_val<double>("rgw_dmclock_admin_wgt"),
                       conf->get_val<double>("rgw_dmclock_admin_lim"));
  static_assert(1 == static_cast<int>(client_id::auth));
  clients.emplace_back(conf->get_val<double>("rgw_dmclock_auth_res"),
                       conf->get_val<double>("rgw_dmclock_auth_wgt"),
                       conf->get_val<double>("rgw_dmclock_auth_lim"));
  static_assert(2 == static_cast<int>(client_id::data));
  clients.emplace_back(conf->get_val<double>("rgw_dmclock_data_res"),
                       conf->get_val<double>("rgw_dmclock_data_wgt"),
                       conf->get_val<double>("rgw_dmclock_data_lim"));
  static_assert(3 == static_cast<int>(client_id::metadata));
  clients.emplace_back(conf->get_val<double>("rgw_dmclock_metadata_res"),
                       conf->get_val<double>("rgw_dmclock_metadata_wgt"),
                       conf->get_val<double>("rgw_dmclock_metadata_lim"));
}

void ClientConfig::handle_conf_change(const md_config_t *conf,
                                      const std::set<std::string>& changed)
{
  update(conf);
}

} // namespace rgw::dmclock
