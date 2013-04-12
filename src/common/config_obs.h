// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_CONFIG_OBS_H
#define CEPH_CONFIG_OBS_H

#include <set>
#include <string>

struct md_config_t;

class md_config_obs_t {
public:
  virtual ~md_config_obs_t();
  virtual const char** get_tracked_conf_keys() const = 0;
  virtual void handle_conf_change(const struct md_config_t *conf,
				  const std::set <std::string> &changed) = 0;
  virtual void handle_subsys_change(const struct md_config_t *conf,
				    const std::set<int>& changed) { }
};

#endif
