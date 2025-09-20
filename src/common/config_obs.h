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
#include <vector>
#include <string>

#include "common/config_fwd.h"

namespace ceph {
/** @brief Base class for configuration observers.
 * Use this as a base class for your object if it has to respond to configuration changes,
 * for example by updating some values or modifying its behavior.
 * Subscribe for configuration changes by calling the md_config_t::add_observer() method
 * and unsubscribe using md_config_t::remove_observer().
 */
template<class ConfigProxy>
class md_config_obs_impl {
public:
  virtual ~md_config_obs_impl() {}

  /**
   * Returns a vector of strings specifying the configuration keys in which
   * the object is interested. This is called when the object is subscribed to
   * configuration changes with add_observer().
   *
   * Note - the strings in the returned vector are 'moveable'. The caller
   * (ostensibly the observer manager) is expected to move them into its
   * map.
   */
  virtual std::vector<std::string> get_tracked_keys() const noexcept = 0;

  /// React to a configuration change.
  virtual void handle_conf_change(const ConfigProxy& conf,
				  const std::set <std::string> &changed) = 0;
};
}

using md_config_obs_t = ceph::md_config_obs_impl<ConfigProxy>;

#endif
