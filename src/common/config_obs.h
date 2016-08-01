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

/** @brief Base class for configuration observers.
 * Use this as a base class for your object if it has to respond to configuration changes,
 * for example by updating some values or modifying its behavior.
 * Subscribe for configuration changes by calling the md_config_t::add_observer() method
 * and unsubscribe using md_config_t::remove_observer().
 */
class md_config_obs_t {
public:
  virtual ~md_config_obs_t() {}
  /** @brief Get a table of strings specifying the configuration keys in which the object is interested.
   * This is called when the object is subscribed to configuration changes with add_observer().
   * The returned table should not be freed until the observer is removed with remove_observer().
   * Note that it is not possible to change the set of tracked keys without re-subscribing. */
  virtual const char** get_tracked_conf_keys() const = 0;
  /// React to a configuration change.
  virtual void handle_conf_change(const struct md_config_t *conf,
				  const std::set <std::string> &changed) = 0;
  /// Unused for now
  virtual void handle_subsys_change(const struct md_config_t *conf,
				    const std::set<int>& changed) { }
};

#endif
