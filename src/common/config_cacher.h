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

#ifndef CEPH_CONFIG_CACHER_H
#define CEPH_CONFIG_CACHER_H

#include "common/config_obs.h"
#include "common/config.h"

/**
 * A simple class to cache a single configuration value.
 *
 * The md_config_cacher_t object registers itself to receive
 * notifications of changes to the specified single configuration
 * option.  When the option changes, the new value is stored
 * in an atomic variable.  The value can be accessed using
 * the dereference operator.
 */
template <typename ValueT>
class md_config_cacher_t : public md_config_obs_t {
  ConfigProxy& conf;
  const std::string option_name;
  std::atomic<ValueT> value_cache;

  std::vector<std::string> get_tracked_keys() const noexcept override {
    return std::vector<std::string>{option_name};
  }

  void handle_conf_change(const ConfigProxy& conf,
                          const std::set<std::string>& changed) override {
    if (changed.contains(option_name)) {
      value_cache.store(conf.get_val<ValueT>(option_name));
    }
  }

public:
  md_config_cacher_t(ConfigProxy& conf,
                     const char* const option_name)
    : conf(conf)
    , option_name{option_name} {
    conf.add_observer(this);
    std::atomic_init(&value_cache,
                     conf.get_val<ValueT>(option_name));
  }

  ~md_config_cacher_t() {
    conf.remove_observer(this);
  }

  ValueT operator*() const {
    return value_cache.load();
  }
};

#endif // CEPH_CONFIG_CACHER_H

