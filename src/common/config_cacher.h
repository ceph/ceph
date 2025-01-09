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
 * Points to note:
 * - as get_tracked_conf_keys() must return a pointer to a null-terminated
 *   array of C-strings, 'keys' - an array - is used to hold the sole key
 *   that this observer is interested in.
 * - the const cast should be removed once we change the
 *   get_tracked_conf_keys() to return const char* const * (or something
 *   similar).
 */
template <typename ValueT>
class md_config_cacher_t : public md_config_obs_t {
  ConfigProxy& conf;
  const char* keys[2];
  std::atomic<ValueT> value_cache;

  const char** get_tracked_conf_keys() const override {
    return const_cast<const char**>(keys);
  }

  void handle_conf_change(const ConfigProxy& conf,
                          const std::set<std::string>& changed) override {
    if (changed.count(keys[0])) {
      value_cache.store(conf.get_val<ValueT>(keys[0]));
    }
  }

public:
  md_config_cacher_t(ConfigProxy& conf,
                     const char* const option_name)
    : conf(conf),
      keys{option_name, nullptr} {
    conf.add_observer(this);
    std::atomic_init(&value_cache,
                     conf.get_val<ValueT>(keys[0]));
  }

  ~md_config_cacher_t() {
    conf.remove_observer(this);
  }

  operator ValueT() const {
    return value_cache.load();
  }
};

#endif // CEPH_CONFIG_CACHER_H

