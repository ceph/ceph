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

template <typename ValueT>
class md_config_cacher_t : public md_config_obs_t {
  ConfigProxy& conf;
  const char* const option_name;
  std::atomic<ValueT> value_cache;

  const char** get_tracked_conf_keys() const override {
    const static char* keys[] = { option_name, nullptr };
    return keys;
  }

  void handle_conf_change(const ConfigProxy& conf,
                          const std::set<std::string>& changed) override {
    if (changed.count(option_name)) {
      value_cache.store(conf.get_val<ValueT>(option_name));
    }
  }

public:
  md_config_cacher_t(ConfigProxy& conf,
                     const char* const option_name)
    : conf(conf),
      option_name(option_name) {
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

