// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "config_proxy.h"

namespace crimson::common {

ConfigProxy::ConfigProxy(const EntityName& name, std::string_view cluster)
{
  if (seastar::this_shard_id() != 0) {
    return;
  }
  // set the initial value on CPU#0
  values.reset(seastar::make_lw_shared<ConfigValues>());
  values.get()->name = name;
  values.get()->cluster = cluster;
  // and the only copy of md_config_impl<> is allocated on CPU#0
  local_config.reset(new md_config_t{*values, obs_mgr, true});
  if (name.is_mds()) {
    local_config->set_val_default(*values, obs_mgr,
				  "keyring", "$mds_data/keyring");
  } else if (name.is_osd()) {
    local_config->set_val_default(*values, obs_mgr,
				  "keyring", "$osd_data/keyring");
  }
}

seastar::future<> ConfigProxy::start()
{
  // populate values and config to all other shards
  if (!values) {
    return seastar::make_ready_future<>();
  }
  return container().invoke_on_others([this](auto& proxy) {
    return values.copy().then([config=local_config.get(),
			       &proxy](auto foreign_values) {
      proxy.values.reset();
      proxy.values = std::move(foreign_values);
      proxy.remote_config = config;
      return seastar::make_ready_future<>();
    });
  });
}

void ConfigProxy::show_config(ceph::Formatter* f) const {
  get_config().show_config(*values, f);
}

ConfigProxy::ShardedConfig ConfigProxy::sharded_conf;
}
