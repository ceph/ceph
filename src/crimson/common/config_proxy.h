// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include "common/config.h"
#include "common/config_obs.h"
#include "common/config_obs_mgr.h"

namespace ceph::common {

// a facade for managing config. each shard has its own copy of ConfigProxy.
//
// In seastar-osd, there could be multiple instances of @c ConfigValues in a
// single process, as we are using a variant of read-copy-update mechinary to
// update the settings at runtime.
class ConfigProxy : public seastar::peering_sharded_service<ConfigProxy>
{
  using LocalConfigValues = seastar::lw_shared_ptr<ConfigValues>;
  seastar::foreign_ptr<LocalConfigValues> values;

  md_config_t* remote_config = nullptr;
  std::unique_ptr<md_config_t> local_config;

  using ConfigObserver = ceph::internal::md_config_obs_impl<ConfigProxy>;
  ObserverMgr<ConfigObserver> obs_mgr;

  const md_config_t& get_config() const {
    return remote_config ? *remote_config : * local_config;
  }
  md_config_t& get_config() {
    return remote_config ? *remote_config : * local_config;
  }

  // apply changes to all shards
  // @param func a functor which accepts @c "ConfigValues&", and returns
  //             a boolean indicating if the values is changed or not
  template<typename Func>
  seastar::future<> do_change(Func&& func) {
    auto new_values = seastar::make_lw_shared(*values);
    func(*new_values);
    return seastar::do_with(seastar::make_foreign(new_values),
			    [this](auto& foreign_values) {
      return container().invoke_on_all([&foreign_values](ConfigProxy& proxy) {
        return foreign_values.copy().then([&proxy](auto foreign_values) {
	  proxy.values.reset();
	  proxy.values = std::move(foreign_values);
	  proxy.obs_mgr.apply_changes(proxy.values->changed,
				      proxy, nullptr);
        });
      });
    }).then([this] {
      values->changed.clear();
    });
  }
public:
  ConfigProxy();
  const ConfigValues* operator->() const noexcept {
    return values.get();
  }
  ConfigValues* operator->() noexcept {
    return values.get();
  }

  // required by sharded<>
  seastar::future<> start();
  seastar::future<> stop() {
    return seastar::make_ready_future<>();
  }
  void add_observer(ConfigObserver* obs) {
    obs_mgr.add_observer(obs);
  }
  void remove_observer(ConfigObserver* obs) {
    obs_mgr.remove_observer(obs);
  }
  seastar::future<> rm_val(const std::string& key) {
    return do_change([key, this](ConfigValues& values) {
      return get_config().rm_val(values, key) >= 0;
    });
  }
  seastar::future<> set_val(const std::string& key,
			    const std::string& val) {
    return do_change([key, val, this](ConfigValues& values) {
      std::stringstream err;
      auto ret = get_config().set_val(values, obs_mgr, key, val, &err);
      if (ret < 0) {
	throw std::invalid_argument(err.str());
      }
      return ret > 0;
    });
  }
  int get_val(const std::string &key, std::string *val) const {
    return get_config().get_val(*values, key, val);
  }
  template<typename T>
  const T get_val(const std::string& key) const {
    return get_config().template get_val<T>(*values, key);
  }

  seastar::future<> set_mon_vals(const std::map<std::string,std::string>& kv) {
    return do_change([kv, this](ConfigValues& values) {
	auto ret = get_config().set_mon_vals(nullptr, values, obs_mgr,
					     kv, nullptr);
      return ret > 0;
    });
  }

  using ShardedConfig = seastar::sharded<ConfigProxy>;

private:
  static ShardedConfig sharded_conf;
  friend ConfigProxy& local_conf();
  friend ShardedConfig& sharded_conf();
};

inline ConfigProxy& local_conf() {
  return ConfigProxy::sharded_conf.local();
}

inline ConfigProxy::ShardedConfig& sharded_conf() {
  return ConfigProxy::sharded_conf;
}

}
