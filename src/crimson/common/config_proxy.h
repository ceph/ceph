// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include "common/config.h"
#include "common/config_obs.h"
#include "common/config_obs_mgr.h"
#include "common/errno.h"

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

  using ConfigObserver = ceph::md_config_obs_impl<ConfigProxy>;
  ObserverMgr<ConfigObserver> obs_mgr;

  const md_config_t& get_config() const {
    return remote_config ? *remote_config : * local_config;
  }
  md_config_t& get_config() {
    return remote_config ? *remote_config : * local_config;
  }

  // apply changes to all shards
  // @param func a functor which accepts @c "ConfigValues&"
  template<typename Func>
  seastar::future<> do_change(Func&& func) {
    return container().invoke_on(values.get_owner_shard(),
                                 [func = std::move(func)](ConfigProxy& owner) {
      // apply the changes to a copy of the values
      auto new_values = seastar::make_lw_shared(*owner.values);
      new_values->changed.clear();
      func(*new_values);

      // always apply the new settings synchronously on the owner shard, to
      // avoid racings with other do_change() calls in parallel.
      ObserverMgr<ConfigObserver>::rev_obs_map rev_obs;
      owner.values.reset(new_values);
      owner.obs_mgr.for_each_change(owner.values->changed, owner,
                                    [&rev_obs](ConfigObserver *obs,
                                               const std::string &key) {
                                      rev_obs[obs].insert(key);
                                    }, nullptr);
      for (auto& [obs, keys] : rev_obs) {
        obs->handle_conf_change(owner, keys);
      }

      return seastar::parallel_for_each(boost::irange(1u, seastar::smp::count),
                                        [&owner, new_values] (auto cpu) {
        return owner.container().invoke_on(cpu,
          [foreign_values = seastar::make_foreign(new_values)](ConfigProxy& proxy) mutable {
            proxy.values.reset();
            proxy.values = std::move(foreign_values);

            ObserverMgr<ConfigObserver>::rev_obs_map rev_obs;
            proxy.obs_mgr.for_each_change(proxy.values->changed, proxy,
                                          [&rev_obs](md_config_obs_t *obs,
                                                     const std::string &key) {
                                            rev_obs[obs].insert(key);
                                          }, nullptr);
            for (auto& obs_keys : rev_obs) {
              obs_keys.first->handle_conf_change(proxy, obs_keys.second);
            }
          });
        }).finally([new_values] {
          new_values->changed.clear();
        });
      });
  }
public:
  ConfigProxy(const EntityName& name, std::string_view cluster);
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
      auto ret = get_config().rm_val(values, key);
      if (ret < 0) {
        throw std::invalid_argument(cpp_strerror(ret));
      }
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
    });
  }
  int get_val(const std::string &key, std::string *val) const {
    return get_config().get_val(*values, key, val);
  }
  template<typename T>
  const T get_val(const std::string& key) const {
    return get_config().template get_val<T>(*values, key);
  }

  int get_all_sections(std::vector<std::string>& sections) const {
    return get_config().get_all_sections(sections);
  }

  int get_val_from_conf_file(const std::vector<std::string>& sections,
			     const std::string& key, std::string& out,
			     bool expand_meta) const {
    return get_config().get_val_from_conf_file(*values, sections, key,
					       out, expand_meta);
  }

  unsigned get_osd_pool_default_min_size(uint8_t size) const {
    return get_config().get_osd_pool_default_min_size(*values, size);
  }

  seastar::future<> set_mon_vals(const std::map<std::string,std::string>& kv) {
    return do_change([kv, this](ConfigValues& values) {
      get_config().set_mon_vals(nullptr, values, obs_mgr, kv, nullptr);
    });
  }

  seastar::future<> parse_config_files(const std::string& conf_files) {
    return do_change([this, conf_files](ConfigValues& values) {
      const char* conf_file_paths =
	conf_files.empty() ? nullptr : conf_files.c_str();
      get_config().parse_config_files(values,
				      obs_mgr,
				      conf_file_paths,
				      &std::cerr,
				      CODE_ENVIRONMENT_DAEMON);
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
