// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include "common/config.h"
#include "common/config_obs.h"
#include "common/config_obs_mgr.h"
#include "common/errno.h"

namespace ceph {
class Formatter;
}

namespace ceph::global {
int g_conf_set_val(const std::string& key, const std::string& s);
int g_conf_rm_val(const std::string& key);
}

namespace crimson::common {

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
      std::map<std::string, bool> changes_present;
      for (const auto& change : owner.values->changed) {
        std::string dummy;
        changes_present[change] = owner.get_val(change, &dummy);
      }
      owner.obs_mgr.for_each_change(changes_present,
                                    [&rev_obs](auto obs,
                                               const std::string &key) {
                                      rev_obs[obs].insert(key);
                                    }, nullptr);
      for (auto& [obs, keys] : rev_obs) {
        (*obs)->handle_conf_change(owner, keys);
      }

      return seastar::parallel_for_each(boost::irange(1u, seastar::smp::count),
                                        [&owner, new_values] (auto cpu) {
        return owner.container().invoke_on(cpu,
          [foreign_values = seastar::make_foreign(new_values)](ConfigProxy& proxy) mutable {
            proxy.values.reset();
            proxy.values = std::move(foreign_values);

            std::map<std::string, bool> changes_present;
            for (const auto& change : proxy.values->changed) {
              std::string dummy;
              changes_present[change] = proxy.get_val(change, &dummy);
            }

            ObserverMgr<ConfigObserver>::rev_obs_map rev_obs;
            proxy.obs_mgr.for_each_change(changes_present,
              [&rev_obs](auto obs, const std::string& key) {
                rev_obs[obs].insert(key);
              }, nullptr);
            for (auto& [obs, keys] : rev_obs) {
              (*obs)->handle_conf_change(proxy, keys);
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
  const ConfigValues get_config_values() {
     return *values.get();
  }
  ConfigValues* operator->() noexcept {
    return values.get();
  }

  void get_config_bl(uint64_t have_version,
		     ceph::buffer::list *bl,
		     uint64_t *got_version) {
    get_config().get_config_bl(get_config_values(), have_version,
                               bl, got_version);
  }
  void get_defaults_bl(ceph::buffer::list *bl) {
    get_config().get_defaults_bl(get_config_values(), bl);
  }
  seastar::future<> start();
  // required by sharded<>
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
    ceph::global::g_conf_rm_val(key);
    return do_change([key, this](ConfigValues& values) {
      auto ret = get_config().rm_val(values, key);
      if (ret < 0) {
        throw std::invalid_argument(cpp_strerror(ret));
      }
    });
  }
  seastar::future<> set_val(const std::string& key,
			    const std::string& val) {
    ceph::global::g_conf_set_val(key, val);
    return do_change([key, val, this](ConfigValues& values) {
      std::stringstream err;
      auto ret = get_config().set_val(values, obs_mgr, key, val, &err);
      if (ret < 0) {
	throw std::invalid_argument(err.str());
      }
    });
  }
  int get_val(std::string_view key, std::string *val) const {
    return get_config().get_val(*values, key, val);
  }
  template<typename T>
  const T get_val(std::string_view key) const {
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

  seastar::future<>
  set_mon_vals(const std::map<std::string,std::string,std::less<>>& kv) {
    return do_change([kv, this](ConfigValues& values) {
      get_config().set_mon_vals(nullptr, values, obs_mgr, kv, nullptr);
    });
  }

  seastar::future<> inject_args(const std::string& s) {
    return do_change([s, this](ConfigValues& values) {
      std::stringstream err;
      if (get_config().injectargs(values, obs_mgr, s, &err)) {
        throw std::invalid_argument(err.str());
      }
    });
  }
  void show_config(ceph::Formatter* f) const;

  seastar::future<> parse_argv(std::vector<const char*>& argv) {
    // we could pass whatever is unparsed to seastar, but seastar::app_template
    // is used for driving the seastar application, and
    // crimson::common::ConfigProxy is not available until seastar engine is up
    // and running, so we have to feed the command line args to app_template
    // first, then pass them to ConfigProxy.
    return do_change([&argv, this](ConfigValues& values) {
      get_config().parse_argv(values,
			      obs_mgr,
			      argv,
			      CONF_CMDLINE);
    });
  }

  seastar::future<> parse_env() {
    return do_change([this](ConfigValues& values) {
      get_config().parse_env(CEPH_ENTITY_TYPE_OSD,
			     values,
			     obs_mgr);
    });
  }

  seastar::future<> parse_config_files(const std::string& conf_files);

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

template<typename T>
const T get_conf(const std::string& key) {
  return local_conf().template get_val<T>(key);
}

}
