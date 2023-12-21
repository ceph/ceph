// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

#include <type_traits>
#include "common/config.h"
#include "common/config_obs.h"
#include "common/config_obs_mgr.h"
#include "common/ceph_mutex.h"

// @c ConfigProxy is a facade of multiple config related classes. it exposes
// the legacy settings with arrow operator, and the new-style config with its
// member methods.
namespace ceph::common {
class ConfigProxy {
  /**
   * The current values of all settings described by the schema
   */
  ConfigValues values;
  using md_config_obs_t = ceph::md_config_obs_impl<ConfigProxy>;
  using ObsMgr = ObserverMgr<md_config_obs_t>;
  ObsMgr obs_mgr;
  md_config_t config;
  /** A lock that protects the md_config_t internals. It is
   * recursive, for simplicity.
   * It is best if this lock comes first in the lock hierarchy. We will
   * hold this lock when calling configuration observers.  */
  mutable ceph::mutex lock = ceph::make_mutex("ConfigProxy::lock");
  ceph::condition_variable cond;

  using rev_obs_map_t = ObsMgr::rev_obs_map;

  void _call_observers(rev_obs_map_t& rev_obs) {
    ceph_assert(!ceph::mutex_debugging || !ceph_mutex_is_locked_by_me(lock));
    for (auto& [obs, keys] : rev_obs) {
      (*obs)->handle_conf_change(*this, keys);
    }
    rev_obs.clear(); // drop shared_ptrs
    {
      std::lock_guard l{lock};
      cond.notify_all();
    }
  }
  void _gather_changes(std::set<std::string> &changes,
                       rev_obs_map_t *rev_obs, std::ostream* oss) {
    ceph_assert(ceph_mutex_is_locked_by_me(lock));
    std::map<std::string,bool> changes_present;
    for (auto& change : changes) {
      std::string dummy;
      changes_present[change] = (0 == config.get_val(values, change, &dummy));
    }
    obs_mgr.for_each_change(
      changes_present,
      [this, rev_obs](auto obs, const std::string &key) {
        _map_observer_changes(obs, key, rev_obs);
      }, oss);
    changes.clear();
  }

  void _map_observer_changes(ObsMgr::config_obs_ptr obs, const std::string& key,
                            rev_obs_map_t *rev_obs) {
    ceph_assert(ceph_mutex_is_locked_by_me(lock));

    auto [it, new_entry] = rev_obs->emplace(obs, std::set<std::string>{});
    it->second.emplace(key);
  }

public:
  explicit ConfigProxy(bool is_daemon)
    : config{values, obs_mgr, is_daemon}
  {}
  ConfigProxy(const ConfigProxy &config_proxy)
    : values(config_proxy.get_config_values()),
      config{values, obs_mgr, config_proxy.config.is_daemon}
  {}
  const ConfigValues* operator->() const noexcept {
    return &values;
  }
  ConfigValues* operator->() noexcept {
    return &values;
  }
  ConfigValues get_config_values() const {
    std::lock_guard l{lock};
    return values;
  }
  void set_config_values(const ConfigValues& val) {
#ifndef WITH_SEASTAR
    std::lock_guard l{lock};
#endif
    values = val;
  }
  int get_val(const std::string_view key, char** buf, int len) const {
    std::lock_guard l{lock};
    return config.get_val(values, key, buf, len);
  }
  int get_val(const std::string_view key, std::string *val) const {
    std::lock_guard l{lock};
    return config.get_val(values, key, val);
  }
  template<typename T>
  const T get_val(const std::string_view key) const {
    std::lock_guard l{lock};
    return config.template get_val<T>(values, key);
  }
  template<typename T, typename Callback, typename...Args>
  auto with_val(const std::string_view key, Callback&& cb, Args&&... args) const {
    std::lock_guard l{lock};
    return config.template with_val<T>(values, key,
				       std::forward<Callback>(cb),
				       std::forward<Args>(args)...);
  }
  void config_options(ceph::Formatter *f) const {
    std::lock_guard l{lock};
    config.config_options(f);
  }
  const decltype(md_config_t::schema)& get_schema() const {
    std::lock_guard l{lock};
    return config.schema;
  }
  const Option* get_schema(const std::string_view key) const {
    std::lock_guard l{lock};
    auto found = config.schema.find(key);
    if (found == config.schema.end()) {
      return nullptr;
    } else {
      return &found->second;
    }
  }
  const Option *find_option(const std::string& name) const {
    std::lock_guard l{lock};
    return config.find_option(name);
  }
  void diff(ceph::Formatter *f, const std::string& name = {}) const {
    std::lock_guard l{lock};
    return config.diff(values, f, name);
  }
  std::vector<std::string> get_my_sections() const {
    std::lock_guard l{lock};
    return config.get_my_sections(values);
  }
  int get_all_sections(std::vector<std::string>& sections) const {
    std::lock_guard l{lock};
    return config.get_all_sections(sections);
  }
  int get_val_from_conf_file(const std::vector<std::string>& sections,
			     const std::string_view key, std::string& out,
			     bool emeta) const {
    std::lock_guard l{lock};
    return config.get_val_from_conf_file(values,
					 sections, key, out, emeta);
  }
  unsigned get_osd_pool_default_min_size(uint8_t size) const {
    std::lock_guard l{lock};
    return config.get_osd_pool_default_min_size(values, size);
  }
  void early_expand_meta(std::string &val,
			 std::ostream *oss) const {
    std::lock_guard l{lock};
    return config.early_expand_meta(values, val, oss);
  }
  // for those want to reexpand special meta, e.g, $pid
  void finalize_reexpand_meta() {
    rev_obs_map_t rev_obs;
    {
      std::lock_guard locker(lock);
      if (config.finalize_reexpand_meta(values, obs_mgr)) {
        _gather_changes(values.changed, &rev_obs, nullptr);
      }
    }

    _call_observers(rev_obs);
  }
  void add_observer(md_config_obs_t* obs) {
    std::lock_guard l(lock);
    obs_mgr.add_observer(obs);
    cond.notify_all();
  }
  void remove_observer(md_config_obs_t* obs) {
    std::unique_lock l(lock);
    auto wptr = obs_mgr.remove_observer(obs);
    while (!wptr.expired()) {
      cond.wait(l);
    }
  }
  void call_all_observers() {
    rev_obs_map_t rev_obs;
    {
      std::lock_guard locker(lock);
      obs_mgr.for_each_observer(
        [this, &rev_obs](auto obs, const std::string& key) {
          _map_observer_changes(obs, key, &rev_obs);
        });
    }

    _call_observers(rev_obs);
  }
  void set_safe_to_start_threads() {
    std::lock_guard l(lock);
    config.set_safe_to_start_threads();
  }
  void _clear_safe_to_start_threads() {
    std::lock_guard l(lock);
    config._clear_safe_to_start_threads();
  }
  void show_config(std::ostream& out) {
    std::lock_guard l{lock};
    config.show_config(values, out);
  }
  void show_config(ceph::Formatter *f) {
    std::lock_guard l{lock};
    config.show_config(values, f);
  }
  void config_options(ceph::Formatter *f) {
    std::lock_guard l{lock};
    config.config_options(f);
  }
  int rm_val(const std::string_view key) {
    std::lock_guard l{lock};
    return config.rm_val(values, key);
  }
  // Expand all metavariables. Make any pending observer callbacks.
  void apply_changes(std::ostream* oss) {
    rev_obs_map_t rev_obs;

    {
      std::lock_guard locker(lock);
      // apply changes until the cluster name is assigned
      if (!values.cluster.empty()) {
        // meta expands could have modified anything.  Copy it all out again.
        _gather_changes(values.changed, &rev_obs, oss);
      }
    }

    _call_observers(rev_obs);
  }
  int set_val(const std::string_view key, const std::string& s,
              std::stringstream* err_ss=nullptr) {
    std::lock_guard l{lock};
    return config.set_val(values, obs_mgr, key, s, err_ss);
  }
  void set_val_default(const std::string_view key, const std::string& val) {
    std::lock_guard l{lock};
    config.set_val_default(values, obs_mgr, key, val);
  }
  void set_val_or_die(const std::string_view key, const std::string& val) {
    std::lock_guard l{lock};
    config.set_val_or_die(values, obs_mgr, key, val);
  }
  int set_mon_vals(CephContext *cct,
		   const std::map<std::string,std::string,std::less<>>& kv,
		   md_config_t::config_callback config_cb) {
    int ret;
    rev_obs_map_t rev_obs;

    {
      std::lock_guard locker(lock);
      ret = config.set_mon_vals(cct, values, obs_mgr, kv, config_cb);
      _gather_changes(values.changed, &rev_obs, nullptr);
    }

    _call_observers(rev_obs);
    return ret;
  }
  int injectargs(const std::string &s, std::ostream *oss) {
    int ret;
    rev_obs_map_t rev_obs;
    {
      std::lock_guard locker(lock);
      ret = config.injectargs(values, obs_mgr, s, oss);
      _gather_changes(values.changed, &rev_obs, oss);
    }
    _call_observers(rev_obs);
    return ret;
  }
  void parse_env(unsigned entity_type,
		 const char *env_var = "CEPH_ARGS") {
    std::lock_guard l{lock};
    config.parse_env(entity_type, values, obs_mgr, env_var);
  }
  int parse_argv(std::vector<const char*>& args, int level=CONF_CMDLINE) {
    std::lock_guard l{lock};
    return config.parse_argv(values, obs_mgr, args, level);
  }
  int parse_config_files(const char *conf_files,
			 std::ostream *warnings, int flags) {
    std::lock_guard l{lock};
    return config.parse_config_files(values, obs_mgr,
				     conf_files, warnings, flags);
  }
  bool has_parse_error() const {
    std::lock_guard l(lock);
    return !config.parse_error.empty();
  }
  std::string get_parse_error() {
    std::lock_guard l(lock);
    return config.parse_error;
  }
  void complain_about_parse_error(CephContext *cct) {
    std::lock_guard l(lock);
    return config.complain_about_parse_error(cct);
  }
  void do_argv_commands() const {
    std::lock_guard l{lock};
    config.do_argv_commands(values);
  }
  void get_config_bl(uint64_t have_version,
		     ceph::buffer::list *bl,
		     uint64_t *got_version) {
    std::lock_guard l{lock};
    config.get_config_bl(values, have_version, bl, got_version);
  }
  void get_defaults_bl(ceph::buffer::list *bl) {
    std::lock_guard l{lock};
    config.get_defaults_bl(values, bl);
  }
  const std::string& get_conf_path() const {
    std::lock_guard l(lock);
    return config.get_conf_path();
  }
  std::optional<std::string> get_val_default(std::string_view key) {
    std::lock_guard l(lock);
    return config.get_val_default(key);
  }
};

}
