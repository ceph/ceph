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
class ConfigProxy {
  static ConfigValues get_config_values(const ConfigProxy &config_proxy) {
    std::lock_guard locker(config_proxy.lock);
    return config_proxy.values;
  }

  /**
   * The current values of all settings described by the schema
   */
  ConfigValues values;
  using md_config_obs_t = ceph::md_config_obs_impl<ConfigProxy>;
  ObserverMgr<md_config_obs_t> obs_mgr;
  md_config_t config;
  /** A lock that protects the md_config_t internals. It is
   * recursive, for simplicity.
   * It is best if this lock comes first in the lock hierarchy. We will
   * hold this lock when calling configuration observers.  */
  mutable ceph::recursive_mutex lock =
    ceph::make_recursive_mutex("ConfigProxy::lock");

  class CallGate {
  private:
    uint32_t call_count = 0;
    ceph::mutex lock;
    ceph::condition_variable cond;
  public:
    CallGate()
      : lock(ceph::make_mutex("call::gate::lock")) {
    }

    void enter() {
      std::lock_guard<ceph::mutex> locker(lock);
      ++call_count;
    }
    void leave() {
      std::lock_guard<ceph::mutex> locker(lock);
      ceph_assert(call_count > 0);
      if (--call_count == 0) {
        cond.notify_all();
      }
    }
    void close() {
      std::unique_lock<ceph::mutex> locker(lock);
      while (call_count != 0) {
        cond.wait(locker);
      }
    }
  };

  void call_gate_enter(md_config_obs_t *obs) {
    auto p = obs_call_gate.find(obs);
    ceph_assert(p != obs_call_gate.end());
    p->second->enter();
  }
  void call_gate_leave(md_config_obs_t *obs) {
    auto p = obs_call_gate.find(obs);
    ceph_assert(p != obs_call_gate.end());
    p->second->leave();
  }
  void call_gate_close(md_config_obs_t *obs) {
    auto p = obs_call_gate.find(obs);
    ceph_assert(p != obs_call_gate.end());
    p->second->close();
  }

  using rev_obs_map_t = ObserverMgr<md_config_obs_t>::rev_obs_map;
  typedef std::unique_ptr<CallGate> CallGateRef;

  std::map<md_config_obs_t*, CallGateRef> obs_call_gate;

  void call_observers(rev_obs_map_t &rev_obs) {
    for (auto& [obs, keys] : rev_obs) {
      obs->handle_conf_change(*this, keys);
      // this can be done outside the lock as call_gate_enter()
      // and remove_observer() are serialized via lock
      call_gate_leave(obs);
    }
  }

  void map_observer_changes(md_config_obs_t *obs, const std::string &key,
                            rev_obs_map_t *rev_obs) {
    ceph_assert(ceph_mutex_is_locked(lock));

    auto [it, new_entry] = rev_obs->emplace(obs, std::set<std::string>{});
    it->second.emplace(key);
    if (new_entry) {
      // this needs to be done under lock as once this lock is
      // dropped (before calling observers) a remove_observer()
      // can sneak in and cause havoc.
      call_gate_enter(obs);
    }
  }

public:
  explicit ConfigProxy(bool is_daemon)
    : config{values, obs_mgr, is_daemon}
  {}
  explicit ConfigProxy(const ConfigProxy &config_proxy)
    : values(get_config_values(config_proxy)),
      config{values, obs_mgr, config_proxy.config.is_daemon}
  {}
  const ConfigValues* operator->() const noexcept {
    return &values;
  }
  ConfigValues* operator->() noexcept {
    return &values;
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
    config.config_options(f);
  }
  const decltype(md_config_t::schema)& get_schema() const {
    return config.schema;
  }
  const Option* get_schema(const std::string_view key) const {
    auto found = config.schema.find(key);
    if (found == config.schema.end()) {
      return nullptr;
    } else {
      return &found->second;
    }
  }
  const Option *find_option(const std::string& name) const {
    return config.find_option(name);
  }
  void diff(ceph::Formatter *f, const std::string& name = {}) const {
    std::lock_guard l{lock};
    return config.diff(values, f, name);
  }
  void get_my_sections(std::vector <std::string> &sections) const {
    std::lock_guard l{lock};
    config.get_my_sections(values, sections);
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
      std::lock_guard l(lock);
      if (config.finalize_reexpand_meta(values, obs_mgr)) {
        _gather_changes(values.changed, &rev_obs, nullptr);
        values.changed.clear();
      }
    }

    call_observers(rev_obs);
  }
  void add_observer(md_config_obs_t* obs) {
    std::lock_guard l(lock);
    obs_mgr.add_observer(obs);
    obs_call_gate.emplace(obs, std::make_unique<CallGate>());
  }
  void remove_observer(md_config_obs_t* obs) {
    std::lock_guard l(lock);
    call_gate_close(obs);
    obs_call_gate.erase(obs);
    obs_mgr.remove_observer(obs);
  }
  void call_all_observers() {
    rev_obs_map_t rev_obs;
    {
      std::lock_guard l(lock);
      obs_mgr.for_each_observer(
        [this, &rev_obs](md_config_obs_t *obs, const std::string &key) {
          map_observer_changes(obs, key, &rev_obs);
        });
    }

    call_observers(rev_obs);
  }
  void set_safe_to_start_threads() {
    config.set_safe_to_start_threads();
  }
  void _clear_safe_to_start_threads() {
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
      std::lock_guard l{lock};
      // apply changes until the cluster name is assigned
      if (!values.cluster.empty()) {
        // meta expands could have modified anything.  Copy it all out again.
        _gather_changes(values.changed, &rev_obs, oss);
        values.changed.clear();
      }
    }

    call_observers(rev_obs);
  }
  void _gather_changes(std::set<std::string> &changes,
                       rev_obs_map_t *rev_obs, std::ostream* oss) {
    obs_mgr.for_each_change(
      changes, *this,
      [this, rev_obs](md_config_obs_t *obs, const std::string &key) {
        map_observer_changes(obs, key, rev_obs);
      }, oss);
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
      std::lock_guard l{lock};
      ret = config.set_mon_vals(cct, values, obs_mgr, kv, config_cb);
      _gather_changes(values.changed, &rev_obs, nullptr);
      values.changed.clear();
    }

    call_observers(rev_obs);
    return ret;
  }
  int injectargs(const std::string &s, std::ostream *oss) {
    int ret;
    rev_obs_map_t rev_obs;
    {
      std::lock_guard l{lock};
      ret = config.injectargs(values, obs_mgr, s, oss);
      _gather_changes(values.changed, &rev_obs, oss);
      values.changed.clear();
    }

    call_observers(rev_obs);
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
  size_t num_parse_errors() const {
    return config.parse_errors.size();
  }
  void complain_about_parse_errors(CephContext *cct) {
    return config.complain_about_parse_errors(cct);
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
};
