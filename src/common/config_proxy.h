// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

#include <type_traits>
#include "common/config.h"
#include "common/config_fwd.h"

class ConfigProxy {
  /**
   * The current values of all settings described by the schema
   */
  ConfigValues values;
  md_config_t config;

public:
  explicit ConfigProxy(bool is_daemon)
    : config{values, is_daemon}
  {}
  const ConfigValues* operator->() const noexcept {
    return &values;
  }
  ConfigValues* operator->() noexcept {
    return &values;
  }
  int get_val(const std::string& key, char** buf, int len) const {
    return config.get_val(values, key, buf, len);
  }
  int get_val(const std::string &key, std::string *val) const {
    return config.get_val(values, key, val);
  }
  template<typename T>
  const T get_val(const std::string& key) const {
    return config.template get_val<T>(values, key);
  }
  template<typename T, typename Callback, typename...Args>
  auto with_val(const string& key, Callback&& cb, Args&&... args) const {
    return config.template with_val<T>(values, key,
				       std::forward<Callback>(cb),
				       std::forward<Args>(args)...);
  }
  void config_options(Formatter *f) {
    config.config_options(f);
  }
  const Option* get_schema(const std::string& key) const {
    auto found = config.schema.find(key);
    if (found == config.schema.end()) {
      return nullptr;
    } else {
      return &found->second;
    }
  }
  const Option *find_option(const string& name) const {
    return config.find_option(name);
  }
  void diff(Formatter *f, const std::string& name=string{}) const {
    return config.diff(values, f, name);
  }
  void get_my_sections(std::vector <std::string> &sections) const {
    config.get_my_sections(values, sections);
  }
  int get_all_sections(std::vector<std::string>& sections) const {
    return config.get_all_sections(sections);
  }
  int get_val_from_conf_file(const std::vector<std::string>& sections,
			     const std::string& key, std::string& out,
			     bool emeta) const {
    return config.get_val_from_conf_file(values,
					 sections, key, out, emeta);
  }
  unsigned get_osd_pool_default_min_size() const {
    return config.get_osd_pool_default_min_size(values);
  }
  void early_expand_meta(std::string &val,
			 std::ostream *oss) const {
    return config.early_expand_meta(values, val, oss);
  }
  // change `values` in-place
  void finalize_reexpand_meta() {
    config.finalize_reexpand_meta(values, *this);
  }
  void add_observer(md_config_obs_t* obs) {
    config.add_observer(obs);
  }
  void remove_observer(md_config_obs_t* obs) {
    config.remove_observer(obs);
  }
  void set_safe_to_start_threads() {
    config.set_safe_to_start_threads();
  }
  void _clear_safe_to_start_threads() {
    config._clear_safe_to_start_threads();
  }
  void call_all_observers() {
    config.call_all_observers(*this);
  }
  void show_config(std::ostream& out) {
    config.show_config(values, out);
  }
  void show_config(Formatter *f) {
    config.show_config(values, f);
  }
  void config_options(Formatter *f) {
    config.config_options(f);
  }
  int rm_val(const std::string& key) {
    return config.rm_val(values, key);
  }
  void apply_changes(std::ostream* oss) {
    config.apply_changes(values, *this, oss);
  }
  int set_val(const std::string& key, const std::string& s,
              std::stringstream* err_ss=nullptr) {
    return config.set_val(values, key, s);
  }
  void set_val_default(const std::string& key, const std::string& val) {
    config.set_val_default(values, key, val);
  }
  void set_val_or_die(const std::string& key, const std::string& val) {
    config.set_val_or_die(values, key, val);
  }
  int set_mon_vals(CephContext *cct,
		   const map<std::string,std::string>& kv,
		   md_config_t::config_callback config_cb) {
    config.set_mon_vals(cct, values, *this, kv, config_cb);
  }
  int injectargs(const std::string &s, std::ostream *oss) {
    config.injectargs(values, *this, s, oss);
  }
  void parse_env(const char *env_var = "CEPH_ARGS") {
    config.parse_env(values, env_var);
  }
  int parse_argv(std::vector<const char*>& args, int level=CONF_CMDLINE) {
    return config.parse_argv(values, args, level);
  }
  int parse_config_files(const char *conf_files,
			 std::ostream *warnings, int flags) {
    return config.parse_config_files(values, conf_files, warnings, flags);
  }
  size_t num_parse_errors() const {
    return config.parse_errors.size();
  }
  void complain_about_parse_errors(CephContext *cct) {
    return config.complain_about_parse_errors(cct);
  }
  void do_argv_commands() {
    config.do_argv_commands(values);
  }
  void get_config_bl(uint64_t have_version,
		     bufferlist *bl,
		     uint64_t *got_version) {
    config.get_config_bl(values, have_version, bl, got_version);
  }
  void get_defaults_bl(bufferlist *bl) {
    config.get_defaults_bl(values, bl);
  }
};
