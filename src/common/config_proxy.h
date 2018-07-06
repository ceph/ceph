// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

#include <memory>
#include <type_traits>

#include "common/config.h"
#include "common/config_fwd.h"

template<bool is_const>
class ConfigProxyBase {
protected:
  using config_t = std::conditional_t<is_const,
				      const md_config_t,
				      md_config_t>;
  config_t* config;
  ConfigProxyBase(config_t* c)
    : config{c}
  {}
public:
  const ConfigValues* operator->() const noexcept {
    return &config->values;
  }
  int get_val(const std::string& key, char** buf, int len) const {
    return config->get_val(key, buf, len);
  }
  int get_val(const std::string &key, std::string *val) const {
    return config->get_val(key, val);
  }
  template<typename T>
  const T get_val(const std::string& key) const {
    return config->template get_val<T>(key);
  }
  template<typename T, typename Callback, typename...Args>
  auto with_val(const string& key, Callback&& cb, Args&&... args) const {
    return config->template with_val<T>(key, std::forward<Callback>(cb),
					std::forward<Args>(args)...);
  }
  const Option* get_schema(const std::string& key) const {
    auto found = config->schema.find(key);
    if (found == config->schema.end()) {
      return nullptr;
    } else {
      return &found->second;
    }
  }
  const Option *find_option(const string& name) const {
    return config->find_option(name);
  }
  void diff(Formatter *f, const std::string& name=string{}) const {
    return config->diff(f, name);
  }
  void get_my_sections(std::vector <std::string> &sections) const {
    config->get_my_sections(sections);
  }
  int get_all_sections(std::vector<std::string>& sections) const {
    return config->get_all_sections(sections);
  }
  int get_val_from_conf_file(const std::vector<std::string>& sections,
			     const std::string& key, std::string& out,
			     bool emeta) const {
    return config->get_val_from_conf_file(sections, key, out, emeta);
  }
  unsigned get_osd_pool_default_min_size() const {
    return config->get_osd_pool_default_min_size();
  }
  void early_expand_meta(std::string &val,
			 std::ostream *oss) const {
    return config->early_expand_meta(val, oss);
  }
};

class ConfigReader final : public ConfigProxyBase<true> {
public:
  explicit ConfigReader(const md_config_t* config)
    : ConfigProxyBase<true>{config}
  {}
};

class ConfigProxy final : public ConfigProxyBase<false> {
  std::unique_ptr<md_config_t> conf;
public:
  explicit ConfigProxy(bool is_daemon)
    : ConfigProxyBase{nullptr},
      conf{std::make_unique<md_config_t>(is_daemon)}
  {
    config = conf.get();
  }
  void add_observer(md_config_obs_t* obs) {
    config->add_observer(obs);
  }
  void remove_observer(md_config_obs_t* obs) {
    config->remove_observer(obs);
  }
  void set_safe_to_start_threads() {
    config->set_safe_to_start_threads();
  }
  void _clear_safe_to_start_threads() {
    config->_clear_safe_to_start_threads();
  }
  void call_all_observers() {
    config->call_all_observers();
  }
  void show_config(std::ostream& out) {
    config->show_config(out);
  }
  void show_config(Formatter *f) {
    config->show_config(f);
  }
  void config_options(Formatter *f) {
    config->config_options(f);
  }
  int rm_val(const std::string& key) {
    return config->rm_val(key);
  }
  void apply_changes(std::ostream* oss) {
    config->apply_changes(oss);
  }
  int set_val(const std::string& key, const string& s,
              std::stringstream* err_ss=nullptr) {
    return config->set_val(key, s, err_ss);
  }
  void set_val_default(const std::string& key, const std::string& val) {
    config->set_val_default(key, val);
  }
  void set_val_or_die(const std::string& key, const std::string& val) {
    config->set_val_or_die(key, val);
  }
  int set_mon_vals(CephContext *cct,
		   const map<std::string,std::string>& kv,
		   md_config_t::config_callback config_cb) {
    return config->set_mon_vals(cct, kv, config_cb);
  }
  int injectargs(const std::string &s, std::ostream *oss) {
    return config->injectargs(s, oss);
  }
  void parse_env(const char *env_var = "CEPH_ARGS") {
    config->parse_env(env_var);
  }
  int parse_argv(std::vector<const char*>& args, int level=CONF_CMDLINE) {
    return config->parse_argv(args, level);
  }
  int parse_config_files(const char *conf_files,
			 std::ostream *warnings, int flags) {
    return config->parse_config_files(conf_files, warnings, flags);
  }
  void complain_about_parse_errors(CephContext *cct) {
    return config->complain_about_parse_errors(cct);
  }
  void do_argv_commands() {
    config->do_argv_commands();
  }
  void get_config_bl(uint64_t have_version,
		     bufferlist *bl,
		     uint64_t *got_version) {
    config->get_config_bl(have_version, bl, got_version);
  }
  void get_defaults_bl(bufferlist *bl) {
    config->get_defaults_bl(bl);
  }
};
