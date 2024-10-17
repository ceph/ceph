// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <type_traits>
#include "common/config.h"
#include "common/config_obs.h"
#include "common/config_obs_mgr.h"
#include "common/ceph_mutex.h"
#include "common/config_proxy.h"
#include "common/ceph_context.h"
#include "global/global_context.h"
#include "include/cephfs/libcephfs.h"

// @c ImageConfigProxy is a different version of ConfigProxy specialised 
// for per image configuration in order to skip unneeded copies of md_config_t(~2.5x slower).
namespace librbd {
class ImageConfigProxy {
  /**
   * The current values of all settings described by the schema
   */
  ConfigValues values;
  using md_config_obs_t = ceph::md_config_obs_impl<ImageConfigProxy>;
  ObserverMgr<md_config_obs_t> obs_mgr;
  boost::intrusive_ptr<CephContext> cct;
  /** A lock that protects the md_config_t internals. It is
   * recursive, for simplicity.
   * It is best if this lock comes first in the lock hierarchy. We will
   * hold this lock when calling configuration observers.  */
  mutable ceph::recursive_mutex lock =
    ceph::make_recursive_mutex("ConfigProxy::lock");



public:
  explicit ImageConfigProxy(CephContext* cct) : cct(cct)
  {}
  ImageConfigProxy(ImageConfigProxy &config_proxy)
    : values(config_proxy.get_config_values()), cct(config_proxy.get_ceph_context())
  {}
  ImageConfigProxy(ConfigProxy &config_proxy)
    : values(config_proxy.get_config_values()), cct(g_ceph_context)
  {}
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
    return config().get_val(values, key, buf, len);
  }
  int get_val(const std::string_view key, std::string *val) const {
    std::lock_guard l{lock};
    return config().get_val(values, key, val);
  }
  template<typename T>
  const T get_val(const std::string_view key) const {
    std::lock_guard l{lock};
    return config().template get_val<T>(values, key);
  }
  template<typename T, typename Callback, typename...Args>
  auto with_val(const std::string_view key, Callback&& cb, Args&&... args) const {
    std::lock_guard l{lock};
    return config().template with_val<T>(values, key,
				       std::forward<Callback>(cb),
				       std::forward<Args>(args)...);
  }
  void config_options(ceph::Formatter *f) const {
    config().config_options(f);
  }
  const decltype(md_config_t::schema)& get_schema() const {
    return config().schema;
  }
  const Option* get_schema(const std::string_view key) const {
    auto found = config().schema.find(key);
    if (found == config().schema.end()) {
      return nullptr;
    } else {
      return &found->second;
    }
  }
  const Option *find_option(const std::string& name) const {
    return config().find_option(name);
  }
  void diff(ceph::Formatter *f, const std::string& name = {}) const {
    std::lock_guard l{lock};
    return config().diff(values, f, name);
  }
  std::vector<std::string> get_my_sections() const {
    std::lock_guard l{lock};
    return config().get_my_sections(values);
  }
  int get_all_sections(std::vector<std::string>& sections) const {
    std::lock_guard l{lock};
    return config().get_all_sections(sections);
  }
  int get_val_from_conf_file(const std::vector<std::string>& sections,
			     const std::string_view key, std::string& out,
			     bool emeta) const {
    std::lock_guard l{lock};
    return config().get_val_from_conf_file(values,
					 sections, key, out, emeta);
  }
  unsigned get_osd_pool_default_min_size(uint8_t size) const {
    return config().get_osd_pool_default_min_size(values, size);
  }
  void early_expand_meta(std::string &val,
			 std::ostream *oss) const {
    std::lock_guard l{lock};
    return config().early_expand_meta(values, val, oss);
  }
  void set_safe_to_start_threads() {
    config().set_safe_to_start_threads();
  }
  void _clear_safe_to_start_threads() {
    config()._clear_safe_to_start_threads();
  }
  void show_config(std::ostream& out) {
    std::lock_guard l{lock};
    config().show_config(values, out);
  }
  void show_config(ceph::Formatter *f) {
    std::lock_guard l{lock};
    config().show_config(values, f);
  }
  void config_options(ceph::Formatter *f) {
    std::lock_guard l{lock};
    config().config_options(f);
  }
  int rm_val(const std::string_view key) {
    std::lock_guard l{lock};
    return config().rm_val(values, key);
  }
  int set_val(const std::string_view key, const std::string& s,
              std::stringstream* err_ss=nullptr) {
    std::lock_guard l{lock};
    return config().set_val(values, obs_mgr, key, s, err_ss);
  }
  void set_val_default(const std::string_view key, const std::string& val) {
    std::lock_guard l{lock};
    config().set_val_default(values, obs_mgr, key, val);
  }
  void set_val_or_die(const std::string_view key, const std::string& val) {
    std::lock_guard l{lock};
    config().set_val_or_die(values, obs_mgr, key, val);
  }
  void parse_env(unsigned entity_type,
		 const char *env_var = "CEPH_ARGS") {
    std::lock_guard l{lock};
    config().parse_env(entity_type, values, obs_mgr, env_var);
  }
  int parse_argv(std::vector<const char*>& args, int level=CONF_CMDLINE) {
    std::lock_guard l{lock};
    return config().parse_argv(values, obs_mgr, args, level);
  }
  int parse_config_files(const char *conf_files,
			 std::ostream *warnings, int flags) {
    std::lock_guard l{lock};
    return config().parse_config_files(values, obs_mgr,
				     conf_files, warnings, flags);
  }
  bool has_parse_error() const {
    return !config().parse_error.empty();
  }
  std::string get_parse_error() {
    return config().parse_error;
  }
  void complain_about_parse_error(CephContext *cct) {
    return config().complain_about_parse_error(cct);
  }
  void do_argv_commands() const {
    std::lock_guard l{lock};
    config().do_argv_commands(values);
  }
  void get_config_bl(uint64_t have_version,
		     ceph::buffer::list *bl,
		     uint64_t *got_version) {
    std::lock_guard l{lock};
    config().get_config_bl(values, have_version, bl, got_version);
  }
  void get_defaults_bl(ceph::buffer::list *bl) {
    std::lock_guard l{lock};
    config().get_defaults_bl(values, bl);
  }
  const std::string& get_conf_path() const {
    return config().get_conf_path();
  }
  std::optional<std::string> get_val_default(std::string_view key) {
    return config().get_val_default(key);
  }

  CephContext* get_ceph_context() {
    return cct.get();
  }

private:
  md_config_t &config() const {
    return cct->_conf.get_config();
  }
};

}
