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

#ifndef CEPH_CONFIG_H
#define CEPH_CONFIG_H

#include <map>
#include <boost/container/small_vector.hpp>
#include "common/ConfUtils.h"
#include "common/code_environment.h"
#include "log/SubsystemMap.h"
#include "common/options.h"
#include "common/subsys_types.h"
#include "common/config_tracker.h"
#include "common/config_values.h"

class CephContext;

enum {
  CONF_DEFAULT,
  CONF_MON,
  CONF_FILE,
  CONF_ENV,
  CONF_CMDLINE,
  CONF_OVERRIDE,
  CONF_FINAL
};

extern const char *ceph_conf_level_name(int level);

/** This class represents the current Ceph configuration.
 *
 * For Ceph daemons, this is the daemon configuration.  Log levels, caching
 * settings, btrfs settings, and so forth can all be found here.  For libcephfs
 * and librados users, this is the configuration associated with their context.
 *
 * For information about how this class is loaded from a configuration file,
 * see common/ConfUtils.
 *
 * ACCESS
 *
 * There are 3 ways to read the ceph context-- the old way and two new ways.
 * In the old way, code would simply read the public variables of the
 * configuration, without taking a lock. In the new way #1, code registers a
 * configuration observer which receives callbacks when a value changes. These
 * callbacks take place under the md_config_t lock. Alternatively one can use
 * get_val(const char *name) method to safely get a copy of the value.
 *
 * To prevent serious problems resulting from thread-safety issues, we disallow
 * changing std::string configuration values after
 * md_config_t::safe_to_start_threads becomes true. You can still
 * change integer or floating point values, and the option declared with
 * SAFE_OPTION macro. Notice the latter options can not be read directly
 * (conf->foo), one should use either observers or get_val() method
 * (conf->get_val("foo")).
 *
 * FIXME: really we shouldn't allow changing integer or floating point values
 * while another thread is reading them, either.
 */
struct md_config_t {
public:
  typedef boost::variant<int64_t ConfigValues::*,
                         uint64_t ConfigValues::*,
                         std::string ConfigValues::*,
                         double ConfigValues::*,
                         bool ConfigValues::*,
                         entity_addr_t ConfigValues::*,
			 entity_addrvec_t ConfigValues::*,
                         uuid_d ConfigValues::*> member_ptr_t;

  // For use when intercepting configuration updates
  typedef std::function<bool(
      const std::string &k, const std::string &v)> config_callback;

  /// true if we are a daemon (as per CephContext::code_env)
  const bool is_daemon;

  /*
   * Mapping from legacy config option names to class members
   */
  std::map<std::string, member_ptr_t> legacy_values;

  /**
   * The configuration schema, in the form of Option objects describing
   * possible settings.
   */
  std::map<std::string, const Option&> schema;

  /// values from mon that we failed to set
  std::map<std::string,std::string> ignored_mon_values;

  /// original raw values saved that may need to re-expand at certain time
  mutable std::map<std::string, std::string> may_reexpand_meta;

  /// encoded, cached copy of of values + ignored_mon_values
  bufferlist values_bl;

  /// version for values_bl; increments each time there is a change
  uint64_t values_bl_version = 0;

  /// encoded copy of defaults (map<string,string>)
  bufferlist defaults_bl;

  typedef enum {
    OPT_INT, OPT_LONGLONG, OPT_STR, OPT_DOUBLE, OPT_FLOAT, OPT_BOOL,
    OPT_ADDR, OPT_ADDRVEC, OPT_U32, OPT_U64, OPT_UUID
  } opt_type_t;

  // Create a new md_config_t structure.
  explicit md_config_t(ConfigValues& values,
		       const ConfigTracker& tracker,
		       bool is_daemon=false);
  ~md_config_t();

  // Parse a config file
  int parse_config_files(ConfigValues& values, const ConfigTracker& tracker,
			 const char *conf_files,
			 std::ostream *warnings, int flags);

  // Absorb config settings from the environment
  void parse_env(ConfigValues& values, const ConfigTracker& tracker,
		 const char *env_var = "CEPH_ARGS");

  // Absorb config settings from argv
  int parse_argv(ConfigValues& values, const ConfigTracker& tracker,
		 std::vector<const char*>& args, int level=CONF_CMDLINE);

  // do any commands we got from argv (--show-config, --show-config-val)
  void do_argv_commands(const ConfigValues& values) const;

  bool _internal_field(const string& k);

  void set_safe_to_start_threads();
  void _clear_safe_to_start_threads();  // this is only used by the unit test

  /// Look up an option in the schema
  const Option *find_option(const string& name) const;

  /// Set a default value
  void set_val_default(ConfigValues& values,
		       const ConfigTracker& tracker,
		       const std::string& key, const std::string &val);

  /// Set a values from mon
  int set_mon_vals(CephContext *cct,
      ConfigValues& values,
      const ConfigTracker& tracker,
      const map<std::string,std::string>& kv,
      config_callback config_cb);

  // Called by the Ceph daemons to make configuration changes at runtime
  int injectargs(ConfigValues& values,
		 const ConfigTracker& tracker,
		 const std::string &s,
		 std::ostream *oss);

  // Set a configuration value, or crash
  // Metavariables will be expanded.
  void set_val_or_die(ConfigValues& values, const ConfigTracker& tracker,
		      const std::string &key, const std::string &val);

  // Set a configuration value.
  // Metavariables will be expanded.
  int set_val(ConfigValues& values, const ConfigTracker& tracker,
	      const std::string &key, const char *val,
              std::stringstream *err_ss=nullptr);
  int set_val(ConfigValues& values, const ConfigTracker& tracker,
	      const std::string &key, const string& s,
              std::stringstream *err_ss=nullptr) {
    return set_val(values, tracker, key, s.c_str(), err_ss);
  }

  /// clear override value
  int rm_val(ConfigValues& values, const std::string& key);

  /// get encoded map<string,map<int32_t,string>> of entire config
  void get_config_bl(const ConfigValues& values,
		     uint64_t have_version,
		     bufferlist *bl,
		     uint64_t *got_version);

  /// get encoded map<string,string> of compiled-in defaults
  void get_defaults_bl(const ConfigValues& values, bufferlist *bl);

  // Get a configuration value.
  // No metavariables will be returned (they will have already been expanded)
  int get_val(const ConfigValues& values, const std::string &key, char **buf, int len) const;
  int get_val(const ConfigValues& values, const std::string &key, std::string *val) const;
  Option::value_t get_val_generic(const ConfigValues& values, const std::string &key) const;
  template<typename T> const T get_val(const ConfigValues& values, const std::string &key) const;
  template<typename T, typename Callback, typename...Args>
  auto with_val(const ConfigValues& values, const string& key,
		Callback&& cb, Args&&... args) const ->
    std::result_of_t<Callback(const T&, Args...)> {
    return std::forward<Callback>(cb)(
      boost::get<T>(this->get_val_generic(values, key)),
      std::forward<Args>(args)...);
  }

  void get_all_keys(std::vector<std::string> *keys) const;

  // Return a list of all the sections that the current entity is a member of.
  void get_my_sections(const ConfigValues& values,
		       std::vector <std::string> &sections) const;

  // Return a list of all sections
  int get_all_sections(std::vector <std::string> &sections) const;

  // Get a value from the configuration file that we read earlier.
  // Metavariables will be expanded if emeta is true.
  int get_val_from_conf_file(const ConfigValues& values,
		   const std::vector <std::string> &sections,
		   std::string const &key, std::string &out, bool emeta) const;

  /// dump all config values to a stream
  void show_config(const ConfigValues& values, std::ostream& out) const;
  /// dump all config values to a formatter
  void show_config(const ConfigValues& values, Formatter *f) const;
  
  /// dump all config settings to a formatter
  void config_options(Formatter *f) const;

  /// dump config diff from default, conf, mon, etc.
  void diff(const ConfigValues& values,
	    Formatter *f,
	    std::string name=string{}) const;

  /// print/log warnings/errors from parsing the config
  void complain_about_parse_errors(CephContext *cct);

private:
  // we use this to avoid variable expansion loops
  typedef boost::container::small_vector<pair<const Option*,
					      const Option::value_t*>,
					 4> expand_stack_t;

  void validate_schema();
  void validate_default_settings();

  int _get_val_cstr(const ConfigValues& values,
		    const std::string &key, char **buf, int len) const;
  Option::value_t _get_val(const ConfigValues& values,
			   const std::string &key,
			   expand_stack_t *stack=0,
			   std::ostream *err=0) const;
  Option::value_t _get_val(const ConfigValues& values,
			   const Option& o,
			   expand_stack_t *stack=0,
			   std::ostream *err=0) const;
  const Option::value_t& _get_val_default(const Option& o) const;
  Option::value_t _get_val_nometa(const ConfigValues& values,
				  const Option& o) const;

  int _rm_val(ConfigValues& values, const std::string& key, int level);

  void _refresh(ConfigValues& values, const Option& opt);

  void _show_config(const ConfigValues& values,
		    std::ostream *out, Formatter *f) const;

  void _get_my_sections(const ConfigValues& values,
			std::vector <std::string> &sections) const;

  int _get_val_from_conf_file(const std::vector <std::string> &sections,
			      const std::string &key, std::string &out) const;

  int parse_option(ConfigValues& values,
		   const ConfigTracker& tracker,
		   std::vector<const char*>& args,
		   std::vector<const char*>::iterator& i,
		   std::ostream *oss,
		   int level);
  int parse_injectargs(ConfigValues& values,
		       const ConfigTracker& tracker,
		       std::vector<const char*>& args,
		       std::ostream *oss);

  // @returns negative number for an error, otherwise a
  //          @c ConfigValues::set_value_result_t is returned.
  int _set_val(
    ConfigValues& values,
    const ConfigTracker& tracker,
    const std::string &val,
    const Option &opt,
    int level,  // CONF_*
    std::string *error_message);

  template <typename T>
  void assign_member(member_ptr_t ptr, const Option::value_t &val);


  void update_legacy_vals(ConfigValues& values);
  void update_legacy_val(ConfigValues& values,
			 const Option &opt,
			 member_ptr_t member);

  Option::value_t _expand_meta(
    const ConfigValues& values,
    const Option::value_t& in,
    const Option *o,
    expand_stack_t *stack,
    std::ostream *err) const;

public:  // for global_init
  void early_expand_meta(const ConfigValues& values,
			 std::string &val,
			 std::ostream *oss) const;

  // for those want to reexpand special meta, e.g, $pid
  bool finalize_reexpand_meta(ConfigValues& values,
			      const ConfigTracker& tracker);
private:

  /// expand all metavariables in config structure.
  void expand_all_meta();

  // The configuration file we read, or NULL if we haven't read one.
  ConfFile cf;
public:
  std::deque<std::string> parse_errors;
private:

  // This will be set to true when it is safe to start threads.
  // Once it is true, it will never change.
  bool safe_to_start_threads = false;

  bool do_show_config = false;
  string do_show_config_value;

  vector<Option> subsys_options;

public:
  string data_dir_option;  ///< data_dir config option, if any

public:
  unsigned get_osd_pool_default_min_size(const ConfigValues& values,
                                         uint8_t size) const {
    uint8_t min_size = get_val<uint64_t>(values, "osd_pool_default_min_size");
    return min_size ? std::min(min_size, size) : (size - size / 2);
  }

  friend class test_md_config_t;
};

template<typename T>
const T md_config_t::get_val(const ConfigValues& values,
			     const std::string &key) const {
  return boost::get<T>(this->get_val_generic(values, key));
}

inline std::ostream& operator<<(std::ostream& o, const boost::blank& ) {
      return o << "INVALID_CONFIG_VALUE";
}

int ceph_resolve_file_search(const std::string& filename_list,
			     std::string& result);

#endif
