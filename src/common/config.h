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
#include "common/entity_name.h"
#include "common/code_environment.h"
#include "common/Mutex.h"
#include "log/SubsystemMap.h"
#include "common/config_obs.h"
#include "common/options.h"
#include "common/subsys_types.h"

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
 * configuration obserever which receives callbacks when a value changes. These
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
  typedef boost::variant<int64_t md_config_t::*,
                         uint64_t md_config_t::*,
                         std::string md_config_t::*,
                         double md_config_t::*,
                         bool md_config_t::*,
                         entity_addr_t md_config_t::*,
                         uuid_d md_config_t::*> member_ptr_t;
  /// true if we are a daemon (as per CephContext::code_env)
  const bool is_daemon;

  /* Maps configuration options to the observer listening for them. */
  typedef std::multimap <std::string, md_config_obs_t*> obs_map_t;

  /* Set of configuration options that have changed since the last
   * apply_changes */
  typedef std::set < std::string > changed_set_t;

  /*
   * Mapping from legacy config option names to class members
   */
  std::map<std::string, md_config_t::member_ptr_t> legacy_values;

  /**
   * The configuration schema, in the form of Option objects describing
   * possible settings.
   */
  std::map<std::string, const Option&> schema;

  /**
   * The current values of all settings described by the schema
   */
  std::map<std::string, map<int32_t,Option::value_t>> values;

  /// values from mon that we failed to set
  std::map<std::string,std::string> ignored_mon_values;

  /// encoded, cached copy of of values + ignored_mon_values
  bufferlist values_bl;

  /// version for values_bl; increments each time there is a change
  uint64_t values_bl_version = 0;

  /// encoded copy of defaults (map<string,string>)
  bufferlist defaults_bl;

  typedef enum {
    OPT_INT, OPT_LONGLONG, OPT_STR, OPT_DOUBLE, OPT_FLOAT, OPT_BOOL,
    OPT_ADDR, OPT_U32, OPT_U64, OPT_UUID
  } opt_type_t;

  // Create a new md_config_t structure.
  md_config_t(bool is_daemon=false);
  ~md_config_t();

  // Adds a new observer to this configuration. You can do this at any time,
  // but it will only receive notifications for the changes that happen after
  // you attach it, obviously.
  //
  // Most developers will probably attach their observers after global_init,
  // but before anyone can call injectargs.
  //
  // The caller is responsible for allocating observers.
  void add_observer(md_config_obs_t* observer_);

  // Remove an observer from this configuration.
  // This doesn't delete the observer! If you allocated it with new(),
  // you need to delete it yourself.
  // This function will assert if you try to delete an observer that isn't
  // there.
  void remove_observer(md_config_obs_t* observer_);

  // Parse a config file
  int parse_config_files(const char *conf_files,
			 std::ostream *warnings, int flags);

  // Absorb config settings from the environment
  void parse_env(const char *env_var = "CEPH_ARGS");

  // Absorb config settings from argv
  int parse_argv(std::vector<const char*>& args, int level=CONF_CMDLINE);

  // do any commands we got from argv (--show-config, --show-config-val)
  void do_argv_commands();

  // Expand all metavariables. Make any pending observer callbacks.
  void apply_changes(std::ostream *oss);
  void _apply_changes(std::ostream *oss);
  bool _internal_field(const string& k);
  void call_all_observers();

  void set_safe_to_start_threads();
  void _clear_safe_to_start_threads();  // this is only used by the unit test

  /// Look up an option in the schema
  const Option *find_option(const string& name) const;

  /// Set a default value
  void set_val_default(const std::string& key, const std::string &val);

  /// Set a values from mon
  int set_mon_vals(CephContext *cct, const map<std::string,std::string>& kv);

  // Called by the Ceph daemons to make configuration changes at runtime
  int injectargs(const std::string &s, std::ostream *oss);

  // Set a configuration value, or crash
  // Metavariables will be expanded.
  void set_val_or_die(const std::string &key, const std::string &val);

  // Set a configuration value.
  // Metavariables will be expanded.
  int set_val(const std::string &key, const char *val,
              std::stringstream *err_ss=nullptr);
  int set_val(const std::string &key, const string& s,
              std::stringstream *err_ss=nullptr) {
    return set_val(key, s.c_str(), err_ss);
  }

  /// clear override value
  int rm_val(const std::string& key);

  /// get encoded map<string,map<int32_t,string>> of entire config
  void get_config_bl(uint64_t have_version,
		     bufferlist *bl,
		     uint64_t *got_version);

  /// get encoded map<string,string> of compiled-in defaults
  void get_defaults_bl(bufferlist *bl);

  // Get a configuration value.
  // No metavariables will be returned (they will have already been expanded)
  int get_val(const std::string &key, char **buf, int len) const;
  int get_val(const std::string &key, std::string *val) const;
  Option::value_t get_val_generic(const std::string &key) const;
  template<typename T> const T get_val(const std::string &key) const;
  template<typename T, typename Callback, typename...Args>
  auto with_val(const string& key, Callback&& cb, Args&&... args) const ->
    std::result_of_t<Callback(const T&, Args...)> {
    return std::forward<Callback>(cb)(
      boost::get<T>(this->get_val_generic(key)),
      std::forward<Args>(args)...);
  }

  void get_all_keys(std::vector<std::string> *keys) const;

  // Return a list of all the sections that the current entity is a member of.
  void get_my_sections(std::vector <std::string> &sections) const;

  // Return a list of all sections
  int get_all_sections(std::vector <std::string> &sections) const;

  // Get a value from the configuration file that we read earlier.
  // Metavariables will be expanded if emeta is true.
  int get_val_from_conf_file(const std::vector <std::string> &sections,
		   std::string const &key, std::string &out, bool emeta) const;

  /// dump all config values to a stream
  void show_config(std::ostream& out);
  /// dump all config values to a formatter
  void show_config(Formatter *f);
  
  /// dump all config settings to a formatter
  void config_options(Formatter *f);

  /// dump config diff from default, conf, mon, etc.
  void diff(Formatter *f, std::string name=string{}) const;

  /// print/log warnings/errors from parsing the config
  void complain_about_parse_errors(CephContext *cct);

private:
  // we use this to avoid variable expansion loops
  typedef boost::container::small_vector<pair<const Option*,
					      const Option::value_t*>,
					 4> expand_stack_t;

  void validate_schema();
  void validate_default_settings();

  int _get_val_cstr(const std::string &key, char **buf, int len) const;
  Option::value_t _get_val(const std::string &key,
			   expand_stack_t *stack=0,
			   std::ostream *err=0) const;
  Option::value_t _get_val(const Option& o,
			   expand_stack_t *stack=0,
			   std::ostream *err=0) const;
  const Option::value_t& _get_val_default(const Option& o) const;
  Option::value_t _get_val_nometa(const Option& o) const;

  int _rm_val(const std::string& key, int level);

  void _refresh(const Option& opt);

  void _show_config(std::ostream *out, Formatter *f);

  void _get_my_sections(std::vector <std::string> &sections) const;

  int _get_val_from_conf_file(const std::vector <std::string> &sections,
			      const std::string &key, std::string &out) const;

  int parse_option(std::vector<const char*>& args,
		   std::vector<const char*>::iterator& i,
		   std::ostream *oss,
		   int level);
  int parse_injectargs(std::vector<const char*>& args,
		      std::ostream *oss);

  int _set_val(
    const std::string &val,
    const Option &opt,
    int level,  // CONF_*
    std::string *error_message);

  template <typename T>
  void assign_member(member_ptr_t ptr, const Option::value_t &val);


  void update_legacy_vals();
  void update_legacy_val(const Option &opt,
      md_config_t::member_ptr_t member);

  Option::value_t _expand_meta(
    const Option::value_t& in,
    const Option *o,
    expand_stack_t *stack,
    std::ostream *err) const;

public:  // for global_init
  void early_expand_meta(std::string &val,
			 std::ostream *oss) const;
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

  obs_map_t observers;
  changed_set_t changed;

  vector<Option> subsys_options;

public:
  ceph::logging::SubsystemMap subsys;

  EntityName name;
  string data_dir_option;  ///< data_dir config option, if any

  /// cluster name
  string cluster;

  bool no_mon_config = false;

// This macro block defines C members of the md_config_t struct
// corresponding to the definitions in legacy_config_opts.h.
// These C members are consumed by code that was written before
// the new options.cc infrastructure: all newer code should
// be consume options via explicit get() rather than C members.
#define OPTION_OPT_INT(name) int64_t name;
#define OPTION_OPT_LONGLONG(name) int64_t name;
#define OPTION_OPT_STR(name) std::string name;
#define OPTION_OPT_DOUBLE(name) double name;
#define OPTION_OPT_FLOAT(name) double name;
#define OPTION_OPT_BOOL(name) bool name;
#define OPTION_OPT_ADDR(name) entity_addr_t name;
#define OPTION_OPT_U32(name) uint64_t name;
#define OPTION_OPT_U64(name) uint64_t name;
#define OPTION_OPT_UUID(name) uuid_d name;
#define OPTION(name, ty) \
  public:                      \
    OPTION_##ty(name)          
#define SAFE_OPTION(name, ty) \
  protected:                        \
    OPTION_##ty(name)               
#include "common/legacy_config_opts.h"
#undef OPTION_OPT_INT
#undef OPTION_OPT_LONGLONG
#undef OPTION_OPT_STR
#undef OPTION_OPT_DOUBLE
#undef OPTION_OPT_FLOAT
#undef OPTION_OPT_BOOL
#undef OPTION_OPT_ADDR
#undef OPTION_OPT_U32
#undef OPTION_OPT_U64
#undef OPTION_OPT_UUID
#undef OPTION
#undef SAFE_OPTION

public:
  unsigned get_osd_pool_default_min_size() const {
    auto min_size = get_val<uint64_t>("osd_pool_default_min_size");
    auto size = get_val<uint64_t>("osd_pool_default_size");
    return min_size ? std::min(min_size, size) : (size - size / 2);
  }

  /** A lock that protects the md_config_t internals. It is
   * recursive, for simplicity.
   * It is best if this lock comes first in the lock hierarchy. We will
   * hold this lock when calling configuration observers.  */
  mutable Mutex lock;

  friend class test_md_config_t;
};

template<typename T>
const T md_config_t::get_val(const std::string &key) const {
  return boost::get<T>(this->get_val_generic(key));
}

inline std::ostream& operator<<(std::ostream& o, const boost::blank& ) {
      return o << "INVALID_CONFIG_VALUE";
}

int ceph_resolve_file_search(const std::string& filename_list,
			     std::string& result);

#endif
