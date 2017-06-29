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

#include "common/ConfUtils.h"
#include "common/entity_name.h"
#include "common/Mutex.h"
#include "log/SubsystemMap.h"
#include "common/config_obs.h"

#define OSD_REP_PRIMARY 0
#define OSD_REP_SPLAY   1
#define OSD_REP_CHAIN   2

class CephContext;

extern const char *CEPH_CONF_FILE_DEFAULT;

#define LOG_TO_STDERR_NONE 0
#define LOG_TO_STDERR_SOME 1
#define LOG_TO_STDERR_ALL 2

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
 * md_config_t::internal_safe_to_start_threads becomes true. You can still
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
  /* Maps configuration options to the observer listening for them. */
  typedef std::multimap <std::string, md_config_obs_t*> obs_map_t;

  /* Set of configuration options that have changed since the last
   * apply_changes */
  typedef std::set < std::string > changed_set_t;

  struct invalid_config_value_t { };
  typedef boost::variant<invalid_config_value_t,
                         int,
                         long long,
                         std::string,
                         double,
                         float,
                         bool,
                         entity_addr_t,
                         uint32_t,
                         uint64_t,
                         uuid_d> config_value_t;
  typedef boost::variant<const int md_config_t::*,
                         const long long md_config_t::*,
                         const std::string md_config_t::*,
                         const double md_config_t::*,
                         const float md_config_t::*,
                         const bool md_config_t::*,
                         const entity_addr_t md_config_t::*,
                         const uint32_t md_config_t::*,
                         const uint64_t md_config_t::*,
                         const uuid_d md_config_t::*> member_ptr_t;

   typedef enum {
	OPT_INT, OPT_LONGLONG, OPT_STR, OPT_DOUBLE, OPT_FLOAT, OPT_BOOL,
	OPT_ADDR, OPT_U32, OPT_U64, OPT_UUID
   } opt_type_t;

  typedef std::function<int(std::string*, std::string*)> validator_t;

  class config_option {
  public:
    const char *name;
    opt_type_t type;
    md_config_t::member_ptr_t md_member_ptr;
    bool safe; // promise to access it only via md_config_t::get_val
    validator_t validator;
  private:
    template<typename T> struct get_typed_pointer_visitor : public boost::static_visitor<T const *> {
      md_config_t const *conf;
      explicit get_typed_pointer_visitor(md_config_t const *conf_) : conf(conf_) { }
      template<typename U,
	typename boost::enable_if<boost::is_same<T, U>, int>::type = 0>
	  T const *operator()(const U md_config_t::* member_ptr) {
	    return &(conf->*member_ptr);
	  }
      template<typename U,
	typename boost::enable_if_c<!boost::is_same<T, U>::value, int>::type = 0>
	  T const *operator()(const U md_config_t::* member_ptr) {
	    return nullptr;
	  }
    };
  public:
    // is it OK to alter the value when threads are running?
    bool is_safe() const;
    // Given a configuration, return a pointer to this option inside
    // that configuration.
    template<typename T> void conf_ptr(T const *&ptr, md_config_t const *conf) const {
      get_typed_pointer_visitor<T> gtpv(conf);
      ptr = boost::apply_visitor(gtpv, md_member_ptr);
    }
    template<typename T> void conf_ptr(T *&ptr, md_config_t *conf) const {
      get_typed_pointer_visitor<T> gtpv(conf);
      ptr = const_cast<T *>(boost::apply_visitor(gtpv, md_member_ptr));
    }
    template<typename T> T const *conf_ptr(md_config_t const *conf) const {
      get_typed_pointer_visitor<T> gtpv(conf);
      return boost::apply_visitor(gtpv, md_member_ptr);
    }
    template<typename T> T *conf_ptr(md_config_t *conf) const {
      get_typed_pointer_visitor<T> gtpv(conf);
      return const_cast<T *>(boost::apply_visitor(gtpv, md_member_ptr));
    }
  };

  // Create a new md_config_t structure.
  md_config_t();
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
  void parse_env();

  // Absorb config settings from argv
  int parse_argv(std::vector<const char*>& args);

  // Expand all metavariables. Make any pending observer callbacks.
  void apply_changes(std::ostream *oss);
  void _apply_changes(std::ostream *oss);
  bool _internal_field(const string& k);
  void call_all_observers();

  // Called by the Ceph daemons to make configuration changes at runtime
  int injectargs(const std::string &s, std::ostream *oss);

  // Set a configuration value, or crash
  // Metavariables will be expanded.
  void set_val_or_die(const char *key, const char *val);

  // Set a configuration value.
  // Metavariables will be expanded.
  int set_val(const char *key, const char *val, bool meta=true);
  int set_val(const char *key, const string& s, bool meta=true) {
    return set_val(key, s.c_str(), meta);
  }

  // Get a configuration value.
  // No metavariables will be returned (they will have already been expanded)
  int get_val(const char *key, char **buf, int len) const;
  int _get_val(const char *key, char **buf, int len) const;
  config_value_t get_val_generic(const char *key) const;
  template<typename T> T get_val(const char *key) const;

  void get_all_keys(std::vector<std::string> *keys) const;

  // Return a list of all the sections that the current entity is a member of.
  void get_my_sections(std::vector <std::string> &sections) const;

  // Return a list of all sections
  int get_all_sections(std::vector <std::string> &sections) const;

  // Get a value from the configuration file that we read earlier.
  // Metavariables will be expanded if emeta is true.
  int get_val_from_conf_file(const std::vector <std::string> &sections,
		   const char *key, std::string &out, bool emeta) const;

  /// dump all config values to a stream
  void show_config(std::ostream& out);
  /// dump all config values to a formatter
  void show_config(Formatter *f);

  /// obtain a diff between our config values and another md_config_t values
  void diff(const md_config_t *other,
            map<string,pair<string,string> > *diff, set<string> *unknown);

  /// obtain a diff between config values and another md_config_t 
  /// values for a specific setting. 
  void diff(const md_config_t *other,
            map<string,pair<string,string>> *diff, set<string> *unknown, 
            const string& setting);

  /// print/log warnings/errors from parsing the config
  void complain_about_parse_errors(CephContext *cct);

private:
  void validate_default_settings();

  int _get_val(const char *key, std::string *value) const;
  config_value_t _get_val(const char *key) const;
  void _show_config(std::ostream *out, Formatter *f);

  void _get_my_sections(std::vector <std::string> &sections) const;

  int _get_val_from_conf_file(const std::vector <std::string> &sections,
			      const char *key, std::string &out, bool emeta) const;

  int parse_option(std::vector<const char*>& args,
		   std::vector<const char*>::iterator& i,
		   std::ostream *oss);
  int parse_injectargs(std::vector<const char*>& args,
		      std::ostream *oss);
  int parse_config_files_impl(const std::list<std::string> &conf_files,
			      std::ostream *warnings);

  int set_val_impl(const std::string &val, config_option const *opt,
                   std::string *error_message);
  int set_val_raw(const char *val, config_option const *opt);

  void init_subsys();

  bool expand_meta(std::string &val,
		   std::ostream *oss) const;

  void diff_helper(const md_config_t* other,
                   map<string, pair<string, string>>* diff,
                   set<string>* unknown, const string& setting = string{});

public:  // for global_init
  bool early_expand_meta(std::string &val,
			 std::ostream *oss) const {
    Mutex::Locker l(lock);
    return expand_meta(val, oss);
  }
private:
  bool expand_meta(std::string &val,
		   config_option const *opt,
		   std::list<config_option const *> stack,
		   std::ostream *oss) const;

  /// expand all metavariables in config structure.
  void expand_all_meta();

  // The configuration file we read, or NULL if we haven't read one.
  ConfFile cf;
public:
  std::deque<std::string> parse_errors;
private:

  obs_map_t observers;
  changed_set_t changed;

public:
  ceph::logging::SubsystemMap subsys;

  EntityName name;
  string data_dir_option;  ///< data_dir config option, if any

  /// cluster name
  string cluster;

#define OPTION_OPT_INT(name) const int name;
#define OPTION_OPT_LONGLONG(name) const long long name;
#define OPTION_OPT_STR(name) const std::string name;
#define OPTION_OPT_DOUBLE(name) const double name;
#define OPTION_OPT_FLOAT(name) const float name;
#define OPTION_OPT_BOOL(name) const bool name;
#define OPTION_OPT_ADDR(name) const entity_addr_t name;
#define OPTION_OPT_U32(name) const uint32_t name;
#define OPTION_OPT_U64(name) const uint64_t name;
#define OPTION_OPT_UUID(name) const uuid_d name;
#define OPTION(name, ty, init) \
  public:                      \
    OPTION_##ty(name)          \
    struct option_##name##_t;
#define OPTION_VALIDATOR(name)
#define SAFE_OPTION(name, ty, init) \
  protected:                        \
    OPTION_##ty(name)               \
  public:                           \
    struct option_##name##_t;
#define SUBSYS(name, log, gather)
#define DEFAULT_SUBSYS(log, gather)
#include "common/config_opts.h"
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
#undef OPTION_VALIDATOR
#undef SAFE_OPTION
#undef SUBSYS
#undef DEFAULT_SUBSYS

  unsigned get_osd_pool_default_min_size() const {
    return osd_pool_default_min_size ?
      MIN(osd_pool_default_min_size, osd_pool_default_size) :
      osd_pool_default_size - osd_pool_default_size / 2;
  }

  /** A lock that protects the md_config_t internals. It is
   * recursive, for simplicity.
   * It is best if this lock comes first in the lock hierarchy. We will
   * hold this lock when calling configuration observers.  */
  mutable Mutex lock;

  friend class test_md_config_t;
protected:
  // Tests and possibly users expect options to appear in the output
  // of ceph-conf in the same order as declared in config_opts.h
  std::shared_ptr<const std::vector<config_option>> config_options;
  config_option const *find_config_option(const std::string& normalized_key) const;
};

template<typename T>
struct get_typed_value_visitor : public boost::static_visitor<T> {
  template<typename U,
    typename boost::enable_if<boost::is_same<T, U>, int>::type = 0>
      T operator()(U & val) {
	return std::move(val);
      }
  template<typename U,
    typename boost::enable_if_c<!boost::is_same<T, U>::value, int>::type = 0>
      T operator()(U &val) {
	assert("wrong type or option does not exist" == nullptr);
      }
};

template<typename T> T md_config_t::get_val(const char *key) const {
  config_value_t generic_val = this->get_val_generic(key);
  get_typed_value_visitor<T> gtv;
  return boost::apply_visitor(gtv, generic_val);
}

inline std::ostream& operator<<(std::ostream& o, const md_config_t::invalid_config_value_t& ) {
      return o << "INVALID_CONFIG_VALUE";
}

int ceph_resolve_file_search(const std::string& filename_list,
			     std::string& result);

typedef md_config_t::config_option config_option;


enum config_subsys_id {
  ceph_subsys_,   // default
#define OPTION(a,b,c)
#define OPTION_VALIDATOR(name)
#define SAFE_OPTION(a,b,c)
#define SUBSYS(name, log, gather) \
  ceph_subsys_##name,
#define DEFAULT_SUBSYS(log, gather)
#include "common/config_opts.h"
#undef SUBSYS
#undef OPTION
#undef OPTION_VALIDATOR
#undef SAFE_OPTION
#undef DEFAULT_SUBSYS
  ceph_subsys_max
};

#endif
