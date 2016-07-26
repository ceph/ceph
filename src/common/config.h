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

#include <iosfwd>
#include <vector>
#include <map>
#include <set>
#include <tuple>
#include <iostream>
#include <unordered_map>
#include <boost/variant.hpp>


#include "common/ConfUtils.h"
#include "common/entity_name.h"
#include "common/Mutex.h"
#include "log/SubsystemMap.h"
#include "common/config_obs.h"
#include "msg/msg_types.h"

enum {
  CEPH_DEFAULT_CRUSH_REPLICATED_RULESET,
  CEPH_DEFAULT_CRUSH_ERASURE_RULESET,
};

#define OSD_REP_PRIMARY 0
#define OSD_REP_SPLAY   1
#define OSD_REP_CHAIN   2

#define OSD_POOL_ERASURE_CODE_STRIPE_WIDTH 4096

struct config_option;
class CephContext;

//--
enum class ceph_conversion_behavior_t; 
enum class ceph_guard_behavior_t; 
bool is_uint_negative (const int);

extern const char *CEPH_CONF_FILE_DEFAULT;

#define LOG_TO_STDERR_NONE 0
#define LOG_TO_STDERR_SOME 1
#define LOG_TO_STDERR_ALL 2

typedef enum {
  OPT_INT, OPT_LONGLONG, OPT_STR, OPT_DOUBLE, OPT_FLOAT, OPT_BOOL,
  OPT_ADDR, OPT_U32, OPT_U64, OPT_UUID
} opt_type_t;

//--
enum class ceph_conversion_behavior_t {
  ABORT_ON_ERROR, WARNING_ON_ERROR, DEFAULT_ON_ERROR, CONVERT_IF_POSSIBLE
};  //-- enum class ceph_conversion_behavior_t

enum class ceph_guard_behavior_t {
  DO_NOTHING, UNSIGNED_NO_NEGATIVE
};  //-- enum class ceph_guard_behavior_t


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
 * There are two ways to read the ceph context-- the old way and the new way.
 * In the old way, code would simply read the public variables of the
 * configuration, without taking a lock. In the new way, code registers a
 * configuration obserever which receives callbacks when a value changes. These
 * callbacks take place under the md_config_t lock.
 *
 * To prevent serious problems resulting from thread-safety issues, we disallow
 * changing std::string configuration values after
 * md_config_t::internal_safe_to_start_threads becomes true. You can still
 * change integer or floating point values, however.
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
  int set_val(const char *key, const char *val, bool meta=true, bool safe=true);
  int set_val(const char *key, const string& s, bool meta=true, bool safe=true) {
    return set_val(key, s.c_str(), meta, safe);
  }

  // Get a configuration value.
  // No metavariables will be returned (they will have already been expanded)
  int get_val(const char *key, char **buf, int len) const;
  int _get_val(const char *key, char **buf, int len) const;

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

  /// print/log warnings/errors from parsing the config
  void complain_about_parse_errors(CephContext *cct);

private:
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

  int set_val_impl(const char *val, const config_option *opt);
  int set_val_raw(const char *val, const config_option *opt);

  void init_subsys();

  bool expand_meta(std::string &val,
		   std::ostream *oss) const;
public:  // for global_init
  bool early_expand_meta(std::string &val,
			 std::ostream *oss) const {
    Mutex::Locker l(lock);
    return expand_meta(val, oss);
  }
private:
  bool expand_meta(std::string &val,
		   config_option *opt,
		   std::list<config_option *> stack,
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
  ceph::log::SubsystemMap subsys;
  EntityName name;
  string data_dir_option;  ///< data_dir config option, if any
  /// cluster name
  string cluster;

  //-- same data types as in set_val_raw().
  using ceph_param_data_t = boost::variant<std::string, int, long long, float, 
    double, bool, uint32_t, uint64_t, entity_addr_t, uuid_d>;      
  //-- The last 'ceph_param_data_t' is the default value. The first is what it's set to.
  using ceph_behavior_t = std::tuple<ceph_conversion_behavior_t, 
    ceph_param_data_t, ceph_guard_behavior_t, opt_type_t, ceph_param_data_t>;    
  using ceph_settings_table_t = std::unordered_map<std::string, ceph_behavior_t>;
  using ceph_settings_tbl_const_iter = ceph_settings_table_t::const_iterator;
  using ceph_settings_tbl_iter = ceph_settings_table_t::iterator;
  bool ceph_use_guard_system() { return m_ceph_use_guard_system; }
  bool valid_ceph_setting(const string&, ceph_settings_table_t::const_iterator&); 


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
#define OPTION(name, ty, init) OPTION_##ty(name)
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

private:
  //-- 
  bool m_ceph_use_guard_system = true;
  ceph_param_data_t m_ceph_params_type;
  ceph_conversion_behavior_t m_ceph_behavior_type;
  ceph_guard_behavior_t m_ceph_guard_type;
  ceph_settings_table_t m_ceph_config_params;

  void copydata_to_ceph_settings_table(const config_option[], ceph_settings_table_t&);
  void reset_ceph_settings_table();

  template<typename Type>
  void update_ceph_settings_table(ceph_settings_table_t&, const std::string&, 
       const Type&, const opt_type_t, 
       const ceph_conversion_behavior_t& behavior_type=ceph_conversion_behavior_t::DEFAULT_ON_ERROR,
       const ceph_guard_behavior_t& guard_type=ceph_guard_behavior_t::DO_NOTHING);
  
  template<typename Type>
  int start_ceph_setting_update(const std::string&, const Type&);

  template<typename Type>
  int update_ceph_setting(const std::string&, const Type&, 
      ceph_settings_tbl_const_iter&);

  template<typename Type>
  int update_ceph_setting_value(const std::string&, const Type&, 
      const opt_type_t, const ceph_conversion_behavior_t&, 
      const ceph_guard_behavior_t&);

  template<typename Type>
  Type get_ceph_default_setting_value(const std::string&); 

};


int ceph_resolve_file_search(const std::string& filename_list,
			     std::string& result);

struct config_option {
  const char *name;
  opt_type_t type;
  size_t md_conf_off;

  // Given a configuration, return a pointer to this option inside
  // that configuration.
  void *conf_ptr(md_config_t *conf) const;

  const void *conf_ptr(const md_config_t *conf) const;
};
                                                    
template<typename Type>
void md_config_t::update_ceph_settings_table(ceph_settings_table_t& settings_tbl,
     const std::string& ceph_setting_name, const Type& ceph_setting_value,
     const opt_type_t option_type, const ceph_conversion_behavior_t& behavior_type,
     const ceph_guard_behavior_t& guard_type) 
{
  //-- The last 'ceph_param_data_t' is the default value. The first is what it's set to.
  settings_tbl.emplace(ceph_setting_name, std::make_tuple(behavior_type, 
                       (Type)ceph_setting_value, guard_type, option_type, 
                       (Type)ceph_setting_value));
}		/* template<typename Type>
       void md_config_t::update_ceph_settings_table(ceph_settings_table_t& settings_tbl,
       const std::string& ceph_setting_name, const Type& ceph_setting_value,
       const opt_type_t option_type, const ceph_conversion_behavior_t& behavior_type,
       const ceph_guard_behavior_t& guard_type) 
		*/

template<typename Type>
int md_config_t::update_ceph_setting_value(const std::string& ceph_setting_name, 
    const Type& ceph_setting_value, const opt_type_t option_type, 
    const ceph_conversion_behavior_t& behavior_type, 
    const ceph_guard_behavior_t& guard_type)
{
  auto iter = m_ceph_config_params.find(ceph_setting_name);
  if (iter != m_ceph_config_params.end()) {
    //-- Retrieve the default.
    ceph_param_data_t ceph_params_type;
    ceph_param_data_t ceph_params_type_default;
    ceph_conversion_behavior_t ceph_behavior_type;
    ceph_guard_behavior_t ceph_guard_type;
    opt_type_t ceph_param_type; 

    //-- The last 'ceph_param_data_t' is the default value. The first is what it's set to.
    std::tie(ceph_behavior_type, ceph_params_type, ceph_guard_type, 
             ceph_param_type, ceph_params_type_default) = iter->second;
    auto defaultValue = boost::get<Type>(ceph_params_type_default);
    //-- Update it the tuple.  
    iter->second = std::make_tuple(behavior_type, (Type)ceph_setting_value, 
                                   guard_type, option_type, defaultValue);
    return (1);
  }	//-- if (iter != m_ceph_config_params.end())
  //-- Could not find the 'ceph_setting_name'.
  return -ENOENT; 
}	/* template<typename Type>
     int md_config_t::update_ceph_setting_value(const std::string& ceph_setting_name, 
     const Type& ceph_setting_value, const opt_type_t option_type, 
     const ceph_conversion_behavior_t& behavior_type, 
     const ceph_guard_behavior_t& guard_type)
	*/

template<typename Type>
Type md_config_t::get_ceph_default_setting_value(const std::string& ceph_setting_name)
{
  auto iter = m_ceph_config_params.find(ceph_setting_name);
  if (iter != m_ceph_config_params.end()) {
    //-- Retrieve the default.
    ceph_param_data_t ceph_params_type;
    ceph_param_data_t ceph_params_type_default;
    ceph_conversion_behavior_t ceph_behavior_type;
    ceph_guard_behavior_t ceph_guard_type;
    opt_type_t ceph_param_type; 

    //-- The last 'ceph_param_data_t' is the default value. The first is what it's set to.
    std::tie(ceph_behavior_type, ceph_params_type, ceph_guard_type, 
             ceph_param_type, ceph_params_type_default) = iter->second;
    auto defaultValue = boost::get<Type>(ceph_params_type_default);
    return defaultValue;
  }	//-- if (iter != m_ceph_config_params.end())
  //-- Could not find the 'ceph_setting_name'.
  return -ENOENT; 
} /* template<typename Type>
     Type md_config_t::get_ceph_default_setting_value(const std::string& ceph_setting_name)
	*/

template<typename Type>
int md_config_t::start_ceph_setting_update(const std::string& ceph_setting_name,
    const Type& ceph_setting_value) 
{
  //-- If guard is enabled.
  if (ceph_use_guard_system()) {
    auto iter = m_ceph_config_params.find(ceph_setting_name);
    if (iter != m_ceph_config_params.end()) {
      //-- 
      ceph_param_data_t ceph_params_type;
      ceph_conversion_behavior_t ceph_behavior_type;
      ceph_guard_behavior_t ceph_guard_type;
      opt_type_t ceph_param_type; 

      //-- Dispatch it to the proper data type. 
      ceph_settings_tbl_const_iter ceph_settings_tbl_iter(iter);
      return (update_ceph_setting<Type>(ceph_setting_name, ceph_setting_value, 
                                        ceph_settings_tbl_iter));
    }   //--   if (iter != m_ceph_config_params.end()) {
    //-- Could not find the 'ceph_setting_name'.
    return -ENOENT;
  } //-- if (ceph_use_guard_system())
  //-- if not enabled, we don't do anything. 
  return (1);
}  /* template<typename Type>
      int md_config_t::start_ceph_setting_update(const std::string& ceph_setting_name,
      const Type& ceph_setting_value) 
   */

//-- Once just 1 big switch with all the cases and calls to function templates
//   can be a problem, just specializing them here with their specific test case. 
template<>
inline int md_config_t::update_ceph_setting<int>
    (const std::string& ceph_setting_name, const int& ceph_setting_value, 
     ceph_settings_tbl_const_iter& ceph_settings_tbl_iter) 
{
  //-- 
  ceph_param_data_t ceph_params_type;
  ceph_param_data_t ceph_params_type_default;
  ceph_conversion_behavior_t ceph_behavior_type;
  ceph_guard_behavior_t ceph_guard_type;
  opt_type_t ceph_param_type;
  std::tie(ceph_behavior_type, ceph_params_type, ceph_guard_type, 
      ceph_param_type, ceph_params_type_default) = ceph_settings_tbl_iter->second;
  if (ceph_param_type == OPT_INT) {
    int optValue = (ceph_setting_value);
    //-- We check for guards here and return an EINVAL if not allowed. 
    //   We don't have any as of now for INT.
    return (update_ceph_setting_value<int>(ceph_setting_name, (int)(optValue), 
                ceph_param_type, ceph_behavior_type, ceph_guard_type));
  } 
  //-- Could not find the 'ceph_setting_name' with proper syntax.
  return -ENOENT;
} /* template<>
     inline int md_config_t::update_ceph_setting<int>
         (const std::string& ceph_setting_name, const int& ceph_setting_value, 
         ceph_settings_tbl_const_iter& ceph_settings_tbl_iter) 
  */

template<>
inline int md_config_t::update_ceph_setting<long long>
    (const std::string& ceph_setting_name, const long long& ceph_setting_value,
     ceph_settings_tbl_const_iter& ceph_settings_tbl_iter)
{
  //-- 
  ceph_param_data_t ceph_params_type;
  ceph_param_data_t ceph_params_type_default;
  ceph_conversion_behavior_t ceph_behavior_type;
  ceph_guard_behavior_t ceph_guard_type;
  opt_type_t ceph_param_type; 
  std::tie(ceph_behavior_type, ceph_params_type, ceph_guard_type, 
       ceph_param_type, ceph_params_type_default) = ceph_settings_tbl_iter->second;
  if (ceph_param_type == OPT_LONGLONG) {
    long long optValue = (ceph_setting_value);
    //-- We check for guards here and return an EINVAL if not allowed. 
    //   We don't have any as of now for LONGLONG. 
    return (update_ceph_setting_value<long long>(ceph_setting_name, 
                                                 (long long)(optValue), 
                                                 ceph_param_type, 
                                                 ceph_behavior_type, 
                                                 ceph_guard_type));
  } 
  //-- Could not find the 'ceph_setting_name' with proper syntax.
  return -ENOENT;
} /* template<>
     inline int md_config_t::update_ceph_setting<long long>
       (const std::string& ceph_setting_name, const long long& ceph_setting_value,
       ceph_settings_tbl_const_iter& ceph_settings_tbl_iter)
  */

template<>
inline int md_config_t::update_ceph_setting<std::string>
    (const std::string& ceph_setting_name, const std::string& ceph_setting_value,
     ceph_settings_tbl_const_iter& ceph_settings_tbl_iter) 
{
  //-- 
  ceph_param_data_t ceph_params_type;
  ceph_param_data_t ceph_params_type_default;
  ceph_conversion_behavior_t ceph_behavior_type;
  ceph_guard_behavior_t ceph_guard_type;
  opt_type_t ceph_param_type; 
  std::tie(ceph_behavior_type, ceph_params_type, ceph_guard_type, 
       ceph_param_type, ceph_params_type_default) = ceph_settings_tbl_iter->second;
  if (ceph_param_type == OPT_STR) {
    std::string optValue = (ceph_setting_value);
    //-- We check for guards here and return an EINVAL if not allowed. 
    //   We don't have any as of now for STRING. 
    return (update_ceph_setting_value<std::string>(ceph_setting_name, 
                                                   (std::string)(optValue), 
                                                   ceph_param_type, 
                                                   ceph_behavior_type, 
                                                   ceph_guard_type));
  } 
  //-- Could not find the 'ceph_setting_name' with proper syntax.
  return -ENOENT;
} /* template<>
     inline int md_config_t::update_ceph_setting<std::string>
     (const std::string& ceph_setting_name, const std::string& ceph_setting_value,
      ceph_settings_tbl_const_iter& ceph_settings_tbl_iter) 
  */

template<>
inline int md_config_t::update_ceph_setting<float>
    (const std::string& ceph_setting_name, const float& ceph_setting_value,
     ceph_settings_tbl_const_iter& ceph_settings_tbl_iter) 
{
  //-- 
  ceph_param_data_t ceph_params_type;
  ceph_param_data_t ceph_params_type_default;
  ceph_conversion_behavior_t ceph_behavior_type;
  ceph_guard_behavior_t ceph_guard_type;
  opt_type_t ceph_param_type; 
  std::tie(ceph_behavior_type, ceph_params_type, ceph_guard_type, 
       ceph_param_type, ceph_params_type_default) = ceph_settings_tbl_iter->second;
  if (ceph_param_type == OPT_FLOAT) {
    float optValue = (ceph_setting_value);
    //-- We check for guards here and return an EINVAL if not allowed. 
    //   We don't have any as of now for FLOAT. 
    return (update_ceph_setting_value<float>(ceph_setting_name, 
                                             (float)(optValue), 
                                             ceph_param_type, 
                                             ceph_behavior_type, 
                                             ceph_guard_type));
  } 
  //-- Could not find the 'ceph_setting_name' with proper syntax.
  return -ENOENT;
} /* template<>
     inline int md_config_t::update_ceph_setting<float>
     (const std::string& ceph_setting_name, const float& ceph_setting_value,
     ceph_settings_tbl_const_iter& ceph_settings_tbl_iter) 
  */

template<>
inline int md_config_t::update_ceph_setting<double>
    (const std::string& ceph_setting_name, const double& ceph_setting_value,
     ceph_settings_tbl_const_iter& ceph_settings_tbl_iter)
{
  //-- 
  ceph_param_data_t ceph_params_type;
  ceph_param_data_t ceph_params_type_default;
  ceph_conversion_behavior_t ceph_behavior_type;
  ceph_guard_behavior_t ceph_guard_type;
  opt_type_t ceph_param_type; 
  std::tie(ceph_behavior_type, ceph_params_type, ceph_guard_type, 
       ceph_param_type, ceph_params_type_default) = ceph_settings_tbl_iter->second;
  if (ceph_param_type == OPT_DOUBLE) {
    double optValue = (ceph_setting_value);
    //-- We check for guards here and return an EINVAL if not allowed. 
    //   We don't have any as of now for DOUBLE. 
    return (update_ceph_setting_value<double>(ceph_setting_name, 
                                              (double)(optValue), 
                                              ceph_param_type, 
                                              ceph_behavior_type, 
                                              ceph_guard_type));
  } 
  //-- Could not find the 'ceph_setting_name' with proper syntax.
  return -ENOENT;
} /* template<>
     inline int md_config_t::update_ceph_setting<double>
     (const std::string& ceph_setting_name, const double& ceph_setting_value,
     ceph_settings_tbl_const_iter& ceph_settings_tbl_iter) 
  */

template<>
inline int md_config_t::update_ceph_setting<bool>
    (const std::string& ceph_setting_name, const bool& ceph_setting_value,
     ceph_settings_tbl_const_iter& ceph_settings_tbl_iter)
{
  //-- 
  ceph_param_data_t ceph_params_type;
  ceph_param_data_t ceph_params_type_default;
  ceph_conversion_behavior_t ceph_behavior_type;
  ceph_guard_behavior_t ceph_guard_type;
  opt_type_t ceph_param_type; 
  std::tie(ceph_behavior_type, ceph_params_type, ceph_guard_type, 
       ceph_param_type, ceph_params_type_default) = ceph_settings_tbl_iter->second;
  if (ceph_param_type == OPT_BOOL) {
    bool optValue = (ceph_setting_value);
    //-- We check for guards here and return an EINVAL if not allowed. 
    //   We don't have any as of now for BOOL. 
    return (update_ceph_setting_value<bool>(ceph_setting_name,
                                            (bool)(optValue), 
                                            ceph_param_type, 
                                            ceph_behavior_type, 
                                            ceph_guard_type));
  } 
  //-- Could not find the 'ceph_setting_name' with proper syntax.
  return -ENOENT;
} /* template<>
     inline int md_config_t::update_ceph_setting<bool>
     (const std::string& ceph_setting_name, const bool& ceph_setting_value,
     ceph_settings_tbl_const_iter& ceph_settings_tbl_iter)
  */

template<>
inline int md_config_t::update_ceph_setting<uint32_t>
    (const std::string& ceph_setting_name, const uint32_t& ceph_setting_value,
     ceph_settings_tbl_const_iter& ceph_settings_tbl_iter)
{
  //-- 
  ceph_param_data_t ceph_params_type;
  ceph_param_data_t ceph_params_type_default;
  ceph_conversion_behavior_t ceph_behavior_type;
  ceph_guard_behavior_t ceph_guard_type;
  opt_type_t ceph_param_type; 
  std::tie(ceph_behavior_type, ceph_params_type, ceph_guard_type, 
       ceph_param_type, ceph_params_type_default) = ceph_settings_tbl_iter->second;
  if (ceph_param_type == OPT_U32) {
    uint32_t optValue = (ceph_setting_value);
    //-- We check for guards here and return an EINVAL if not allowed. 
    //-- If a negative value was passed into the unsigned int (valid bitwise).
    if (is_uint_negative(ceph_setting_value)) {
      std::cerr << "ERROR: An invalid value (negative) was passed to parameter ("
                << ceph_setting_name << " [unsigned]). aborting..." << std::endl;
      if (ceph_behavior_type == ceph_conversion_behavior_t::DEFAULT_ON_ERROR) {
        auto defaultValue = get_ceph_default_setting_value<uint32_t>(ceph_setting_name);
        std::cerr << "WARNING: Using the default value for parameter (" 
                  << ceph_setting_name << ") -> [ " << defaultValue << " ]." 
                  << std::endl;
        return (update_ceph_setting_value<uint32_t>(ceph_setting_name, 
                                                    (uint32_t)(defaultValue), 
                                                    ceph_param_type, 
                                                    ceph_behavior_type, 
                                                    ceph_guard_type));
      } 
      return EINVAL;
    } 
    return (update_ceph_setting_value<uint32_t>(ceph_setting_name, 
                                                (uint32_t)(optValue), 
                                                ceph_param_type, 
                                                ceph_behavior_type, 
                                                ceph_guard_type));
  } 
  //-- Could not find the 'ceph_setting_name' with proper syntax.
  return -ENOENT;
} /* template<>
     inline int md_config_t::update_ceph_setting<uint32_t>
     (const std::string& ceph_setting_name, const uint32_t& ceph_setting_value, 
     ceph_settings_tbl_const_iter& ceph_settings_tbl_iter) 
  */

template<>
inline int md_config_t::update_ceph_setting<uint64_t>
    (const std::string& ceph_setting_name, const uint64_t& ceph_setting_value,
     ceph_settings_tbl_const_iter& ceph_settings_tbl_iter)
{
  //-- 
  ceph_param_data_t ceph_params_type;
  ceph_param_data_t ceph_params_type_default;
  ceph_conversion_behavior_t ceph_behavior_type;
  ceph_guard_behavior_t ceph_guard_type;
  opt_type_t ceph_param_type; 
  std::tie(ceph_behavior_type, ceph_params_type, ceph_guard_type,
       ceph_param_type, ceph_params_type_default) = ceph_settings_tbl_iter->second;
  if (ceph_param_type == OPT_U64) {
    uint64_t optValue = (ceph_setting_value);
    //-- We check for guards here and return an EINVAL if not allowed. 
    //-- If a negative value was passed into the unsigned int (valid bitwise).
    if (is_uint_negative(ceph_setting_value)) {
      std::cerr << "ERROR: An invalid value (negative) was passed to parameter (" 
                << ceph_setting_name << " [unsigned]). aborting..." << std::endl;
      if (ceph_behavior_type == ceph_conversion_behavior_t::DEFAULT_ON_ERROR) {
        auto defaultValue = get_ceph_default_setting_value<uint64_t>(ceph_setting_name);
        std::cerr << "WARNING: Using the default value for parameter (" 
                  << ceph_setting_name << ") -> [ " << defaultValue << " ]." 
                  << std::endl;
        return (update_ceph_setting_value<uint64_t>(ceph_setting_name, 
                                                    (uint64_t)(defaultValue), 
                                                    ceph_param_type, 
                                                    ceph_behavior_type, 
                                                    ceph_guard_type));
      } 
      return EINVAL;
    } 
    return (update_ceph_setting_value<uint64_t>(ceph_setting_name, 
                                                (uint64_t)(optValue), 
                                                ceph_param_type, 
                                                ceph_behavior_type, 
                                                ceph_guard_type));
  } 
  //-- Could not find the 'ceph_setting_name' with proper syntax.
  return -ENOENT;
} /* template<>
     inline int md_config_t::update_ceph_setting<uint64_t>
     (const std::string& ceph_setting_name, const uint64_t& ceph_setting_value,
     ceph_settings_tbl_const_iter& ceph_settings_tbl_iter)
  */

template<>
inline int md_config_t::update_ceph_setting<entity_addr_t>
    (const std::string& ceph_setting_name, const entity_addr_t& ceph_setting_value,
     ceph_settings_tbl_const_iter& ceph_settings_tbl_iter) 
{
  //-- 
  ceph_param_data_t ceph_params_type;
  ceph_param_data_t ceph_params_type_default;
  ceph_conversion_behavior_t ceph_behavior_type;
  ceph_guard_behavior_t ceph_guard_type;
  opt_type_t ceph_param_type; 
  std::tie(ceph_behavior_type, ceph_params_type, ceph_guard_type, 
       ceph_param_type, ceph_params_type_default) = ceph_settings_tbl_iter->second;
  if (ceph_param_type == OPT_ADDR) {
    entity_addr_t optValue = (ceph_setting_value);
    //-- We check for guards here and return an EINVAL if not allowed. 
    //   We don't have any as of now for ADDR. 
    return (update_ceph_setting_value<entity_addr_t>(ceph_setting_name, 
                                                     (entity_addr_t)(optValue), 
                                                     ceph_param_type, 
                                                     ceph_behavior_type, 
                                                     ceph_guard_type));
  } 
  //-- Could not find the 'ceph_setting_name' with proper syntax.
  return -ENOENT;
} /* template<>
     inline int md_config_t::update_ceph_setting<entity_addr_t>
     (const std::string& ceph_setting_name, const entity_addr_t& ceph_setting_value,
     ceph_settings_tbl_const_iter& ceph_settings_tbl_iter) 
  */

template<>
inline int md_config_t::update_ceph_setting<uuid_d>
    (const std::string& ceph_setting_name, const uuid_d& ceph_setting_value, 
     ceph_settings_tbl_const_iter& ceph_settings_tbl_iter) 
{
  //-- 
  ceph_param_data_t ceph_params_type;
  ceph_param_data_t ceph_params_type_default;
  ceph_conversion_behavior_t ceph_behavior_type;
  ceph_guard_behavior_t ceph_guard_type;
  opt_type_t ceph_param_type; 
  std::tie(ceph_behavior_type, ceph_params_type, ceph_guard_type, 
       ceph_param_type, ceph_params_type_default) = ceph_settings_tbl_iter->second;
  if (ceph_param_type == OPT_UUID) {
    uuid_d optValue = (ceph_setting_value);
    //-- We check for guards here and return an EINVAL if not allowed. 
    //   We don't have any as of now for UUID. 
    return (update_ceph_setting_value<uuid_d>(ceph_setting_name, 
                                              (uuid_d)(optValue), 
                                              ceph_param_type, 
                                              ceph_behavior_type, 
                                              ceph_guard_type));
  } 
  //-- Could not find the 'ceph_setting_name' with proper syntax.
  return -ENOENT;
} /* template<>
     inline int md_config_t::update_ceph_setting<uuid_d>
     (const std::string& ceph_setting_name, const uuid_d& ceph_setting_value, 
     ceph_settings_tbl_const_iter& ceph_settings_tbl_iter) 
  */

enum config_subsys_id {
  ceph_subsys_,   // default
#define OPTION(a,b,c)
#define SUBSYS(name, log, gather) \
  ceph_subsys_##name,
#define DEFAULT_SUBSYS(log, gather)
#include "common/config_opts.h"
#undef SUBSYS
#undef OPTION
#undef DEFAULT_SUBSYS
  ceph_subsys_max
};

#endif

