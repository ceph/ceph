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

extern struct ceph_file_layout g_default_file_layout;

#include <iosfwd>
#include <vector>
#include <map>
#include <set>

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
			 std::deque<std::string> *parse_errors,
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
			      std::deque<std::string> *parse_errors,
			      std::ostream *warnings);

  int set_val_impl(const char *val, const config_option *opt);
  int set_val_raw(const char *val, const config_option *opt);

  void init_subsys();

  bool expand_meta(std::string &val,
		   std::ostream *oss) const;

  bool expand_meta(std::string &val,
		   config_option *opt,
		   std::list<config_option *> stack,
		   std::ostream *oss) const;

  /// expand all metavariables in config structure.
  void expand_all_meta();

  // The configuration file we read, or NULL if we haven't read one.
  ConfFile cf;

  obs_map_t observers;
  changed_set_t changed;

public:
  ceph::log::SubsystemMap subsys;

  EntityName name;

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
};

typedef enum {
	OPT_INT, OPT_LONGLONG, OPT_STR, OPT_DOUBLE, OPT_FLOAT, OPT_BOOL,
	OPT_ADDR, OPT_U32, OPT_U64, OPT_UUID
} opt_type_t;

bool ceph_resolve_file_search(const std::string& filename_list,
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
