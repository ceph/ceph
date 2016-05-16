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

#include "auth/Auth.h"
#include "common/ConfUtils.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/static_assert.h"
#include "common/strtol.h"
#include "common/version.h"
#include "include/str_list.h"
#include "include/types.h"
#include "include/stringify.h"
#include "msg/msg_types.h"
#include "osd/osd_types.h"
#include "common/errno.h"

#include "include/assert.h"

#include <errno.h>
#include <sstream>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#if defined(__FreeBSD__)
/* FreeBSD/Clang requires basename() whereas Linux preffers the version in <string.h> */
#include <libgen.h>
#endif

/* Don't use standard Ceph logging in this file.
 * We can't use logging until it's initialized, and a lot of the necessary
 * initialization happens here.
 */
#undef dout
#undef ldout
#undef pdout
#undef derr
#undef lderr
#undef generic_dout
#undef dendl

using std::map;
using std::list;
using std::multimap;
using std::ostringstream;
using std::pair;
using std::set;
using std::string;

const char *CEPH_CONF_FILE_DEFAULT = "$data_dir/config, /etc/ceph/$cluster.conf, ~/.ceph/$cluster.conf, $cluster.conf";

#define _STR(x) #x
#define STRINGIFY(x) _STR(x)


void *config_option::conf_ptr(md_config_t *conf) const
{
  void *v = (void*)(((char*)conf) + md_conf_off);
  return v;
}

const void *config_option::conf_ptr(const md_config_t *conf) const
{
  const void *v = (const void*)(((const char*)conf) + md_conf_off);
  return v;
}

struct config_option config_optionsp[] = {
#define OPTION(name, type, def_val) \
       { STRINGIFY(name), type, offsetof(struct md_config_t, name) },
#define SUBSYS(name, log, gather)
#define DEFAULT_SUBSYS(log, gather)
#include "common/config_opts.h"
#undef OPTION
#undef SUBSYS
#undef DEFAULT_SUBSYS
};

const int NUM_CONFIG_OPTIONS = sizeof(config_optionsp) / sizeof(config_option);

int ceph_resolve_file_search(const std::string& filename_list,
			     std::string& result)
{
  list<string> ls;
  get_str_list(filename_list, ls);

  int ret = -ENOENT;
  list<string>::iterator iter;
  for (iter = ls.begin(); iter != ls.end(); ++iter) {
    int fd = ::open(iter->c_str(), O_RDONLY);
    if (fd < 0) {
      ret = -errno;
      continue;
    }
    close(fd);
    result = *iter;
    return 0;
  }

  return ret;
}

md_config_t::md_config_t()
  : cluster(""),

#define OPTION_OPT_INT(name, def_val) name(def_val),
#define OPTION_OPT_LONGLONG(name, def_val) name((1LL) * def_val),
#define OPTION_OPT_STR(name, def_val) name(def_val),
#define OPTION_OPT_DOUBLE(name, def_val) name(def_val),
#define OPTION_OPT_FLOAT(name, def_val) name(def_val),
#define OPTION_OPT_BOOL(name, def_val) name(def_val),
#define OPTION_OPT_ADDR(name, def_val) name(def_val),
#define OPTION_OPT_U32(name, def_val) name(def_val),
#define OPTION_OPT_U64(name, def_val) name(((uint64_t)1) * def_val),
#define OPTION_OPT_UUID(name, def_val) name(def_val),
#define OPTION(name, type, def_val) OPTION_##type(name, def_val)
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
  lock("md_config_t", true, false)
{
  init_subsys();
}

void md_config_t::init_subsys()
{
#define SUBSYS(name, log, gather) \
  subsys.add(ceph_subsys_##name, STRINGIFY(name), log, gather);
#define DEFAULT_SUBSYS(log, gather) \
  subsys.add(ceph_subsys_, "none", log, gather);
#define OPTION(a, b, c)
#include "common/config_opts.h"
#undef OPTION
#undef SUBSYS
#undef DEFAULT_SUBSYS
}

md_config_t::~md_config_t()
{
}

void md_config_t::add_observer(md_config_obs_t* observer_)
{
  Mutex::Locker l(lock);
  const char **keys = observer_->get_tracked_conf_keys();
  for (const char ** k = keys; *k; ++k) {
    obs_map_t::value_type val(*k, observer_);
    observers.insert(val);
  }
}

void md_config_t::remove_observer(md_config_obs_t* observer_)
{
  Mutex::Locker l(lock);
  bool found_obs = false;
  for (obs_map_t::iterator o = observers.begin(); o != observers.end(); ) {
    if (o->second == observer_) {
      observers.erase(o++);
      found_obs = true;
    }
    else {
      ++o;
    }
  }
  assert(found_obs);
}

int md_config_t::parse_config_files(const char *conf_files,
				    std::ostream *warnings,
				    int flags)
{
  Mutex::Locker l(lock);

  if (internal_safe_to_start_threads)
    return -ENOSYS;

  if (!cluster.size() && !conf_files) {
    /*
     * set the cluster name to 'ceph' when neither cluster name nor
     * configuration file are specified.
     */
    cluster = "ceph";
  }

  if (!conf_files) {
    const char *c = getenv("CEPH_CONF");
    if (c) {
      conf_files = c;
    }
    else {
      if (flags & CINIT_FLAG_NO_DEFAULT_CONFIG_FILE)
	return 0;
      conf_files = CEPH_CONF_FILE_DEFAULT;
    }
  }

  std::list<std::string> cfl;
  get_str_list(conf_files, cfl);

  auto p = cfl.begin();
  while (p != cfl.end()) {
    // expand $data_dir?
    string &s = *p;
    if (s.find("$data_dir") != string::npos) {
      if (data_dir_option.length()) {
	list<config_option*> stack;
	expand_meta(s, NULL, stack, warnings);
	p++;
      } else {
	cfl.erase(p++);  // ignore this item
      }
    } else {
      ++p;
    }
  }
  return parse_config_files_impl(cfl, warnings);
}

int md_config_t::parse_config_files_impl(const std::list<std::string> &conf_files,
					 std::ostream *warnings)
{
  assert(lock.is_locked());

  // open new conf
  list<string>::const_iterator c;
  for (c = conf_files.begin(); c != conf_files.end(); ++c) {
    cf.clear();
    string fn = *c;
    expand_meta(fn, warnings);
    int ret = cf.parse_file(fn.c_str(), &parse_errors, warnings);
    if (ret == 0)
      break;
    else if (ret != -ENOENT)
      return ret;
  }
  if (c == conf_files.end())
    return -EINVAL;

  if (cluster.size() == 0) {
    /*
     * If cluster name is not set yet, use the prefix of the
     * basename of configuration file as cluster name.
     */
    const char *fn = c->c_str();
    std::string name(basename(fn));
    int pos = name.find(".conf");
    if (pos < 0) {
      /*
       * If the configuration file does not follow $cluster.conf
       * convention, we do the last try and assign the cluster to
       * 'ceph'.
       */
      cluster = "ceph";
    } else {
      cluster = name.substr(0, pos);      
    }
  }

  std::vector <std::string> my_sections;
  _get_my_sections(my_sections);
  for (int i = 0; i < NUM_CONFIG_OPTIONS; i++) {
    config_option *opt = &config_optionsp[i];
    std::string val;
    int ret = _get_val_from_conf_file(my_sections, opt->name, val, false);
    if (ret == 0) {
      set_val_impl(val.c_str(), opt);
    }
  }
  
  // subsystems?
  for (int o = 0; o < subsys.get_num(); o++) {
    std::string as_option("debug_");
    as_option += subsys.get_name(o);
    std::string val;
    int ret = _get_val_from_conf_file(my_sections, as_option.c_str(), val, false);
    if (ret == 0) {
      int log, gather;
      int r = sscanf(val.c_str(), "%d/%d", &log, &gather);
      if (r >= 1) {
	if (r < 2)
	  gather = log;
	//	cout << "config subsys " << subsys.get_name(o) << " log " << log << " gather " << gather << std::endl;
	subsys.set_log_level(o, log);
	subsys.set_gather_level(o, gather);
      }
    }	
  }

  // Warn about section names that look like old-style section names
  std::deque < std::string > old_style_section_names;
  for (ConfFile::const_section_iter_t s = cf.sections_begin();
       s != cf.sections_end(); ++s) {
    const string &str(s->first);
    if (((str.find("mds") == 0) || (str.find("mon") == 0) ||
	 (str.find("osd") == 0)) && (str.size() > 3) && (str[3] != '.')) {
      old_style_section_names.push_back(str);
    }
  }
  if (!old_style_section_names.empty()) {
    ostringstream oss;
    cerr << "ERROR! old-style section name(s) found: ";
    string sep;
    for (std::deque < std::string >::const_iterator os = old_style_section_names.begin();
	 os != old_style_section_names.end(); ++os) {
      cerr << sep << *os;
      sep = ", ";
    }
    cerr << ". Please use the new style section names that include a period.";
  }
  return 0;
}

void md_config_t::parse_env()
{
  Mutex::Locker l(lock);
  if (internal_safe_to_start_threads)
    return;
  if (getenv("CEPH_KEYRING")) {
    set_val_or_die("keyring", getenv("CEPH_KEYRING"));
  }
}

void md_config_t::show_config(std::ostream& out)
{
  Mutex::Locker l(lock);
  _show_config(&out, NULL);
}

void md_config_t::show_config(Formatter *f)
{
  Mutex::Locker l(lock);
  _show_config(NULL, f);
}

void md_config_t::_show_config(std::ostream *out, Formatter *f)
{
  if (out) {
    *out << "name = " << name << std::endl;
    *out << "cluster = " << cluster << std::endl;
  }
  if (f) {
    f->dump_string("name", stringify(name));
    f->dump_string("cluster", cluster);
  }
  for (int o = 0; o < subsys.get_num(); o++) {
    if (out)
      *out << "debug_" << subsys.get_name(o)
	   << " = " << subsys.get_log_level(o)
	   << "/" << subsys.get_gather_level(o) << std::endl;
    if (f) {
      ostringstream ss;
      std::string debug_name = "debug_";
      debug_name += subsys.get_name(o);
      ss << subsys.get_log_level(o)
	 << "/" << subsys.get_gather_level(o);
      f->dump_string(debug_name.c_str(), ss.str());
    }
  }
  for (int i = 0; i < NUM_CONFIG_OPTIONS; i++) {
    config_option *opt = config_optionsp + i;
    char *buf;
    _get_val(opt->name, &buf, -1);
    if (out)
      *out << opt->name << " = " << buf << std::endl;
    if (f)
      f->dump_string(opt->name, buf);
    free(buf);
  }
}

int md_config_t::parse_argv(std::vector<const char*>& args)
{
  Mutex::Locker l(lock);
  if (internal_safe_to_start_threads) {
    return -ENOSYS;
  }

  bool show_config = false;
  bool show_config_value = false;
  string show_config_value_arg;

  // In this function, don't change any parts of the configuration directly.
  // Instead, use set_val to set them. This will allow us to send the proper
  // observer notifications later.
  std::string val;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (strcmp(*i, "--") == 0) {
      /* Normally we would use ceph_argparse_double_dash. However, in this
       * function we *don't* want to remove the double dash, because later
       * argument parses will still need to see it. */
      break;
    }
    else if (ceph_argparse_flag(args, i, "--show_conf", (char*)NULL)) {
      cerr << cf << std::endl;
      _exit(0);
    }
    else if (ceph_argparse_flag(args, i, "--show_config", (char*)NULL)) {
      show_config = true;
    }
    else if (ceph_argparse_witharg(args, i, &val, "--show_config_value", (char*)NULL)) {
      show_config_value = true;
      show_config_value_arg = val;
    }
    else if (ceph_argparse_flag(args, i, "--foreground", "-f", (char*)NULL)) {
      set_val_or_die("daemonize", "false");
    }
    else if (ceph_argparse_flag(args, i, "-d", (char*)NULL)) {
      set_val_or_die("daemonize", "false");
      set_val_or_die("log_file", "");
      set_val_or_die("log_to_stderr", "true");
      set_val_or_die("err_to_stderr", "true");
      set_val_or_die("log_to_syslog", "false");
    }
    // Some stuff that we wanted to give universal single-character options for
    // Careful: you can burn through the alphabet pretty quickly by adding
    // to this list.
    else if (ceph_argparse_witharg(args, i, &val, "--monmap", "-M", (char*)NULL)) {
      set_val_or_die("monmap", val.c_str());
    }
    else if (ceph_argparse_witharg(args, i, &val, "--mon_host", "-m", (char*)NULL)) {
      set_val_or_die("mon_host", val.c_str());
    }
    else if (ceph_argparse_witharg(args, i, &val, "--bind", (char*)NULL)) {
      set_val_or_die("public_addr", val.c_str());
    }
    else if (ceph_argparse_witharg(args, i, &val, "--keyfile", "-K", (char*)NULL)) {
      set_val_or_die("keyfile", val.c_str());
    }
    else if (ceph_argparse_witharg(args, i, &val, "--keyring", "-k", (char*)NULL)) {
      set_val_or_die("keyring", val.c_str());
    }
    else if (ceph_argparse_witharg(args, i, &val, "--client_mountpoint", "-r", (char*)NULL)) {
      set_val_or_die("client_mountpoint", val.c_str());
    }
    else {
      parse_option(args, i, NULL);
    }
  }

  if (show_config) {
    expand_all_meta();
    _show_config(&cout, NULL);
    _exit(0);
  }

  if (show_config_value) {
    char *buf = 0;
    int r = _get_val(show_config_value_arg.c_str(), &buf, -1);
    if (r < 0) {
      if (r == -ENOENT)
	std::cerr << "failed to get config option '" <<
	  show_config_value_arg << "': option not found" << std::endl;
      else
	std::cerr << "failed to get config option '" <<
	  show_config_value_arg << "': " << cpp_strerror(r) << std::endl;
      _exit(1);
    }
    string s = buf;
    expand_meta(s, &std::cerr);
    std::cout << s << std::endl;
    _exit(0);
  }

  return 0;
}

int md_config_t::parse_option(std::vector<const char*>& args,
			       std::vector<const char*>::iterator& i,
			       ostream *oss)
{
  int ret = 0;
  int o;
  std::string val;

  // subsystems?
  for (o = 0; o < subsys.get_num(); o++) {
    std::string as_option("--");
    as_option += "debug_";
    as_option += subsys.get_name(o);
    if (ceph_argparse_witharg(args, i, &val,
			      as_option.c_str(), (char*)NULL)) {
      int log, gather;
      int r = sscanf(val.c_str(), "%d/%d", &log, &gather);
      if (r >= 1) {
	if (r < 2)
	  gather = log;
	//	  cout << "subsys " << subsys.get_name(o) << " log " << log << " gather " << gather << std::endl;
	subsys.set_log_level(o, log);
	subsys.set_gather_level(o, gather);
	if (oss)
	  *oss << "debug_" << subsys.get_name(o) << "=" << log << "/" << gather << " ";
      }
      break;
    }	
  }
  if (o < subsys.get_num()) {
    return ret;
  }

  for (o = 0; o < NUM_CONFIG_OPTIONS; ++o) {
    ostringstream err;
    const config_option *opt = config_optionsp + o;
    std::string as_option("--");
    as_option += opt->name;
    if (opt->type == OPT_BOOL) {
      int res;
      if (ceph_argparse_binary_flag(args, i, &res, oss, as_option.c_str(),
				    (char*)NULL)) {
	if (res == 0)
	  set_val_impl("false", opt);
	else if (res == 1)
	  set_val_impl("true", opt);
	else
	  ret = res;
	break;
      } else {
	std::string no("--no-");
	no += opt->name;
	if (ceph_argparse_flag(args, i, no.c_str(), (char*)NULL)) {
	  set_val_impl("false", opt);
	  break;
	}
      }
    }
    else if (ceph_argparse_witharg(args, i, &val, err,
				   as_option.c_str(), (char*)NULL)) {
      if (!err.str().empty()) {
	*oss << err.str();
	ret = -EINVAL;
	break;
      }
      if (oss && (
		  ((opt->type == OPT_STR) || (opt->type == OPT_ADDR) ||
		   (opt->type == OPT_UUID)) &&
		  (observers.find(opt->name) == observers.end()))) {
	*oss << "You cannot change " << opt->name << " using injectargs.\n";
	ret = -ENOSYS;
	break;
      }
      int res = set_val_impl(val.c_str(), opt);
      if (res) {
	if (oss) {
	  *oss << "Parse error setting " << opt->name << " to '"
	       << val << "' using injectargs.\n";
	  ret = res;
	  break;
	} else {
	  cerr << "parse error setting '" << opt->name << "' to '"
	       << val << "'\n" << std::endl;
	}
      }
      break;
    }
  }
  if (o == NUM_CONFIG_OPTIONS) {
    // ignore
    ++i;
  }
  return ret;
}

int md_config_t::parse_injectargs(std::vector<const char*>& args,
				  std::ostream *oss)
{
  assert(lock.is_locked());
  int ret = 0;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    int r = parse_option(args, i, oss);
    if (r < 0)
      ret = r;
  }
  return ret;
}

void md_config_t::apply_changes(std::ostream *oss)
{
  Mutex::Locker l(lock);
  /*
   * apply changes until the cluster name is assigned
   */
  if (cluster.size())
    _apply_changes(oss);
}

bool md_config_t::_internal_field(const string& s)
{
  if (s == "internal_safe_to_start_threads")
    return true;
  return false;
}

void md_config_t::_apply_changes(std::ostream *oss)
{
  /* Maps observers to the configuration options that they care about which
   * have changed. */
  typedef std::map < md_config_obs_t*, std::set <std::string> > rev_obs_map_t;

  expand_all_meta();

  // create the reverse observer mapping, mapping observers to the set of
  // changed keys that they'll get.
  rev_obs_map_t robs;
  std::set <std::string> empty_set;
  char buf[128];
  char *bufptr = (char*)buf;
  for (changed_set_t::const_iterator c = changed.begin();
       c != changed.end(); ++c) {
    const std::string &key(*c);
    pair < obs_map_t::iterator, obs_map_t::iterator >
      range(observers.equal_range(key));
    if ((oss) &&
	(!_get_val(key.c_str(), &bufptr, sizeof(buf))) &&
	!_internal_field(key)) {
      (*oss) << key << " = '" << buf << "' ";
      if (range.first == range.second) {
	(*oss) << "(unchangeable) ";
      }
    }
    for (obs_map_t::iterator r = range.first; r != range.second; ++r) {
      rev_obs_map_t::value_type robs_val(r->second, empty_set);
      pair < rev_obs_map_t::iterator, bool > robs_ret(robs.insert(robs_val));
      std::set <std::string> &keys(robs_ret.first->second);
      keys.insert(key);
    }
  }

  // Make any pending observer callbacks
  for (rev_obs_map_t::const_iterator r = robs.begin(); r != robs.end(); ++r) {
    md_config_obs_t *obs = r->first;
    obs->handle_conf_change(this, r->second);
  }

  changed.clear();
}

void md_config_t::call_all_observers()
{
  std::map<md_config_obs_t*,std::set<std::string> > obs;
  {
    Mutex::Locker l(lock);

    expand_all_meta();

    for (obs_map_t::iterator r = observers.begin(); r != observers.end(); ++r) {
      obs[r->second].insert(r->first);
    }
  }
  for (std::map<md_config_obs_t*,std::set<std::string> >::iterator p = obs.begin();
       p != obs.end();
       ++p) {
    p->first->handle_conf_change(this, p->second);
  }
}

int md_config_t::injectargs(const std::string& s, std::ostream *oss)
{
  int ret;
  Mutex::Locker l(lock);
  char b[s.length()+1];
  strcpy(b, s.c_str());
  std::vector<const char*> nargs;
  char *p = b;
  while (*p) {
    nargs.push_back(p);
    while (*p && *p != ' ') p++;
    if (!*p)
      break;
    *p++ = 0;
    while (*p && *p == ' ') p++;
  }
  ret = parse_injectargs(nargs, oss);
  if (!nargs.empty()) {
    *oss << " failed to parse arguments: ";
    std::string prefix;
    for (std::vector<const char*>::const_iterator i = nargs.begin();
	 i != nargs.end(); ++i) {
      *oss << prefix << *i;
      prefix = ",";
    }
    *oss << "\n";
    ret = -EINVAL;
  }
  _apply_changes(oss);
  return ret;
}

void md_config_t::set_val_or_die(const char *key, const char *val)
{
  int ret = set_val(key, val);
  assert(ret == 0);
}

int md_config_t::set_val(const char *key, const char *val, bool meta, bool safe)
{
  Mutex::Locker l(lock);
  if (!key)
    return -EINVAL;
  if (!val)
    return -EINVAL;

  std::string v(val);
  if (meta)
    expand_meta(v, &std::cerr);

  string k(ConfFile::normalize_key_name(key));

  // subsystems?
  if (strncmp(k.c_str(), "debug_", 6) == 0) {
    for (int o = 0; o < subsys.get_num(); o++) {
      std::string as_option = "debug_" + subsys.get_name(o);
      if (k == as_option) {
	int log, gather;
	int r = sscanf(v.c_str(), "%d/%d", &log, &gather);
	if (r >= 1) {
	  if (r < 2)
	    gather = log;
	  //	  cout << "subsys " << subsys.get_name(o) << " log " << log << " gather " << gather << std::endl;
	  subsys.set_log_level(o, log);
	  subsys.set_gather_level(o, gather);
	  return 0;
	}
	return -EINVAL;
      }
    }	
  }

  for (int i = 0; i < NUM_CONFIG_OPTIONS; ++i) {
    config_option *opt = &config_optionsp[i];
    if (strcmp(opt->name, k.c_str()) == 0) {
      if (safe && internal_safe_to_start_threads) {
	// If threads have been started...
	if ((opt->type == OPT_STR) || (opt->type == OPT_ADDR) ||
	    (opt->type == OPT_UUID)) {
	  // And this is NOT an integer valued variable....
	  if (observers.find(opt->name) == observers.end()) {
	    // And there is no observer to safely change it...
	    // You lose.
	    return -ENOSYS;
	  }
	}
      }
      return set_val_impl(v.c_str(), opt);
    }
  }

  // couldn't find a configuration option with key 'key'
  return -ENOENT;
}


int md_config_t::get_val(const char *key, char **buf, int len) const
{
  Mutex::Locker l(lock);
  return _get_val(key, buf,len);
}

int md_config_t::_get_val(const char *key, char **buf, int len) const
{
  assert(lock.is_locked());

  if (!key)
    return -EINVAL;

  // In key names, leading and trailing whitespace are not significant.
  string k(ConfFile::normalize_key_name(key));

  for (int i = 0; i < NUM_CONFIG_OPTIONS; ++i) {
    const config_option *opt = &config_optionsp[i];
    if (strcmp(opt->name, k.c_str()))
      continue;

    ostringstream oss;
    switch (opt->type) {
      case OPT_INT:
        oss << *(int*)opt->conf_ptr(this);
        break;
      case OPT_LONGLONG:
        oss << *(long long*)opt->conf_ptr(this);
        break;
      case OPT_STR:
	oss << *((std::string*)opt->conf_ptr(this));
	break;
      case OPT_FLOAT:
        oss << *(float*)opt->conf_ptr(this);
        break;
      case OPT_DOUBLE:
        oss << *(double*)opt->conf_ptr(this);
        break;
      case OPT_BOOL: {
	  bool b = *(bool*)opt->conf_ptr(this);
	  oss << (b ? "true" : "false");
	}
        break;
      case OPT_U32:
        oss << *(uint32_t*)opt->conf_ptr(this);
        break;
      case OPT_U64:
        oss << *(uint64_t*)opt->conf_ptr(this);
        break;
      case OPT_ADDR:
        oss << *(entity_addr_t*)opt->conf_ptr(this);
        break;
      case OPT_UUID:
	oss << *(uuid_d*)opt->conf_ptr(this);
        break;
    }
    string str(oss.str());
    int l = strlen(str.c_str()) + 1;
    if (len == -1) {
      *buf = (char*)malloc(l);
      if (!*buf)
        return -ENOMEM;
      strcpy(*buf, str.c_str());
      return 0;
    }
    snprintf(*buf, len, "%s", str.c_str());
    return (l > len) ? -ENAMETOOLONG : 0;
  }

  // subsys?
  for (int o = 0; o < subsys.get_num(); o++) {
    std::string as_option = "debug_" + subsys.get_name(o);
    if (k == as_option) {
      if (len == -1) {
	*buf = (char*)malloc(20);
	len = 20;
      }
      int l = snprintf(*buf, len, "%d/%d", subsys.get_log_level(o), subsys.get_gather_level(o));
      return (l == len) ? -ENAMETOOLONG : 0;
    }
  }

  // couldn't find a configuration option with key 'k'
  return -ENOENT;
}

void md_config_t::get_all_keys(std::vector<std::string> *keys) const {
  const std::string negative_flag_prefix("no_");

  keys->clear();
  keys->reserve(NUM_CONFIG_OPTIONS);
  for (size_t i = 0; i < NUM_CONFIG_OPTIONS; ++i) {
    keys->push_back(config_optionsp[i].name);
    if (config_optionsp[i].type == OPT_BOOL) {
      keys->push_back(negative_flag_prefix + config_optionsp[i].name);
    }
  }
  for (int i = 0; i < subsys.get_num(); ++i) {
    keys->push_back("debug_" + subsys.get_name(i));
  }
}

/* The order of the sections here is important.  The first section in the
 * vector is the "highest priority" section; if we find it there, we'll stop
 * looking. The lowest priority section is the one we look in only if all
 * others had nothing.  This should always be the global section.
 */
void md_config_t::get_my_sections(std::vector <std::string> &sections) const
{
  Mutex::Locker l(lock);
  _get_my_sections(sections);
}

void md_config_t::_get_my_sections(std::vector <std::string> &sections) const
{
  assert(lock.is_locked());
  sections.push_back(name.to_str());

  sections.push_back(name.get_type_name());

  sections.push_back("global");
}

// Return a list of all sections
int md_config_t::get_all_sections(std::vector <std::string> &sections) const
{
  Mutex::Locker l(lock);
  for (ConfFile::const_section_iter_t s = cf.sections_begin();
       s != cf.sections_end(); ++s) {
    sections.push_back(s->first);
  }
  return 0;
}

int md_config_t::get_val_from_conf_file(const std::vector <std::string> &sections,
		    const char *key, std::string &out, bool emeta) const
{
  Mutex::Locker l(lock);
  return _get_val_from_conf_file(sections, key, out, emeta);
}

int md_config_t::_get_val_from_conf_file(const std::vector <std::string> &sections,
					 const char *key, std::string &out, bool emeta) const
{
  assert(lock.is_locked());
  std::vector <std::string>::const_iterator s = sections.begin();
  std::vector <std::string>::const_iterator s_end = sections.end();
  for (; s != s_end; ++s) {
    int ret = cf.read(s->c_str(), key, out);
    if (ret == 0) {
      if (emeta)
	expand_meta(out, &std::cerr);
      return 0;
    }
    else if (ret != -ENOENT)
      return ret;
  }
  return -ENOENT;
}

int md_config_t::set_val_impl(const char *val, const config_option *opt)
{
  assert(lock.is_locked());
  int ret = set_val_raw(val, opt);
  if (ret)
    return ret;
  changed.insert(opt->name);
  return 0;
}

int md_config_t::set_val_raw(const char *val, const config_option *opt)
{
  assert(lock.is_locked());
  switch (opt->type) {
    case OPT_INT: {
      std::string err;
      int f = strict_si_cast<int>(val, &err);
      if (!err.empty())
	return -EINVAL;
      *(int*)opt->conf_ptr(this) = f;
      return 0;
    }
    case OPT_LONGLONG: {
      std::string err;
      long long f = strict_si_cast<long long>(val, &err);
      if (!err.empty())
	return -EINVAL;
      *(long long*)opt->conf_ptr(this) = f;
      return 0;
    }
    case OPT_STR:
      *(std::string*)opt->conf_ptr(this) = val ? val : "";
      return 0;
    case OPT_FLOAT: {
      std::string err;
      float f = strict_strtof(val, &err);
      if (!err.empty())
	return -EINVAL;
      *(float*)opt->conf_ptr(this) = f;
      return 0;
    }
    case OPT_DOUBLE: {
      std::string err;
      double f = strict_strtod(val, &err);
      if (!err.empty())
	return -EINVAL;
      *(double*)opt->conf_ptr(this) = f;
      return 0;
    }
    case OPT_BOOL:
      if (strcasecmp(val, "false") == 0)
	*(bool*)opt->conf_ptr(this) = false;
      else if (strcasecmp(val, "true") == 0)
	*(bool*)opt->conf_ptr(this) = true;
      else {
	std::string err;
	int b = strict_strtol(val, 10, &err);
	if (!err.empty())
	  return -EINVAL;
	*(bool*)opt->conf_ptr(this) = !!b;
      }
      return 0;
    case OPT_U32: {
      std::string err;
      int f = strict_si_cast<int>(val, &err);
      if (!err.empty())
	return -EINVAL;
      *(uint32_t*)opt->conf_ptr(this) = f;
      return 0;
    }
    case OPT_U64: {
      std::string err;
      uint64_t f = strict_si_cast<uint64_t>(val, &err);
      if (!err.empty())
	return -EINVAL;
      *(uint64_t*)opt->conf_ptr(this) = f;
      return 0;
    }
    case OPT_ADDR: {
      entity_addr_t *addr = (entity_addr_t*)opt->conf_ptr(this);
      if (!addr->parse(val)) {
	return -EINVAL;
      }
      return 0;
    }
    case OPT_UUID: {
      uuid_d *u = (uuid_d*)opt->conf_ptr(this);
      if (!u->parse(val))
	return -EINVAL;
      return 0;
    }
  }
  return -ENOSYS;
}

static const char *CONF_METAVARIABLES[] = {
  "data_dir", // put this first: it may contain some of the others
  "cluster", "type", "name", "host", "num", "id", "pid", "cctid"
};
static const int NUM_CONF_METAVARIABLES =
      (sizeof(CONF_METAVARIABLES) / sizeof(CONF_METAVARIABLES[0]));

void md_config_t::expand_all_meta()
{
  // Expand all metavariables
  ostringstream oss;
  for (int i = 0; i < NUM_CONFIG_OPTIONS; i++) {
    config_option *opt = config_optionsp + i;
    if (opt->type == OPT_STR) {
      std::string *str = (std::string *)opt->conf_ptr(this);
      list<config_option *> stack;
      expand_meta(*str, opt, stack, &oss);
    }
  }
  cerr << oss.str();
}

bool md_config_t::expand_meta(std::string &origval,
			      std::ostream *oss) const
{
  list<config_option *> stack;
  return expand_meta(origval, NULL, stack, oss);
}

bool md_config_t::expand_meta(std::string &origval,
			      config_option *opt,
			      std::list<config_option *> stack,
			      std::ostream *oss) const
{
  assert(lock.is_locked());

  // no $ means no variable expansion is necessary
  if (origval.find("$") == string::npos)
    return false;

  // ignore an expansion loop and create a human readable
  // message about it
  if (opt) {
    for (list<config_option *>::iterator i = stack.begin();
	 i != stack.end();
	 ++i) {
      if (strcmp(opt->name, (*i)->name) == 0) {
	*oss << "variable expansion loop at "
	     << opt->name << "=" << origval << std::endl;
	*oss << "expansion stack: " << std::endl;
	for (list<config_option *>::iterator j = stack.begin();
	     j != stack.end();
	     ++j) {
	  *oss << (*j)->name << "=" << *(string *)(*j)->conf_ptr(this) << std::endl;
	}
	return false;
      }
    }
  }

  if (opt)
    stack.push_front(opt);

  bool found_meta = false;
  string out;
  string val = origval;
  for (string::size_type s = 0; s < val.size(); ) {
    if (val[s] != '$') {
      out += val[s++];
      continue;
    }

    // try to parse the variable name into var, either \$\{(.+)\} or
    // \$[a-z\_]+
    const char *valid_chars = "abcdefghijklmnopqrstuvwxyz_";
    string var;
    size_t endpos = 0;
    if (val[s+1] == '{') {
      // ...${foo_bar}...
      endpos = val.find_first_not_of(valid_chars, s+2);
      if (endpos != std::string::npos &&
	  val[endpos] == '}') {
	var = val.substr(s+2, endpos-s-2);
	endpos++;
      }
    } else {
      // ...$foo...
      endpos = val.find_first_not_of(valid_chars, s+1);
      if (endpos != std::string::npos)
	var = val.substr(s+1, endpos-s-1);
      else
	var = val.substr(s+1);
    }

    bool expanded = false;
    if (var.length()) {
      // special metavariable?
      for (int i = 0; i < NUM_CONF_METAVARIABLES; ++i) {
	if (var != CONF_METAVARIABLES[i])
	  continue;
	//cout << "  meta match of " << var << " " << CONF_METAVARIABLES[i] << std::endl;
	if (var == "type")
	  out += name.get_type_name();
	else if (var == "cluster")
	  out += cluster;
	else if (var == "name")
	  out += name.to_cstr();
	else if (var == "host")
	  out += host;
	else if (var == "num")
	  out += name.get_id().c_str();
	else if (var == "id")
	  out += name.get_id().c_str();
	else if (var == "pid")
	  out += stringify(getpid());
	else if (var == "cctid")
	  out += stringify((unsigned long long)this);
	else if (var == "data_dir") {
	  if (data_dir_option.length()) {
	    char *vv = NULL;
	    _get_val(data_dir_option.c_str(), &vv, -1);
	    string tmp = vv;
	    free(vv);
	    expand_meta(tmp, NULL, stack, oss);
	    out += tmp;
	  } else {
	    // this isn't really right, but it'll result in a mangled
	    // non-existent path that will fail any search list
	    out += "$data_dir";
	  }
	} else
	  assert(0); // unreachable
	expanded = true;
      }

      if (!expanded) {
	// config option?
	for (int i = 0; i < NUM_CONFIG_OPTIONS; i++) {
	  config_option *opt = &config_optionsp[i];
	  if (var == opt->name) {
	    if (opt->type == OPT_STR) {
	      string *origval = (string *)opt->conf_ptr(this);
	      expand_meta(*origval, opt, stack, oss);
	      out += *origval;
	    } else {
	      char *vv = NULL;
	      _get_val(opt->name, &vv, -1);
	      out += vv;
	      free(vv);
	    }
	    expanded = true;
	    break;
	  }
	}
      }
    }

    if (expanded) {
      found_meta = true;
      s = endpos;
    } else {
      out += val[s++];
    }
  }
  // override the original value with the expanded value
  origval = out;
  return found_meta;
}

void md_config_t::diff(
    const md_config_t *other,
    map<string,pair<string,string> > *diff,
    set<string> *unknown)
{
  Mutex::Locker l(lock);

  char local_buf[4096];
  char other_buf[4096];
  for (int i = 0; i < NUM_CONFIG_OPTIONS; i++) {
    config_option *opt = &config_optionsp[i];
    memset(local_buf, 0, sizeof(local_buf));
    memset(other_buf, 0, sizeof(other_buf));

    char *other_val = other_buf;
    int err = other->get_val(opt->name, &other_val, sizeof(other_buf));
    if (err < 0) {
      if (err == -ENOENT) {
        unknown->insert(opt->name);
      }
      continue;
    }

    char *local_val = local_buf;
    err = _get_val(opt->name, &local_val, sizeof(local_buf));
    if (err != 0)
      continue;

    if (strcmp(local_val, other_val))
      diff->insert(make_pair(opt->name, make_pair(local_val, other_val)));
  }
}

void md_config_t::complain_about_parse_errors(CephContext *cct)
{
  ::complain_about_parse_errors(cct, &parse_errors);
}
