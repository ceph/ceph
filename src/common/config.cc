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

#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/config.h"
#include "include/str_list.h"
#include "include/stringify.h"
#include "osd/osd_types.h"
#include "common/errno.h"
#include "common/hostname.h"
#include "common/backport14.h"

#include <boost/type_traits.hpp>

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
using std::ostringstream;
using std::pair;
using std::string;

const char *CEPH_CONF_FILE_DEFAULT = "$data_dir/config, /etc/ceph/$cluster.conf, ~/.ceph/$cluster.conf, $cluster.conf"
#if defined(__FreeBSD__)
    ", /usr/local/etc/ceph/$cluster.conf"
#endif
    ;

#define _STR(x) #x
#define STRINGIFY(x) _STR(x)

int ceph_resolve_file_search(const std::string& filename_list,
			     std::string& result)
{
  list<string> ls;
  get_str_list(filename_list, ls);

  int ret = -ENOENT;
  list<string>::iterator iter;
  for (iter = ls.begin(); iter != ls.end(); ++iter) {
    int fd = ::open(iter->c_str(), O_RDONLY|O_CLOEXEC);
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



md_config_t::md_config_t(bool is_daemon)
  : cluster(""),
  lock("md_config_t", true, false)
{
  init_subsys();

  // Load the compile-time list of Option into
  // a map so that we can resolve keys quickly.
  for (const auto &i : ceph_options) {
    if (schema.count(i.name)) {
      // We may be instantiated pre-logging so send 
      std::cerr << "Duplicate config key in schema: '" << i.name << "'"
                << std::endl;
      assert(false);
    }
    schema.insert({i.name, i});
  }

  // Populate list of legacy_values according to the OPTION() definitions
  // Note that this is just setting up our map of name->member ptr.  The
  // default values etc will get loaded in along with new-style data,
  // as all loads write to both the values map, and the legacy
  // members if present.
  legacy_values = {
#define OPTION(name, type) \
    {std::string(STRINGIFY(name)), &md_config_t::name},
#define SAFE_OPTION(name, type) OPTION(name, type)
#include "common/legacy_config_opts.h"
#undef OPTION
#undef SAFE_OPTION
  };

  validate_schema();

  // Load default values from the schema
  for (const auto &i : schema) {
    const Option &opt = i.second;
    bool has_daemon_default = !boost::get<boost::blank>(&opt.daemon_value);
    Option::value_t default_val;
    if (is_daemon && has_daemon_default) {
      default_val = opt.daemon_value;
    } else {
      default_val = opt.value;
    }

    if (opt.type == Option::TYPE_STR) {
      // We call pre_validate as a sanity check, but also to get any
      // side effect (value modification) from the validator.
      std::string *def_str = boost::get<std::string>(&default_val);
      std::string err;
      if (opt.pre_validate(def_str, &err) != 0) {
        std::cerr << "Default value " << opt.name << "=" << *def_str << " is "
                     "invalid: " << err << std::endl;

        // This is the compiled-in default that is failing its own option's
        // validation, so this is super-invalid and should never make it
        // past a pull request: crash out.
        assert(false);
      }
    }

    values[i.first] = default_val;
  }

  // Copy out values (defaults) into any legacy (C struct member) fields
  for (const auto &i : legacy_values) {
    const auto &name = i.first;
    const auto &option = schema.at(name);
    auto ptr = i.second;

    update_legacy_val(option, ptr);
  }
}

/**
 * Sanity check schema.  Assert out on failures, to ensure any bad changes
 * cannot possibly pass any testing and make it into a release.
 */
void md_config_t::validate_schema()
{
  for (const auto &i : schema) {
    const auto &opt = i.second;
    for (const auto &see_also_key : opt.see_also) {
      if (schema.count(see_also_key) == 0) {
        std::cerr << "Non-existent see-also key '" << see_also_key
                  << "' on option '" << opt.name << "'" << std::endl;
        assert(false);
      }
    }
  }

  for (const auto &i : legacy_values) {
    if (schema.count(i.first) == 0) {
      std::cerr << "Schema is missing legacy field '" << i.first << "'"
                << std::endl;
      assert(false);
    }
  }
}

void md_config_t::init_subsys()
{
#define SUBSYS(name, log, gather) \
  subsys.add(ceph_subsys_##name, STRINGIFY(name), log, gather);
#define DEFAULT_SUBSYS(log, gather) \
  subsys.add(ceph_subsys_, "none", log, gather);
#include "common/subsys.h"
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
  obs_call_gate.emplace(observer_, ceph::make_unique<CallGate>());
}

void md_config_t::remove_observer(md_config_obs_t* observer_)
{
  Mutex::Locker l(lock);

  call_gate_close(observer_);
  obs_call_gate.erase(observer_);

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
	list<const Option *> stack;
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
  // it must have been all ENOENTs, that's the only way we got here
  if (c == conf_files.end())
    return -ENOENT;

  if (cluster.size() == 0) {
    /*
     * If cluster name is not set yet, use the prefix of the
     * basename of configuration file as cluster name.
     */
    auto start = c->rfind('/') + 1;
    auto end = c->find(".conf", start);
    if (end == c->npos) {
        /*
         * If the configuration file does not follow $cluster.conf
         * convention, we do the last try and assign the cluster to
         * 'ceph'.
         */
        cluster = "ceph";
    } else {
      cluster = c->substr(start, end - start);
    }
  }

  std::vector <std::string> my_sections;
  _get_my_sections(my_sections);
  for (const auto &i : schema) {
    const auto &opt = i.second;
    std::string val;
    int ret = _get_val_from_conf_file(my_sections, opt.name, val, false);
    if (ret == 0) {
      std::string error_message;
      int r = set_val_impl(val, opt, &error_message);
      if (warnings != nullptr && (r != 0 || !error_message.empty())) {
        *warnings << "parse error setting '" << opt.name << "' to '" << val
                  << "'";
        if (!error_message.empty()) {
          *warnings << " (" << error_message << ")";
        }
        *warnings << std::endl;
      }
    }
  }

  // subsystems?
  for (size_t o = 0; o < subsys.get_num(); o++) {
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

void md_config_t::config_options(Formatter *f)
{
  Mutex::Locker l(lock);
  f->open_array_section("options");
  for (const auto& i: schema) {
    const Option &opt = i.second;
    opt.dump(f);
  }
  f->close_section();
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
  for (size_t o = 0; o < subsys.get_num(); o++) {
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
  for (const auto& i: schema) {
    const Option &opt = i.second;
    char *buf;
    _get_val(opt.name, &buf, -1);
    if (out)
      *out << opt.name << " = " << buf << std::endl;
    if (f)
      f->dump_string(opt.name.c_str(), buf);
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
      int r = parse_option(args, i, NULL);
      if (r < 0) {
        return r;
      }
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
  size_t o = 0;
  std::string val;

  // subsystems?
  for (o = 0; o < subsys.get_num(); o++) {
    std::string as_option("--");
    as_option += "debug_";
    as_option += subsys.get_name(o);
    ostringstream err;
    if (ceph_argparse_witharg(args, i, &val, err,
			      as_option.c_str(), (char*)NULL)) {
      if (err.tellp()) {
        if (oss) {
          *oss << err.str();
        }
        ret = -EINVAL;
        break;
      }
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

  std::string option_name;
  std::string error_message;
  o = 0;
  for (const auto& opt_iter: schema) {
    const Option &opt = opt_iter.second;
    ostringstream err;
    std::string as_option("--");
    as_option += opt.name;
    option_name = opt.name;
    if (opt.type == Option::TYPE_BOOL) {
      int res;
      if (ceph_argparse_binary_flag(args, i, &res, oss, as_option.c_str(),
				    (char*)NULL)) {
	if (res == 0)
	  ret = set_val_impl("false", opt, &error_message);
	else if (res == 1)
	  ret = set_val_impl("true", opt, &error_message);
	else
	  ret = res;
	break;
      } else {
	std::string no("--no-");
	no += opt.name;
	if (ceph_argparse_flag(args, i, no.c_str(), (char*)NULL)) {
	  ret = set_val_impl("false", opt, &error_message);
	  break;
	}
      }
    } else if (ceph_argparse_witharg(args, i, &val, err,
                                     as_option.c_str(), (char*)NULL)) {
      if (!err.str().empty()) {
        error_message = err.str();
	ret = -EINVAL;
	break;
      }
      if (oss && ((!opt.is_safe()) &&
		  (observers.find(opt.name) == observers.end()))) {
	*oss << "You cannot change " << opt.name << " using injectargs.\n";
        return -ENOSYS;
      }
      ret = set_val_impl(val, opt, &error_message);
      break;
    }
    ++o;
  }

  if (ret != 0 || !error_message.empty()) {
    assert(!option_name.empty());
    if (oss) {
      *oss << "Parse error setting " << option_name << " to '"
           << val << "' using injectargs";
      if (!error_message.empty()) {
        *oss << " (" << error_message << ")";
      }
      *oss << ".\n";
    } else {
      cerr << "parse error setting '" << option_name << "' to '"
	   << val << "'";
      if (!error_message.empty()) {
        cerr << " (" << error_message << ")";
      }
      cerr << "\n" << std::endl;
    }
  }

  if (o == schema.size()) {
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
  Mutex::Locker locker(lock);
  rev_obs_map_t rev_obs;

  /*
   * apply changes until the cluster name is assigned
   */
  if (cluster.size()) {
    for_each_change(
      oss, [this, &rev_obs](md_config_obs_t *obs, const std::string &key) {
        map_observer_changes(obs, key, &rev_obs);
      });
  }

  call_observers(rev_obs);
}

bool md_config_t::_internal_field(const string& s)
{
  if (s == "internal_safe_to_start_threads")
    return true;
  return false;
}

void md_config_t::for_each_change(std::ostream *oss, config_gather_cb callback)
{
  expand_all_meta();

  // expand_all_meta could have modified anything.  Copy it all out again.
  for (const auto &i : legacy_values) {
    const auto &name = i.first;
    const auto &option = schema.at(name);
    auto ptr = i.second;

    update_legacy_val(option, ptr);
  }

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
	(*oss) << "(not observed, change may require restart) ";
      }
    }
    for (obs_map_t::iterator r = range.first; r != range.second; ++r) {
      callback(r->second, key);
    }
  }

  changed.clear();
}

void md_config_t::call_all_observers()
{
  Mutex::Locker locker(lock);
  rev_obs_map_t rev_obs;

  expand_all_meta();

  for (auto r = observers.begin(); r != observers.end(); ++r) {
    map_observer_changes(r->second, r->first, &rev_obs);
  }

  call_observers(rev_obs);
}

int md_config_t::injectargs(const std::string& s, std::ostream *oss)
{
  Mutex::Locker locker(lock);
  rev_obs_map_t rev_obs;

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

  int ret = parse_injectargs(nargs, oss);
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

  for_each_change(
    oss, [this, &rev_obs](md_config_obs_t *obs, const std::string &key) {
      map_observer_changes(obs, key, &rev_obs);
    });

  call_observers(rev_obs);
  return ret;
}

void md_config_t::set_val_or_die(const std::string &key,
                                 const std::string &val,
                                 bool meta)
{
  std::stringstream err;
  int ret = set_val(key, val, meta, &err);
  if (ret != 0) {
    std::cerr << "set_val_or_die(" << key << "): " << err.str();
  }
  assert(ret == 0);
}

int md_config_t::set_val(const std::string &key, const char *val,
    bool meta, std::stringstream *err_ss)
{
  Mutex::Locker l(lock);
  if (key.empty()) {
    if (err_ss) *err_ss << "No key specified";
    return -EINVAL;
  }
  if (!val) {
    return -EINVAL;
  }

  std::string v(val);
  if (meta)
    expand_meta(v, &std::cerr);

  string k(ConfFile::normalize_key_name(key));

  // subsystems?
  if (strncmp(k.c_str(), "debug_", 6) == 0) {
    for (size_t o = 0; o < subsys.get_num(); o++) {
      std::string as_option = "debug_" + subsys.get_name(o);
      if (k == as_option) {
	int log, gather;
	int r = sscanf(v.c_str(), "%d/%d", &log, &gather);
	if (r >= 1) {
	  if (r < 2) {
	    gather = log;
          }
	  subsys.set_log_level(o, log);
	  subsys.set_gather_level(o, gather);
          if (err_ss) *err_ss << "Set " << k << " to " << log << "/" << gather;
	  return 0;
	}
        if (err_ss) {
          *err_ss << "Invalid debug level, should be <int> or <int>/<int>";
        }
	return -EINVAL;
      }
    }	
  }

  const auto &opt_iter = schema.find(k);
  if (opt_iter != schema.end()) {
    const Option &opt = opt_iter->second;
    if ((!opt.is_safe()) && internal_safe_to_start_threads) {
      // If threads have been started and the option is not thread safe
      if (observers.find(opt.name) == observers.end()) {
        // And there is no observer to safely change it...
        // You lose.
        if (err_ss) *err_ss << "Configuration option '" << key << "' may "
                    "not be modified at runtime";
        return -ENOSYS;
      }
    }

    std::string error_message;
    int r = set_val_impl(v, opt, &error_message);
    if (r == 0) {
      if (err_ss) *err_ss << "Set " << opt.name << " to " << v;
    } else {
      if (err_ss) *err_ss << error_message;
    }
    return r;
  }

  if (err_ss) *err_ss << "Configuration option not found: '" << key << "'";
  return -ENOENT;
}


int md_config_t::get_val(const std::string &key, char **buf, int len) const
{
  Mutex::Locker l(lock);
  return _get_val(key, buf,len);
}

Option::value_t md_config_t::get_val_generic(const std::string &key) const
{
  Mutex::Locker l(lock);
  return _get_val(key);
}

Option::value_t md_config_t::_get_val(const std::string &key) const
{
  assert(lock.is_locked());

  if (key.empty()) {
    return Option::value_t(boost::blank());
  }

  // In key names, leading and trailing whitespace are not significant.
  string k(ConfFile::normalize_key_name(key));

  const auto &opt_iter = schema.find(k);
  if (opt_iter != schema.end()) {
    // Using .at() is safe because all keys in the schema always have
    // entries in ::values
    return values.at(k);
  } else {
    return Option::value_t(boost::blank());
  }
}

int md_config_t::_get_val(const std::string &key, std::string *value) const {
  assert(lock.is_locked());

  std::string normalized_key(ConfFile::normalize_key_name(key));
  Option::value_t config_value = _get_val(normalized_key.c_str());
  if (!boost::get<boost::blank>(&config_value)) {
    ostringstream oss;
    if (bool *flag = boost::get<bool>(&config_value)) {
      oss << (*flag ? "true" : "false");
    } else if (double *dp = boost::get<double>(&config_value)) {
      oss << std::fixed << *dp;
    } else {
      oss << config_value;
    }
    *value = oss.str();
    return 0;
  }
  return -ENOENT;
}

int md_config_t::_get_val(const std::string &key, char **buf, int len) const
{
  assert(lock.is_locked());

  if (key.empty())
    return -EINVAL;

  string val ;
  if (_get_val(key, &val) == 0) {
    int l = val.length() + 1;
    if (len == -1) {
      *buf = (char*)malloc(l);
      if (!*buf)
        return -ENOMEM;
      strncpy(*buf, val.c_str(), l);
      return 0;
    }
    snprintf(*buf, len, "%s", val.c_str());
    return (l > len) ? -ENAMETOOLONG : 0;
  }

  string k(ConfFile::normalize_key_name(key));
  // subsys?
  for (size_t o = 0; o < subsys.get_num(); o++) {
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
  keys->reserve(schema.size());
  for (const auto &i: schema) {
    const Option &opt = i.second;
    keys->push_back(opt.name);
    if (opt.type == Option::TYPE_BOOL) {
      keys->push_back(negative_flag_prefix + opt.name);
    }
  }
  for (size_t i = 0; i < subsys.get_num(); ++i) {
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
		    const std::string &key, std::string &out, bool emeta) const
{
  Mutex::Locker l(lock);
  return _get_val_from_conf_file(sections, key, out, emeta);
}

int md_config_t::_get_val_from_conf_file(const std::vector <std::string> &sections,
					 const std::string &key, std::string &out, bool emeta) const
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

int md_config_t::set_val_impl(const std::string &raw_val, const Option &opt,
                              std::string *error_message)
{
  assert(lock.is_locked());

  std::string val = raw_val;

  int r = opt.pre_validate(&val, error_message);
  if (r != 0) {
    return r;
  }

  Option::value_t new_value;
  if (opt.type == Option::TYPE_INT) {
    int64_t f = strict_si_cast<int64_t>(val.c_str(), error_message);
    if (!error_message->empty()) {
      return -EINVAL;
    }
    new_value = f;
  } else if (opt.type == Option::TYPE_UINT) {
    uint64_t f = strict_si_cast<uint64_t>(val.c_str(), error_message);
    if (!error_message->empty()) {
      return -EINVAL;
    }
    new_value = f;
  } else if (opt.type == Option::TYPE_STR) {
    new_value = val;
  } else if (opt.type == Option::TYPE_FLOAT) {
    double f = strict_strtod(val.c_str(), error_message);
    if (!error_message->empty()) {
      return -EINVAL;
    } else {
      new_value = f;
    }
  } else if (opt.type == Option::TYPE_BOOL) {
    if (strcasecmp(val.c_str(), "false") == 0) {
      new_value = false;
    } else if (strcasecmp(val.c_str(), "true") == 0) {
      new_value = true;
    } else {
      int b = strict_strtol(val.c_str(), 10, error_message);
      if (!error_message->empty()) {
	return -EINVAL;
      }
      new_value = !!b;
    }
  } else if (opt.type == Option::TYPE_ADDR) {
    entity_addr_t addr;
    if (!addr.parse(val.c_str())){
      return -EINVAL;
    }
    new_value = addr;
  } else if (opt.type == Option::TYPE_UUID) {
    uuid_d uuid;
    if (!uuid.parse(val.c_str())) {
      return -EINVAL;
    }
    new_value = uuid;
  } else {
    ceph_abort();
  }

  r = opt.validate(new_value, error_message);
  if (r != 0) {
    return r;
  }


  // Apply the value to its entry in the `values` map
  values[opt.name] = new_value;

  // Apply the value to its legacy field, if it has one
  auto legacy_ptr_iter = legacy_values.find(std::string(opt.name));
  if (legacy_ptr_iter != legacy_values.end()) {
    update_legacy_val(opt, legacy_ptr_iter->second);
  }

  changed.insert(opt.name);
  return 0;
}

/**
 * Handles assigning from a variant-of-types to a variant-of-pointers-to-types
 */
class assign_visitor : public boost::static_visitor<>
{
  md_config_t *conf;
  Option::value_t val;
  public:

  assign_visitor(md_config_t *conf_, Option::value_t val_)
    : conf(conf_), val(val_)
  {}

  template <typename T>
  void operator()( T md_config_t::* ptr) const
  {
    T *member = const_cast<T *>(&(conf->*(boost::get<const T md_config_t::*>(ptr))));

    *member = boost::get<T>(val);
  }
};

void md_config_t::update_legacy_val(const Option &opt,
                                    md_config_t::member_ptr_t member_ptr)
{
  if (boost::get<boost::blank>(&values.at(opt.name))) {
    // This shouldn't happen, but if it does then just don't even
    // try to assign to the legacy field.
    return;
  }

  boost::apply_visitor(assign_visitor(this, values.at(opt.name)), member_ptr);
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
  for (const auto &i : schema) {
    const Option &opt = i.second;

    if (opt.type == Option::TYPE_STR) {
      list<const Option*> stack;
      std::string *str = boost::get<std::string>(&(values.at(opt.name)));
      assert(str != nullptr);  // Non-string values should never get in
      expand_meta(*str, &opt, stack, &oss);
    }
  }
  cerr << oss.str();
}

bool md_config_t::expand_meta(std::string &val,
			      std::ostream *oss) const
{
  list<const Option*> stack;
  return expand_meta(val, NULL, stack, oss);
}

bool md_config_t::expand_meta(std::string &origval,
			      const Option *opt,
			      std::list<const Option *> stack,
			      std::ostream *oss) const
{
  assert(lock.is_locked());

  // no $ means no variable expansion is necessary
  if (origval.find("$") == string::npos)
    return false;

  // ignore an expansion loop and create a human readable
  // message about it
  if (opt) {
    for (const auto stack_ptr : stack) {
      if (opt->name == stack_ptr->name) {
	*oss << "variable expansion loop at "
	     << opt->name << "=" << origval << std::endl;
	*oss << "expansion stack: " << std::endl;
	for (const auto j : stack) {
          std::string val;
          _get_val(j->name, &val);
	  *oss << j->name << "=" << val << std::endl;
	}
	return false;
      }
    }

    stack.push_front(opt);
  }

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
        {
          if (host == "")
            out += ceph_get_short_hostname();
          else
	    out += host;
        }
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
	  ceph_abort(); // unreachable
	expanded = true;
      }

      if (!expanded) {
	// config option?
        const auto other_opt_iter = schema.find(var);
        if (other_opt_iter != schema.end()) {
          const Option &other_opt = other_opt_iter->second;
          if (other_opt.type == Option::TYPE_STR) {
            // The referenced option is a string, it may need substitution
            // before inserting.
            Option::value_t *other_val_ptr = const_cast<Option::value_t*>(&(values.at(other_opt.name)));
            std::string *other_opt_val = boost::get<std::string>(other_val_ptr);
            expand_meta(*other_opt_val, &other_opt, stack, oss);
            out += *other_opt_val;
          } else {
            // The referenced option is not a string: retrieve and insert
            // its stringized form.
            char *vv = NULL;
            _get_val(other_opt.name, &vv, -1);
            out += vv;
            free(vv);
          }
          expanded = true;
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
  map<string, pair<string, string> > *diff,
  set<string> *unknown) 
{
  diff_helper(other, diff, unknown);
}
void md_config_t::diff(
  const md_config_t *other,
  map<string, pair<string, string> > *diff,
  set<string> *unknown, const string& setting) 
{
  diff_helper(other, diff, unknown, setting);
}

void md_config_t::diff_helper(
    const md_config_t *other,
    map<string,pair<string,string> > *diff,
    set<string> *unknown, const string& setting)
{
  Mutex::Locker l(lock);

  char local_buf[4096];
  char other_buf[4096];
  for (const auto &i : schema) {
    const Option &opt = i.second;
    if (!setting.empty()) {
      if (setting != opt.name) {
        continue;
      }
    }
    memset(local_buf, 0, sizeof(local_buf));
    memset(other_buf, 0, sizeof(other_buf));

    char *other_val = other_buf;
    int err = other->get_val(opt.name, &other_val, sizeof(other_buf));
    if (err < 0) {
      if (err == -ENOENT) {
        unknown->insert(opt.name);
      }
      continue;
    }

    char *local_val = local_buf;
    err = _get_val(opt.name, &local_val, sizeof(local_buf));
    if (err != 0)
      continue;

    if (strcmp(local_val, other_val))
      diff->insert(make_pair(opt.name, make_pair(local_val, other_val)));
    else if (!setting.empty()) {
        diff->insert(make_pair(opt.name, make_pair(local_val, other_val)));
        break;
    }
  }
}

void md_config_t::complain_about_parse_errors(CephContext *cct)
{
  ::complain_about_parse_errors(cct, &parse_errors);
}

void md_config_t::call_observers(rev_obs_map_t &rev_obs) {
  // observers are notified outside of lock
  ceph_assert(lock.is_locked());
  lock.Unlock();
  for (auto p : rev_obs) {
    p.first->handle_conf_change(this, p.second);
  }
  lock.Lock();

  for (auto& p : rev_obs) {
    call_gate_leave(p.first);
  }
}

void md_config_t::map_observer_changes(md_config_obs_t *obs, const std::string &key,
                                       rev_obs_map_t *rev_obs) {
  ceph_assert(lock.is_locked());

  auto p = rev_obs->emplace(obs, std::set<std::string>{});

  p.first->second.emplace(key);
  if (p.second) {
    // this needs to be done under lock as once this lock is
    // dropped (before calling observers) a remove_observer()
    // can sneak in and cause havoc.
    call_gate_enter(p.first->first);
  }
}
