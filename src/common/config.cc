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

#include <boost/type_traits.hpp>

#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/config.h"
#include "include/str_list.h"
#include "include/stringify.h"
#include "osd/osd_types.h"
#include "common/errno.h"
#include "common/hostname.h"
#include "common/dout.h"

/* Don't use standard Ceph logging in this file.
 * We can't use logging until it's initialized, and a lot of the necessary
 * initialization happens here.
 */
#undef dout
#undef pdout
#undef derr
#undef generic_dout

// set set_mon_vals()
#define dout_subsys ceph_subsys_monc

using std::map;
using std::list;
using std::ostringstream;
using std::pair;
using std::string;

static const char *CEPH_CONF_FILE_DEFAULT = "$data_dir/config, /etc/ceph/$cluster.conf, ~/.ceph/$cluster.conf, $cluster.conf"
#if defined(__FreeBSD__)
    ", /usr/local/etc/ceph/$cluster.conf"
#endif
    ;

#define _STR(x) #x
#define STRINGIFY(x) _STR(x)

const char *ceph_conf_level_name(int level)
{
  switch (level) {
  case CONF_DEFAULT: return "default";   // built-in default
  case CONF_MON: return "mon";           // monitor config database
  case CONF_ENV: return "env";           // process environment (CEPH_ARGS)
  case CONF_FILE: return "file";         // ceph.conf file
  case CONF_CMDLINE: return "cmdline";   // process command line args
  case CONF_OVERRIDE: return "override"; // injectargs or 'config set' at runtime
  case CONF_FINAL: return "final";
  default: return "???";
  }
}

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

static int conf_stringify(const Option::value_t& v, string *out)
{
  if (boost::get<boost::blank>(&v)) {
    return -ENOENT;
  }
  *out = Option::to_str(v);
  return 0;
}

md_config_t::md_config_t(bool is_daemon)
  : is_daemon(is_daemon),
    cluster(""),
    lock("md_config_t", true, false)
{
  // Load the compile-time list of Option into
  // a map so that we can resolve keys quickly.
  for (const auto &i : ceph_options) {
    if (schema.count(i.name)) {
      // We may be instantiated pre-logging so send 
      std::cerr << "Duplicate config key in schema: '" << i.name << "'"
                << std::endl;
      ceph_abort();
    }
    schema.emplace(std::piecewise_construct,
		   std::forward_as_tuple(i.name),
		   std::forward_as_tuple(i));
  }

  // Define the debug_* options as well.
  subsys_options.reserve(subsys.get_num());
  for (unsigned i = 0; i < subsys.get_num(); ++i) {
    string name = string("debug_") + subsys.get_name(i);
    subsys_options.push_back(
      Option(name, Option::TYPE_STR, Option::LEVEL_ADVANCED));
    Option& opt = subsys_options.back();
    opt.set_default(stringify(subsys.get_log_level(i)) + "/" +
		    stringify(subsys.get_gather_level(i)));
    string desc = string("Debug level for ") + subsys.get_name(i);
    opt.set_description(desc.c_str());
    opt.set_flag(Option::FLAG_RUNTIME);
    opt.set_long_description("The value takes the form 'N' or 'N/M' where N and M are values between 0 and 99.  N is the debug level to log (all values below this are included), and M is the level to gather and buffer in memory.  In the event of a crash, the most recent items <= M are dumped to the log file.");
    opt.set_subsys(i);
    opt.set_validator([](std::string *value, std::string *error_message) {
	int m, n;
	int r = sscanf(value->c_str(), "%d/%d", &m, &n);
	if (r >= 1) {
	  if (m < 0 || m > 99) {
	    *error_message = "value must be in range [0, 99]";
	    return -ERANGE;
	  }
	  if (r == 2) {
	    if (n < 0 || n > 99) {
	      *error_message = "value must be in range [0, 99]";
	      return -ERANGE;
	    }
	  } else {
	    // normalize to M/N
	    n = m;
	    *value = stringify(m) + "/" + stringify(n);
	  }
	} else {
	  *error_message = "value must take the form N or N/M, where N and M are integers";
	  return -EINVAL;
	}
	return 0;
      });
  }
  for (auto& opt : subsys_options) {
    schema.emplace(opt.name, opt);
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

  // Validate default values from the schema
  for (const auto &i : schema) {
    const Option &opt = i.second;
    if (opt.type == Option::TYPE_STR) {
      bool has_daemon_default = !boost::get<boost::blank>(&opt.daemon_value);
      Option::value_t default_val;
      if (is_daemon && has_daemon_default) {
	default_val = opt.daemon_value;
      } else {
	default_val = opt.value;
      }
      // We call pre_validate as a sanity check, but also to get any
      // side effect (value modification) from the validator.
      std::string *def_str = boost::get<std::string>(&default_val);
      std::string val = *def_str;
      std::string err;
      if (opt.pre_validate(&val, &err) != 0) {
        std::cerr << "Default value " << opt.name << "=" << *def_str << " is "
                     "invalid: " << err << std::endl;

        // This is the compiled-in default that is failing its own option's
        // validation, so this is super-invalid and should never make it
        // past a pull request: crash out.
        ceph_abort();
      }
      if (val != *def_str) {
	// if the validator normalizes the string into a different form than
	// what was compiled in, use that.
	set_val_default(opt.name, val);
      }
    }
  }

  // Copy out values (defaults) into any legacy (C struct member) fields
  update_legacy_vals();
}

md_config_t::~md_config_t()
{
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
        ceph_abort();
      }
    }
  }

  for (const auto &i : legacy_values) {
    if (schema.count(i.first) == 0) {
      std::cerr << "Schema is missing legacy field '" << i.first << "'"
                << std::endl;
      ceph_abort();
    }
  }
}

const Option *md_config_t::find_option(const string& name) const
{
  auto p = schema.find(name);
  if (p != schema.end()) {
    return &p->second;
  }
  return nullptr;
}

void md_config_t::set_val_default(const string& name, const std::string& val)
{
  Mutex::Locker l(lock);
  const Option *o = find_option(name);
  assert(o);
  string err;
  int r = _set_val(val, *o, CONF_DEFAULT, &err);
  assert(r >= 0);
}

int md_config_t::set_mon_vals(CephContext *cct, const map<string,string>& kv)
{
  Mutex::Locker l(lock);
  ignored_mon_values.clear();
  for (auto& i : kv) {
    const Option *o = find_option(i.first);
    if (!o) {
      ldout(cct,10) << __func__ << " " << i.first << " = " << i.second
		    << " (unrecognized option)" << dendl;
      continue;
    }
    if (o->has_flag(Option::FLAG_NO_MON_UPDATE)) {
      ignored_mon_values.emplace(i);
      continue;
    }
    std::string err;
    int r = _set_val(i.second, *o, CONF_MON, &err);
    if (r < 0) {
      lderr(cct) << __func__ << " failed to set " << i.first << " = "
		 << i.second << ": " << err << dendl;
      ignored_mon_values.emplace(i);
    } else if (r == 0) {
      ldout(cct,20) << __func__ << " " << i.first << " = " << i.second
		    << " (no change)" << dendl;
    } else if (r == 1) {
      ldout(cct,10) << __func__ << " " << i.first << " = " << i.second << dendl;
    } else {
      ceph_abort();
    }
  }
  for (const auto& [name,configs] : values) {
    auto j = configs.find(CONF_MON);
    if (j != configs.end()) {
      if (kv.find(name) == kv.end()) {
	ldout(cct,10) << __func__ << " " << name
		      << " cleared (was " << Option::to_str(j->second) << ")"
		      << dendl;
	_rm_val(name, CONF_MON);
      }
    }
  }
  values_bl.clear();
  return 0;
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

int md_config_t::parse_config_files(const char *conf_files_str,
				    std::ostream *warnings,
				    int flags)
{
  Mutex::Locker l(lock);

  if (safe_to_start_threads)
    return -ENOSYS;

  if (!cluster.size() && !conf_files_str) {
    /*
     * set the cluster name to 'ceph' when neither cluster name nor
     * configuration file are specified.
     */
    cluster = "ceph";
  }

  if (!conf_files_str) {
    const char *c = getenv("CEPH_CONF");
    if (c) {
      conf_files_str = c;
    }
    else {
      if (flags & CINIT_FLAG_NO_DEFAULT_CONFIG_FILE)
	return 0;
      conf_files_str = CEPH_CONF_FILE_DEFAULT;
    }
  }

  std::list<std::string> conf_files;
  get_str_list(conf_files_str, conf_files);
  auto p = conf_files.begin();
  while (p != conf_files.end()) {
    string &s = *p;
    if (s.find("$data_dir") != string::npos &&
	data_dir_option.empty()) {
      // useless $data_dir item, skip
      p = conf_files.erase(p);
    } else {
      early_expand_meta(s, warnings);
      ++p;
    }
  }

  // open new conf
  list<string>::const_iterator c;
  for (c = conf_files.begin(); c != conf_files.end(); ++c) {
    cf.clear();
    string fn = *c;

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
    int ret = _get_val_from_conf_file(my_sections, opt.name, val);
    if (ret == 0) {
      std::string error_message;
      int r = _set_val(val, opt, CONF_FILE, &error_message);
      if (warnings != nullptr && (r < 0 || !error_message.empty())) {
        *warnings << "parse error setting '" << opt.name << "' to '" << val
                  << "'";
        if (!error_message.empty()) {
          *warnings << " (" << error_message << ")";
        }
        *warnings << std::endl;
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

  update_legacy_vals();

  return 0;
}

void md_config_t::parse_env(const char *args_var)
{
  if (safe_to_start_threads)
    return;
  if (!args_var) {
    args_var = "CEPH_ARGS";
  }
  if (getenv("CEPH_KEYRING")) {
    Mutex::Locker l(lock);
    string k = getenv("CEPH_KEYRING");
    values["keyring"][CONF_ENV] = Option::value_t(k);
  }
  if (const char *dir = getenv("CEPH_LIB")) {
    Mutex::Locker l(lock);
    for (auto name : { "erasure_code_dir", "plugin_dir", "osd_class_dir" }) {
    std::string err;
      const Option *o = find_option(name);
      assert(o);
      _set_val(dir, *o, CONF_ENV, &err);
    }
  }
  if (getenv(args_var)) {
    vector<const char *> env_args;
    env_to_vec(env_args, args_var);
    parse_argv(env_args, CONF_ENV);
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
    f->dump_object("option", i.second);
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
  for (const auto& i: schema) {
    const Option &opt = i.second;
    string val;
    conf_stringify(_get_val(opt), &val);
    if (out) {
      *out << opt.name << " = " << val << std::endl;
    }
    if (f) {
      f->dump_string(opt.name.c_str(), val);
    }
  }
}

int md_config_t::parse_argv(std::vector<const char*>& args, int level)
{
  Mutex::Locker l(lock);
  if (safe_to_start_threads) {
    return -ENOSYS;
  }

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
      do_show_config = true;
    }
    else if (ceph_argparse_witharg(args, i, &val, "--show_config_value", (char*)NULL)) {
      do_show_config_value = val;
    }
    else if (ceph_argparse_flag(args, i, "--no-mon-config", (char*)NULL)) {
      no_mon_config = true;
    }
    else if (ceph_argparse_flag(args, i, "--mon-config", (char*)NULL)) {
      no_mon_config = false;
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
      bufferlist bl;
      string err;
      int r;
      if (val == "-") {
	r = bl.read_fd(STDIN_FILENO, 1024);
      } else {
	r = bl.read_file(val.c_str(), &err);
      }
      if (r >= 0) {
	string k(bl.c_str(), bl.length());
	set_val_or_die("key", k.c_str());
      }
    }
    else if (ceph_argparse_witharg(args, i, &val, "--keyring", "-k", (char*)NULL)) {
      set_val_or_die("keyring", val.c_str());
    }
    else if (ceph_argparse_witharg(args, i, &val, "--client_mountpoint", "-r", (char*)NULL)) {
      set_val_or_die("client_mountpoint", val.c_str());
    }
    else {
      int r = parse_option(args, i, NULL, level);
      if (r < 0) {
        return r;
      }
    }
  }
  return 0;
}

void md_config_t::do_argv_commands()
{
  Mutex::Locker l(lock);

  if (do_show_config) {
    _show_config(&cout, NULL);
    _exit(0);
  }

  if (do_show_config_value.size()) {
    string val;
    int r = conf_stringify(_get_val(do_show_config_value, 0, &cerr), &val);
    if (r < 0) {
      if (r == -ENOENT)
	std::cerr << "failed to get config option '"
		  << do_show_config_value << "': option not found" << std::endl;
      else
	std::cerr << "failed to get config option '"
		  << do_show_config_value << "': " << cpp_strerror(r)
		  << std::endl;
      _exit(1);
    }
    std::cout << val << std::endl;
    _exit(0);
  }
}

int md_config_t::parse_option(std::vector<const char*>& args,
			      std::vector<const char*>::iterator& i,
			      ostream *oss,
			      int level)
{
  int ret = 0;
  size_t o = 0;
  std::string val;

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
	  ret = _set_val("false", opt, level, &error_message);
	else if (res == 1)
	  ret = _set_val("true", opt, level, &error_message);
	else
	  ret = res;
	break;
      } else {
	std::string no("--no-");
	no += opt.name;
	if (ceph_argparse_flag(args, i, no.c_str(), (char*)NULL)) {
	  ret = _set_val("false", opt, level, &error_message);
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
      ret = _set_val(val, opt, level, &error_message);
      break;
    }
    ++o;
  }

  if (ret < 0 || !error_message.empty()) {
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
  return ret >= 0 ? 0 : ret;
}

int md_config_t::parse_injectargs(std::vector<const char*>& args,
				  std::ostream *oss)
{
  assert(lock.is_locked());
  int ret = 0;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    int r = parse_option(args, i, oss, CONF_OVERRIDE);
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

void md_config_t::_apply_changes(std::ostream *oss)
{
  /* Maps observers to the configuration options that they care about which
   * have changed. */
  typedef std::map < md_config_obs_t*, std::set <std::string> > rev_obs_map_t;

  // meta expands could have modified anything.  Copy it all out again.
  update_legacy_vals();

  // create the reverse observer mapping, mapping observers to the set of
  // changed keys that they'll get.
  rev_obs_map_t robs;
  std::set <std::string> empty_set;
  string val;
  for (changed_set_t::const_iterator c = changed.begin();
       c != changed.end(); ++c) {
    const std::string &key(*c);
    pair < obs_map_t::iterator, obs_map_t::iterator >
      range(observers.equal_range(key));
    if ((oss) && !conf_stringify(_get_val(key), &val)) {
      (*oss) << key << " = '" << val << "' ";
      if (range.first == range.second) {
	(*oss) << "(not observed, change may require restart) ";
      }
    }
    for (obs_map_t::iterator r = range.first; r != range.second; ++r) {
      rev_obs_map_t::value_type robs_val(r->second, empty_set);
      pair < rev_obs_map_t::iterator, bool > robs_ret(robs.insert(robs_val));
      std::set <std::string> &keys(robs_ret.first->second);
      keys.insert(key);
    }
  }

  changed.clear();

  // Make any pending observer callbacks
  for (rev_obs_map_t::const_iterator r = robs.begin(); r != robs.end(); ++r) {
    md_config_obs_t *obs = r->first;
    obs->handle_conf_change(this, r->second);
  }

}

void md_config_t::call_all_observers()
{
  std::map<md_config_obs_t*,std::set<std::string> > obs;
  // Have the scope of the lock extend to the scope of
  // handle_conf_change since that function expects to be called with
  // the lock held. (And the comment in config.h says that is the
  // expected behavior.)
  //
  // An alternative might be to pass a std::unique_lock to
  // handle_conf_change and have a version of get_var that can take it
  // by reference and lock as appropriate.
  Mutex::Locker l(lock);
  {
    for (auto r = observers.begin(); r != observers.end(); ++r) {
      obs[r->second].insert(r->first);
    }
  }
  for (auto p = obs.begin();
       p != obs.end();
       ++p) {
    p->first->handle_conf_change(this, p->second);
  }
}

void md_config_t::set_safe_to_start_threads()
{
  safe_to_start_threads = true;
}

void md_config_t::_clear_safe_to_start_threads()
{
  safe_to_start_threads = false;
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

void md_config_t::set_val_or_die(const std::string &key,
                                 const std::string &val)
{
  std::stringstream err;
  int ret = set_val(key, val, &err);
  if (ret != 0) {
    std::cerr << "set_val_or_die(" << key << "): " << err.str();
  }
  assert(ret == 0);
}

int md_config_t::set_val(const std::string &key, const char *val,
			 std::stringstream *err_ss)
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

  string k(ConfFile::normalize_key_name(key));

  const auto &opt_iter = schema.find(k);
  if (opt_iter != schema.end()) {
    const Option &opt = opt_iter->second;
    std::string error_message;
    int r = _set_val(v, opt, CONF_OVERRIDE, &error_message);
    if (r >= 0) {
      if (err_ss) *err_ss << "Set " << opt.name << " to " << v;
      r = 0;
    } else {
      if (err_ss) *err_ss << error_message;
    }
    return r;
  }

  if (err_ss) *err_ss << "Configuration option not found: '" << key << "'";
  return -ENOENT;
}

int md_config_t::rm_val(const std::string& key)
{
  Mutex::Locker l(lock);
  return _rm_val(key, CONF_OVERRIDE);
}

void md_config_t::get_defaults_bl(bufferlist *bl)
{
  Mutex::Locker l(lock);
  if (defaults_bl.length() == 0) {
    uint32_t n = 0;
    bufferlist bl;
    for (const auto &i : schema) {
      ++n;
      encode(i.second.name, bl);
      auto j = values.find(i.second.name);
      if (j != values.end()) {
	auto k = j->second.find(CONF_DEFAULT);
	if (k != j->second.end()) {
	  encode(Option::to_str(k->second), bl);
	  continue;
	}
      }
      string val;
      conf_stringify(_get_val_default(i.second), &val);
      encode(val, bl);
    }
    encode(n, defaults_bl);
    defaults_bl.claim_append(bl);
  }
  *bl = defaults_bl;
}

void md_config_t::get_config_bl(
  uint64_t have_version,
  bufferlist *bl,
  uint64_t *got_version)
{
  Mutex::Locker l(lock);
  if (values_bl.length() == 0) {
    uint32_t n = 0;
    bufferlist bl;
    for (auto& i : values) {
      if (i.first == "fsid" ||
	  i.first == "host") {
	continue;
      }
      ++n;
      encode(i.first, bl);
      encode((uint32_t)i.second.size(), bl);
      for (auto& j : i.second) {
	encode(j.first, bl);
	encode(Option::to_str(j.second), bl);
      }
    }
    // make sure overridden items appear, and include the default value
    for (auto& i : ignored_mon_values) {
      if (values.count(i.first)) {
	continue;
      }
      if (i.first == "fsid" ||
	  i.first == "host") {
	continue;
      }
      const Option *opt = find_option(i.first);
      if (!opt) {
	continue;
      }
      ++n;
      encode(i.first, bl);
      encode((uint32_t)1, bl);
      encode((int32_t)CONF_DEFAULT, bl);
      string val;
      conf_stringify(_get_val_default(*opt), &val);
      encode(val, bl);
    }
    encode(n, values_bl);
    values_bl.claim_append(bl);
    encode(ignored_mon_values, values_bl);
    ++values_bl_version;
  }
  if (have_version != values_bl_version) {
    *bl = values_bl;
    *got_version = values_bl_version;
  }
}

int md_config_t::get_val(const std::string &key, char **buf, int len) const
{
  Mutex::Locker l(lock);
  string k(ConfFile::normalize_key_name(key));
  return _get_val_cstr(k, buf, len);
}

int md_config_t::get_val(
  const std::string &key,
  std::string *val) const
{
  return conf_stringify(get_val_generic(key), val);
}

Option::value_t md_config_t::get_val_generic(const std::string &key) const
{
  Mutex::Locker l(lock);
  string k(ConfFile::normalize_key_name(key));
  return _get_val(k);
}

Option::value_t md_config_t::_get_val(
  const std::string &key,
  expand_stack_t *stack,
  std::ostream *err) const
{
  assert(lock.is_locked());
  if (key.empty()) {
    return Option::value_t(boost::blank());
  }

  // In key names, leading and trailing whitespace are not significant.
  string k(ConfFile::normalize_key_name(key));

  const Option *o = find_option(key);
  if (!o) {
    // not a valid config option
    return Option::value_t(boost::blank());
  }

  return _get_val(*o, stack, err);
}

Option::value_t md_config_t::_get_val(
  const Option& o,
  expand_stack_t *stack,
  std::ostream *err) const
{
  expand_stack_t a_stack;
  if (!stack) {
    stack = &a_stack;
  }

  auto p = values.find(o.name);
  if (p != values.end() && !p->second.empty()) {
    // use highest-priority value available (see CONF_*)
    return _expand_meta(p->second.rbegin()->second, &o, stack, err);
  }

  return _expand_meta(_get_val_default(o), &o, stack, err);
}

Option::value_t md_config_t::_get_val_nometa(const Option& o) const
{
  auto p = values.find(o.name);
  if (p != values.end() && !p->second.empty()) {
    // use highest-priority value available (see CONF_*)
    return p->second.rbegin()->second;
  }
  return _get_val_default(o);
}

const Option::value_t& md_config_t::_get_val_default(const Option& o) const
{
  bool has_daemon_default = !boost::get<boost::blank>(&o.daemon_value);
  if (is_daemon && has_daemon_default) {
    return o.daemon_value;
  } else {
    return o.value;
  }
}

void md_config_t::early_expand_meta(
  std::string &val,
  std::ostream *err) const
{
  Mutex::Locker l(lock);
  expand_stack_t stack;
  Option::value_t v = _expand_meta(Option::value_t(val), nullptr, &stack, err);
  conf_stringify(v, &val);
}

Option::value_t md_config_t::_expand_meta(
  const Option::value_t& in,
  const Option *o,
  expand_stack_t *stack,
  std::ostream *err) const
{
  //cout << __func__ << " in '" << in << "' stack " << stack << std::endl;
  if (!stack) {
    return in;
  }
  const std::string *str = boost::get<const std::string>(&in);
  if (!str) {
    // strings only!
    return in;
  }

  auto pos = str->find('$');
  if (pos == std::string::npos) {
    // no substitutions!
    return in;
  }

  if (o) {
    stack->push_back(make_pair(o, &in));
  }
  string out;
  decltype(pos) last_pos = 0;
  while (pos != std::string::npos) {
    assert((*str)[pos] == '$');
    if (pos > last_pos) {
      out += str->substr(last_pos, pos - last_pos);
    }

    // try to parse the variable name into var, either \$\{(.+)\} or
    // \$[a-z\_]+
    const char *valid_chars = "abcdefghijklmnopqrstuvwxyz_";
    string var;
    size_t endpos = 0;
    if ((*str)[pos+1] == '{') {
      // ...${foo_bar}...
      endpos = str->find_first_not_of(valid_chars, pos + 2);
      if (endpos != std::string::npos &&
	  (*str)[endpos] == '}') {
	var = str->substr(pos + 2, endpos - pos - 2);
	endpos++;
      }
    } else {
      // ...$foo...
      endpos = str->find_first_not_of(valid_chars, pos + 1);
      if (endpos != std::string::npos)
	var = str->substr(pos + 1, endpos - pos - 1);
      else
	var = str->substr(pos + 1);
    }
    last_pos = endpos;

    if (!var.size()) {
      out += '$';
    } else {
      //cout << " found var " << var << std::endl;
      // special metavariable?
      if (var == "type") {
	out += name.get_type_name();
      } else if (var == "cluster") {
	out += cluster;
      } else if (var == "name") {
	out += name.to_cstr();
      } else if (var == "host") {
	if (host == "") {
	  out += ceph_get_short_hostname();
	} else {
	  out += host;
	}
      } else if (var == "num") {
	out += name.get_id().c_str();
      } else if (var == "id") {
	out += name.get_id();
      } else if (var == "pid") {
	out += stringify(getpid());
      } else if (var == "cctid") {
	out += stringify((unsigned long long)this);
      } else {
	if (var == "data_dir") {
	  var = data_dir_option;
	}
	const Option *o = find_option(var);
	if (!o) {
	  out += str->substr(pos, endpos - pos);
	} else {
	  auto match = std::find_if(
	    stack->begin(), stack->end(),
	    [o](pair<const Option *,const Option::value_t*>& item) {
	      return item.first == o;
	    });
	  if (match != stack->end()) {
	    // substitution loop; break the cycle
	    if (err) {
	      *err << "variable expansion loop at " << var << "="
		   << Option::to_str(*match->second) << "\n"
		   << "expansion stack:\n";
	      for (auto i = stack->rbegin(); i != stack->rend(); ++i) {
		*err << i->first->name << "="
		     << Option::to_str(*i->second) << "\n";
	      }
	    }
	    return Option::value_t(std::string("$") + o->name);
	  } else {
	    // recursively evaluate!
	    string n;
	    conf_stringify(_get_val(*o, stack, err), &n);
	    out += n;
	  }
	}
      }
    }
    pos = str->find('$', last_pos);
  }
  if (last_pos != std::string::npos) {
    out += str->substr(last_pos);
  }
  if (o) {
    stack->pop_back();
  }

  return Option::value_t(out);
}

int md_config_t::_get_val_cstr(
  const std::string &key, char **buf, int len) const
{
  assert(lock.is_locked());

  if (key.empty())
    return -EINVAL;

  string val;
  if (conf_stringify(_get_val(key), &val) == 0) {
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

int md_config_t::get_val_from_conf_file(
  const std::vector <std::string> &sections,
  const std::string &key,
  std::string &out,
  bool emeta) const
{
  Mutex::Locker l(lock);
  int r = _get_val_from_conf_file(sections, key, out);
  if (r < 0) {
    return r;
  }
  if (emeta) {
    expand_stack_t stack;
    auto v = _expand_meta(Option::value_t(out), nullptr, &stack, nullptr);
    conf_stringify(v, &out);
  }
  return 0;
}

int md_config_t::_get_val_from_conf_file(
  const std::vector <std::string> &sections,
  const std::string &key,
  std::string &out) const
{
  assert(lock.is_locked());
  std::vector <std::string>::const_iterator s = sections.begin();
  std::vector <std::string>::const_iterator s_end = sections.end();
  for (; s != s_end; ++s) {
    int ret = cf.read(s->c_str(), key, out);
    if (ret == 0) {
      return 0;
    } else if (ret != -ENOENT) {
      return ret;
    }
  }
  return -ENOENT;
}

int md_config_t::_set_val(
  const std::string &raw_val,
  const Option &opt,
  int level,
  std::string *error_message)
{
  assert(lock.is_locked());

  Option::value_t new_value;
  int r = opt.parse_value(raw_val, &new_value, error_message);
  if (r < 0) {
    return r;
  }

  // unsafe runtime change?
  if (!opt.can_update_at_runtime() &&
      safe_to_start_threads &&
      observers.count(opt.name) == 0) {
    // accept value if it is not actually a change
    if (new_value != _get_val_nometa(opt)) {
      *error_message = string("Configuration option '") + opt.name +
	"' may not be modified at runtime";
      return -ENOSYS;
    }
  }

  // Apply the value to its entry in the `values` map
  auto p = values.find(opt.name);
  if (p != values.end()) {
    auto q = p->second.find(level);
    if (q != p->second.end()) {
      if (new_value == q->second) {
	// no change!
	return 0;
      }
      q->second = new_value;
    } else {
      p->second[level] = new_value;
    }
    values_bl.clear();
    if (p->second.rbegin()->first > level) {
      // there was a higher priority value; no effect
      return 0;
    }
  } else {
    values_bl.clear();
    values[opt.name][level] = new_value;
  }

  _refresh(opt);
  return 1;
}

void md_config_t::_refresh(const Option& opt)
{
  // Apply the value to its legacy field, if it has one
  auto legacy_ptr_iter = legacy_values.find(std::string(opt.name));
  if (legacy_ptr_iter != legacy_values.end()) {
    update_legacy_val(opt, legacy_ptr_iter->second);
  }

  // Was this a debug_* option update?
  if (opt.subsys >= 0) {
    string actual_val;
    conf_stringify(_get_val(opt), &actual_val);
    int log, gather;
    int r = sscanf(actual_val.c_str(), "%d/%d", &log, &gather);
    if (r >= 1) {
      if (r < 2) {
	gather = log;
      }
      subsys.set_log_level(opt.subsys, log);
      subsys.set_gather_level(opt.subsys, gather);
    }
  } else {
    // normal option, advertise the change.
    changed.insert(opt.name);
  }
}

int md_config_t::_rm_val(const std::string& key, int level)
{
  auto i = values.find(key);
  if (i == values.end()) {
    return -ENOENT;
  }
  auto j = i->second.find(level);
  if (j == i->second.end()) {
    return -ENOENT;
  }
  bool matters = (j->first == i->second.rbegin()->first);
  i->second.erase(j);
  if (matters) {
    _refresh(*find_option(key));
  }
  values_bl.clear();
  return 0;
}

namespace {
template<typename Size>
struct get_size_visitor : public boost::static_visitor<Size>
{
  template<typename T>
  Size operator()(const T&) const {
    return -1;
  }
  Size operator()(const Option::size_t& sz) const {
    return static_cast<Size>(sz.value);
  }
  Size operator()(const Size& v) const {
    return v;
  }
};

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
  void operator()(uint64_t md_config_t::* ptr) const
  {
    using T = uint64_t;
    auto member = const_cast<T*>(&(conf->*(boost::get<const T md_config_t::*>(ptr))));
    *member = boost::apply_visitor(get_size_visitor<T>{}, val);
  }
  void operator()(int64_t md_config_t::* ptr) const
  {
    using T = int64_t;
    auto member = const_cast<T*>(&(conf->*(boost::get<const T md_config_t::*>(ptr))));
    *member = boost::apply_visitor(get_size_visitor<T>{}, val);
  }
};
} // anonymous namespace

void md_config_t::update_legacy_vals()
{
  for (const auto &i : legacy_values) {
    const auto &name = i.first;
    const auto &option = schema.at(name);
    auto ptr = i.second;
    update_legacy_val(option, ptr);
  }
}

void md_config_t::update_legacy_val(const Option &opt,
                                    md_config_t::member_ptr_t member_ptr)
{
  Option::value_t v = _get_val(opt);
  boost::apply_visitor(assign_visitor(this, v), member_ptr);
}

static void dump(Formatter *f, int level, Option::value_t in)
{
  if (const bool *v = boost::get<const bool>(&in)) {
    f->dump_bool(ceph_conf_level_name(level), *v);
  } else if (const int64_t *v = boost::get<const int64_t>(&in)) {
    f->dump_int(ceph_conf_level_name(level), *v);
  } else if (const uint64_t *v = boost::get<const uint64_t>(&in)) {
    f->dump_unsigned(ceph_conf_level_name(level), *v);
  } else if (const double *v = boost::get<const double>(&in)) {
    f->dump_float(ceph_conf_level_name(level), *v);
  } else {
    f->dump_stream(ceph_conf_level_name(level)) << Option::to_str(in);
  }
}

void md_config_t::diff(
  Formatter *f,
  string name) const
{
  Mutex::Locker l(lock);
  for (auto& i : values) {
    if (i.second.size() == 1 &&
	i.second.begin()->first == CONF_DEFAULT) {
      // we only have a default value; exclude from diff
      continue;
    }
    f->open_object_section(i.first.c_str());
    const Option *o = find_option(i.first);
    dump(f, CONF_DEFAULT, _get_val_default(*o));
    for (auto& j : i.second) {
      dump(f, j.first, j.second);
    }
    dump(f, CONF_FINAL, _get_val(*o));
    f->close_section();
  }
}

void md_config_t::complain_about_parse_errors(CephContext *cct)
{
  ::complain_about_parse_errors(cct, &parse_errors);
}

