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

#include <filesystem>
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/config_obs.h"
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

namespace fs = std::filesystem;

using std::cerr;
using std::cout;
using std::map;
using std::less;
using std::list;
using std::ostream;
using std::ostringstream;
using std::pair;
using std::string;
using std::string_view;
using std::vector;

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;
using ceph::Formatter;

static const char *CEPH_CONF_FILE_DEFAULT = "$data_dir/config,/etc/ceph/$cluster.conf,$home/.ceph/$cluster.conf,$cluster.conf"
#if defined(__FreeBSD__)
    ",/usr/local/etc/ceph/$cluster.conf"
#elif defined(_WIN32)
    ",$programdata/ceph/$cluster.conf"
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
  get_str_list(filename_list, ";,", ls);

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

static int conf_stringify(const Option::value_t& v, string *out)
{
  if (v == Option::value_t{}) {
    return -ENOENT;
  }
  *out = Option::to_str(v);
  return 0;
}

md_config_t::md_config_t(ConfigValues& values,
			 const ConfigTracker& tracker,
			 bool is_daemon)
  : is_daemon(is_daemon)
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
    schema.emplace(i.name, i);
  }

  // Define the debug_* options as well.
  subsys_options.reserve(values.subsys.get_num());
  for (unsigned i = 0; i < values.subsys.get_num(); ++i) {
    string name = string("debug_") + values.subsys.get_name(i);
    subsys_options.push_back(
      Option(name, Option::TYPE_STR, Option::LEVEL_ADVANCED));
    Option& opt = subsys_options.back();
    opt.set_default(stringify(values.subsys.get_log_level(i)) + "/" +
		    stringify(values.subsys.get_gather_level(i)));
    string desc = string("Debug level for ") + values.subsys.get_name(i);
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
    {STRINGIFY(name), &ConfigValues::name},
#define SAFE_OPTION(name, type) OPTION(name, type)
#include "options/legacy_config_opts.h"
#undef OPTION
#undef SAFE_OPTION
  };

  validate_schema();

  // Validate default values from the schema
  for (const auto &i : schema) {
    const Option &opt = i.second;
    if (opt.type == Option::TYPE_STR) {
      bool has_daemon_default = (opt.daemon_value != Option::value_t{});
      Option::value_t default_val;
      if (is_daemon && has_daemon_default) {
	default_val = opt.daemon_value;
      } else {
	default_val = opt.value;
      }
      // We call pre_validate as a sanity check, but also to get any
      // side effect (value modification) from the validator.
      auto* def_str = std::get_if<std::string>(&default_val);
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
	set_val_default(values, tracker, opt.name, val);
      }
    }
  }

  // Copy out values (defaults) into any legacy (C struct member) fields
  update_legacy_vals(values);
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

const Option *md_config_t::find_option(const std::string_view name) const
{
  auto p = schema.find(name);
  if (p != schema.end()) {
    return &p->second;
  }
  return nullptr;
}

void md_config_t::set_val_default(ConfigValues& values,
				  const ConfigTracker& tracker,
				  const string_view name, const std::string& val)
{
  const Option *o = find_option(name);
  ceph_assert(o);
  string err;
  int r = _set_val(values, tracker, val, *o, CONF_DEFAULT, &err);
  ceph_assert(r >= 0);
}

int md_config_t::set_mon_vals(CephContext *cct,
    ConfigValues& values,
    const ConfigTracker& tracker,
    const map<string,string,less<>>& kv,
    config_callback config_cb)
{
  ignored_mon_values.clear();

  if (!config_cb) {
    ldout(cct, 4) << __func__ << " no callback set" << dendl;
  }

  for (auto& i : kv) {
    if (config_cb) {
      if (config_cb(i.first, i.second)) {
	ldout(cct, 4) << __func__ << " callback consumed " << i.first << dendl;
	continue;
      }
      ldout(cct, 4) << __func__ << " callback ignored " << i.first << dendl;
    }
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
    int r = _set_val(values, tracker, i.second, *o, CONF_MON, &err);
    if (r < 0) {
      ldout(cct, 4) << __func__ << " failed to set " << i.first << " = "
		    << i.second << ": " << err << dendl;
      ignored_mon_values.emplace(i);
    } else if (r == ConfigValues::SET_NO_CHANGE ||
	       r == ConfigValues::SET_NO_EFFECT) {
      ldout(cct,20) << __func__ << " " << i.first << " = " << i.second
		    << " (no change)" << dendl;
    } else if (r == ConfigValues::SET_HAVE_EFFECT) {
      ldout(cct,10) << __func__ << " " << i.first << " = " << i.second << dendl;
    } else {
      ceph_abort();
    }
  }
  values.for_each([&] (auto name, auto configs) {
    auto config = configs.find(CONF_MON);
    if (config == configs.end()) {
      return;
    }
    if (kv.find(name) != kv.end()) {
      return;
    }
    ldout(cct,10) << __func__ << " " << name
		  << " cleared (was " << Option::to_str(config->second) << ")"
		  << dendl;
    values.rm_val(name, CONF_MON);
    // if this is a debug option, it needs to propagate to teh subsys;
    // this isn't covered by update_legacy_vals() below.  similarly,
    // we want to trigger a config notification for these items.
    const Option *o = find_option(name);
    _refresh(values, *o);
  });
  values_bl.clear();
  update_legacy_vals(values);
  return 0;
}

int md_config_t::parse_config_files(ConfigValues& values,
				    const ConfigTracker& tracker,
				    const char *conf_files_str,
				    std::ostream *warnings,
				    int flags)
{
  if (safe_to_start_threads)
    return -ENOSYS;

  if (values.cluster.empty() && !conf_files_str) {
    values.cluster = get_cluster_name(nullptr);
  }
  // open new conf
  for (auto& fn : get_conffile_paths(values, conf_files_str, warnings, flags)) {
    bufferlist bl;
    std::string error;
    if (bl.read_file(fn.c_str(), &error)) {
      parse_error = error;
      continue;
    }
    ostringstream oss;
    int ret = parse_buffer(values, tracker, bl.c_str(), bl.length(), &oss);
    if (ret == 0) {
      parse_error.clear();
      conf_path = fn;
      break;
    }
    parse_error = oss.str();
    if (ret != -ENOENT) {
      return ret;
    }
  }
  // it must have been all ENOENTs, that's the only way we got here
  if (conf_path.empty()) {
    return -ENOENT;
  }
  if (values.cluster.empty()) {
    values.cluster = get_cluster_name(conf_path.c_str());
  }
  update_legacy_vals(values);
  return 0;
}

int
md_config_t::parse_buffer(ConfigValues& values,
			  const ConfigTracker& tracker,
			  const char* buf, size_t len,
			  std::ostream* warnings)
{
  if (!cf.parse_buffer(string_view{buf, len}, warnings)) {
    return -EINVAL;
  }
  const auto my_sections = get_my_sections(values);
  for (const auto &i : schema) {
    const auto &opt = i.second;
    std::string val;
    if (_get_val_from_conf_file(my_sections, opt.name, val)) {
      continue;
    }
    std::string error_message;
    if (_set_val(values, tracker, val, opt, CONF_FILE, &error_message) < 0) {
      if (warnings != nullptr) {
        *warnings << "parse error setting " << std::quoted(opt.name)
                  << " to " << std::quoted(val);
        if (!error_message.empty()) {
          *warnings << " (" << error_message << ")";
        }
        *warnings << '\n';
      }
    }
  }
  cf.check_old_style_section_names({"mds", "mon", "osd"}, cerr);
  return 0;
}

std::list<std::string>
md_config_t::get_conffile_paths(const ConfigValues& values,
				const char *conf_files_str,
				std::ostream *warnings,
				int flags) const
{
  if (!conf_files_str) {
    const char *c = getenv("CEPH_CONF");
    if (c) {
      conf_files_str = c;
    } else {
      if (flags & CINIT_FLAG_NO_DEFAULT_CONFIG_FILE)
	return {};
      conf_files_str = CEPH_CONF_FILE_DEFAULT;
    }
  }

  std::list<std::string> paths;
  get_str_list(conf_files_str, ";,", paths);
  for (auto i = paths.begin(); i != paths.end(); ) {
    string& path = *i;
    if (path.find("$data_dir") != path.npos &&
	data_dir_option.empty()) {
      // useless $data_dir item, skip
      i = paths.erase(i);
    } else {
      early_expand_meta(values, path, warnings);
      ++i;
    }
  }
  return paths;
}

std::string md_config_t::get_cluster_name(const char* conffile)
{
  if (conffile) {
    // If cluster name is not set yet, use the prefix of the
    // basename of configuration file as cluster name.
    if (fs::path path{conffile}; path.extension() == ".conf") {
      return path.stem().string();
    } else {
      // If the configuration file does not follow $cluster.conf
      // convention, we do the last try and assign the cluster to
      // 'ceph'.
      return "ceph";
    }
  } else {
    // set the cluster name to 'ceph' when configuration file is not specified.
    return "ceph";
  }
}

void md_config_t::parse_env(unsigned entity_type,
			    ConfigValues& values,
			    const ConfigTracker& tracker,
			    const char *args_var)
{
  if (safe_to_start_threads)
    return;
  if (!args_var) {
    args_var = "CEPH_ARGS";
  }
  if (auto s = getenv("CEPH_KEYRING"); s) {
    string err;
    _set_val(values, tracker, s, *find_option("keyring"), CONF_ENV, &err);
  }
  if (auto dir = getenv("CEPH_LIB"); dir) {
    for (auto name : { "erasure_code_dir", "plugin_dir", "osd_class_dir" }) {
    std::string err;
      const Option *o = find_option(name);
      ceph_assert(o);
      _set_val(values, tracker, dir, *o, CONF_ENV, &err);
    }
  }

  if (auto s = getenv("TMPDIR"); s) {
    string err;
    _set_val(values, tracker, s, *find_option("tmp_dir"), CONF_ENV, &err);
  }

  // Apply pod memory limits:
  //
  // There are two types of resource requests: `limits` and `requests`.
  //
  // - Requests: Used by the K8s scheduler to determine on which nodes to
  //   schedule the pods. This helps spread the pods to different nodes. This
  //   value should be conservative in order to make sure all the pods are
  //   schedulable. This corresponds to POD_MEMORY_REQUEST (set by the Rook
  //   CRD) and is the target memory utilization we try to maintain for daemons
  //   that respect it.
  //
  //   If POD_MEMORY_REQUEST is present, we use it as the target.
  //
  // - Limits: At runtime, the container runtime (and Linux) will use the
  //   limits to see if the pod is using too many resources. In that case, the
  //   pod will be killed/restarted automatically if the pod goes over the limit.
  //   This should be higher than what is specified for requests (potentially
  //   much higher). This corresponds to the cgroup memory limit that will
  //   trigger the Linux OOM killer.
  //
  //   If POD_MEMORY_LIMIT is present, we use it as the /default/ value for
  //   the target, which means it will only apply if the *_memory_target option
  //   isn't set via some other path (e.g., POD_MEMORY_REQUEST, or the cluster
  //   config, or whatever.)
  //
  // Here are the documented best practices:
  //   https://kubernetes.io/docs/tasks/configure-pod-container/assign-cpu-resource/#motivation-for-cpu-requests-and-limits
  //
  // When the operator creates the CephCluster CR, it will need to generate the
  // desired requests and limits. As long as we are conservative in our choice
  // for requests and generous with the limits we should be in a good place to
  // get started.
  //
  // The support in Rook is already there for applying the limits as seen in
  // these links.
  //
  // Rook docs on the resource requests and limits:
  //   https://rook.io/docs/rook/v1.0/ceph-cluster-crd.html#cluster-wide-resources-configuration-settings
  // Example CR settings:
  //   https://github.com/rook/rook/blob/6d2ef936698593036185aabcb00d1d74f9c7bfc1/cluster/examples/kubernetes/ceph/cluster.yaml#L90
  //
  uint64_t pod_limit = 0, pod_request = 0;
  if (auto pod_lim = getenv("POD_MEMORY_LIMIT"); pod_lim) {
    string err;
    uint64_t v = atoll(pod_lim);
    if (v) {
      switch (entity_type) {
      case CEPH_ENTITY_TYPE_OSD:
        {
	  double cgroup_ratio = get_val<double>(
	    values, "osd_memory_target_cgroup_limit_ratio");
	  if (cgroup_ratio > 0.0) {
	    pod_limit = v * cgroup_ratio;
	    // set osd_memory_target *default* based on cgroup limit, so that
	    // it can be overridden by any explicit settings elsewhere.
	    set_val_default(values, tracker,
			    "osd_memory_target", stringify(pod_limit));
	  }
	}
      }
    }
  }
  if (auto pod_req = getenv("POD_MEMORY_REQUEST"); pod_req) {
    if (uint64_t v = atoll(pod_req); v) {
      pod_request = v;
    }
  }
  if (pod_request && pod_limit) {
    // If both LIMIT and REQUEST are set, ensure that we use the
    // min of request and limit*ratio.  This is important
    // because k8s set set LIMIT == REQUEST if only LIMIT is
    // specified, and we want to apply the ratio in that case,
    // even though REQUEST is present.
    pod_request = std::min<uint64_t>(pod_request, pod_limit);
  }
  if (pod_request) {
    string err;
    switch (entity_type) {
    case CEPH_ENTITY_TYPE_OSD:
      _set_val(values, tracker, stringify(pod_request),
	       *find_option("osd_memory_target"),
	       CONF_ENV, &err);
      break;
    }
  }

  if (getenv(args_var)) {
    vector<const char *> env_args;
    env_to_vec(env_args, args_var);
    parse_argv(values, tracker, env_args, CONF_ENV);
  }
}

void md_config_t::show_config(const ConfigValues& values,
			      std::ostream& out) const
{
  _show_config(values, &out, nullptr);
}

void md_config_t::show_config(const ConfigValues& values,
			      Formatter *f) const
{
  _show_config(values, nullptr, f);
}

void md_config_t::config_options(Formatter *f) const
{
  f->open_array_section("options");
  for (const auto& i: schema) {
    f->dump_object("option", i.second);
  }
  f->close_section();
}

void md_config_t::_show_config(const ConfigValues& values,
			       std::ostream *out, Formatter *f) const
{
  if (out) {
    *out << "name = " << values.name << std::endl;
    *out << "cluster = " << values.cluster << std::endl;
  }
  if (f) {
    f->dump_string("name", stringify(values.name));
    f->dump_string("cluster", values.cluster);
  }
  for (const auto& i: schema) {
    const Option &opt = i.second;
    string val;
    conf_stringify(_get_val(values, opt), &val);
    if (out) {
      *out << opt.name << " = " << val << std::endl;
    }
    if (f) {
      f->dump_string(opt.name.c_str(), val);
    }
  }
}

int md_config_t::parse_argv(ConfigValues& values,
			    const ConfigTracker& tracker,
			    std::vector<const char*>& args, int level)
{
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
      values.no_mon_config = true;
    }
    else if (ceph_argparse_flag(args, i, "--mon-config", (char*)NULL)) {
      values.no_mon_config = false;
    }
    else if (ceph_argparse_flag(args, i, "--foreground", "-f", (char*)NULL)) {
      set_val_or_die(values, tracker, "daemonize", "false");
    }
    else if (ceph_argparse_flag(args, i, "-d", (char*)NULL)) {
      set_val_or_die(values, tracker, "fuse_debug", "true");
      set_val_or_die(values, tracker, "daemonize", "false");
      set_val_or_die(values, tracker, "log_file", "");
      set_val_or_die(values, tracker, "log_to_stderr", "true");
      set_val_or_die(values, tracker, "err_to_stderr", "true");
      set_val_or_die(values, tracker, "log_to_syslog", "false");
    }
    // Some stuff that we wanted to give universal single-character options for
    // Careful: you can burn through the alphabet pretty quickly by adding
    // to this list.
    else if (ceph_argparse_witharg(args, i, &val, "--monmap", "-M", (char*)NULL)) {
      set_val_or_die(values, tracker, "monmap", val.c_str());
    }
    else if (ceph_argparse_witharg(args, i, &val, "--mon_host", "-m", (char*)NULL)) {
      set_val_or_die(values, tracker, "mon_host", val.c_str());
    }
    else if (ceph_argparse_witharg(args, i, &val, "--bind", (char*)NULL)) {
      set_val_or_die(values, tracker, "public_addr", val.c_str());
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
	set_val_or_die(values, tracker, "key", k.c_str());
      }
    }
    else if (ceph_argparse_witharg(args, i, &val, "--keyring", "-k", (char*)NULL)) {
      set_val_or_die(values, tracker, "keyring", val.c_str());
    }
    else if (ceph_argparse_witharg(args, i, &val, "--client_mountpoint", "-r", (char*)NULL)) {
      set_val_or_die(values, tracker, "client_mountpoint", val.c_str());
    }
    else {
      int r = parse_option(values, tracker, args, i, NULL, level);
      if (r < 0) {
        return r;
      }
    }
  }
  // meta expands could have modified anything.  Copy it all out again.
  update_legacy_vals(values);
  return 0;
}

void md_config_t::do_argv_commands(const ConfigValues& values) const
{

  if (do_show_config) {
    _show_config(values, &cout, NULL);
    _exit(0);
  }

  if (do_show_config_value.size()) {
    string val;
    int r = conf_stringify(_get_val(values, do_show_config_value, 0, &cerr),
			   &val);
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

int md_config_t::parse_option(ConfigValues& values,
			      const ConfigTracker& tracker,
			      std::vector<const char*>& args,
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
    if (ceph_argparse_witharg(
	  args, i, &val, err,
	  string(string("--default-") + opt.name).c_str(), (char*)NULL)) {
      if (!err.str().empty()) {
        error_message = err.str();
	ret = -EINVAL;
	break;
      }
      ret = _set_val(values, tracker,  val, opt, CONF_DEFAULT, &error_message);
      break;
    } else if (opt.type == Option::TYPE_BOOL) {
      int res;
      if (ceph_argparse_binary_flag(args, i, &res, oss, as_option.c_str(),
				    (char*)NULL)) {
	if (res == 0)
	  ret = _set_val(values, tracker, "false", opt, level, &error_message);
	else if (res == 1)
	  ret = _set_val(values, tracker, "true", opt, level, &error_message);
	else
	  ret = res;
	break;
      } else {
	std::string no("--no-");
	no += opt.name;
	if (ceph_argparse_flag(args, i, no.c_str(), (char*)NULL)) {
	  ret = _set_val(values, tracker, "false", opt, level, &error_message);
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
      ret = _set_val(values, tracker,  val, opt, level, &error_message);
      break;
    }
    ++o;
  }

  if (ret < 0 || !error_message.empty()) {
    ceph_assert(!option_name.empty());
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

int md_config_t::parse_injectargs(ConfigValues& values,
				  const ConfigTracker& tracker,
				  std::vector<const char*>& args,
				  std::ostream *oss)
{
  int ret = 0;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    int r = parse_option(values, tracker, args, i, oss, CONF_OVERRIDE);
    if (r < 0)
      ret = r;
  }
  return ret;
}

void md_config_t::set_safe_to_start_threads()
{
  safe_to_start_threads = true;
}

void md_config_t::_clear_safe_to_start_threads()
{
  safe_to_start_threads = false;
}

int md_config_t::injectargs(ConfigValues& values,
			    const ConfigTracker& tracker,
			    const std::string& s, std::ostream *oss)
{
  int ret;
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
  ret = parse_injectargs(values, tracker, nargs, oss);
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
  update_legacy_vals(values);
  return ret;
}

void md_config_t::set_val_or_die(ConfigValues& values,
				 const ConfigTracker& tracker,
				 const std::string_view key,
				 const std::string &val)
{
  std::stringstream err;
  int ret = set_val(values, tracker, key, val, &err);
  if (ret != 0) {
    std::cerr << "set_val_or_die(" << key << "): " << err.str();
  }
  ceph_assert(ret == 0);
}

int md_config_t::set_val(ConfigValues& values,
			 const ConfigTracker& tracker,
			 const std::string_view key, const char *val,
			 std::stringstream *err_ss)
{
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
    int r = _set_val(values, tracker, v, opt, CONF_OVERRIDE, &error_message);
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

int md_config_t::rm_val(ConfigValues& values, const std::string_view key)
{
  return _rm_val(values, key, CONF_OVERRIDE);
}

void md_config_t::get_defaults_bl(const ConfigValues& values,
					 bufferlist *bl)
{
  if (defaults_bl.length() == 0) {
    uint32_t n = 0;
    bufferlist bl;
    for (const auto &i : schema) {
      ++n;
      encode(i.second.name, bl);
      auto [value, found] = values.get_value(i.second.name, CONF_DEFAULT);
      if (found) {
	encode(Option::to_str(value), bl);
      } else {
	string val;
	conf_stringify(_get_val_default(i.second), &val);
	encode(val, bl);
      }
    }
    encode(n, defaults_bl);
    defaults_bl.claim_append(bl);
  }
  *bl = defaults_bl;
}

void md_config_t::get_config_bl(
  const ConfigValues& values,
  uint64_t have_version,
  bufferlist *bl,
  uint64_t *got_version)
{
  if (values_bl.length() == 0) {
    uint32_t n = 0;
    bufferlist bl;
    values.for_each([&](auto& name, auto& configs) {
      if (name == "fsid" ||
	  name == "host") {
	return;
      }
      ++n;
      encode(name, bl);
      encode((uint32_t)configs.size(), bl);
      for (auto& j : configs) {
	encode(j.first, bl);
	encode(Option::to_str(j.second), bl);
      }
    });
    // make sure overridden items appear, and include the default value
    for (auto& i : ignored_mon_values) {
      if (values.contains(i.first)) {
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

std::optional<std::string> md_config_t::get_val_default(std::string_view key)
{
  std::string val;
  const Option *opt = find_option(key);
  if (opt && (conf_stringify(_get_val_default(*opt), &val) == 0)) {
    return std::make_optional(std::move(val));
  }
  return std::nullopt;
}

int md_config_t::get_val(const ConfigValues& values,
			 const std::string_view key, char **buf, int len) const
{
  string k(ConfFile::normalize_key_name(key));
  return _get_val_cstr(values, k, buf, len);
}

int md_config_t::get_val(
  const ConfigValues& values,
  const std::string_view key,
  std::string *val) const
{
  return conf_stringify(get_val_generic(values, key), val);
}

Option::value_t md_config_t::get_val_generic(
  const ConfigValues& values,
  const std::string_view key) const
{
  return _get_val(values, key);
}

Option::value_t md_config_t::_get_val(
  const ConfigValues& values,
  const std::string_view key,
  expand_stack_t *stack,
  std::ostream *err) const
{
  if (key.empty()) {
    return {};
  }

  // In key names, leading and trailing whitespace are not significant.
  string k(ConfFile::normalize_key_name(key));

  const Option *o = find_option(k);
  if (!o) {
    // not a valid config option
    return {};
  }

  return _get_val(values, *o, stack, err);
}

Option::value_t md_config_t::_get_val(
  const ConfigValues& values,
  const Option& o,
  expand_stack_t *stack,
  std::ostream *err) const
{
  expand_stack_t a_stack;
  if (!stack) {
    stack = &a_stack;
  }
  return _expand_meta(values,
		      _get_val_nometa(values, o),
		      &o, stack, err);
}

Option::value_t md_config_t::_get_val_nometa(const ConfigValues& values,
					     const Option& o) const
{
  if (auto [value, found] = values.get_value(o.name, -1); found) {
    return value;
  } else {
    return _get_val_default(o);
  }
}

const Option::value_t& md_config_t::_get_val_default(const Option& o) const
{
  bool has_daemon_default = (o.daemon_value != Option::value_t{});
  if (is_daemon && has_daemon_default) {
    return o.daemon_value;
  } else {
    return o.value;
  }
}

void md_config_t::early_expand_meta(
  const ConfigValues& values,
  std::string &val,
  std::ostream *err) const
{
  expand_stack_t stack;
  Option::value_t v = _expand_meta(values,
				   Option::value_t(val),
				   nullptr, &stack, err);
  conf_stringify(v, &val);
}

bool md_config_t::finalize_reexpand_meta(ConfigValues& values,
					 const ConfigTracker& tracker)
{
  std::vector<std::string> reexpands;
  reexpands.swap(may_reexpand_meta);
  for (auto& name : reexpands) {
    // always refresh the options if they are in the may_reexpand_meta
    // map, because the options may have already been expanded with old
    // meta.
    const auto &opt_iter = schema.find(name);
    ceph_assert(opt_iter != schema.end());
    const Option &opt = opt_iter->second;
    _refresh(values, opt);
  }

  return !may_reexpand_meta.empty();
}

Option::value_t md_config_t::_expand_meta(
  const ConfigValues& values,
  const Option::value_t& in,
  const Option *o,
  expand_stack_t *stack,
  std::ostream *err) const
{
  //cout << __func__ << " in '" << in << "' stack " << stack << std::endl;
  if (!stack) {
    return in;
  }
  const auto str = std::get_if<std::string>(&in);
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
    ceph_assert((*str)[pos] == '$');
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
	out += values.name.get_type_name();
      } else if (var == "cluster") {
	out += values.cluster;
      } else if (var == "name") {
	out += values.name.to_cstr();
      } else if (var == "host") {
	if (values.host == "") {
	  out += ceph_get_short_hostname();
	} else {
	  out += values.host;
	}
      } else if (var == "num") {
	out += values.name.get_id().c_str();
      } else if (var == "id") {
	out += values.name.get_id();
      } else if (var == "pid") {
        char *_pid = getenv("PID");
        if (_pid) {
          out += _pid;
        } else {
          out += stringify(getpid());
        }
        if (o) {
          may_reexpand_meta.push_back(o->name);
        }
      } else if (var == "cctid") {
	out += stringify((unsigned long long)this);
      } else if (var == "home") {
	const char *home = getenv("HOME");
	out = home ? std::string(home) : std::string();
      } else if (var == "programdata") {
        const char *home = getenv("ProgramData");
        out = home ? std::string(home) : std::string();
      }else {
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
	    conf_stringify(_get_val(values, *o, stack, err), &n);
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
  const ConfigValues& values,
  const std::string& key, char **buf, int len) const
{
  if (key.empty())
    return -EINVAL;

  string val;
  if (conf_stringify(_get_val(values, key), &val) == 0) {
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
std::vector <std::string>
md_config_t::get_my_sections(const ConfigValues& values) const
{
  return {values.name.to_str(),
	  values.name.get_type_name().data(),
	  "global"};
}

// Return a list of all sections
int md_config_t::get_all_sections(std::vector <std::string> &sections) const
{
  for (auto [section_name, section] : cf) {
    sections.push_back(section_name);
    std::ignore = section;
  }
  return 0;
}

int md_config_t::get_val_from_conf_file(
  const ConfigValues& values,
  const std::vector <std::string> &sections,
  const std::string_view key,
  std::string &out,
  bool emeta) const
{
  int r = _get_val_from_conf_file(sections, key, out);
  if (r < 0) {
    return r;
  }
  if (emeta) {
    expand_stack_t stack;
    auto v = _expand_meta(values, Option::value_t(out), nullptr, &stack, nullptr);
    conf_stringify(v, &out);
  }
  return 0;
}

int md_config_t::_get_val_from_conf_file(
  const std::vector <std::string> &sections,
  const std::string_view key,
  std::string &out) const
{
  for (auto &s : sections) {
    int ret = cf.read(s, key, out);
    if (ret == 0) {
      return 0;
    } else if (ret != -ENOENT) {
      return ret;
    }
  }
  return -ENOENT;
}

int md_config_t::_set_val(
  ConfigValues& values,
  const ConfigTracker& observers,
  const std::string &raw_val,
  const Option &opt,
  int level,
  std::string *error_message)
{
  Option::value_t new_value;
  ceph_assert(error_message);
  int r = opt.parse_value(raw_val, &new_value, error_message);
  if (r < 0) {
    return r;
  }

  // unsafe runtime change?
  if (!opt.can_update_at_runtime() &&
      safe_to_start_threads &&
      !observers.is_tracking(opt.name)) {
    // accept value if it is not actually a change
    if (new_value != _get_val_nometa(values, opt)) {
      *error_message = string("Configuration option '") + opt.name +
	"' may not be modified at runtime";
      return -EPERM;
    }
  }

  // Apply the value to its entry in the `values` map
  auto result = values.set_value(opt.name, std::move(new_value), level);
  switch (result) {
  case ConfigValues::SET_NO_CHANGE:
    break;
  case ConfigValues::SET_NO_EFFECT:
    values_bl.clear();
    break;
  case ConfigValues::SET_HAVE_EFFECT:
    values_bl.clear();
    _refresh(values, opt);
    break;
  }
  return result;
}

void md_config_t::_refresh(ConfigValues& values, const Option& opt)
{
  // Apply the value to its legacy field, if it has one
  auto legacy_ptr_iter = legacy_values.find(std::string(opt.name));
  if (legacy_ptr_iter != legacy_values.end()) {
    update_legacy_val(values, opt, legacy_ptr_iter->second);
  }
  // Was this a debug_* option update?
  if (opt.subsys >= 0) {
    string actual_val;
    conf_stringify(_get_val(values, opt), &actual_val);
    values.set_logging(opt.subsys, actual_val.c_str());
  } else {
    // normal option, advertise the change.
    values.changed.insert(opt.name);
  }
}

int md_config_t::_rm_val(ConfigValues& values,
			 const std::string_view key,
			 int level)
{
  if (schema.count(key) == 0) {
    return -EINVAL;
  }
  auto ret = values.rm_val(std::string{key}, level);
  if (ret < 0) {
    return ret;
  }
  if (ret == ConfigValues::SET_HAVE_EFFECT) {
    _refresh(values, *find_option(key));
  }
  values_bl.clear();
  return 0;
}

namespace {
template<typename Size>
struct get_size_visitor
{
  get_size_visitor() {}

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
class assign_visitor
{
  ConfigValues *conf;
  Option::value_t val;
  public:

  assign_visitor(ConfigValues *conf_, Option::value_t val_)
    : conf(conf_), val(val_)
  {}

  template <typename T>
  void operator()(T ConfigValues::* ptr) const
  {
    T *member = const_cast<T *>(&(conf->*(ptr)));

    *member = std::get<T>(val);
  }
  void operator()(uint64_t ConfigValues::* ptr) const
  {
    using T = uint64_t;
    auto member = const_cast<T*>(&(conf->*(ptr)));
    *member = std::visit(get_size_visitor<T>{}, val);
  }
  void operator()(int64_t ConfigValues::* ptr) const
  {
    using T = int64_t;
    auto member = const_cast<T*>(&(conf->*(ptr)));
    *member = std::visit(get_size_visitor<T>{}, val);
  }
};
} // anonymous namespace

void md_config_t::update_legacy_vals(ConfigValues& values)
{
  for (const auto &i : legacy_values) {
    const auto &name = i.first;
    const auto &option = schema.at(name);
    auto ptr = i.second;
    update_legacy_val(values, option, ptr);
  }
}

void md_config_t::update_legacy_val(ConfigValues& values,
				    const Option &opt,
                                    md_config_t::member_ptr_t member_ptr)
{
  Option::value_t v = _get_val(values, opt);
  std::visit(assign_visitor(&values, v), member_ptr);
}

static void dump(Formatter *f, int level, Option::value_t in)
{
  if (const auto v = std::get_if<bool>(&in)) {
    f->dump_bool(ceph_conf_level_name(level), *v);
  } else if (const auto v = std::get_if<int64_t>(&in)) {
    f->dump_int(ceph_conf_level_name(level), *v);
  } else if (const auto v = std::get_if<uint64_t>(&in)) {
    f->dump_unsigned(ceph_conf_level_name(level), *v);
  } else if (const auto v = std::get_if<double>(&in)) {
    f->dump_float(ceph_conf_level_name(level), *v);
  } else {
    f->dump_stream(ceph_conf_level_name(level)) << Option::to_str(in);
  }
}

void md_config_t::diff(
  const ConfigValues& values,
  Formatter *f,
  string name) const
{
  values.for_each([this, f, &values] (auto& name, auto& configs) {
    if (configs.empty()) {
      return;
    }
    f->open_object_section(std::string{name}.c_str());
    const Option *o = find_option(name);
    if (configs.size() &&
	configs.begin()->first != CONF_DEFAULT) {
      // show compiled-in default only if an override default wasn't provided
      dump(f, CONF_DEFAULT, _get_val_default(*o));
    }
    for (auto& j : configs) {
      dump(f, j.first, j.second);
    }
    dump(f, CONF_FINAL, _get_val(values, *o));
    f->close_section();
  });
}

void md_config_t::complain_about_parse_error(CephContext *cct)
{
  ::complain_about_parse_error(cct, parse_error);
}
