// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2010 Dreamhost
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iomanip>
#include <string>

#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "log/Log.h"
#include "mon/AuthMonitor.h"
#include "common/Formatter.h"

using std::deque;
using std::string;
using std::unique_ptr;
using std::cerr;
using std::cout;
using std::vector;

static void usage(std::ostream& out)
{
  // TODO: add generic_usage once cerr/derr issues are resolved
  out << R"(Ceph configuration query tool

USAGE
ceph-conf <flags> <action>

ACTIONS
  -L|--list-all-sections          List all sections
  -l|--list-sections <prefix>     List sections with the given prefix
  --filter-key <key>              Filter section list to only include sections
                                  with given key defined.
  --filter-key-value <key>=<val>  Filter section list to only include sections
                                  with given key/value pair.
  --lookup <key>                  Print a configuration setting to stdout.
                                  Returns 0 (success) if the configuration setting is
                                  found; 1 otherwise.
  -r|--resolve-search             search for the first file that exists and
                                  can be opened in the resulted comma
                                  delimited search list.
  -D|--dump-all                   dump all variables.
  --show-config-value <key>       Print the corresponding ceph.conf value
                                  that matches the specified key. Also searches
                                  global defaults.

FLAGS
  --name name                     Set type.id
  [-s <section>]                  Add to list of sections to search
  [--format plain|json|json-pretty]
                                  dump variables in plain text, json or pretty
                                  json
  [--pid <pid>]                   Override the $pid when expanding options

If there is no action given, the action will default to --lookup.

EXAMPLES
$ ceph-conf --name mon.0 -c /etc/ceph/ceph.conf 'mon addr'
Find out what the value of 'mon addr' is for monitor 0.

$ ceph-conf -l mon
List sections beginning with 'mon'.

RETURN CODE
Return code will be 0 on success; error code otherwise.
)";
}

static int list_sections(const std::string &prefix,
			 const std::list<string>& filter_key,
			 const std::map<string,string>& filter_key_value)
{
  std::vector <std::string> sections;
  int ret = g_conf().get_all_sections(sections);
  if (ret)
    return 2;
  for (std::vector<std::string>::const_iterator p = sections.begin();
       p != sections.end(); ++p) {
    if (strncmp(prefix.c_str(), p->c_str(), prefix.size()))
      continue;

    std::vector<std::string> sec;
    sec.push_back(*p);

    int r = 0;
    for (std::list<string>::const_iterator q = filter_key.begin(); q != filter_key.end(); ++q) {
      string v;
      r = g_conf().get_val_from_conf_file(sec, q->c_str(), v, false);
      if (r < 0)
	break;
    }
    if (r < 0)
      continue;

    for (std::map<string,string>::const_iterator q = filter_key_value.begin();
	 q != filter_key_value.end();
	 ++q) {
      string v;
      r = g_conf().get_val_from_conf_file(sec, q->first.c_str(), v, false);
      if (r < 0 || v != q->second) {
	r = -1;
	break;
      }
    }
    if (r < 0)
      continue;
    
    cout << *p << std::endl;
  }
  return 0;
}

static int lookup(const std::deque<std::string> &sections,
		  const std::string &key, bool resolve_search)
{
  std::vector<std::string> my_sections{sections.begin(), sections.end()};
  for (auto& section : g_conf().get_my_sections()) {
    my_sections.push_back(section);
  }
  std::string val;
  int ret = g_conf().get_val_from_conf_file(my_sections, key.c_str(), val, true);
  if (ret == -ENOENT)
    return 1;
  else if (ret == 0) {
    if (resolve_search) {
      string result;
      ret = ceph_resolve_file_search(val, result);
      if (!ret)
	puts(result.c_str());
    }
    else {
      puts(val.c_str());
    }
    return 0;
  }
  else {
    cerr << "error looking up '" << key << "': error " << ret << std::endl;
    return 2;
  }
}

static int dump_all(const string& format)
{
  if (format == "" || format == "plain") {
    g_conf().show_config(std::cout);
    return 0;
  } else {
    unique_ptr<Formatter> f(Formatter::create(format));
    if (f) {
      f->open_object_section("ceph-conf");
      g_conf().show_config(f.get());
      f->close_section();
      f->flush(std::cout);
      return 0;
    }
    cerr << "format '" << format << "' not recognized." << std::endl;
    usage(cerr);
    return 1;
  }
}

static void maybe_override_pid(vector<const char*>& args)
{
  for (auto i = args.begin(); i != args.end(); ++i) {
    string val;
    if (ceph_argparse_witharg(args, i, &val, "--pid", (char*)NULL)) {
      setenv("PID", val.c_str(), 1);
      break;
    }
  }
}

int main(int argc, const char **argv)
{
  deque<std::string> sections;
  bool resolve_search = false;
  std::string action;
  std::string lookup_key;
  std::string section_list_prefix;
  std::list<string> filter_key;
  std::map<string,string> filter_key_value;
  std::string dump_format;

  auto args = argv_to_vec(argc, argv);

  auto orig_args = args;
  auto cct = [&args] {
    // override the PID before options are expanded
    maybe_override_pid(args);
    std::map<std::string,std::string> defaults = {{"log_to_file", "false"}};
    return global_init(&defaults, args, CEPH_ENTITY_TYPE_CLIENT,
		       CODE_ENVIRONMENT_DAEMON,
		       CINIT_FLAG_NO_DAEMON_ACTIONS |
		       CINIT_FLAG_NO_MON_CONFIG);
  }();

  // do not common_init_finish(); do not start threads; do not do any of thing
  // wonky things the daemon whose conf we are examining would do (like initialize
  // the admin socket).
  //common_init_finish(g_ceph_context);

  std::string val;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val, "-s", "--section", (char*)NULL)) {
      sections.push_back(val);
    } else if (ceph_argparse_flag(args, i, "-r", "--resolve_search", (char*)NULL)) {
      resolve_search = true;
    } else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      action = "help";
    } else if (ceph_argparse_witharg(args, i, &val, "--lookup", (char*)NULL)) {
      action = "lookup";
      lookup_key = val;
    } else if (ceph_argparse_flag(args, i, "-L", "--list_all_sections", (char*)NULL)) {
      action = "list-sections";
      section_list_prefix = "";
    } else if (ceph_argparse_witharg(args, i, &val, "-l", "--list_sections", (char*)NULL)) {
      action = "list-sections";
      section_list_prefix = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--filter_key", (char*)NULL)) {
      filter_key.push_back(val);
    } else if (ceph_argparse_witharg(args, i, &val, "--filter_key_value", (char*)NULL)) {
      size_t pos = val.find_first_of('=');
      if (pos == string::npos) {
	cerr << "expecting argument like 'key=value' for --filter-key-value (not '" << val << "')" << std::endl;
	usage(cerr);
	return EXIT_FAILURE;
      } 
      string key(val, 0, pos);
      string value(val, pos+1);
      filter_key_value[key] = value;
    } else if (ceph_argparse_flag(args, i, "-D", "--dump_all", (char*)NULL)) {
      action = "dumpall";
    } else if (ceph_argparse_witharg(args, i, &val, "--format", (char*)NULL)) {
      dump_format = val;
    } else {
      if (((action == "lookup") || (action == "")) && (lookup_key.empty())) {
	action = "lookup";
	lookup_key = *i++;
      } else {
	cerr << "unable to parse option: '" << *i << "'" << std::endl;
	cerr << "args:";
	for (auto arg : orig_args) {
	  cerr << " " << std::quoted(arg);
	}
	cerr << std::endl;
	usage(cerr);
	return EXIT_FAILURE;
      }
    }
  }

  cct->_log->flush();
  if (action == "help") {
    usage(cout);
    return EXIT_SUCCESS;
  } else if (action == "list-sections") {
    return list_sections(section_list_prefix, filter_key, filter_key_value);
  } else if (action == "lookup") {
    return lookup(sections, lookup_key, resolve_search);
  } else if (action == "dumpall") {
    return dump_all(dump_format);
  } else {
    cerr << "You must give an action, such as --lookup or --list-all-sections." << std::endl;
    cerr << "Pass --help for more help." << std::endl;
    return EXIT_FAILURE;
  }
}
