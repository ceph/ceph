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

#include <string>

#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "mon/AuthMonitor.h"

using std::deque;
using std::string;

static void usage()
{
  // TODO: add generic_usage once cerr/derr issues are resolved
  cerr << "Ceph configuration query tool\n\
\n\
USAGE\n\
ceph-conf <flags> <action>\n\
\n\
ACTIONS\n\
  -L|--list-all-sections          List all sections\n\
  -l|--list-sections <prefix>     List sections with the given prefix\n\
  --filter-key <key>              Filter section list to only include sections\n\
                                  with given key defined.\n\
  --filter-key-value <key>=<val>  Filter section list to only include sections\n\
                                  with given key/value pair.\n\
  --lookup <key>                  Print a configuration setting to stdout.\n\
                                  Returns 0 (success) if the configuration setting is\n\
                                  found; 1 otherwise.\n\
  -r|--resolve-search             search for the first file that exists and\n\
                                  can be opened in the resulted comma\n\
                                  delimited search list.\n\
\n\
FLAGS\n\
  --name name                     Set type.id\n\
  [-s <section>]                  Add to list of sections to search\n\
\n\
If there is no action given, the action will default to --lookup.\n\
\n\
EXAMPLES\n\
$ ceph-conf --name mon.0 -c /etc/ceph/ceph.conf 'mon addr'\n\
Find out what the value of 'mon add' is for monitor 0.\n\
\n\
$ ceph-conf -l mon\n\
List sections beginning with 'mon'.\n\
\n\
RETURN CODE\n\
Return code will be 0 on success; error code otherwise.\n\
";
  exit(1);
}

static int list_sections(const std::string &prefix,
			 const std::list<string>& filter_key,
			 const std::map<string,string>& filter_key_value)
{
  std::vector <std::string> sections;
  int ret = g_conf->get_all_sections(sections);
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
      r = g_conf->get_val_from_conf_file(sec, q->c_str(), v, false);
      if (r < 0)
	break;
    }
    if (r < 0)
      continue;

    for (std::map<string,string>::const_iterator q = filter_key_value.begin();
	 q != filter_key_value.end();
	 ++q) {
      string v;
      r = g_conf->get_val_from_conf_file(sec, q->first.c_str(), v, false);
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
  std::vector <std::string> my_sections;
  for (deque<string>::const_iterator s = sections.begin(); s != sections.end(); ++s) {
    my_sections.push_back(*s);
  }
  g_conf->get_my_sections(my_sections);
  std::string val;
  int ret = g_conf->get_val_from_conf_file(my_sections, key.c_str(), val, true);
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

int main(int argc, const char **argv)
{
  vector<const char*> args;
  deque<std::string> sections;
  bool resolve_search = false;
  std::string action;
  std::string lookup_key;
  std::string section_list_prefix;
  std::list<string> filter_key;
  std::map<string,string> filter_key_value;

  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  vector<const char*> orig_args = args;

  global_pre_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_DAEMON,
		  CINIT_FLAG_NO_DAEMON_ACTIONS);
  std::unique_ptr<CephContext,
		  std::function<void(CephContext*)> > cct_deleter{
      g_ceph_context,
      [](CephContext *p) {p->put();}
  };

  g_conf->apply_changes(NULL);
  g_conf->complain_about_parse_errors(g_ceph_context);

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
	usage();
	exit(1);
      } 
      string key(val, 0, pos);
      string value(val, pos+1);
      filter_key_value[key] = value;
    } else {
      if (((action == "lookup") || (action == "")) && (lookup_key.empty())) {
	action = "lookup";
	lookup_key = *i++;
      } else {
	cerr << "unable to parse option: '" << *i << "'" << std::endl;
	cerr << "args:";
	for (std::vector<const char *>::iterator ci = orig_args.begin(); ci != orig_args.end(); ++ci) {
	  cerr << " '" << *ci << "'";
	}
	cerr << std::endl;
	usage();
	exit(1);
      }
    }
  }

  g_ceph_context->_log->flush();
  if (action == "help") {
    usage();
    exit(0);
  } else if (action == "list-sections") {
    return list_sections(section_list_prefix, filter_key, filter_key_value);
  } else if (action == "lookup") {
    return lookup(sections, lookup_key, resolve_search);
  } else {
    cerr << "You must give an action, such as --lookup or --list-all-sections." << std::endl;
    cerr << "Pass --help for more help." << std::endl;
    exit(1);
  }
}
