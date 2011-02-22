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

#include <fcntl.h>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>

#include "mon/AuthMonitor.h"
#include "common/ConfUtils.h"
#include "common/common_init.h"
#include "common/ceph_argparse.h"
#include "common/config.h"
#include "include/str_list.h"

const char *type = NULL;

static void usage()
{
  // TODO: add generic_usage once cerr/derr issues are resolved
  cerr << "Ceph configuration query tool\n\
\n\
USAGE\n\
cconf <flags> <action>\n\
\n\
ACTIONS\n\
  -l|--list-sections <prefix>     List sections in prefix\n\
  --lookup <key> [defval]         Print a configuration setting to stdout.\n\
				  If the setting is not defined, and the\n\
				  optional argument defval is provide, it will\n\
				  be printed instead. variables in defval are\n\
				  interpolated.\n\
  -r|--resolve-search             search for the first file that exists and\n\
                                  can be opened in the resulted comma\n\
                                  delimited search list.\n\
\n\
FLAGS\n\
  -i id                           Set id\n\
  [-s <section>]                  Add to list of sections to search\n\
\n\
If there is no action given, the action will default to --lookup.\n\
\n\
EXAMPLES\n\
$ cconf -i cconf -c /etc/ceph/ceph.conf -t mon -i 0 'mon addr'\n\
Find out if there is a 'mon addr' defined in /etc/ceph/ceph.conf\n\
\n\
$ cconf -l mon\n\
List sections beginning with 'mon'.\n\
\n\
RETURN CODE\n\
Return code will be 0 on success; error code otherwise.\n\
";
}

void error_exit()
{
  usage();
  exit(1);
}

static int list_sections(const char *s)
{
  if (!g_conf.cf)
    return 2;
  for (std::list<ConfSection*>::const_iterator p =
	    g_conf.cf->get_section_list().begin();
       p != g_conf.cf->get_section_list().end(); ++p)
  {
    if (strncmp(s, (*p)->get_name().c_str(), strlen(s)) == 0)
      cout << (*p)->get_name() << std::endl;
  }
  return 0;
}

static void print_val(const char *val, bool resolve_search)
{
  if (!resolve_search) {
    puts(val);
  } else {
    string search_path(val);
    string result;
    if (ceph_resolve_file_search(search_path, result))
      puts(result.c_str());
  }
}

static int lookup_impl(const deque<const char *> &sections,
		    const char *key, const char *defval,
                    bool resolve_search)
{
  char *val = NULL;
  if (!g_conf.cf)
    return 2;
  conf_read_key(NULL, key, OPT_STR, (char **)&val, NULL);
  if (val) {
    print_val(val, resolve_search);
    free(val);
    return 0;
  }

  for (unsigned int i=0; i<sections.size(); i++) {
    g_conf.cf->read(sections[i], key, (char **)&val, NULL);
    if (val) {
      print_val(val, resolve_search);
      free(val);
      return 0;
    }
  }

  if (defval) {
    val = conf_post_process_val(defval);
    if (val) {
      print_val(val, resolve_search);
      free(val);
      return 0;
    }
  }

  {
    // TODO: document exactly what we are doing here?
    std::vector<const char *> empty_args;
    bool force_fg_logging = false;
    parse_startup_config_options(empty_args, type,
			 STARTUP_FLAG_FORCE_FG_LOGGING, &force_fg_logging);
    char buf[1024];
    memset(buf, 0, sizeof(buf));
    if (ceph_def_conf_by_name(key, buf, sizeof(buf))) {
      print_val(buf, resolve_search);
      return 0;
    }
  }

  return 1;
}

static int lookup(const deque<const char *> &sections,
		  const vector<const char*> &nargs,
		  vector<const char*>::const_iterator n,
                  bool resolve_search)
{
  const char *key = *n;
  ++n;
  if (n == nargs.end())
    return lookup_impl(sections, key, NULL, resolve_search);
  const char *defval = *n;
  ++n;
  if (n == nargs.end())
    return lookup_impl(sections, key, defval, resolve_search);

  cerr << "lookup: Too many arguments. Expected only 1 or 2."
       << std::endl;
  error_exit();
  return 1;
}

int main(int argc, const char **argv)
{
  char *section;
  vector<const char*> args, nargs;
  deque<const char *> sections;
  DEFINE_CONF_VARS(usage);
  bool resolve_search = false;
  bool do_help = false;
  bool do_list = false;
  bool do_lookup = false;

  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("type", 't')) {
      CONF_SAFE_SET_ARG_VAL(&type, OPT_STR);
    } else if (CONF_ARG_EQ("id", 'i')) {
      CONF_SAFE_SET_ARG_VAL(&g_conf.id, OPT_STR);
    } else if (CONF_ARG_EQ("section", 's')) {
      CONF_SAFE_SET_ARG_VAL(&section, OPT_STR);
      sections.push_back(section);
    } else if (CONF_ARG_EQ("resolve-search", 'r')) {
      CONF_SAFE_SET_ARG_VAL(&resolve_search, OPT_BOOL);
    } else if (CONF_ARG_EQ("help", 'h')) {
      CONF_SAFE_SET_ARG_VAL(&do_help, OPT_BOOL);
    } else if (CONF_ARG_EQ("list-sections", 'l')) {
      CONF_SAFE_SET_ARG_VAL(&do_list, OPT_BOOL);
    } else if (CONF_ARG_EQ("lookup", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&do_lookup, OPT_BOOL);
    }
    else {
      nargs.push_back(args[i]);
    }
  }

  common_set_defaults(false);
  common_init(nargs, type, STARTUP_FLAG_FORCE_FG_LOGGING);

  if (do_help) {
    usage();
    exit(0);
  }
  if (!do_lookup && !do_list)
    do_lookup = true;

  if (do_list) {
    if (nargs.size() != 1)
      error_exit();
    return list_sections(nargs[0]);
  } else if (do_lookup) {
    if (nargs.size() < 1 || nargs.size() > 2)
      error_exit();
    vector<const char*>::const_iterator n = nargs.begin();
    return lookup(sections, nargs, n, resolve_search);
  } else if ((nargs.size() >= 1) && (nargs[0][0] == '-')) {
    cerr << "Parse error at argument: " << nargs[0] << std::endl;
    error_exit();
  } else {
    vector<const char*>::const_iterator n = nargs.begin();
    return lookup(sections, nargs, n, resolve_search);
  }
}
