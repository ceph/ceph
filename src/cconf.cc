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
  --lookup <key>                  Print a configuration setting to stdout.\n\
                                  Returns 0 (success) if the configuration setting is\n\
                                  found; 1 otherwise.\n\
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

static int lookup(const deque<const char *> &sections,
		  const char *key, bool resolve_search)
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

  common_init(nargs, type, STARTUP_FLAG_FORCE_FG_LOGGING);

  if (do_help) {
    usage();
    exit(0);
  }
  if (!do_lookup && !do_list)
    do_lookup = true;

  if (do_list) {
    if (nargs.size() != 1)
      usage();
    return list_sections(nargs[0]);
  } else if (do_lookup) {
    if (nargs.size() != 1) {
      cerr << "lookup: expected exactly one argument" << std::endl;
      usage();
    }
    return lookup(sections, nargs[0], resolve_search);
  } else if ((nargs.size() >= 1) && (nargs[0][0] == '-')) {
    cerr << "Parse error at argument: " << nargs[0] << std::endl;
    usage();
  } else {
    if (nargs.size() != 1) {
      cerr << "lookup: expected exactly one argument" << std::endl;
      usage();
    }
    return lookup(sections, nargs[0], resolve_search);
  }
}
