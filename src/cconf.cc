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
#include "common/entity_name.h"
#include "common/ceph_argparse.h"
#include "common/config.h"
#include "include/str_list.h"

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
  --name name                     Set type.id\n\
  [-s <section>]                  Add to list of sections to search\n\
\n\
If there is no action given, the action will default to --lookup.\n\
\n\
EXAMPLES\n\
$ cconf --name client.cconf -c /etc/ceph/ceph.conf -t mon -i 0 'mon addr'\n\
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

static int list_sections(const char *prefix)
{
  std::vector <std::string> sections;
  int ret = g_conf.get_all_sections(sections);
  if (ret)
    return 2;
  for (std::vector<std::string>::const_iterator p = sections.begin();
       p != sections.end(); ++p) {
    if (strncmp(prefix, p->c_str(), strlen(prefix)) == 0) {
      cout << *p << std::endl;
    }
  }
  return 0;
}

static int lookup(const deque<const char *> &sections,
		  const char *key, bool resolve_search)
{
  std::vector <std::string> my_sections;
  for (deque<const char *>::const_iterator s = sections.begin(); s != sections.end(); ++s) {
    my_sections.push_back(*s);
  }
  g_conf.get_my_sections(my_sections);
  std::string val;
  int ret = g_conf.get_val_from_conf_file(my_sections, key, val, true);
  if (ret == -ENOENT)
    return 1;
  else if (ret == 0) {
    if (resolve_search) {
      string result;
      if (ceph_resolve_file_search(val, result))
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
  char *section;
  vector<const char*> args, nargs;
  deque<const char *> sections;
  DEFINE_CONF_VARS(usage);
  bool resolve_search = false;
  std::string action("lookup");

  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  FOR_EACH_ARG(args) {
    if (CEPH_ARGPARSE_EQ("section", 's')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&section, OPT_STR);
      sections.push_back(section);
    } else if (CEPH_ARGPARSE_EQ("resolve-search", 'r')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&resolve_search, OPT_BOOL);
    } else if (CEPH_ARGPARSE_EQ("help", 'h')) {
      action = "help";
    } else if (CEPH_ARGPARSE_EQ("list-sections", 'l')) {
      action = "list-sections";
    } else if (CEPH_ARGPARSE_EQ("lookup", '\0')) {
      action = "lookup";
    }
    else {
      nargs.push_back(args[i]);
    }
  }

  common_init(nargs, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);

  if (action == "help") {
    usage();
    exit(0);
  }
  else if (action == "list-sections") {
    if (nargs.size() != 1)
      usage();
    return list_sections(nargs[0]);
  }
  else if (action == "lookup") {
    if (nargs.size() != 1) {
      cerr << "lookup: expected exactly one argument" << std::endl;
      usage();
    }
    return lookup(sections, nargs[0], resolve_search);
  }
  else if ((nargs.size() >= 1) && (nargs[0][0] == '-')) {
    cerr << "Parse error at argument: " << nargs[0] << std::endl;
    usage();
  }
  else {
    if (nargs.size() != 1) {
      cerr << "lookup: expected exactly one argument" << std::endl;
      usage();
    }
    return lookup(sections, nargs[0], resolve_search);
  }
}
