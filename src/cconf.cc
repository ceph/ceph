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
#include "config.h"
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
\n\
  --lookup <key> [defval]         Print a configuration setting to stdout.\n\
				  If the setting is not defined, and the\n\
				  optional argument defval is provide, it will\n\
				  be printed instead. variables in defval are\n\
				  interpolated.\n\
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
  ConfFile *cf = conf_get_conf_file();
  if (!cf)
    return 2;
  for (std::list<ConfSection*>::const_iterator p =
	    cf->get_section_list().begin();
       p != cf->get_section_list().end(); ++p)
  {
    if (strncmp(s, (*p)->get_name().c_str(), strlen(s)) == 0)
      cout << (*p)->get_name() << std::endl;
  }
  return 0;
}

static int lookup_impl(const deque<const char *> &sections,
		    const char *key, const char *defval)
{
  char *val = NULL;
  ConfFile *cf = conf_get_conf_file();
  if (!cf)
    return 2;
  conf_read_key(NULL, key, OPT_STR, (char **)&val, NULL);

  if (val) {
    puts(val);
    free(val);
    return 0;
  }

  for (unsigned int i=0; i<sections.size(); i++) {
    cf->read(sections[i], key, (char **)&val, NULL);

    if (val) {
      puts(val);
      free(val);
      return 0;
    }
  }

  if (defval) {
    val = conf_post_process_val(defval);
    if (val) {
      puts(val);
      free(val);
      return 0;
    }
  }

  {
    // TODO: document exactly what we are doing here?
    std::vector<const char *> empty_args;
    parse_startup_config_options(empty_args, type);
    char buf[1024];
    memset(buf, 0, sizeof(buf));
    if (ceph_def_conf_by_name(key, buf, sizeof(buf))) {
      cout << buf << std::endl;
      return 0;
    }
  }

  return 1;
}

static int lookup(const deque<const char *> &sections,
		   const vector<const char*> &nargs,
		   vector<const char*>::const_iterator n)
{
  const char *key = *n;
  ++n;
  if (n == nargs.end())
    return lookup_impl(sections, key, NULL);
  const char *defval = *n;
  ++n;
  if (n == nargs.end())
    return lookup_impl(sections, key, defval);

  cerr << "lookup: Too many arguments. Expected only 1 or 2."
       << std::endl;
  usage();
  return 1;
}

int main(int argc, const char **argv)
{
  char *section;
  vector<const char*> args, nargs;
  deque<const char *> sections;
  DEFINE_CONF_VARS(usage);

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
    }
    else {
      nargs.push_back(args[i]);
    }
  }

  common_set_defaults(false);
  common_init(nargs, type, false);
  set_foreground_logging();

  if ((nargs.size() == 1) && (!strcmp(nargs[0], "-h"))) {
    usage();
  }
  else if ((nargs.size() == 2) &&
      (!strcmp(nargs[0], "--list_sections") || !strcmp(nargs[0], "-l"))) {
    return list_sections(nargs[1]);
  }
  else if ((nargs.size() >= 2) && (!strcmp(nargs[0], "--lookup"))) {
    vector<const char*>::const_iterator n = nargs.begin();
    ++n;
    return lookup(sections, nargs, n);
  }
  else if ((nargs.size() >= 1) && (nargs[0][0] == '-')) {
    cerr << "Parse error at argument: " << nargs[0] << std::endl;
    usage();
  }
  else {
    vector<const char*>::const_iterator n = nargs.begin();
    return lookup(sections, nargs, n);
  }
}
