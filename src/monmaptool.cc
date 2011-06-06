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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include <sys/stat.h>
#include <iostream>
#include <string>
using namespace std;

#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "mon/MonMap.h"

void usage()
{
  cout << " usage: [--print] [--create [--clobber]] [--add name 1.2.3.4:567] [--rm name] <mapfilename>" << std::endl;
  exit(1);
}

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  DEFINE_CONF_VARS(usage);

  const char *me = argv[0];

  const char *fn = 0;
  bool print = false;
  bool create = false;
  bool clobber = false;
  bool modified = false;
  map<string,entity_addr_t> add;
  list<string> rm;

  common_init(args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
	      CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(&g_ceph_context);
  FOR_EACH_ARG(args) {
    if (CEPH_ARGPARSE_EQ("print", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&print, OPT_BOOL);
    } else if (CEPH_ARGPARSE_EQ("create", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&create, OPT_BOOL);
    } else if (CEPH_ARGPARSE_EQ("clobber", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&clobber, OPT_BOOL);
    } else if (CEPH_ARGPARSE_EQ("add", '\0')) {
      if (++i >= args.size())
	usage();
      string name = args[i];
      if (++i >= args.size())
	usage();
      entity_addr_t addr;
      if (!addr.parse(args[i])) {
	cerr << me << ": invalid ip:port '" << args[i] << "'" << std::endl;
	return -1;
      }
      if (addr.get_port() == 0)
	addr.set_port(CEPH_MON_PORT);
      add[name] = addr;
      modified = true;
    } else if (CEPH_ARGPARSE_EQ("rm", '\0')) {
      if (++i >= args.size())
	usage();
      string name = args[i];
      rm.push_back(name);
      modified = true;
    } else if (!fn)
      fn = args[i];
    else {
      cout << "invalid argument: '" << args[i] << "'" << std::endl;
      usage();
    }
  }
  if (!fn)
    usage();
  
  MonMap monmap;

  cout << me << ": monmap file " << fn << std::endl;

  int r = 0;
  if (!(create && clobber)) {
    try {
      r = monmap.read(fn);
    } catch (...) {
      cerr << me << ": unable to read monmap file" << std::endl;
      return -1;
    }
  }

  char buf[80];
  if (!create && r < 0) {
    cerr << me << ": couldn't open " << fn << ": " << strerror_r(-r, buf, sizeof(buf)) << std::endl;
    return -1;
  }    
  else if (create && !clobber && r == 0) {
    cerr << me << ": " << fn << " exists, --clobber to overwrite" << std::endl;
    return -1;
  }

  if (create) {
    srand(getpid() + time(0));
    monmap.generate_fsid();
    cout << me << ": generated fsid " << monmap.fsid << std::endl;
    modified++;
  }

  for (map<string,entity_addr_t>::iterator p = add.begin(); p != add.end(); p++) {
    if (monmap.contains(p->first)) {
      cerr << me << ": map already contains mon." << p->first << std::endl;
      usage();
    }
    if (monmap.contains(p->second)) {
      cerr << me << ": map already contains " << p->second << std::endl;
      usage();
    }
    monmap.add(p->first, p->second);
  }
  for (list<string>::iterator p = rm.begin(); p != rm.end(); p++) {
    cout << me << ": removing " << *p << std::endl;
    if (!monmap.contains(*p)) {
      cerr << me << ": map does not contain " << *p << std::endl;
      usage();
    }
    monmap.remove(*p);
  }

  if (!print && !modified)
    usage();

  if (modified)
    monmap.epoch++;

  if (print) 
    monmap.print(cout);

  if (modified) {
    // write it out
    cout << me << ": writing epoch " << monmap.epoch
	 << " to " << fn
	 << " (" << monmap.size() << " monitors)" 
	 << std::endl;
    int r = monmap.write(fn);
    if (r < 0) {
      cerr << "monmaptool: error writing to '" << fn << "': " << strerror_r(-r, buf, sizeof(buf)) << std::endl;
      return 1;
    }
  }
  

  return 0;
}
