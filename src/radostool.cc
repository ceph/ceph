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

#include "include/librados.h"
#include "config.h"
#include "common/common_init.h"

#include <iostream>

#include <stdlib.h>
#include <time.h>

void usage() 
{
  cerr << "usage: radostool [options] [commands]" << std::endl;
  /*  cerr << "If no commands are specified, enter interactive mode.\n";
  cerr << "Commands:" << std::endl;
  cerr << "   stop              -- cleanly shut down file system" << std::endl
       << "   (osd|pg|mds) stat -- get monitor subsystem status" << std::endl
       << "   ..." << std::endl;
  */
  cerr << "Options:" << std::endl;
  cerr << "   -i infile\n";
  cerr << "   -o outfile\n";
  cerr << "        specify input or output file (for certain commands)\n";
  generic_client_usage();
}

int main(int argc, const char **argv) 
{
  DEFINE_CONF_VARS(usage);
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  common_init(args, "rados", false);

  vector<const char*> nargs;
  bufferlist indata, outdata;
  const char *outfile = 0;
  
  const char *pool = 0;

  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("out_file", 'o')) {
      CONF_SAFE_SET_ARG_VAL(&outfile, OPT_STR);
    } else if (CONF_ARG_EQ("in_data", 'i')) {
      const char *fname;
      CONF_SAFE_SET_ARG_VAL(&fname, OPT_STR);
      int r = indata.read_file(fname);
      if (r < 0) {
	cerr << "error reading " << fname << ": " << strerror(-r) << std::endl;
	exit(0);
      } else {
	cout << "read " << indata.length() << " bytes from " << fname << std::endl;
      }
    } else if (CONF_ARG_EQ("pool", 'p')) {
      CONF_SAFE_SET_ARG_VAL(&pool, OPT_STR);
    } else if (CONF_ARG_EQ("help", 'h')) {
      usage();
    } else if (args[i][0] == '-' && nargs.empty()) {
      cerr << "unrecognized option " << args[i] << std::endl;
      usage();
    } else
      nargs.push_back(args[i]);
  }

  if (nargs.empty())
    usage();

  // open rados
  Rados rados;
  if (rados.initialize(0, NULL) < 0) {
     cerr << "couldn't initialize rados!" << std::endl;
     exit(1);
  }

  // open pool?
  rados_pool_t p;
  if (pool) {
    int r = rados.open_pool(pool, &p);
    if (r < 0) {
      cerr << "error opening pool " << pool << ": " << strerror(-r) << std::endl;
      exit(0);
    }
  }

  // list pools?
  if (strcmp(nargs[0], "lspools") == 0) {
    vector<string> vec;
    rados.list_pools(vec);
    for (vector<string>::iterator i = vec.begin(); i != vec.end(); ++i)
      cout << *i << std::endl;

  } else if (strcmp(nargs[0], "ls") == 0) {
    if (!pool)
      usage();

    Rados::ListCtx ctx;
    while (1) {
      list<object_t> vec;
      int r = rados.list(p, 1 << 10, vec, ctx);
      cout << "list result=" << r << " entries=" << vec.size() << std::endl;
      if (r < 0) {
	cerr << "got error: " << strerror(-r) << std::endl;
	break;
      }
      if (vec.empty())
	break;
      for (list<object_t>::iterator iter = vec.begin(); iter != vec.end(); ++iter)
	cout << *iter << std::endl;
    }


  } else if (strcmp(nargs[0], "get") == 0) {
    if (!pool || nargs.size() < 2)
      usage();
    object_t oid(nargs[1]);
    int r = rados.read(p, oid, 0, outdata, 0);
    if (r < 0) {
      cerr << "error reading " << oid << " from pool " << pool << ": " << strerror(-r) << std::endl;
      exit(0);
    }

  } else if (strcmp(nargs[0], "put") == 0) {
    if (!pool || nargs.size() < 2)
      usage();
    if (!indata.length()) {
      cerr << "must specify input file" << std::endl;
      usage();
    }
    object_t oid(nargs[1]);
    int r = rados.write(p, oid, 0, indata, indata.length());
    if (r < 0) {
      cerr << "error writing " << oid << " to pool " << pool << ": " << strerror(-r) << std::endl;
      exit(0);
    }

  } else {
    cerr << "unrecognized command " << nargs[0] << std::endl;
    usage();
  }

  // write data?
  int len = outdata.length();
  if (len) {
    if (outfile) {
      if (strcmp(outfile, "-") == 0) {
	::write(1, outdata.c_str(), len);
      } else {
	outdata.write_file(outfile);
      }
      generic_dout(0) << "wrote " << len << " byte payload to " << outfile << dendl;
    } else {
      generic_dout(0) << "got " << len << " byte payload, discarding (specify -o <outfile)" << dendl;
    }
  }

  if (pool)
    rados.close_pool(p);

  rados_deinitialize();
  return 0;
}
