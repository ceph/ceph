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

#include "osdc/rados_bencher.h"
#include "osdc/librados.h"

#include "config.h"
#include "common/common_init.h"
#include "common/Cond.h"
#include <iostream>
#include <fstream>

#include <stdlib.h>
#include <time.h>
#include <sstream>


void usage() 
{
  cerr << "usage: radostool [options] [commands]" << std::endl;
  /*  cerr << "If no commands are specified, enter interactive mode.\n";
  cerr << "Commands:" << std::endl;
  cerr << "   stop              -- cleanly shut down file system" << std::endl
       << "   (osd|pg|mds) stat -- get monitor subsystem status" << std::endl
       << "   ..." << std::endl;
  */
  cerr << "Commands:\n";
  cerr << "   lspools     -- list pools\n";
  cerr << "   df          -- show per-pool and total usage\n\n";

  cerr << "Pool commands:\n";
  cerr << "   get objname -- fetch object\n";
  cerr << "   put objname -- write object\n";
  cerr << "   rm objname  -- remove object\n";
  cerr << "   ls          -- list objects in pool\n\n";

  cerr << "   lssnap      -- list snaps\n";
  cerr << "   mksnap foo  -- create snap 'foo'\n";
  cerr << "   rmsnap foo  -- remove snap 'foo'\n\n";

  cerr << "   bench <seconds> write|seq|rand [-t concurrent_operations] [-b op_size]\n";
  cerr << "              default is 16 concurrent IOs and 4 MB op size\n\n";

  cerr << "Options:\n";
  cerr << "   -p pool\n";
  cerr << "   --pool=pool\n";
  cerr << "        select given pool by name\n";
  cerr << "   -s name\n";
  cerr << "   --snap name\n";
  cerr << "        select given snap name for (read) IO\n";
  cerr << "   -i infile\n";
  cerr << "   -o outfile\n";
  cerr << "        specify input or output file (for certain commands)\n";
  exit(1);
}


/**********************************************

**********************************************/

int main(int argc, const char **argv) 
{
  DEFINE_CONF_VARS(usage);
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  common_init(args, "rados", false, true);

  vector<const char*> nargs;
  bufferlist indata, outdata;

  const char *pool = 0;
 
  int concurrent_ios = 16;
  int op_size = 1 << 22;

  const char *snapname = 0;
  rados_snap_t snapid = CEPH_NOSNAP;

  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("pool", 'p')) {
      CONF_SAFE_SET_ARG_VAL(&pool, OPT_STR);
    } else if (CONF_ARG_EQ("snapid", 'S')) {
      CONF_SAFE_SET_ARG_VAL(&snapid, OPT_LONGLONG);
    } else if (CONF_ARG_EQ("snap", 's')) {
      CONF_SAFE_SET_ARG_VAL(&snapname, OPT_STR);
    } else if (CONF_ARG_EQ("help", 'h')) {
      usage();
    } else if (CONF_ARG_EQ("concurrent-ios", 't')) {
      CONF_SAFE_SET_ARG_VAL(&concurrent_ios, OPT_INT);
    } else if (CONF_ARG_EQ("block-size", 'b')) {
      CONF_SAFE_SET_ARG_VAL(&op_size, OPT_INT);
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

  int ret = 0;
  char buf[80];

  // open pool?
  rados_pool_t p;
  if (pool) {
    ret = rados.open_pool(pool, &p);
    if (ret < 0) {
      cerr << "error opening pool " << pool << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto no_pool_out;
    }
  }

  // snapname?
  if (snapname) {
    ret = rados.snap_lookup(p, snapname, &snapid);
    if (ret < 0) {
      cerr << "error looking up snap '" << snapname << "': " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
  }
  if (snapid != CEPH_NOSNAP) {
    string name;
    ret = rados.snap_get_name(p, snapid, &name);
    if (ret < 0) {
      cerr << "snapid " << snapid << " doesn't exist in pool " << pool << std::endl;
      goto out;
    }
    rados.set_snap(p, snapid);
    cout << "selected snap " << snapid << " '" << snapname << "'" << std::endl;
  }

  // list pools?
  if (strcmp(nargs[0], "lspools") == 0) {
    vector<string> vec;
    rados.list_pools(vec);
    for (vector<string>::iterator i = vec.begin(); i != vec.end(); ++i)
      cout << *i << std::endl;
  }
  else if (strcmp(nargs[0], "df") == 0) {
    // pools
    vector<string> vec;
    rados.list_pools(vec);
    
    map<string,rados_pool_stat_t> stats;
    rados.get_pool_stats(vec, stats);

    printf("%-15s %12s %12s %12s %12s %12s %12s %12s %12s\n",
	   "pool name", "KB", "objects", "clones", "degraded", "rd", "rd KB", "wr", "wr KB");
    for (map<string,rados_pool_stat_t>::iterator i = stats.begin(); i != stats.end(); ++i) {
      printf("%-15s %12lld %12lld %12lld %12lld %12lld %12lld %12lld %12lld\n",
	     i->first.c_str(),
	     i->second.num_kb,
	     i->second.num_objects,
	     i->second.num_object_clones,
	     i->second.num_objects_degraded,
	     i->second.num_rd, i->second.num_rd_kb,
	     i->second.num_wr, i->second.num_wr_kb);
    }

    // total
    rados_statfs_t tstats;
    rados.get_fs_stats(tstats);
    printf("  total used    %12lld %12lld\n", (long long unsigned)tstats.kb_used,
	   (long long unsigned)tstats.num_objects);
    printf("  total avail   %12lld\n", (long long unsigned)tstats.kb_avail);
    printf("  total space   %12lld\n", (long long unsigned)tstats.kb);
  }

  else if (strcmp(nargs[0], "ls") == 0) {
    if (!pool || nargs.size() < 2)
      usage();

    bool stdout = (strcmp(nargs[1], "-") == 0);
    ostream *outstream;
    if(stdout)
      outstream = &cout;
    else
      outstream = new ofstream(nargs[1]);

    Rados::ListCtx ctx;
    while (1) {
      list<object_t> vec;
      ret = rados.list(p, 1 << 10, vec, ctx);
      if (ret < 0) {
	cerr << "got error: " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
	goto out;
      }
      if (vec.empty())
	break;

      for (list<object_t>::iterator iter = vec.begin(); iter != vec.end(); ++iter)
	*outstream << iter->name << std::endl;
    }
    if (!stdout)
      delete outstream;
  }
  else if (strcmp(nargs[0], "get") == 0) {
    if (!pool || nargs.size() < 3)
      usage();
    object_t oid(nargs[1]);
    ret = rados.read(p, oid, 0, outdata, 0);
    if (ret < 0) {
      cerr << "error reading " << pool << "/" << oid << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }

    if (strcmp(nargs[2], "-") == 0) {
      ::write(1, outdata.c_str(), outdata.length());
    } else {
      outdata.write_file(nargs[2]);
      generic_dout(0) << "wrote " << outdata.length() << " byte payload to " << nargs[2] << dendl;
    }
  }

  else if (strcmp(nargs[0], "put") == 0) {
    if (!pool || nargs.size() < 3)
      usage();

    object_t oid(nargs[1]);

    if (strcmp(nargs[2], "-") == 0) {
      char buf[256];
      while(!cin.eof()) {
	cin.getline(buf, 256);
	indata.append(buf);
	indata.append('\n');
      }
    } else {
      ret = indata.read_file(nargs[2]);
      if (ret) {
	cerr << "error reading input file " << nargs[2] << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
	goto out;
      }
    }

    ret = rados.write_full(p, oid, indata);
    if (ret < 0) {
      cerr << "error writing " << pool << "/" << oid << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
  }
  else if (strcmp(nargs[0], "rm") == 0) {
    if (!pool || nargs.size() < 2)
      usage();
    object_t oid(nargs[1]);
    ret = rados.remove(p, oid);
    if (ret < 0) {
      cerr << "error removing " << pool << "/" << oid << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
  }

  else if (strcmp(nargs[0], "lssnap") == 0) {
    if (!pool || nargs.size() != 1)
      usage();

    vector<rados_snap_t> snaps;
    rados.snap_list(p, &snaps);
    for (vector<rados_snap_t>::iterator i = snaps.begin();
	 i != snaps.end();
	 i++) {
      string s;
      time_t t;
      if (rados.snap_get_name(p, *i, &s) < 0)
	continue;
      if (rados.snap_get_stamp(p, *i, &t) < 0)
	continue;
      struct tm bdt;
      localtime_r(&t, &bdt);
      cout << *i << "\t" << s << "\t";

      cout.setf(std::ios::right);
      cout.fill('0');
      cout << std::setw(4) << (bdt.tm_year+1900)
	   << '.' << std::setw(2) << (bdt.tm_mon+1)
	   << '.' << std::setw(2) << bdt.tm_mday
	   << ' '
	   << std::setw(2) << bdt.tm_hour
	   << ':' << std::setw(2) << bdt.tm_min
	   << ':' << std::setw(2) << bdt.tm_sec
	   << std::endl;
      cout.unsetf(std::ios::right);
    }
    cout << snaps.size() << " snaps" << std::endl;
  }

  else if (strcmp(nargs[0], "mksnap") == 0) {
    if (!pool || nargs.size() < 2)
      usage();
    
    ret = rados.snap_create(p, nargs[1]);
    if (ret < 0) {
      cerr << "error creating pool " << pool << " snapshot " << nargs[1]
	   << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
    cout << "created pool " << pool << " snap " << nargs[1] << std::endl;
  }

  else if (strcmp(nargs[0], "rmsnap") == 0) {
    if (!pool || nargs.size() < 2)
      usage();
    
    ret = rados.snap_remove(p, nargs[1]);
    if (ret < 0) {
      cerr << "error removing pool " << pool << " snapshot " << nargs[1]
	   << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
    cout << "removed pool " << pool << " snap " << nargs[1] << std::endl;
  }
  
  else if (strcmp(nargs[0], "bench") == 0) {
    if (!pool || nargs.size() < 3)
      usage();
    int seconds = atoi(nargs[1]);
    int operation = 0;
    if (strcmp(nargs[2], "write") == 0)
      operation = OP_WRITE;
    else if (strcmp(nargs[2], "seq") == 0)
      operation = OP_SEQ_READ;
    else if (strcmp(nargs[2], "rand") == 0)
      operation = OP_RAND_READ;
    else
      usage();
    ret = aio_bench(rados, p, operation, seconds, concurrent_ios, op_size);
    if (ret != 0)
      cerr << "error during benchmark: " << ret << std::endl;
  }
  else {
    cerr << "unrecognized command " << nargs[0] << std::endl;
    usage();
  }

 out:
  if (pool)
    rados.close_pool(p);

 no_pool_out:
  rados.shutdown();
  if (ret < 0)
    return 1;
  return 0;
}

