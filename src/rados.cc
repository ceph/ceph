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

  cerr << "   bench <seconds> [-t concurrentwrites] [-b writesize] [verify] [sync]\n";
  cerr << "              default is 16 concurrent IOs and 1 MB writes size\n\n";

  cerr << "Options:\n";
  cerr << "   -P pool\n";
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

int aio_bench(Rados& rados, rados_pool_t pool, int secondsToRun, int concurrentios,
	      int writeSize, int readOffResults, int sync) {

  cout << "Maintaining " << concurrentios << " concurrent writes of " << writeSize
       << " bytes for at least " << secondsToRun << " seconds." << std::endl;

  Rados::AioCompletion* completions[concurrentios];
  char* name[concurrentios];
  bufferlist* contents[concurrentios];
  char contentsChars[writeSize];
  double totalLatency = 0;
  utime_t maxLatency;
  utime_t startTimes[concurrentios];
  char bw[20];
  double bandwidth = 0;
  int writesMade = 0;
  int writesCompleted = 0;
  time_t initialTime;
  utime_t startTime;
  utime_t stopTime;

  utime_t ONE_SECOND;
  ONE_SECOND.set_from_double(1.0);

  time(&initialTime);
  stringstream initialTimeS("");
  initialTimeS << initialTime;
  const char* iTime = initialTimeS.str().c_str();
  maxLatency.set_from_double(0);
  //set up writes so I can start them together
  for (int i = 0; i<concurrentios; ++i) {
    name[i] = new char[128];
    contents[i] = new bufferlist();
    snprintf(name[i], 128, "Object %s:%d", iTime, i);
    snprintf(contentsChars, writeSize, "I'm the %dth object!", i);
    contents[i]->append(contentsChars, writeSize);
  }

  //set up the pool, get start time, and go!
  cout << "open pool result = " << rados.open_pool("data",&pool) << " pool = " << pool << std::endl;

  startTime = g_clock.now();

  for (int i = 0; i<concurrentios; ++i) {
    startTimes[i] = g_clock.now();
    rados.aio_write(pool, name[i], 0, *contents[i], writeSize, &completions[i]);
    ++writesMade;
  }
  cerr << "Finished writing first objects\n";

  //keep on adding new writes as old ones complete until we've passed minimum time
  int slot;
  bufferlist* newContents;
  char* newName;
  utime_t currentLatency;
  utime_t runtime;

  utime_t lastPrint = startTime;
  utime_t timePassed = g_clock.now() - startTime;
  int writesAtLastPrint = 0;

  runtime.set_from_double(secondsToRun);
  stopTime = startTime + runtime;
  while( g_clock.now() < stopTime ) {
    slot = writesCompleted % concurrentios;
    //create new contents and name on the heap, and fill them
    newContents = new bufferlist();
    newName = new char[128];
    snprintf(newName, 128, "Object %s:%d", iTime, writesMade);
    snprintf(contentsChars, writeSize, "I'm the %dth object!", writesMade);
    newContents->append(contentsChars, writeSize);
    completions[slot]->wait_for_complete();
    currentLatency = g_clock.now() - startTimes[slot];
    totalLatency += currentLatency;
    if( currentLatency > maxLatency) maxLatency = currentLatency;
    ++writesCompleted;
    completions[slot]->release();
    //print out updating status message
    if ( (g_clock.now() - lastPrint) > ONE_SECOND) {
      timePassed = g_clock.now() - lastPrint;
      bandwidth = ((double)(writesCompleted - writesAtLastPrint) * writeSize / timePassed) / (1024*1024);
      lastPrint = g_clock.now();
      writesAtLastPrint = writesCompleted;
      sprintf(bw, "%3lf \n", bandwidth);
      cout << "Current bandwidth:   " << bw;
    }
    //write new stuff to rados, then delete old stuff
    //and save locations of new stuff for later deletion
    startTimes[slot] = g_clock.now();
    rados.aio_write(pool, newName, 0, *newContents, writeSize, &completions[slot]);
    ++writesMade;
    delete name[slot];
    delete contents[slot];
    name[slot] = newName;
    contents[slot] = newContents;
  }
  
  cerr << "Waiting for last writes to finish\n";
  while (writesCompleted < writesMade) {
    slot = writesCompleted % concurrentios;
    completions[slot]->wait_for_complete();
    currentLatency = g_clock.now() - startTimes[slot];
    totalLatency += currentLatency;
    if (currentLatency > maxLatency) maxLatency = currentLatency;
    completions[slot]-> release();
    ++writesCompleted;
    delete name[slot];
    delete contents[slot];
  }

  timePassed = g_clock.now() - startTime;

  //check objects for consistency if requested
  int errors = 0;
  if (readOffResults) {
    char matchName[128];
    object_t oid;
    bufferlist actualContents;
    for (int i = 0; i < writesCompleted; ++i ) {
      snprintf(matchName, 128, "Object %s:%d", iTime, i);
      oid = object_t(matchName);
      snprintf(contentsChars, writeSize, "I'm the %dth object!", i);
      rados.read(pool, oid, 0, actualContents, writeSize);
      if (strcmp(contentsChars, actualContents.c_str()) != 0 ) {
	cerr << "Object " << matchName << " is not correct!";
	++errors;
      }
      actualContents.clear();
    }
  }

  bandwidth = ((double)writesCompleted)*((double)writeSize)/(double)timePassed;
  bandwidth = bandwidth/(1024*1024); // we want it in MB/sec
  sprintf(bw, "%.3lf \n", bandwidth);

  double averageLatency = totalLatency / writesCompleted;

  cout << "Total time run:        " << timePassed << std::endl
       << "Total writes made:     " << writesCompleted << std::endl
       << "Write size:            " << writeSize << std::endl
       << "Bandwidth (MB/sec):    " << bw << std::endl
       << "Average Latency:       " << averageLatency << std::endl
       << "Max latency:           " << maxLatency << std::endl
       << "Time waiting for Rados:" << totalLatency/concurrentios << std::endl;

  if (readOffResults) {
    if (errors) cout << "WARNING: There were " << errors << " total errors in copying!\n";
    else cout << "No errors in copying!\n";
  }
  return 0;
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

  const char *pool = 0;
 
  int concurrent_ios = 16;
  int write_size = 1 << 20;

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
      CONF_SAFE_SET_ARG_VAL(&write_size, OPT_INT);
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

  // open pool?
  rados_pool_t p;
  if (pool) {
    ret = rados.open_pool(pool, &p);
    if (ret < 0) {
      cerr << "error opening pool " << pool << ": " << strerror(-ret) << std::endl;
      goto out;
    }
  }

  // snapname?
  if (snapname) {
    ret = rados.snap_lookup(p, snapname, &snapid);
    if (ret < 0) {
      cerr << "error looking up snap '" << snapname << "': " << strerror(-ret) << std::endl;
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
	cerr << "got error: " << strerror(-ret) << std::endl;
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
      cerr << "error reading " << pool << "/" << oid << ": " << strerror(-ret) << std::endl;
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
	cerr << "error reading input file " << nargs[2] << ": " << strerror(-ret) << std::endl;
	goto out;
      }
    }

    ret = rados.write_full(p, oid, indata);
    if (ret < 0) {
      cerr << "error writing " << pool << "/" << oid << ": " << strerror(-ret) << std::endl;
      goto out;
    }
  }
  else if (strcmp(nargs[0], "rm") == 0) {
    if (!pool || nargs.size() < 2)
      usage();
    object_t oid(nargs[1]);
    ret = rados.remove(p, oid);
    if (ret < 0) {
      cerr << "error removing " << pool << "/" << oid << ": " << strerror(-ret) << std::endl;
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
    if (nargs.size() < 2)
      usage();
    
    ret = rados.snap_create(p, nargs[1]);
    if (ret < 0) {
      cerr << "error creating pool " << pool << " snapshot " << nargs[1]
	   << ": " << strerror(-ret) << std::endl;
      goto out;
    }
    cout << "created pool " << pool << " snap " << nargs[1] << std::endl;
  }

  else if (strcmp(nargs[0], "rmsnap") == 0) {
    if (nargs.size() < 2)
      usage();
    
    ret = rados.snap_remove(p, nargs[1]);
    if (ret < 0) {
      cerr << "error removing pool " << pool << " snapshot " << nargs[1]
	   << ": " << strerror(-ret) << std::endl;
      goto out;
    }
    cout << "removed pool " << pool << " snap " << nargs[1] << std::endl;
  }
  
  else if (strcmp(nargs[0], "bench") == 0) {
    if (nargs.size() < 2)
      usage();
    int seconds = atoi(nargs[1]);
    int sync = 0;
    int verify = 0;
    for (unsigned i=2; i<nargs.size(); i++) {
      if (strcmp(nargs[i], "sync") == 0)
	sync = 1;
      else if (strcmp(nargs[i], "verify") == 0)
	verify = 1;
      else
	usage();
    }
    aio_bench(rados, p, seconds, concurrent_ios, write_size, verify, sync);
  }
  else {
    cerr << "unrecognized command " << nargs[0] << std::endl;
    usage();
  }

 out:
  if (pool)
    rados.close_pool(p);

  rados.shutdown();
  if (ret < 0)
    return 1;
  return 0;
}

