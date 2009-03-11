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

#include "mon/MonitorStore.cc"
#include "config.h"

#include "mon/Monitor.h"
#include "mon/MonMap.h"
#include "mds/MDSMap.h"
#include "osd/OSDMap.h"
#include "mon/PGMap.h"

void usage() 
{
  cerr << "usage: ./mkmonfs [--clobber] --mon-data <monfsdir> -i <monid> --monmap <file> --osdmap <file>" << std::endl;
  exit(1);
}


int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);

  bool clobber = false;
  const char *fsdir = 0;
  int whoami = -1;
  const char *monmapfn = 0;
  const char *osdmapfn = 0;
  for (unsigned i = 0; i < args.size(); i++) {
    if (strcmp(args[i], "--clobber") == 0)
      clobber = true;
    else if (strcmp(args[i], "--mon") == 0 ||
	     strcmp(args[i], "-i") == 0)
      whoami = atoi(args[++i]);
    else if (strcmp(args[i], "--monmap") == 0) 
      monmapfn = args[++i];
    else if (strcmp(args[i], "--osdmap") == 0) 
      osdmapfn = args[++i];
    else if (strcmp(args[i], "--mon_data") == 0 ||
	     strcmp(args[i], "--mon-data") == 0)
      fsdir = args[++i];
    else 
      usage();
  }
  if (!fsdir || !monmapfn || whoami < 0)
    usage();

  if (!clobber) {
    // make sure it doesn't exist
    struct stat st;
    if (::lstat(fsdir, &st) == 0) {
      cerr << "monfs dir " << fsdir << " already exists; remove it first" << std::endl;
      usage();
    }
  }

  // load monmap
  bufferlist monmapbl, osdmapbl;
  int err = monmapbl.read_file(monmapfn);
  if (err < 0)
    exit(1);
  MonMap monmap;
  monmap.decode(monmapbl);

  err = osdmapbl.read_file(osdmapfn);
  if (err < 0)
    exit(1);

  // go
  MonitorStore store(fsdir);
  Monitor mon(whoami, &store, 0, &monmap);
  mon.mkfs(osdmapbl);
  cout << argv[0] << ": created monfs at " << fsdir 
       << " for mon" << whoami
       << std::endl;
  return 0;
}
