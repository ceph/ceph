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

#include "common/common_init.h"
#include "mon/MonitorStore.cc"
#include "config.h"

#include "mon/Monitor.h"
#include "mon/MonMap.h"
#include "mds/MDSMap.h"
#include "osd/OSDMap.h"
#include "mon/PGMap.h"
#include "common/common_init.h"

void usage() 
{
  cerr << "usage: ./mkmonfs [--clobber] --mon-data <monfsdir> -i <monid> --monmap <file> --osdmap <file>" << std::endl;
  exit(1);
}


int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  DEFINE_CONF_VARS(usage);
  common_init(args, "mon", false, false);

  bool clobber = false;
  int whoami = -1;
  const char *osdmapfn = 0;

  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("clobber", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&clobber, OPT_BOOL);
    } else if (CONF_ARG_EQ("mon", 'i')) {
      CONF_SAFE_SET_ARG_VAL(&whoami, OPT_INT);
    } else if (CONF_ARG_EQ("osdmap", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&osdmapfn, OPT_STR);
    } else {
      usage();
    }
  }
  if (!g_conf.mon_data || !g_conf.monmap || whoami < 0)
    usage();

  if (!clobber) {
    // make sure it doesn't exist
    struct stat st;
    if (::lstat(g_conf.mon_data, &st) == 0) {
      cerr << "monfs dir " << g_conf.mon_data << " already exists; remove it first" << std::endl;
      usage();
    }
  }

  // load monmap
  bufferlist monmapbl, osdmapbl;
  int err = monmapbl.read_file(g_conf.monmap);
  if (err < 0)
    exit(1);
  MonMap monmap;
  monmap.decode(monmapbl);

  err = osdmapbl.read_file(osdmapfn);
  if (err < 0)
    exit(1);

  // go
  MonitorStore store(g_conf.mon_data);
  Monitor mon(whoami, &store, 0, &monmap);
  mon.mkfs(osdmapbl);
  cout << argv[0] << ": created monfs at " << g_conf.mon_data 
       << " for mon" << whoami
       << std::endl;
  return 0;
}
