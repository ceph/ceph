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

#include "config.h"

#include "osd/OSDMap.h"
#include "mon/MonMap.h"
#include "common/common_init.h"

void usage(const char *me)
{
  cout << me << " usage: [--print] [--createsimple <numosd> [--clobber] [--pgbits <bitsperosd>]] <mapfilename>" << std::endl;
  cout << me << "   --export-crush <file>   write osdmap's crush map to <file>" << std::endl;
  cout << me << "   --import-crush <file>   replace osdmap's crush map with <file>" << std::endl;
  exit(1);
}




int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  common_init(args);

  const char *me = argv[0];

  const char *fn = 0;
  bool print = false;
  bool createsimple = false;
  int num_osd = 0, num_dom = 0;
  int pg_bits = g_conf.osd_pg_bits;
  int lpg_bits = g_conf.osd_lpg_bits;
  bool clobber = false;
  bool modified = false;
  const char *export_crush = 0;
  const char *import_crush = 0;
  list<entity_addr_t> add, rm;
  const char *test_map_pg = 0;

  for (unsigned i=0; i<args.size(); i++) {
    if (strcmp(args[i], "--print") == 0 ||
	strcmp(args[i], "-p") == 0)
      print = true;
    else if (strcmp(args[i], "--createsimple") == 0) {
      createsimple = true;
      num_osd = atoi(args[++i]);
    } else if (strcmp(args[i], "--clobber") == 0) 
      clobber = true;
    else if (strcmp(args[i], "--pg_bits") == 0)
      pg_bits = atoi(args[++i]);
    else if (strcmp(args[i], "--lpg_bits") == 0)
      lpg_bits = atoi(args[++i]);
    else if (strcmp(args[i], "--num_dom") == 0)
      num_dom = atoi(args[++i]);
    else if (strcmp(args[i], "--export-crush") == 0)
      export_crush = args[++i];
    else if (strcmp(args[i], "--import-crush") == 0)
      import_crush = args[++i];
    else if (strcmp(args[i], "--test-map-pg") == 0)
      test_map_pg = args[++i];
    else if (!fn)
      fn = args[i];
    else 
      usage(me);
  }
  if (!fn) {
    cerr << me << ": must specify osdmap filename" << std::endl;
    usage(me);
  }
  
  OSDMap osdmap;
  bufferlist bl;

  cout << me << ": osdmap file '" << fn << "'" << std::endl;
  
  int r = 0;
  if (!(createsimple && clobber)) {
    r = bl.read_file(fn);
    osdmap.decode(bl);
  }
  if (!createsimple && r < 0) {
    cerr << me << ": couldn't open " << fn << ": " << strerror(errno) << std::endl;
    return -1;
  }    
  else if (createsimple && !clobber && r == 0) {
    cerr << me << ": " << fn << " exists, --clobber to overwrite" << std::endl;
    return -1;
  }

  if (createsimple) {
    if (num_osd < 1) {
      cerr << me << ": osd count must be > 0" << std::endl;
      exit(1);
    }
    ceph_fsid_t fsid;
    memset(&fsid, 0, sizeof(ceph_fsid_t));
    osdmap.build_simple(0, fsid, num_osd, num_dom, pg_bits, lpg_bits, 0);
    modified = true;
  }

  if (import_crush) {
    bufferlist cbl;
    r = cbl.read_file(import_crush);
    if (r < 0) {
      cerr << me << ": error reading crush map from " << import_crush << std::endl;
      exit(1);
    }
    // validate
    CrushWrapper cw;
    bufferlist::iterator p = cbl.begin();
    cw.decode(p);
    
    // apply
    OSDMap::Incremental inc;
    inc.fsid = osdmap.get_fsid();
    inc.epoch = osdmap.get_epoch()+1;
    inc.crush = cbl;
    osdmap.apply_incremental(inc);
    cout << me << ": imported " << cbl.length() << " byte crush map from " << import_crush << std::endl;
    modified = true;
  }

  if (export_crush) {
    bufferlist cbl;
    osdmap.crush.encode(cbl);
    r = cbl.write_file(export_crush);
    if (r < 0) {
      cerr << me << ": error writing crush map to " << import_crush << std::endl;
      exit(1);
    }
    cout << me << ": exported crush map to " << export_crush << std::endl;
  }  

  if (test_map_pg) {
    pg_t pgid;
    if (pgid.parse(test_map_pg) < 0) {
      cerr << me << ": failed to parse pg '" << test_map_pg
	   << "', r = " << r << std::endl;
      usage(me);
    }
    cout << " parsed '" << test_map_pg << "' -> " << pgid << std::endl;

    vector<int> acting;
    osdmap.pg_to_acting_osds(pgid, acting);
    cout << pgid << " maps to " << acting << std::endl;
  }

  if (!print && !modified && !export_crush && !import_crush && !test_map_pg) {
    cerr << me << ": no action specified?" << std::endl;
    usage(me);
  }

  if (modified)
    osdmap.inc_epoch();

  if (print) 
    osdmap.print(cout);

  if (modified) {
    bl.clear();
    osdmap.encode(bl);

    // write it out
    cout << me << ": writing epoch " << osdmap.get_epoch()
	 << " to " << fn
	 << std::endl;
    int r = bl.write_file(fn);
    if (r < 0) {
      cerr << "osdmaptool: error writing to '" << fn << "': " << strerror(-r) << std::endl;
      return 1;
    }
  }
  

  return 0;
}
