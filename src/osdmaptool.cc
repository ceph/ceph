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

void usage(const char *me)
{
  cout << me << " usage: [--print] [--createsimple <monmapfile> <numosd> [--clobber] [--pgbits <bitsperosd>]] <mapfilename>" << std::endl;
  cout << me << "   --export-crush <file>   write osdmap's crush map to <file>" << std::endl;
  cout << me << "   --import-crush <file>   replace osdmap's crush map with <file>" << std::endl;
  exit(1);
}

void printmap(const char *me, OSDMap *m)
{
  cout << me << ": osdmap: epoch " << m->get_epoch() << std::endl
       << me << ": osdmap: fsid " << m->get_fsid() << std::endl;
  /*for (unsigned i=0; i<m->mon_inst.size(); i++)
    cout << me << ": osdmap:  " //<< "mon" << i << " " 
	 << m->mon_inst[i] << std::endl;
  */
}


int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  parse_config_options(args);

  const char *me = argv[0];

  const char *fn = 0;
  bool print = false;
  bool createsimple = false;
  const char *monmapfn = 0;
  int num_osd = 0;
  int pg_bits = g_conf.osd_pg_bits;
  bool clobber = false;
  bool modified = false;
  const char *export_crush = 0;
  const char *import_crush = 0;
  list<entity_addr_t> add, rm;

  for (unsigned i=0; i<args.size(); i++) {
    if (strcmp(args[i], "--print") == 0)
      print = true;
    else if (strcmp(args[i], "--createsimple") == 0) {
      createsimple = true;
      monmapfn = args[++i];
      num_osd = atoi(args[++i]);
    } else if (strcmp(args[i], "--clobber") == 0) 
      clobber = true;
    else if (strcmp(args[i], "--pgbits") == 0)
      pg_bits = atoi(args[++i]);
    else if (strcmp(args[i], "--export-crush") == 0)
      export_crush = args[++i];
    else if (strcmp(args[i], "--import-crush") == 0)
      import_crush = args[++i];
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
  if (!(createsimple && clobber))
    r = bl.read_file(fn);
  if (!createsimple && r < 0) {
    cerr << me << ": couldn't open " << fn << ": " << strerror(errno) << std::endl;
    return -1;
  }    
  else if (createsimple && !clobber && r == 0) {
    cerr << me << ": " << fn << " exists, --clobber to overwrite" << std::endl;
    return -1;
  }

  if (createsimple) {
    MonMap monmap;
    int r = monmap.read(monmapfn);
    if (r < 0) {
      cerr << me << ": can't read monmap from " << monmapfn << ": " << strerror(r) << std::endl;
      exit(1);
    }
    osdmap.build_simple(0, monmap.fsid, num_osd, pg_bits, 0);
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
    //cw._decode(cbl,    FIXME
    bufferlist::iterator p = cbl.begin();
    osdmap.crush._decode(p);
    cout << me << ": imported crush map from " << import_crush << std::endl;
    modified = true;
  }

  if (export_crush) {
    bufferlist cbl;
    osdmap.crush._encode(cbl);
    r = cbl.write_file(export_crush);
    if (r < 0) {
      cerr << me << ": error writing crush map to " << import_crush << std::endl;
      exit(1);
    }
    cout << me << ": exported crush map to " << export_crush << std::endl;
  }  

  if (!print && !modified) {
    cerr << me << ": no action specified?" << std::endl;
    usage(me);
  }
  if (modified)
    osdmap.inc_epoch();

  if (print) 
    printmap(me, &osdmap);

  if (modified) {
    osdmap.encode(bl);

    // write it out
    cout << me << ": writing epoch " << osdmap.get_epoch()
	 << " to " << fn
	 << std::endl;
    int r = bl.write_file(fn);
    assert(r >= 0);
  }
  

  return 0;
}
