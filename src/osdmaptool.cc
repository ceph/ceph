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

#include "common/errno.h"
#include "osd/OSDMap.h"
#include "mon/MonMap.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"

void usage()
{
  cout << " usage: [--print] [--createsimple <numosd> [--clobber] [--pg_bits <bitsperosd>]] <mapfilename>" << std::endl;
  cout << "   --export-crush <file>   write osdmap's crush map to <file>" << std::endl;
  cout << "   --import-crush <file>   replace osdmap's crush map with <file>" << std::endl;
  cout << "   --test-map-pg <pgid>    map a pgid to osds" << std::endl;
  exit(1);
}




int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  DEFINE_CONF_VARS(usage);

  common_init(args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
	      CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(&g_conf, 0);

  const char *me = argv[0];

  const char *fn = 0;
  bool print = false;
  bool tree = false;
  bool createsimple = false;
  int num_osd = 0, num_dom = 0;
  int pg_bits = g_conf.osd_pg_bits;
  int pgp_bits = g_conf.osd_pgp_bits;
  int lpg_bits = g_conf.osd_lpg_bits;
  bool clobber = false;
  bool modified = false;
  const char *export_crush = 0;
  const char *import_crush = 0;
  list<entity_addr_t> add, rm;
  const char *test_map_pg = 0;
  const char *test_map_object = 0;
  bool test_crush = false;

  FOR_EACH_ARG(args) {
    if (CEPH_ARGPARSE_EQ("help", 'h')) {
      usage();
    } else if (CEPH_ARGPARSE_EQ("print", 'p')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&print, OPT_BOOL);
    } else if (CEPH_ARGPARSE_EQ("tree", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&tree, OPT_BOOL);
    } else if (CEPH_ARGPARSE_EQ("createsimple", '\0')) {
      createsimple = true;
      CEPH_ARGPARSE_SET_ARG_VAL(&num_osd, OPT_INT);
    } else if (CEPH_ARGPARSE_EQ("clobber", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&clobber, OPT_BOOL);
    } else if (CEPH_ARGPARSE_EQ("pg_bits", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&pg_bits, OPT_INT);
    } else if (CEPH_ARGPARSE_EQ("pgp_bits", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&pgp_bits, OPT_INT);
    } else if (CEPH_ARGPARSE_EQ("lpg_bits", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&lpg_bits, OPT_INT);
    } else if (CEPH_ARGPARSE_EQ("num_dom", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&num_dom, OPT_INT);
    } else if (CEPH_ARGPARSE_EQ("export_crush", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&export_crush, OPT_STR);
    } else if (CEPH_ARGPARSE_EQ("import_crush", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&import_crush, OPT_STR);
    } else if (CEPH_ARGPARSE_EQ("test_map_pg", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&test_map_pg, OPT_STR);
    } else if (CEPH_ARGPARSE_EQ("test_map_object", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&test_map_object, OPT_STR);
    } else if (CEPH_ARGPARSE_EQ("test_crush", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&test_crush, OPT_BOOL);
    } else if (!fn)
      fn = args[i];
    else 
      usage();
  }
  if (!fn) {
    cerr << me << ": must specify osdmap filename" << std::endl;
    usage();
  }
  
  OSDMap osdmap;
  bufferlist bl;

  cout << me << ": osdmap file '" << fn << "'" << std::endl;
  
  int r = 0;
  struct stat st;
  if (!createsimple && !clobber) {
    r = bl.read_file(fn);
    if (r == 0) {
      try {
	osdmap.decode(bl);
      }
      catch (const buffer::error &e) {
	cerr << me << ": error decoding osdmap '" << fn << "'" << std::endl;
	return -1;
      }
    }
    else {
      cerr << me << ": couldn't open " << fn << ": " << cpp_strerror(-r)
	   << std::endl;
      return -1;
    }
  }
  else if (createsimple && !clobber && ::stat(fn, &st) == 0) {
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
    osdmap.build_simple(0, fsid, num_osd, num_dom, pg_bits, pgp_bits, lpg_bits);
    modified = true;
  }

  if (import_crush) {
    bufferlist cbl;
    r = cbl.read_file(import_crush);
    if (r) {
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

  if (test_map_object) {
    object_t oid(test_map_object);
    ceph_object_layout ol = osdmap.make_object_layout(oid, 0);
    
    pg_t pgid;
    pgid.v = ol.ol_pgid;

    vector<int> acting;
    osdmap.pg_to_acting_osds(pgid, acting);
    cout << " object '" << oid
	 << "' -> " << pgid
	 << " -> " << acting
	 << std::endl;
  }  
  if (test_map_pg) {
    pg_t pgid;
    if (pgid.parse(test_map_pg) < 0) {
      cerr << me << ": failed to parse pg '" << test_map_pg
	   << "', r = " << r << std::endl;
      usage();
    }
    cout << " parsed '" << test_map_pg << "' -> " << pgid << std::endl;

    vector<int> raw, up, acting;
    osdmap.pg_to_osds(pgid, raw);
    osdmap.pg_to_up_acting_osds(pgid, up, acting);
    cout << pgid << " raw " << raw << " up " << up << " acting " << acting << std::endl;
  }
  if (test_crush) {
    int pass = 0;
    while (1) {
      cout << "pass " << ++pass << std::endl;

      hash_map<pg_t,vector<int> > m;
      for (map<int,pg_pool_t>::const_iterator p = osdmap.get_pools().begin();
	   p != osdmap.get_pools().end();
	   p++) {
	const pg_pool_t *pool = osdmap.get_pg_pool(p->first);
	for (int ps = 0; ps < pool->get_pg_num(); ps++) {
	  pg_t pgid(ps, p->first, -1);
	  for (int i=0; i<100; i++) {
	    cout << pgid << " attempt " << i << std::endl;

	    vector<int> r, s;
	    osdmap.pg_to_acting_osds(pgid, r);
	    //cout << pgid << " " << r << std::endl;
	    if (m.count(pgid)) {
	      if (m[pgid] != r) {
		cout << pgid << " had " << m[pgid] << " now " << r << std::endl;
		assert(0);
	      }
	    } else
	      m[pgid] = r;
	  }
	}
      }
    }
  }

  if (!print && !tree && !modified && !export_crush && !import_crush && !test_map_pg && !test_map_object) {
    cerr << me << ": no action specified?" << std::endl;
    usage();
  }

  if (modified)
    osdmap.inc_epoch();

  if (print) 
    osdmap.print(cout);
  if (tree) 
    osdmap.print_tree(cout);

  if (modified) {
    bl.clear();
    osdmap.encode(bl);

    // write it out
    cout << me << ": writing epoch " << osdmap.get_epoch()
	 << " to " << fn
	 << std::endl;
    int r = bl.write_file(fn);
    if (r) {
      cerr << "osdmaptool: error writing to '" << fn << "': "
	   << cpp_strerror(r) << std::endl;
      return 1;
    }
  }
  

  return 0;
}
