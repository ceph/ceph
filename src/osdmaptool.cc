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

#include <iostream>
#include <string>
using namespace std;

#include "common/config.h"

#include "common/errno.h"
#include "osd/OSDMap.h"
#include "mon/MonMap.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"

void usage()
{
  cout << " usage: [--print] [--createsimple <numosd> [--clobber] [--pg_bits <bitsperosd>]] <mapfilename>" << std::endl;
  cout << "   --export-crush <file>   write osdmap's crush map to <file>" << std::endl;
  cout << "   --import-crush <file>   replace osdmap's crush map with <file>" << std::endl;
  cout << "   --test-map-pg <pgid>    map a pgid to osds" << std::endl;
  cout << "   --test-map-object <objectname> [--pool <poolid>] map an object to osds"
       << std::endl;
  exit(1);
}

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
	      CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  const char *me = argv[0];

  std::string fn;
  bool print = false;
  bool print_json = false;
  bool tree = false;
  bool createsimple = false;
  bool create_from_conf = false;
  int num_osd = 0;
  int pg_bits = g_conf->osd_pg_bits;
  int pgp_bits = g_conf->osd_pgp_bits;
  bool clobber = false;
  bool modified = false;
  std::string export_crush, import_crush, test_map_pg, test_map_object;
  bool test_crush = false;
  int range_first = -1;
  int range_last = -1;
  int pool = 0;

  std::string val;
  std::ostringstream err;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
    } else if (ceph_argparse_flag(args, i, "-p", "--print", (char*)NULL)) {
      print = true;
    } else if (ceph_argparse_flag(args, i, "--dump-json", (char*)NULL)) {
      print_json = true;
    } else if (ceph_argparse_flag(args, i, "--tree", (char*)NULL)) {
      tree = true;
    } else if (ceph_argparse_withint(args, i, &num_osd, &err, "--createsimple", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	exit(EXIT_FAILURE);
      }
      createsimple = true;
    } else if (ceph_argparse_flag(args, i, "--create-from-conf", (char*)NULL)) {
      create_from_conf = true;
    } else if (ceph_argparse_flag(args, i, "--clobber", (char*)NULL)) {
      clobber = true;
    } else if (ceph_argparse_withint(args, i, &pg_bits, &err, "--pg_bits", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	exit(EXIT_FAILURE);
      }
    } else if (ceph_argparse_withint(args, i, &pgp_bits, &err, "--pgp_bits", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	exit(EXIT_FAILURE);
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--export_crush", (char*)NULL)) {
      export_crush = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--import_crush", (char*)NULL)) {
      import_crush = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--test_map_pg", (char*)NULL)) {
      test_map_pg = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--test_map_object", (char*)NULL)) {
      test_map_object = val;
    } else if (ceph_argparse_flag(args, i, "--test_crush", (char*)NULL)) {
      test_crush = true;
    } else if (ceph_argparse_withint(args, i, &range_first, &err, "--range_first", (char*)NULL)) {
    } else if (ceph_argparse_withint(args, i, &range_last, &err, "--range_last", (char*)NULL)) {
    } else if (ceph_argparse_withint(args, i, &pool, &err, "--pool", (char*)NULL)) {
    } else {
      ++i;
    }
  }
  if (args.empty()) {
    cerr << me << ": must specify osdmap filename" << std::endl;
    usage();
  }
  else if (args.size() > 1) {
    cerr << me << ": too many arguments" << std::endl;
    usage();
  }
  fn = args[0];

  if (range_first >= 0 && range_last >= 0) {
    set<OSDMap*> maps;
    OSDMap *prev = NULL;
    for (int i=range_first; i <= range_last; i++) {
      ostringstream f;
      f << fn << "/" << i;
      bufferlist bl;
      string error, s = f.str();
      int r = bl.read_file(s.c_str(), &error);
      if (r < 0) {
	cerr << "unable to read " << s << ": " << cpp_strerror(r) << std::endl;
	exit(1);
      }
      cout << s << " got " << bl.length() << " bytes" << std::endl;
      OSDMap *o = new OSDMap;
      o->decode(bl);
      maps.insert(o);
      if (prev)
	OSDMap::dedup(prev, o);
      prev = o;
    }
    exit(0);
  }
  
  OSDMap osdmap;
  bufferlist bl;

  cout << me << ": osdmap file '" << fn << "'" << std::endl;
  
  int r = 0;
  struct stat st;
  if (!createsimple && !create_from_conf && !clobber) {
    std::string error;
    r = bl.read_file(fn.c_str(), &error);
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
      cerr << me << ": couldn't open " << fn << ": " << error << std::endl;
      return -1;
    }
  }
  else if ((createsimple || create_from_conf) && !clobber && ::stat(fn.c_str(), &st) == 0) {
    cerr << me << ": " << fn << " exists, --clobber to overwrite" << std::endl;
    return -1;
  }

  if (createsimple) {
    if (num_osd < 1) {
      cerr << me << ": osd count must be > 0" << std::endl;
      exit(1);
    }
    uuid_d fsid;
    memset(&fsid, 0, sizeof(uuid_d));
    osdmap.build_simple(g_ceph_context, 0, fsid, num_osd, pg_bits, pgp_bits);
    modified = true;
  }
  if (create_from_conf) {
    uuid_d fsid;
    memset(&fsid, 0, sizeof(uuid_d));
    int r = osdmap.build_simple_from_conf(g_ceph_context, 0, fsid, pg_bits, pgp_bits);
    if (r < 0)
      return -1;
    modified = true;
  }

  if (!import_crush.empty()) {
    bufferlist cbl;
    std::string error;
    r = cbl.read_file(import_crush.c_str(), &error);
    if (r) {
      cerr << me << ": error reading crush map from " << import_crush
	   << ": " << error << std::endl;
      exit(1);
    }

    // validate
    CrushWrapper cw;
    bufferlist::iterator p = cbl.begin();
    cw.decode(p);

    if (cw.get_max_devices() > osdmap.get_max_osd()) {
      cerr << me << ": crushmap max_devices " << cw.get_max_devices()
	   << " > osdmap max_osd " << osdmap.get_max_osd() << std::endl;
      exit(1);
    }
    
    // apply
    OSDMap::Incremental inc;
    inc.fsid = osdmap.get_fsid();
    inc.epoch = osdmap.get_epoch()+1;
    inc.crush = cbl;
    osdmap.apply_incremental(inc);
    cout << me << ": imported " << cbl.length() << " byte crush map from " << import_crush << std::endl;
    modified = true;
  }

  if (!export_crush.empty()) {
    bufferlist cbl;
    osdmap.crush->encode(cbl);
    r = cbl.write_file(export_crush.c_str());
    if (r < 0) {
      cerr << me << ": error writing crush map to " << import_crush << std::endl;
      exit(1);
    }
    cout << me << ": exported crush map to " << export_crush << std::endl;
  }  

  if (!test_map_object.empty()) {
    object_t oid(test_map_object);
    if (!osdmap.have_pg_pool(pool)) {
      cerr << "There is no pool " << pool << std::endl;
      exit(1);
    }
    object_locator_t loc(pool);
    pg_t raw_pgid = osdmap.object_locator_to_pg(oid, loc);
    pg_t pgid = osdmap.raw_pg_to_pg(raw_pgid);
    
    vector<int> acting;
    osdmap.pg_to_acting_osds(pgid, acting);
    cout << " object '" << oid
	 << "' -> " << pgid
	 << " -> " << acting
	 << std::endl;
  }  
  if (!test_map_pg.empty()) {
    pg_t pgid;
    if (!pgid.parse(test_map_pg.c_str())) {
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
      for (map<int64_t,pg_pool_t>::const_iterator p = osdmap.get_pools().begin();
	   p != osdmap.get_pools().end();
	   p++) {
	const pg_pool_t *pool = osdmap.get_pg_pool(p->first);
	for (ps_t ps = 0; ps < pool->get_pg_num(); ps++) {
	  pg_t pgid(ps, p->first, -1);
	  for (int i=0; i<100; i++) {
	    cout << pgid << " attempt " << i << std::endl;

	    vector<int> r;
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

  if (!print && !print_json && !tree && !modified && 
      export_crush.empty() && import_crush.empty() && 
      test_map_pg.empty() && test_map_object.empty()) {
    cerr << me << ": no action specified?" << std::endl;
    usage();
  }

  if (modified)
    osdmap.inc_epoch();

  if (print) 
    osdmap.print(cout);
  if (print_json)
    osdmap.dump_json(cout);
  if (tree) 
    osdmap.print_tree(&cout, NULL);

  if (modified) {
    bl.clear();
    osdmap.encode(bl);

    // write it out
    cout << me << ": writing epoch " << osdmap.get_epoch()
	 << " to " << fn
	 << std::endl;
    int r = bl.write_file(fn.c_str());
    if (r) {
      cerr << "osdmaptool: error writing to '" << fn << "': "
	   << cpp_strerror(r) << std::endl;
      return 1;
    }
  }
  

  return 0;
}
