// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <boost/scoped_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options/option.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/parsers.hpp>
#include <iostream>
#include <set>
#include <sstream>
#include <stdlib.h>
#include <fstream>

#include "common/Formatter.h"

#include "global/global_init.h"
#include "os/ObjectStore.h"
#include "os/FileStore.h"
#include "common/perf_counters.h"
#include "common/errno.h"
#include "osd/PGLog.h"
#include "osd/osd_types.h"
#include "osd/OSD.h"

namespace po = boost::program_options;
using namespace std;

static void invalid_path(string &path)
{
  cout << "Invalid path to osd store specified: " << path << "\n";
  exit(1);
}

int main(int argc, char **argv)
{
  string fspath, jpath, pgidstr;
  bool list_lost_objects = false;
  bool fix_lost_objects = false;
  unsigned LIST_AT_A_TIME = 100;
  unsigned scanned = 0;
  
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "produce help message")
    ("filestore-path", po::value<string>(&fspath),
     "path to filestore directory, mandatory")
    ("journal-path", po::value<string>(&jpath),
     "path to journal, mandatory")
    ("pgid", po::value<string>(&pgidstr),
     "PG id")
    ("list-lost-objects", po::value<bool>(
      &list_lost_objects)->default_value(false),
     "list lost objects")
    ("fix-lost-objects", po::value<bool>(
      &fix_lost_objects)->default_value(false),
     "fix lost objects")
    ;

  po::variables_map vm;
  po::parsed_options parsed =
   po::command_line_parser(argc, argv).options(desc).
    allow_unregistered().run();
  po::store( parsed, vm);
  try {
    po::notify(vm);
  }
  catch(...) {
    cout << desc << std::endl;
    exit(1);
  }
     
  if (vm.count("help")) {
    cout << desc << std::endl;
    return 1;
  }

  if (!vm.count("filestore-path")) {
    cerr << "Must provide filestore-path" << std::endl
	 << desc << std::endl;
    return 1;
  } 
  if (!vm.count("journal-path")) {
    cerr << "Must provide journal-path" << std::endl
	 << desc << std::endl;
    return 1;
  } 

  if ((fspath.length() == 0 || jpath.length() == 0)) {
    cerr << "Invalid params" << desc << std::endl;
    exit(1);
  }

  vector<const char *> ceph_options, def_args;
  vector<string> ceph_option_strings = po::collect_unrecognized(
    parsed.options, po::include_positional);
  ceph_options.reserve(ceph_option_strings.size());
  for (vector<string>::iterator i = ceph_option_strings.begin();
       i != ceph_option_strings.end();
       ++i) {
    ceph_options.push_back(i->c_str());
  }

  global_init(
    &def_args, ceph_options, CEPH_ENTITY_TYPE_OSD,
    CODE_ENVIRONMENT_UTILITY, 0);
    //CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);
  g_conf = g_ceph_context->_conf;

  //Verify that fspath really is an osd store
  struct stat st;
  if (::stat(fspath.c_str(), &st) == -1) {
     perror("fspath");
     invalid_path(fspath);
  }
  if (!S_ISDIR(st.st_mode)) {
    invalid_path(fspath);
  }
  string check = fspath + "/whoami";
  if (::stat(check.c_str(), &st) == -1) {
     perror("whoami");
     invalid_path(fspath);
  }
  if (!S_ISREG(st.st_mode)) {
    invalid_path(fspath);
  }
  check = fspath + "/current";
  if (::stat(check.c_str(), &st) == -1) {
     perror("current");
     invalid_path(fspath);
  }
  if (!S_ISDIR(st.st_mode)) {
    invalid_path(fspath);
  }

  ObjectStore *fs = new FileStore(fspath, jpath);

  int r = fs->mount();
  if (r < 0) {
    if (r == -EBUSY) {
      cout << "OSD has the store locked" << std::endl;
    } else {
      cout << "Mount failed with '" << cpp_strerror(-r) << "'" << std::endl;
    }
    return 1;
  }

  vector<coll_t> colls_to_check;
  if (pgidstr.length()) {
    spg_t pgid;
    if (!pgid.parse(pgidstr.c_str())) {
      cout << "Invalid pgid '" << pgidstr << "' specified" << std::endl;
      exit(1);
    }
    colls_to_check.push_back(coll_t(pgid));
  } else {
    vector<coll_t> candidates;
    r = fs->list_collections(candidates);
    if (r < 0) {
      cerr << "Error listing collections: " << cpp_strerror(r) << std::endl;
      goto UMOUNT;
    }
    for (vector<coll_t>::iterator i = candidates.begin();
	 i != candidates.end();
	 ++i) {
      spg_t pgid;
      snapid_t snap;
      if (i->is_pg(pgid, snap)) {
	colls_to_check.push_back(*i);
      }
    }
  }

  cerr << colls_to_check.size() << " pgs to scan" << std::endl;
  for (vector<coll_t>::iterator i = colls_to_check.begin();
       i != colls_to_check.end();
       ++i, ++scanned) {
    cerr << "Scanning " << *i << ", " << scanned << "/"
	 << colls_to_check.size() << " completed" << std::endl;
    ghobject_t next;
    while (!next.is_max()) {
      vector<ghobject_t> list;
      r = fs->collection_list_partial(
	*i,
	next,
	LIST_AT_A_TIME,
	LIST_AT_A_TIME,
	CEPH_NOSNAP,
	&list,
	&next);
      if (r < 0) {
	cerr << "Error listing collection: " << *i << ", "
	     << cpp_strerror(r) << std::endl;
	goto UMOUNT;
      }
      for (vector<ghobject_t>::iterator obj = list.begin();
	   obj != list.end();
	   ++obj) {
	bufferlist attr;
	r = fs->getattr(*i, *obj, OI_ATTR, attr);
	if (r < 0) {
	  cerr << "Error getting attr on : " << make_pair(*i, *obj) << ", "
	       << cpp_strerror(r) << std::endl;
	  goto UMOUNT;
	}
	object_info_t oi;
	bufferlist::iterator bp = attr.begin();
	try {
	  ::decode(oi, bp);
	} catch (...) {
	  r = -EINVAL;
	  cerr << "Error getting attr on : " << make_pair(*i, *obj) << ", "
	       << cpp_strerror(r) << std::endl;
	  goto UMOUNT;
	}
	if (oi.is_lost()) {
	  if (list_lost_objects) {
	    cout << *i << "/" << *obj << " is lost" << std::endl;
	  }
	  if (fix_lost_objects) {
	    cerr << *i << "/" << *obj << " is lost, fixing" << std::endl;
	    oi.clear_flag(object_info_t::FLAG_LOST);
	    bufferlist bl2;
	    ::encode(oi, bl2);
	    ObjectStore::Transaction t;
	    t.setattr(*i, *obj, OI_ATTR, bl2);
	    r = fs->apply_transaction(t);
	    if (r < 0) {
	      cerr << "Error getting fixing attr on : " << make_pair(*i, *obj)
		   << ", "
		   << cpp_strerror(r) << std::endl;
	      goto UMOUNT;
	    }
	  }
	}
      }
    }
  }
  cerr << "Completed" << std::endl;

 UMOUNT:
  fs->sync_and_flush();
  fs->umount();
  return r;
}
