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
#include "osd/PG.h"
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
  string fspath, jpath, pgid, type;
  Formatter *formatter = new JSONFormatter(true);

  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "produce help message")
    ("filestore-path", po::value<string>(&fspath),
     "path to filestore directory, mandatory")
    ("journal-path", po::value<string>(&jpath),
     "path to journal, mandatory")
    ("pgid", po::value<string>(&pgid),
     "PG id, mandatory")
    ("type", po::value<string>(&type),
     "Type which is 'info' or 'log', mandatory")
    ("debug", "Enable diagnostic output to stderr")
    ;

  po::variables_map vm;
  po::parsed_options parsed =
    po::command_line_parser(argc, argv).options(desc).allow_unregistered().run();
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
    cout << "Must provide filestore-path" << std::endl
	 << desc << std::endl;
    return 1;
  } 
  if (!vm.count("journal-path")) {
    cout << "Must provide journal-path" << std::endl
	 << desc << std::endl;
    return 1;
  } 
  if (!vm.count("pgid")) {
    cout << "Must provide pgid" << std::endl
	 << desc << std::endl;
    return 1;
  } 
  if (!vm.count("type")) {
    cout << "Must provide type ('info' or 'log')" << std::endl
	 << desc << std::endl;
    return 1;
  } 
  
  if (fspath.length() == 0 || jpath.length() == 0 || pgid.length() == 0 ||
    (type != "info" && type != "log")) {
    cerr << "Invalid params" << std::endl;
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

  //Suppress derr() output to stderr by default
  if (!vm.count("debug")) {
    close(2);
    (void)open("/dev/null", O_WRONLY);
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

  pg_t arg_pgid;
  if (!arg_pgid.parse(pgid.c_str())) {
    cerr << "Invalid pgid '" << pgid << "' specified" << std::endl;
    exit(1);
  }

  int ret = 0;

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

  bool found = false;
  vector<coll_t> ls;
  r = fs->list_collections(ls);
  if (r < 0) {
    cerr << "failed to list pgs: " << cpp_strerror(-r) << std::endl;
    exit(1);
  }

  for (vector<coll_t>::iterator it = ls.begin();
       it != ls.end();
       it++) {
    coll_t coll = *it;
    pg_t pgid;
    snapid_t snap;
    if (!it->is_pg(pgid, snap)) {
      continue;
    }

    if (pgid != arg_pgid) {
      continue;
    }
    if (snap != CEPH_NOSNAP) {
      cout << "load_pgs skipping snapped dir " << coll
	       << " (pg " << pgid << " snap " << snap << ")" << std::endl;
      continue;
    }

    bufferlist bl;
    epoch_t map_epoch = PG::peek_map_epoch(fs, coll, &bl);
    (void)map_epoch;

    found = true;

    pg_info_t info;
    map<epoch_t,pg_interval_t> past_intervals;
    hobject_t biginfo_oid = OSD::make_pg_biginfo_oid(pgid);
    interval_set<snapid_t> snap_collections;

    int r = PG::read_info(fs, coll, bl, info, past_intervals, biginfo_oid,
      snap_collections);
    if (r < 0) {
      cerr << "read_info error " << cpp_strerror(-r) << std::endl;
      ret = 1;
      continue;
    }

    if (type == "info") {
      formatter->open_object_section("info");
      info.dump(formatter);
      formatter->close_section();
      formatter->flush(cout);
      cout << std::endl;
      break;
    } else if (type == "log") {
      PG::OndiskLog ondisklog;
      PG::IndexedLog log;
      pg_missing_t missing;
      hobject_t logoid = OSD::make_pg_log_oid(pgid);
      try {
        ostringstream oss;
        PG::read_log(fs, coll, logoid, info, ondisklog, log, missing, oss);
        if (vm.count("debug"))
          cerr << oss;
      }
      catch (const buffer::error &e) {
        cerr << "read_log threw exception error", e.what();
        ret = 1;
        break;
      }
      
      formatter->open_object_section("log");
      log.dump(formatter);
      formatter->close_section();
      formatter->flush(cout);
      cout << std::endl;
      formatter->open_object_section("missing");
      missing.dump(formatter);
      formatter->close_section();
      formatter->flush(cout);
      cout << std::endl;

    }
  }

  if (!found) {
    cerr << "PG '" << arg_pgid << "' not found" << std::endl;
    ret = 1;
  }

  if (fs->umount() < 0) {
    cerr << "umount failed" << std::endl;
    return 1;
  }

  return ret;
}

