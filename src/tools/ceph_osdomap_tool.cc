// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2012 Inktank, Inc.
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License kkjversion 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include <stdlib.h>
#include <string>

#include "common/errno.h"
#include "global/global_init.h"

#include "os/filestore/DBObjectMap.h"
#include "kv/KeyValueDB.h"

namespace po = boost::program_options;
using namespace std;

int main(int argc, char **argv) {
  po::options_description desc("Allowed options");
  string store_path, cmd, out_path, oid;
  bool debug = false;
  desc.add_options()
    ("help", "produce help message")
    ("omap-path", po::value<string>(&store_path),
     "path to mon directory, mandatory (current/omap usually)")
    ("paranoid", "use paranoid checking")
    ("debug", "Additional debug output from DBObjectMap")
    ("oid", po::value<string>(&oid), "Restrict to this object id when dumping objects")
    ("command", po::value<string>(&cmd),
     "command arg is one of [dump-raw-keys, dump-raw-key-vals, dump-objects, dump-objects-with-keys, check, dump-headers, repair], mandatory")
    ;
  po::positional_options_description p;
  p.add("command", 1);

  vector<string> ceph_option_strings;
  po::variables_map vm;
  try {
    po::parsed_options parsed =
      po::command_line_parser(argc, argv).options(desc).positional(p).allow_unregistered().run();
    po::store(
	      parsed,
	      vm);
    po::notify(vm);

    ceph_option_strings = po::collect_unrecognized(parsed.options,
						   po::include_positional);
  } catch(po::error &e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }

  vector<const char *> ceph_options, def_args;
  ceph_options.reserve(ceph_option_strings.size());
  for (vector<string>::iterator i = ceph_option_strings.begin();
       i != ceph_option_strings.end();
       ++i) {
    ceph_options.push_back(i->c_str());
  }

  if (vm.count("debug")) debug = true;

  auto cct = global_init(
    &def_args, ceph_options, CEPH_ENTITY_TYPE_OSD,
    CODE_ENVIRONMENT_UTILITY_NODOUT, 0);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);
  g_conf = g_ceph_context->_conf;
  if (debug) {
    g_conf->set_val_or_die("log_to_stderr", "true");
    g_conf->set_val_or_die("err_to_stderr", "true");
  }
  g_conf->apply_changes(NULL);

  if (vm.count("help")) {
    std::cerr << desc << std::endl;
    return 1;
  }

  if (vm.count("omap-path") == 0) {
    std::cerr << "Required argument --omap-path" << std::endl;
    return 1;
  }

  if (vm.count("command") == 0) {
    std::cerr << "Required argument --command" << std::endl;
    return 1;
  }

  KeyValueDB* store(KeyValueDB::create(g_ceph_context, "leveldb", store_path));
  /*if (vm.count("paranoid")) {
    std::cerr << "Enabling paranoid checks" << std::endl;
    store->options.paranoid_checks = true;
    }*/
  DBObjectMap omap(store);
  stringstream out;
  int r = store->open(out);
  if (r < 0) {
    std::cerr << "Store open got: " << cpp_strerror(r) << std::endl;
    std::cerr << "Output: " << out.str() << std::endl;
    goto done;
  }
  // We don't call omap.init() here because it will repair
  // the DBObjectMap which we might want to examine for diagnostic
  // reasons.  Instead use --command repair.
  r = 0;


  if (cmd == "dump-raw-keys") {
    KeyValueDB::WholeSpaceIterator i = store->get_iterator();
    for (i->seek_to_first(); i->valid(); i->next()) {
      std::cout << i->raw_key() << std::endl;
    }
  } else if (cmd == "dump-raw-key-vals") {
    KeyValueDB::WholeSpaceIterator i = store->get_iterator();
    for (i->seek_to_first(); i->valid(); i->next()) {
      std::cout << i->raw_key() << std::endl;
      i->value().hexdump(std::cout);
    }
  } else if (cmd == "dump-objects") {
    vector<ghobject_t> objects;
    r = omap.list_objects(&objects);
    if (r < 0) {
      std::cerr << "list_objects got: " << cpp_strerror(r) << std::endl;
      goto done;
    }
    for (vector<ghobject_t>::iterator i = objects.begin();
	 i != objects.end();
	 ++i) {
      if (vm.count("oid") != 0 && i->hobj.oid.name != oid)
        continue;
      std::cout << *i << std::endl;
    }
    r = 0;
  } else if (cmd == "dump-objects-with-keys") {
    vector<ghobject_t> objects;
    r = omap.list_objects(&objects);
    if (r < 0) {
      std::cerr << "list_objects got: " << cpp_strerror(r) << std::endl;
      goto done;
    }
    for (vector<ghobject_t>::iterator i = objects.begin();
	 i != objects.end();
	 ++i) {
      if (vm.count("oid") != 0 && i->hobj.oid.name != oid)
        continue;
      std::cout << "Object: " << *i << std::endl;
      ObjectMap::ObjectMapIterator j = omap.get_iterator(ghobject_t(i->hobj));
      for (j->seek_to_first(); j->valid(); j->next()) {
	std::cout << j->key() << std::endl;
	j->value().hexdump(std::cout);
      }
    }
  } else if (cmd == "check" || cmd == "repair") {
    ostringstream ss;
    bool repair = (cmd == "repair");
    r = omap.check(ss, repair);
    if (r) {
      std::cerr << ss.str() << std::endl;
      if (r > 0) {
        std::cerr << "check got " << r << " error(s)" << std::endl;
        r = 1;
        goto done;
      }
    }
    std::cout << (repair ? "repair" : "check") << " succeeded" << std::endl;
  } else if (cmd == "dump-headers") {
    vector<DBObjectMap::_Header> headers;
    r = omap.list_object_headers(&headers);
    if (r < 0) {
      std::cerr << "list_object_headers got: " << cpp_strerror(r) << std::endl;
      r = 1;
      goto done;
    }
    for (auto i : headers)
      std::cout << i << std::endl;
  } else {
    std::cerr << "Did not recognize command " << cmd << std::endl;
    r = 1;
    goto done;
  }
  r = 0;

  done:
  return r;
}
