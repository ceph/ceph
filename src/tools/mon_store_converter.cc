// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2012 Inktank, Inc.
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/
#include <iostream>
#include <string>
#include <sstream>
#include <map>
#include <set>
#include <boost/scoped_ptr.hpp>
#include <errno.h>

#include "include/types.h"
#include "include/buffer.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/debug.h"
#include "common/config.h"

#include "mon/MonitorDBStore.h"
#include "mon/MonitorStore.h"

using namespace std;

class MonitorStoreConverter {

  boost::scoped_ptr<MonitorDBStore> db;
  boost::scoped_ptr<MonitorStore> store;

  set<version_t> gvs;
  version_t highest_last_pn;
  version_t highest_accepted_pn;

  static const int PAXOS_MAX_VERSIONS = 50;
  string MONITOR_NAME;

 public:
  MonitorStoreConverter(string &store_path, string &db_store_path)
    : db(0), store(0),
      highest_last_pn(0), highest_accepted_pn(0),
      MONITOR_NAME("monitor")
  {
    MonitorStore *store_ptr = new MonitorStore(store_path);
    assert(!store_ptr->mount());
    store.reset(store_ptr);

    MonitorDBStore *db_ptr = new MonitorDBStore(db_store_path);
    db.reset(db_ptr);
  }

  int convert() {
    if (db->open(std::cerr) >= 0) {
      std::cerr << "store already exists" << std::endl;
      return EEXIST;
    }
    assert(!db->create_and_open(std::cerr));
    if (db->exists("mon_convert", "on_going")) {
      std::cout << __func__ << " found a mon store in mid-convertion; abort!"
		<< std::endl;
      return EEXIST;
    }
    _mark_convert_start();
    _convert_monitor();
    _convert_machines();
    _mark_convert_finish();

    std::cout << __func__ << " finished conversion" << std::endl;

    return 0;
  }

  bool match() {
    return true;
  }

 private:

  set<string> _get_machines_names() {
    set<string> names;
    names.insert("auth");
    names.insert("logm");
    names.insert("mdsmap");
    names.insert("monmap");
    names.insert("osdmap");
    names.insert("pgmap");

    return names;
  }

  void _mark_convert_start() {
    MonitorDBStore::Transaction tx;
    tx.put("mon_convert", "on_going", 1);
    db->apply_transaction(tx);
  }

  void _mark_convert_finish() {
    MonitorDBStore::Transaction tx;
    tx.erase("mon_convert", "on_going");
    db->apply_transaction(tx);
  }

  void _convert_monitor() {

    assert(store->exists_bl_ss("magic"));
    assert(store->exists_bl_ss("keyring"));
    assert(store->exists_bl_ss("feature_set"));
    assert(store->exists_bl_ss("election_epoch"));

    MonitorDBStore::Transaction tx;

    if (store->exists_bl_ss("joined")) {
      version_t joined = store->get_int("joined");
      tx.put(MONITOR_NAME, "joined", joined);
    }

    vector<string> keys;
    keys.push_back("magic");
    keys.push_back("feature_set");
    keys.push_back("election_epoch");
    keys.push_back("cluster_uuid");

    vector<string>::iterator it;
    for (it = keys.begin(); it != keys.end(); ++it) {
      if (!store->exists_bl_ss((*it).c_str()))
	continue;

      bufferlist bl;
      int r = store->get_bl_ss(bl, (*it).c_str(), 0);
      assert(r > 0);
      tx.put(MONITOR_NAME, *it, bl);
    }

    assert(!tx.empty());
    db->apply_transaction(tx);
  }

  void _convert_machines(string machine) {
    std::cout << __func__ << " " << machine << std::endl;

    version_t first_committed =
      store->get_int(machine.c_str(), "first_committed");
    version_t last_committed =
      store->get_int(machine.c_str(), "last_committed");

    version_t accepted_pn = store->get_int(machine.c_str(), "accepted_pn");
    version_t last_pn = store->get_int(machine.c_str(), "last_pn");

    if (accepted_pn > highest_accepted_pn)
      highest_accepted_pn = accepted_pn;
    if (last_pn > highest_last_pn)
      highest_last_pn = last_pn;

    string machine_gv(machine);
    machine_gv.append("_gv");
    bool has_gv = true;

    if (!store->exists_bl_ss(machine_gv.c_str())) {
      std::cerr << __func__ << " " << machine
		<< " no gv dir '" << machine_gv << "'" << std::endl;
      has_gv = false;
    }

    for (version_t ver = first_committed; ver <= last_committed; ver++) {
      if (!store->exists_bl_sn(machine.c_str(), ver)) {
	std::cerr << __func__ << " " << machine
		  << " ver " << ver << " dne" << std::endl;
	continue;
      }

      bufferlist bl;
      int r = store->get_bl_sn(bl, machine.c_str(), ver);
      assert(r >= 0);
      std::cout << __func__ << " " << machine
		<< " ver " << ver << " bl " << bl.length() << std::endl;

      MonitorDBStore::Transaction tx;
      tx.put(machine, ver, bl);
      tx.put(machine, "last_committed", ver);

      if (has_gv && store->exists_bl_sn(machine_gv.c_str(), ver)) {
	stringstream s;
	s << ver;
	string ver_str = s.str();

	version_t gv = store->get_int(machine_gv.c_str(), ver_str.c_str());
	std::cerr << __func__ << " " << machine
		  << " ver " << ver << " -> " << gv << std::endl;

	if (gvs.count(gv) == 0) {
	  gvs.insert(gv);
	} else {
	  std::cerr << __func__ << " " << machine
		    << " gv " << gv << " already exists"
		    << std::endl;
	}

	bufferlist tx_bl;
	tx.encode(tx_bl);
	tx.put("paxos", gv, tx_bl);
      }
      db->apply_transaction(tx);
    }

    version_t lc = db->get(machine, "last_committed");
    assert(lc == last_committed);

    MonitorDBStore::Transaction tx;
    tx.put(machine, "first_committed", first_committed);
    tx.put(machine, "last_committed", last_committed);

    if (store->exists_bl_ss(machine.c_str(), "latest")) {
      bufferlist latest_bl_raw;
      int r = store->get_bl_ss(latest_bl_raw, machine.c_str(), "latest");
      assert(r >= 0);
      if (!latest_bl_raw.length()) {
	std::cerr << __func__ << " machine " << machine
		  << " skip latest with size 0" << std::endl;
	goto out;
      }

      tx.put(machine, "latest", latest_bl_raw);

      bufferlist::iterator lbl_it = latest_bl_raw.begin();
      bufferlist latest_bl;
      version_t latest_ver;
      ::decode(latest_ver, lbl_it);
      ::decode(latest_bl, lbl_it);

      std::cout << __func__ << " machine " << machine
		<< " latest ver " << latest_ver << std::endl;

      tx.put(machine, "full_latest", latest_ver);
      stringstream os;
      os << "full_" << latest_ver;
      tx.put(machine, os.str(), latest_bl);
    }
  out:
    db->apply_transaction(tx);
  }

  void _convert_paxos() {
    assert(!gvs.empty());

    set<version_t>::reverse_iterator rit = gvs.rbegin();
    version_t highest_gv = *rit;
    version_t last_gv = highest_gv;

    int n = 0;
    for (; (rit != gvs.rend()) && (n < PAXOS_MAX_VERSIONS); ++rit, ++n) {

      version_t gv = *rit;

      if (last_gv == gv)
	continue;

      if ((last_gv - gv) > 1) {
	  // we are done; we found a gap and we are only interested in keeping
	  // contiguous paxos versions.
	  break;
      }
      last_gv = gv;
    }

    // erase all paxos versions between [first, last_gv[, with first being the
    // first gv in the map.
    MonitorDBStore::Transaction tx;
    set<version_t>::iterator it = gvs.begin();
    std::cout << __func__ << " first gv " << (*it)
	      << " last gv " << last_gv << std::endl;
    for (; it != gvs.end() && (*it < last_gv); ++it) {
      tx.erase("paxos", *it);
    }
    tx.put("paxos", "first_committed", last_gv);
    tx.put("paxos", "last_committed", highest_gv);
    tx.put("paxos", "accepted_pn", highest_accepted_pn);
    tx.put("paxos", "last_pn", highest_last_pn);
    db->apply_transaction(tx);
  }

  void _convert_machines() {

    set<string> machine_names = _get_machines_names();
    set<string>::iterator it = machine_names.begin();

    std::cout << __func__ << std::endl;

    for (; it != machine_names.end(); ++it) {
      _convert_machines(*it);
    }

    _convert_paxos();
  }
};


void usage(const char *pname)
{
  std::cerr << "Usage: " << pname << " <old store path>\n"
    << std::endl;
}

int main(int argc, const char *argv[])
{
  vector<const char*> def_args;
  vector<const char*> args;
  const char *our_name = argv[0];
  argv_to_vec(argc, argv, args);

  global_init(&def_args, args,
	      CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
	      CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);

  if (args.empty()) {
    usage(our_name);
    return 1;
  }
  string store(args[0]);
  string new_store(store);
  MonitorStoreConverter converter(store, new_store);
  assert(!converter.convert());
  assert(converter.match());

  std::cout << "store successfully converted to new format" << std::endl;

  return 0;
}
