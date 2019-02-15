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

#include <iostream>
#include <string>

#include "common/config.h"
#include "include/ceph_features.h"

#include "mon/MonMap.h"
#include "mon/Monitor.h"
#include "mon/MonitorDBStore.h"
#include "mon/MonClient.h"

#include "msg/Messenger.h"

#include "include/CompatSet.h"

#include "common/ceph_argparse.h"
#include "common/pick_address.h"
#include "common/Throttle.h"
#include "common/Timer.h"
#include "common/errno.h"
#include "common/Preforker.h"

#include "global/global_init.h"
#include "global/signal_handler.h"

#include "perfglue/heap_profiler.h"

#include "include/ceph_assert.h"

#define dout_subsys ceph_subsys_mon

Monitor *mon = NULL;

void handle_mon_signal(int signum)
{
  if (mon)
    mon->handle_signal(signum);
}


int obtain_monmap(MonitorDBStore &store, bufferlist &bl)
{
  dout(10) << __func__ << dendl;
  /*
   * the monmap may be in one of three places:
   *  'monmap:<latest_version_no>' - the monmap we'd really like to have
   *  'mon_sync:latest_monmap'     - last monmap backed up for the last sync
   *  'mkfs:monmap'                - a monmap resulting from mkfs
   */

  if (store.exists("monmap", "last_committed")) {
    version_t latest_ver = store.get("monmap", "last_committed");
    if (store.exists("monmap", latest_ver)) {
      int err = store.get("monmap", latest_ver, bl);
      ceph_assert(err == 0);
      ceph_assert(bl.length() > 0);
      dout(10) << __func__ << " read last committed monmap ver "
               << latest_ver << dendl;
      return 0;
    }
  }

  if (store.exists("mon_sync", "in_sync")
      || store.exists("mon_sync", "force_sync")) {
    dout(10) << __func__ << " detected aborted sync" << dendl;
    if (store.exists("mon_sync", "latest_monmap")) {
      int err = store.get("mon_sync", "latest_monmap", bl);
      ceph_assert(err == 0);
      ceph_assert(bl.length() > 0);
      dout(10) << __func__ << " read backup monmap" << dendl;
      return 0;
    }
  }

  if (store.exists("mkfs", "monmap")) {
    dout(10) << __func__ << " found mkfs monmap" << dendl;
    int err = store.get("mkfs", "monmap", bl);
    ceph_assert(err == 0);
    ceph_assert(bl.length() > 0);
    return 0;
  }

  derr << __func__ << " unable to find a monmap" << dendl;
  return -ENOENT;
}

int check_mon_data_exists()
{
  string mon_data = g_conf()->mon_data;
  struct stat buf;
  if (::stat(mon_data.c_str(), &buf)) {
    if (errno != ENOENT) {
      derr << "stat(" << mon_data << ") " << cpp_strerror(errno) << dendl;
    }
    return -errno;
  }
  return 0;
}

/** Check whether **mon data** is empty.
 *
 * Being empty means mkfs has not been run and there's no monitor setup
 * at **g_conf()->mon_data**.
 *
 * If the directory g_conf()->mon_data is not empty we will return -ENOTEMPTY.
 * Otherwise we will return 0.  Any other negative returns will represent
 * a failure to be handled by the caller.
 *
 * @return **0** on success, -ENOTEMPTY if not empty or **-errno** otherwise.
 */
int check_mon_data_empty()
{
  string mon_data = g_conf()->mon_data;

  DIR *dir = ::opendir(mon_data.c_str());
  if (!dir) {
    derr << "opendir(" << mon_data << ") " << cpp_strerror(errno) << dendl;
    return -errno;
  }
  int code = 0;
  struct dirent *de = nullptr;
  errno = 0;
  while ((de = ::readdir(dir))) {
    if (string(".") != de->d_name &&
	string("..") != de->d_name &&
	string("kv_backend") != de->d_name) {
      code = -ENOTEMPTY;
      break;
    }
  }
  if (!de && errno) {
    derr << "readdir(" << mon_data << ") " << cpp_strerror(errno) << dendl;
    code = -errno;
  }

  ::closedir(dir);

  return code;
}

static void usage()
{
  cout << "usage: ceph-mon -i <ID> [flags]\n"
       << "  --debug_mon n\n"
       << "        debug monitor level (e.g. 10)\n"
       << "  --mkfs\n"
       << "        build fresh monitor fs\n"
       << "  --force-sync\n"
       << "        force a sync from another mon by wiping local data (BE CAREFUL)\n"
       << "  --yes-i-really-mean-it\n"
       << "        mandatory safeguard for --force-sync\n"
       << "  --compact\n"
       << "        compact the monitor store\n"
       << "  --osdmap <filename>\n"
       << "        only used when --mkfs is provided: load the osdmap from <filename>\n"
       << "  --inject-monmap <filename>\n"
       << "        write the <filename> monmap to the local monitor store and exit\n"
       << "  --extract-monmap <filename>\n"
       << "        extract the monmap from the local monitor store and exit\n"
       << "  --mon-data <directory>\n"
       << "        where the mon store and keyring are located\n"
       << std::endl;
  generic_server_usage();
}

entity_addrvec_t make_mon_addrs(entity_addr_t a)
{
  entity_addrvec_t addrs;
  if (a.get_port() == 0) {
    a.set_type(entity_addr_t::TYPE_MSGR2);
    a.set_port(CEPH_MON_PORT_IANA);
    addrs.v.push_back(a);
    a.set_type(entity_addr_t::TYPE_LEGACY);
    a.set_port(CEPH_MON_PORT_LEGACY);
    addrs.v.push_back(a);
  } else if (a.get_port() == CEPH_MON_PORT_LEGACY) {
    a.set_type(entity_addr_t::TYPE_LEGACY);
    addrs.v.push_back(a);
  } else {
    a.set_type(entity_addr_t::TYPE_MSGR2);
    addrs.v.push_back(a);
  }
  return addrs;
}

int main(int argc, const char **argv)
{
  int err;

  bool mkfs = false;
  bool compact = false;
  bool force_sync = false;
  bool yes_really = false;
  std::string osdmapfn, inject_monmap, extract_monmap;

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage();
    exit(0);
  }

  // We need to specify some default values that may be overridden by the
  // user, that are specific to the monitor.  The options we are overriding
  // are also used on the OSD (or in any other component that uses leveldb),
  // so changing the global defaults is not an option.
  // This is not the prettiest way of doing this, especially since it has us
  // having a different place defining default values, but it's not horribly
  // wrong enough to prevent us from doing it :)
  //
  // NOTE: user-defined options will take precedence over ours.
  //
  //  leveldb_write_buffer_size = 32*1024*1024  = 33554432  // 32MB
  //  leveldb_cache_size        = 512*1024*1204 = 536870912 // 512MB
  //  leveldb_block_size        = 64*1024       = 65536     // 64KB
  //  leveldb_compression       = false
  //  leveldb_log               = ""
  map<string,string> defaults = {
    { "leveldb_write_buffer_size", "33554432" },
    { "leveldb_cache_size", "536870912" },
    { "leveldb_block_size", "65536" },
    { "leveldb_compression", "false"},
    { "leveldb_log", "" },
    { "keyring", "$mon_data/keyring" },
  };

  int flags = 0;
  {
    vector<const char*> args_copy = args;
    std::string val;
    for (std::vector<const char*>::iterator i = args_copy.begin();
	 i != args_copy.end(); ) {
      if (ceph_argparse_double_dash(args_copy, i)) {
	break;
      } else if (ceph_argparse_flag(args_copy, i, "--mkfs", (char*)NULL)) {
	flags |= CINIT_FLAG_NO_DAEMON_ACTIONS;
      } else if (ceph_argparse_witharg(args_copy, i, &val, "--inject_monmap", (char*)NULL)) {
	flags |= CINIT_FLAG_NO_DAEMON_ACTIONS;
      } else if (ceph_argparse_witharg(args_copy, i, &val, "--extract-monmap", (char*)NULL)) {
	flags |= CINIT_FLAG_NO_DAEMON_ACTIONS;
      } else {
	++i;
      }
    }
  }

  // don't try to get config from mon cluster during startup
  flags |= CINIT_FLAG_NO_MON_CONFIG;

  auto cct = global_init(&defaults, args,
			 CEPH_ENTITY_TYPE_MON, CODE_ENVIRONMENT_DAEMON,
			 flags, "mon_data");
  ceph_heap_profiler_init();

  std::string val;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, i, "--mkfs", (char*)NULL)) {
      mkfs = true;
    } else if (ceph_argparse_flag(args, i, "--compact", (char*)NULL)) {
      compact = true;
    } else if (ceph_argparse_flag(args, i, "--force-sync", (char*)NULL)) {
      force_sync = true;
    } else if (ceph_argparse_flag(args, i, "--yes-i-really-mean-it", (char*)NULL)) {
      yes_really = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--osdmap", (char*)NULL)) {
      osdmapfn = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--inject_monmap", (char*)NULL)) {
      inject_monmap = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--extract-monmap", (char*)NULL)) {
      extract_monmap = val;
    } else {
      ++i;
    }
  }
  if (!args.empty()) {
    cerr << "too many arguments: " << args << std::endl;
    exit(1);
  }

  if (force_sync && !yes_really) {
    cerr << "are you SURE you want to force a sync?  this will erase local data and may\n"
	 << "break your mon cluster.  pass --yes-i-really-mean-it if you do." << std::endl;
    exit(1);
  }

  if (g_conf()->mon_data.empty()) {
    cerr << "must specify '--mon-data=foo' data path" << std::endl;
    exit(1);
  }

  if (g_conf()->name.get_id().empty()) {
    cerr << "must specify id (--id <id> or --name mon.<id>)" << std::endl;
    exit(1);
  }

  // -- mkfs --
  if (mkfs) {

    int err = check_mon_data_exists();
    if (err == -ENOENT) {
      if (::mkdir(g_conf()->mon_data.c_str(), 0755)) {
	derr << "mkdir(" << g_conf()->mon_data << ") : "
	     << cpp_strerror(errno) << dendl;
	exit(1);
      }
    } else if (err < 0) {
      derr << "error opening '" << g_conf()->mon_data << "': "
           << cpp_strerror(-err) << dendl;
      exit(-err);
    }

    err = check_mon_data_empty();
    if (err == -ENOTEMPTY) {
      // Mon may exist.  Let the user know and exit gracefully.
      derr << "'" << g_conf()->mon_data << "' already exists and is not empty"
           << ": monitor may already exist" << dendl;
      exit(0);
    } else if (err < 0) {
      derr << "error checking if '" << g_conf()->mon_data << "' is empty: "
           << cpp_strerror(-err) << dendl;
      exit(-err);
    }

    // resolve public_network -> public_addr
    pick_addresses(g_ceph_context, CEPH_PICK_ADDRESS_PUBLIC);

    dout(10) << "public_network " << g_conf()->public_network << dendl;
    dout(10) << "public_addr " << g_conf()->public_network << dendl;

    common_init_finish(g_ceph_context);

    bufferlist monmapbl, osdmapbl;
    std::string error;
    MonMap monmap;

    // load or generate monmap
    const auto monmap_fn = g_conf().get_val<string>("monmap");
    if (monmap_fn.length()) {
      int err = monmapbl.read_file(monmap_fn.c_str(), &error);
      if (err < 0) {
	derr << argv[0] << ": error reading " << monmap_fn << ": " << error << dendl;
	exit(1);
      }
      try {
	monmap.decode(monmapbl);

	// always mark seed/mkfs monmap as epoch 0
	monmap.set_epoch(0);
      } catch (const buffer::error& e) {
	derr << argv[0] << ": error decoding monmap " << monmap_fn << ": " << e.what() << dendl;
	exit(1);
      }

      dout(1) << "imported monmap:\n";
      monmap.print(*_dout);
      *_dout << dendl;
      
    } else {
      ostringstream oss;
      int err = monmap.build_initial(g_ceph_context, true, oss);
      if (oss.tellp())
        derr << oss.str() << dendl;
      if (err < 0) {
	derr << argv[0] << ": warning: no initial monitors; must use admin socket to feed hints" << dendl;
      }

      dout(1) << "initial generated monmap:\n";
      monmap.print(*_dout);
      *_dout << dendl;

      // am i part of the initial quorum?
      if (monmap.contains(g_conf()->name.get_id())) {
	// hmm, make sure the ip listed exists on the current host?
	// maybe later.
      } else if (!g_conf()->public_addr.is_blank_ip()) {
	entity_addrvec_t av = make_mon_addrs(g_conf()->public_addr);
	string name;
	if (monmap.contains(av, &name)) {
	  monmap.rename(name, g_conf()->name.get_id());
	  dout(0) << argv[0] << ": renaming mon." << name << " " << av
		  << " to mon." << g_conf()->name.get_id() << dendl;
	}
      } else {
	// is a local address listed without a name?  if so, name myself.
	list<entity_addr_t> ls;
	monmap.list_addrs(ls);
	dout(0) << " monmap addrs are " << ls << ", checking if any are local"
		<< dendl;

	entity_addr_t local;
	if (have_local_addr(g_ceph_context, ls, &local)) {
	  dout(0) << " have local addr " << local << dendl;
	  string name;
	  local.set_type(entity_addr_t::TYPE_MSGR2);
	  if (!monmap.get_addr_name(local, name)) {
	    local.set_type(entity_addr_t::TYPE_LEGACY);
	    if (!monmap.get_addr_name(local, name)) {
	      dout(0) << "no local addresses appear in bootstrap monmap"
		      << dendl;
	    }
	  }
	  if (name.compare(0, 7, "noname-") == 0) {
	    dout(0) << argv[0] << ": mon." << name << " " << local
		    << " is local, renaming to mon." << g_conf()->name.get_id()
		    << dendl;
	    monmap.rename(name, g_conf()->name.get_id());
	  } else if (name.size()) {
	    dout(0) << argv[0] << ": mon." << name << " " << local
		    << " is local, but not 'noname-' + something; "
		    << "not assuming it's me" << dendl;
	  }
	} else {
	  dout(0) << " no local addrs match monmap" << dendl;
	}
      }
    }

    const auto fsid = g_conf().get_val<uuid_d>("fsid");
    if (!fsid.is_zero()) {
      monmap.fsid = fsid;
      dout(0) << argv[0] << ": set fsid to " << fsid << dendl;
    }
    
    if (monmap.fsid.is_zero()) {
      derr << argv[0] << ": generated monmap has no fsid; use '--fsid <uuid>'" << dendl;
      exit(10);
    }

    //monmap.print(cout);

    // osdmap
    if (osdmapfn.length()) {
      err = osdmapbl.read_file(osdmapfn.c_str(), &error);
      if (err < 0) {
	derr << argv[0] << ": error reading " << osdmapfn << ": "
	     << error << dendl;
	exit(1);
      }
    }

    // go
    MonitorDBStore store(g_conf()->mon_data);
    ostringstream oss;
    int r = store.create_and_open(oss);
    if (oss.tellp())
      derr << oss.str() << dendl;
    if (r < 0) {
      derr << argv[0] << ": error opening mon data directory at '"
           << g_conf()->mon_data << "': " << cpp_strerror(r) << dendl;
      exit(1);
    }
    ceph_assert(r == 0);

    Monitor mon(g_ceph_context, g_conf()->name.get_id(), &store, 0, 0, &monmap);
    r = mon.mkfs(osdmapbl);
    if (r < 0) {
      derr << argv[0] << ": error creating monfs: " << cpp_strerror(r) << dendl;
      exit(1);
    }
    store.close();
    dout(0) << argv[0] << ": created monfs at " << g_conf()->mon_data 
	    << " for " << g_conf()->name << dendl;
    return 0;
  }

  err = check_mon_data_exists();
  if (err < 0 && err == -ENOENT) {
    derr << "monitor data directory at '" << g_conf()->mon_data << "'"
         << " does not exist: have you run 'mkfs'?" << dendl;
    exit(1);
  } else if (err < 0) {
    derr << "error accessing monitor data directory at '"
         << g_conf()->mon_data << "': " << cpp_strerror(-err) << dendl;
    exit(1);
  }

  err = check_mon_data_empty();
  if (err == 0) {
    derr << "monitor data directory at '" << g_conf()->mon_data
      << "' is empty: have you run 'mkfs'?" << dendl;
    exit(1);
  } else if (err < 0 && err != -ENOTEMPTY) {
    // we don't want an empty data dir by now
    derr << "error accessing '" << g_conf()->mon_data << "': "
         << cpp_strerror(-err) << dendl;
    exit(1);
  }

  {
    // check fs stats. don't start if it's critically close to full.
    ceph_data_stats_t stats;
    int err = get_fs_stats(stats, g_conf()->mon_data.c_str());
    if (err < 0) {
      derr << "error checking monitor data's fs stats: " << cpp_strerror(err)
           << dendl;
      exit(-err);
    }
    if (stats.avail_percent <= g_conf()->mon_data_avail_crit) {
      derr << "error: monitor data filesystem reached concerning levels of"
           << " available storage space (available: "
           << stats.avail_percent << "% " << byte_u_t(stats.byte_avail)
           << ")\nyou may adjust 'mon data avail crit' to a lower value"
           << " to make this go away (default: " << g_conf()->mon_data_avail_crit
           << "%)\n" << dendl;
      exit(ENOSPC);
    }
  }

  // we fork early to prevent leveldb's environment static state from
  // screwing us over
  Preforker prefork;
  if (!(flags & CINIT_FLAG_NO_DAEMON_ACTIONS)) {
    if (global_init_prefork(g_ceph_context) >= 0) {
      string err_msg;
      err = prefork.prefork(err_msg);
      if (err < 0) {
        derr << err_msg << dendl;
        prefork.exit(err);
      }
      if (prefork.is_parent()) {
        err = prefork.parent_wait(err_msg);
        if (err < 0)
          derr << err_msg << dendl;
        prefork.exit(err);
      }
      setsid();
      global_init_postfork_start(g_ceph_context);
    }
    common_init_finish(g_ceph_context);
    global_init_chdir(g_ceph_context);
    if (global_init_preload_erasure_code(g_ceph_context) < 0)
      prefork.exit(1);
  }

  // set up signal handlers, now that we've daemonized/forked.
  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);

  MonitorDBStore *store = new MonitorDBStore(g_conf()->mon_data);

  // make sure we aren't upgrading too fast
  {
    string val;
    int r = store->read_meta("min_mon_release", &val);
    if (r >= 0 && val.size()) {
      int min = atoi(val.c_str());
      if (min &&
	  min + 2 < (int)ceph_release()) {
	derr << "recorded min_mon_release is " << min
	     << " (" << ceph_release_name(min)
	     << ") which is >2 releases older than installed "
	     << ceph_release() << " (" << ceph_release_name(ceph_release())
	     << "); you can only upgrade 2 releases at a time" << dendl;
	derr << "you should first upgrade to "
	     << (min + 1) << " (" << ceph_release_name(min + 1) << ") or "
	     << (min + 2) << " (" << ceph_release_name(min + 2) << ")" << dendl;
	prefork.exit(1);
      }
    }
  }

  {
    ostringstream oss;
    err = store->open(oss);
    if (oss.tellp())
      derr << oss.str() << dendl;
    if (err < 0) {
      derr << "error opening mon data directory at '"
           << g_conf()->mon_data << "': " << cpp_strerror(err) << dendl;
      prefork.exit(1);
    }
  }

  bufferlist magicbl;
  err = store->get(Monitor::MONITOR_NAME, "magic", magicbl);
  if (err || !magicbl.length()) {
    derr << "unable to read magic from mon data" << dendl;
    prefork.exit(1);
  }
  string magic(magicbl.c_str(), magicbl.length()-1);  // ignore trailing \n
  if (strcmp(magic.c_str(), CEPH_MON_ONDISK_MAGIC)) {
    derr << "mon fs magic '" << magic << "' != current '" << CEPH_MON_ONDISK_MAGIC << "'" << dendl;
    prefork.exit(1);
  }

  err = Monitor::check_features(store);
  if (err < 0) {
    derr << "error checking features: " << cpp_strerror(err) << dendl;
    prefork.exit(1);
  }

  // inject new monmap?
  if (!inject_monmap.empty()) {
    bufferlist bl;
    std::string error;
    int r = bl.read_file(inject_monmap.c_str(), &error);
    if (r) {
      derr << "unable to read monmap from " << inject_monmap << ": "
	   << error << dendl;
      prefork.exit(1);
    }

    // get next version
    version_t v = store->get("monmap", "last_committed");
    dout(0) << "last committed monmap epoch is " << v << ", injected map will be " << (v+1)
            << dendl;
    v++;

    // set the version
    MonMap tmp;
    tmp.decode(bl);
    if (tmp.get_epoch() != v) {
      dout(0) << "changing monmap epoch from " << tmp.get_epoch()
           << " to " << v << dendl;
      tmp.set_epoch(v);
    }
    bufferlist mapbl;
    tmp.encode(mapbl, CEPH_FEATURES_ALL);
    bufferlist final;
    encode(v, final);
    encode(mapbl, final);

    auto t(std::make_shared<MonitorDBStore::Transaction>());
    // save it
    t->put("monmap", v, mapbl);
    t->put("monmap", "latest", final);
    t->put("monmap", "last_committed", v);
    store->apply_transaction(t);

    dout(0) << "done." << dendl;
    prefork.exit(0);
  }

  // monmap?
  MonMap monmap;
  {
    // note that even if we don't find a viable monmap, we should go ahead
    // and try to build it up in the next if-else block.
    bufferlist mapbl;
    int err = obtain_monmap(*store, mapbl);
    if (err >= 0) {
      try {
        monmap.decode(mapbl);
      } catch (const buffer::error& e) {
        derr << "can't decode monmap: " << e.what() << dendl;
      }
    } else {
      derr << "unable to obtain a monmap: " << cpp_strerror(err) << dendl;
    }

    dout(10) << __func__ << " monmap:\n";
    JSONFormatter jf(true);
    jf.dump_object("monmap", monmap);
    jf.flush(*_dout);
    *_dout << dendl;

    if (!extract_monmap.empty()) {
      int r = mapbl.write_file(extract_monmap.c_str());
      if (r < 0) {
	r = -errno;
	derr << "error writing monmap to " << extract_monmap << ": " << cpp_strerror(r) << dendl;
	prefork.exit(1);
      }
      derr << "wrote monmap to " << extract_monmap << dendl;
      prefork.exit(0);
    }
  }

  // this is what i will bind to
  entity_addrvec_t ipaddrs;

  if (monmap.contains(g_conf()->name.get_id())) {
    ipaddrs = monmap.get_addrs(g_conf()->name.get_id());

    // print helpful warning if the conf file doesn't match
    std::vector <std::string> my_sections;
    g_conf().get_my_sections(my_sections);
    std::string mon_addr_str;
    if (g_conf().get_val_from_conf_file(my_sections, "mon addr",
				       mon_addr_str, true) == 0) {
      entity_addr_t conf_addr;
      if (conf_addr.parse(mon_addr_str.c_str())) {
	entity_addrvec_t conf_addrs = make_mon_addrs(conf_addr);
        if (ipaddrs != conf_addrs) {
	  derr << "WARNING: 'mon addr' config option " << conf_addrs
	       << " does not match monmap file" << std::endl
	       << "         continuing with monmap configuration" << dendl;
        }
      } else
	derr << "WARNING: invalid 'mon addr' config option" << std::endl
	     << "         continuing with monmap configuration" << dendl;
    }
  } else {
    dout(0) << g_conf()->name << " does not exist in monmap, will attempt to join an existing cluster" << dendl;

    pick_addresses(g_ceph_context, CEPH_PICK_ADDRESS_PUBLIC);
    if (!g_conf()->public_addr.is_blank_ip()) {
      ipaddrs = make_mon_addrs(g_conf()->public_addr);
      dout(0) << "using public_addr " << g_conf()->public_addr << " -> "
	      << ipaddrs << dendl;
    } else {
      MonMap tmpmap;
      ostringstream oss;
      int err = tmpmap.build_initial(g_ceph_context, true, oss);
      if (oss.tellp())
        derr << oss.str() << dendl;
      if (err < 0) {
	derr << argv[0] << ": error generating initial monmap: "
             << cpp_strerror(err) << dendl;
	prefork.exit(1);
      }
      if (tmpmap.contains(g_conf()->name.get_id())) {
	ipaddrs = tmpmap.get_addrs(g_conf()->name.get_id());
      } else {
	derr << "no public_addr or public_network specified, and "
	     << g_conf()->name << " not present in monmap or ceph.conf" << dendl;
	prefork.exit(1);
      }
    }
  }

  // bind
  int rank = monmap.get_rank(g_conf()->name.get_id());
  std::string public_msgr_type = g_conf()->ms_public_type.empty() ? g_conf().get_val<std::string>("ms_type") : g_conf()->ms_public_type;
  Messenger *msgr = Messenger::create(g_ceph_context, public_msgr_type,
				      entity_name_t::MON(rank), "mon",
				      0, Messenger::HAS_MANY_CONNECTIONS);
  if (!msgr)
    exit(1);
  msgr->set_cluster_protocol(CEPH_MON_PROTOCOL);
  msgr->set_default_send_priority(CEPH_MSG_PRIO_HIGH);

  msgr->set_default_policy(Messenger::Policy::stateless_server(0));
  msgr->set_policy(entity_name_t::TYPE_MON,
                   Messenger::Policy::lossless_peer_reuse(
		     CEPH_FEATURE_SERVER_LUMINOUS));
  msgr->set_policy(entity_name_t::TYPE_OSD,
                   Messenger::Policy::stateless_server(
		     CEPH_FEATURE_SERVER_LUMINOUS));
  msgr->set_policy(entity_name_t::TYPE_CLIENT,
                   Messenger::Policy::stateless_server(0));
  msgr->set_policy(entity_name_t::TYPE_MDS,
                   Messenger::Policy::stateless_server(0));

  // throttle client traffic
  Throttle *client_throttler = new Throttle(g_ceph_context, "mon_client_bytes",
					    g_conf()->mon_client_bytes);
  msgr->set_policy_throttlers(entity_name_t::TYPE_CLIENT,
				     client_throttler, NULL);

  // throttle daemon traffic
  // NOTE: actual usage on the leader may multiply by the number of
  // monitors if they forward large update messages from daemons.
  Throttle *daemon_throttler = new Throttle(g_ceph_context, "mon_daemon_bytes",
					    g_conf()->mon_daemon_bytes);
  msgr->set_policy_throttlers(entity_name_t::TYPE_OSD, daemon_throttler,
				     NULL);
  msgr->set_policy_throttlers(entity_name_t::TYPE_MDS, daemon_throttler,
				     NULL);

  entity_addrvec_t bind_addrs = ipaddrs;
  entity_addrvec_t public_addrs = ipaddrs;

  // check if the public_bind_addr option is set
  if (!g_conf()->public_bind_addr.is_blank_ip()) {
    bind_addrs = make_mon_addrs(g_conf()->public_bind_addr);
  }

  dout(0) << "starting " << g_conf()->name << " rank " << rank
	  << " at public addrs " << public_addrs
	  << " at bind addrs " << bind_addrs
	  << " mon_data " << g_conf()->mon_data
	  << " fsid " << monmap.get_fsid()
	  << dendl;

  Messenger *mgr_msgr = Messenger::create(g_ceph_context, public_msgr_type,
					  entity_name_t::MON(rank), "mon-mgrc",
					  getpid(), 0);
  if (!mgr_msgr) {
    derr << "unable to create mgr_msgr" << dendl;
    prefork.exit(1);
  }

  mon = new Monitor(g_ceph_context, g_conf()->name.get_id(), store,
		    msgr, mgr_msgr, &monmap);

  mon->orig_argc = argc;
  mon->orig_argv = argv;

  if (force_sync) {
    derr << "flagging a forced sync ..." << dendl;
    ostringstream oss;
    mon->sync_force(NULL, oss);
    if (oss.tellp())
      derr << oss.str() << dendl;
  }

  err = mon->preinit();
  if (err < 0) {
    derr << "failed to initialize" << dendl;
    prefork.exit(1);
  }

  if (compact || g_conf()->mon_compact_on_start) {
    derr << "compacting monitor store ..." << dendl;
    mon->store->compact();
    derr << "done compacting" << dendl;
  }

  // bind
  err = msgr->bindv(bind_addrs);
  if (err < 0) {
    derr << "unable to bind monitor to " << bind_addrs << dendl;
    prefork.exit(1);
  }

  // if the public and bind addr are different set the msgr addr
  // to the public one, now that the bind is complete.
  if (public_addrs != bind_addrs) {
    msgr->set_addrs(public_addrs);
  }

  if (g_conf()->daemonize) {
    global_init_postfork_finish(g_ceph_context);
    prefork.daemonize();
  }

  msgr->start();
  mgr_msgr->start();

  mon->init();

  register_async_signal_handler_oneshot(SIGINT, handle_mon_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_mon_signal);

  if (g_conf()->inject_early_sigterm)
    kill(getpid(), SIGTERM);

  msgr->wait();
  mgr_msgr->wait();

  store->close();

  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGINT, handle_mon_signal);
  unregister_async_signal_handler(SIGTERM, handle_mon_signal);
  shutdown_async_signal_handler();

  delete mon;
  delete store;
  delete msgr;
  delete mgr_msgr;
  delete client_throttler;
  delete daemon_throttler;

  // cd on exit, so that gmon.out (if any) goes into a separate directory for each node.
  char s[20];
  snprintf(s, sizeof(s), "gmon/%d", getpid());
  if ((mkdir(s, 0755) == 0) && (chdir(s) == 0)) {
    dout(0) << "ceph-mon: gmon.out should be in " << s << dendl;
  }

  prefork.signal_exit(0);
  return 0;
}
