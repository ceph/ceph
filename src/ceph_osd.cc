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
#include <boost/scoped_ptr.hpp>

#include <iostream>
#include <string>
using namespace std;

#include "osd/OSD.h"
#include "os/ObjectStore.h"
#include "mon/MonClient.h"
#include "include/ceph_features.h"

#include "common/config.h"

#include "mon/MonMap.h"

#include "msg/Messenger.h"

#include "common/Timer.h"
#include "common/TracepointProvider.h"
#include "common/ceph_argparse.h"

#include "global/global_init.h"
#include "global/signal_handler.h"

#include "include/color.h"
#include "common/errno.h"
#include "common/pick_address.h"

#include "perfglue/heap_profiler.h"

#include "include/assert.h"

#include "erasure-code/ErasureCodePlugin.h"

#define dout_subsys ceph_subsys_osd

namespace {

TracepointProvider::Traits osd_tracepoint_traits("libosd_tp.so",
                                                 "osd_tracing");
TracepointProvider::Traits os_tracepoint_traits("libos_tp.so",
                                                "osd_objectstore_tracing");

} // anonymous namespace

OSD *osd = NULL;

void handle_osd_signal(int signum)
{
  if (osd)
    osd->handle_signal(signum);
}

void usage() 
{
  cout << "usage: ceph-osd -i <osdid>\n"
       << "  --osd-data PATH data directory\n"
       << "  --osd-journal PATH\n"
       << "                    journal file or block device\n"
       << "  --mkfs            create a [new] data directory\n"
       << "  --convert-filestore\n"
       << "                    run any pending upgrade operations\n"
       << "  --flush-journal   flush all data out of journal\n"
       << "  --mkjournal       initialize a new journal\n"
       << "  --check-wants-journal\n"
       << "                    check whether a journal is desired\n"
       << "  --check-allows-journal\n"
       << "                    check whether a journal is allowed\n"
       << "  --check-needs-journal\n"
       << "                    check whether a journal is required\n"
       << "  --debug_osd <N>   set debug level (e.g. 10)\n"
       << "  --get-device-fsid PATH\n"
       << "                    get OSD fsid for the given block device\n"
       << std::endl;
  generic_server_usage();
}

int preload_erasure_code()
{
  string plugins = g_conf->osd_erasure_code_plugins;
  stringstream ss;
  int r = ErasureCodePluginRegistry::instance().preload(
    plugins,
    g_conf->erasure_code_dir,
    &ss);
  if (r)
    derr << ss.str() << dendl;
  else
    dout(10) << ss.str() << dendl;
  return r;
}

int main(int argc, const char **argv) 
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  vector<const char*> def_args;
  // We want to enable leveldb's log, while allowing users to override this
  // option, therefore we will pass it as a default argument to global_init().
  def_args.push_back("--leveldb-log=");

  global_init(&def_args, args, CEPH_ENTITY_TYPE_OSD, CODE_ENVIRONMENT_DAEMON,
	      0, "osd_data");
  ceph_heap_profiler_init();

  // osd specific args
  bool mkfs = false;
  bool mkjournal = false;
  bool check_wants_journal = false;
  bool check_allows_journal = false;
  bool check_needs_journal = false;
  bool mkkey = false;
  bool flushjournal = false;
  bool dump_journal = false;
  bool convertfilestore = false;
  bool get_osd_fsid = false;
  bool get_cluster_fsid = false;
  bool get_journal_fsid = false;
  bool get_device_fsid = false;
  string device_path;
  std::string dump_pg_log;

  std::string val;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
    } else if (ceph_argparse_flag(args, i, "--mkfs", (char*)NULL)) {
      mkfs = true;
    } else if (ceph_argparse_flag(args, i, "--mkjournal", (char*)NULL)) {
      mkjournal = true;
    } else if (ceph_argparse_flag(args, i, "--check-allows-journal", (char*)NULL)) {
      check_allows_journal = true;
    } else if (ceph_argparse_flag(args, i, "--check-wants-journal", (char*)NULL)) {
      check_wants_journal = true;
    } else if (ceph_argparse_flag(args, i, "--check-needs-journal", (char*)NULL)) {
      check_needs_journal = true;
    } else if (ceph_argparse_flag(args, i, "--mkkey", (char*)NULL)) {
      mkkey = true;
    } else if (ceph_argparse_flag(args, i, "--flush-journal", (char*)NULL)) {
      flushjournal = true;
    } else if (ceph_argparse_flag(args, i, "--convert-filestore", (char*)NULL)) {
      convertfilestore = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--dump-pg-log", (char*)NULL)) {
      dump_pg_log = val;
    } else if (ceph_argparse_flag(args, i, "--dump-journal", (char*)NULL)) {
      dump_journal = true;
    } else if (ceph_argparse_flag(args, i, "--get-cluster-fsid", (char*)NULL)) {
      get_cluster_fsid = true;
    } else if (ceph_argparse_flag(args, i, "--get-osd-fsid", "--get-osd-uuid", (char*)NULL)) {
      get_osd_fsid = true;
    } else if (ceph_argparse_flag(args, i, "--get-journal-fsid", "--get-journal-uuid", (char*)NULL)) {
      get_journal_fsid = true;
    } else if (ceph_argparse_witharg(args, i, &device_path,
				     "--get-device-fsid", (char*)NULL)) {
      get_device_fsid = true;
    } else {
      ++i;
    }
  }
  if (!args.empty()) {
    derr << "unrecognized arg " << args[0] << dendl;
    usage();
  }

  if (get_journal_fsid) {
    device_path = g_conf->osd_journal;
    get_device_fsid = true;
  }
  if (get_device_fsid) {
    uuid_d uuid;
    int r = ObjectStore::probe_block_device_fsid(device_path, &uuid);
    if (r < 0) {
      cerr << "failed to get device fsid for " << device_path
	   << ": " << cpp_strerror(r) << std::endl;
      exit(1);
    }
    cout << uuid << std::endl;
    return 0;
  }

  if (!dump_pg_log.empty()) {
    common_init_finish(g_ceph_context);
    bufferlist bl;
    std::string error;
    int r = bl.read_file(dump_pg_log.c_str(), &error);
    if (r >= 0) {
      pg_log_entry_t e;
      bufferlist::iterator p = bl.begin();
      while (!p.end()) {
	uint64_t pos = p.get_off();
	try {
	  ::decode(e, p);
	}
	catch (const buffer::error &e) {
	  derr << "failed to decode LogEntry at offset " << pos << dendl;
	  return 1;
	}
	derr << pos << ":\t" << e << dendl;
      }
    } else {
      derr << "unable to open " << dump_pg_log << ": " << error << dendl;
    }
    return 0;
  }

  // whoami
  char *end;
  const char *id = g_conf->name.get_id().c_str();
  int whoami = strtol(id, &end, 10);
  if (*end || end == id || whoami < 0) {
    derr << "must specify '-i #' where # is the osd number" << dendl;
    usage();
  }

  if (g_conf->osd_data.empty()) {
    derr << "must specify '--osd-data=foo' data path" << dendl;
    usage();
  }

  // the store
  string store_type = g_conf->osd_objectstore;
  {
    char fn[PATH_MAX];
    snprintf(fn, sizeof(fn), "%s/type", g_conf->osd_data.c_str());
    int fd = ::open(fn, O_RDONLY);
    if (fd >= 0) {
      bufferlist bl;
      bl.read_fd(fd, 64);
      if (bl.length()) {
	store_type = string(bl.c_str(), bl.length() - 1);  // drop \n
	dout(5) << "object store type is " << store_type << dendl;
      }
      ::close(fd);
    }
  }
  ObjectStore *store = ObjectStore::create(g_ceph_context,
					   store_type,
					   g_conf->osd_data,
					   g_conf->osd_journal,
                                           g_conf->osd_os_flags);
  if (!store) {
    derr << "unable to create object store" << dendl;
    return -ENODEV;
  }

  if (mkfs) {
    common_init_finish(g_ceph_context);
    MonClient mc(g_ceph_context);
    if (mc.build_initial_monmap() < 0)
      return -1;
    if (mc.get_monmap_privately() < 0)
      return -1;

    int err = OSD::mkfs(g_ceph_context, store, g_conf->osd_data,
			mc.monmap.fsid, whoami);
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error creating empty object store in "
	   << g_conf->osd_data << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      exit(1);
    }
    derr << "created object store " << g_conf->osd_data
	 << " for osd." << whoami << " fsid " << mc.monmap.fsid << dendl;
  }
  if (mkkey) {
    common_init_finish(g_ceph_context);
    KeyRing *keyring = KeyRing::create_empty();
    if (!keyring) {
      derr << "Unable to get a Ceph keyring." << dendl;
      return 1;
    }

    EntityName ename(g_conf->name);
    EntityAuth eauth;

    int ret = keyring->load(g_ceph_context, g_conf->keyring);
    if (ret == 0 &&
	keyring->get_auth(ename, eauth)) {
      derr << "already have key in keyring " << g_conf->keyring << dendl;
    } else {
      eauth.key.create(g_ceph_context, CEPH_CRYPTO_AES);
      keyring->add(ename, eauth);
      bufferlist bl;
      keyring->encode_plaintext(bl);
      int r = bl.write_file(g_conf->keyring.c_str(), 0600);
      if (r)
	derr << TEXT_RED << " ** ERROR: writing new keyring to " << g_conf->keyring
	     << ": " << cpp_strerror(r) << TEXT_NORMAL << dendl;
      else
	derr << "created new key in keyring " << g_conf->keyring << dendl;
    }
  }
  if (mkfs || mkkey)
    exit(0);
  if (mkjournal) {
    common_init_finish(g_ceph_context);
    int err = store->mkjournal();
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error creating fresh journal " << g_conf->osd_journal
	   << " for object store " << g_conf->osd_data
	   << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      exit(1);
    }
    derr << "created new journal " << g_conf->osd_journal
	 << " for object store " << g_conf->osd_data << dendl;
    exit(0);
  }
  if (check_wants_journal) {
    if (store->wants_journal()) {
      cout << "yes" << std::endl;
      exit(0);
    } else {
      cout << "no" << std::endl;
      exit(1);
    }
  }
  if (check_allows_journal) {
    if (store->allows_journal()) {
      cout << "yes" << std::endl;
      exit(0);
    } else {
      cout << "no" << std::endl;
      exit(1);
    }
  }
  if (check_needs_journal) {
    if (store->needs_journal()) {
      cout << "yes" << std::endl;
      exit(0);
    } else {
      cout << "no" << std::endl;
      exit(1);
    }
  }
  if (flushjournal) {
    common_init_finish(g_ceph_context);
    int err = store->mount();
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error flushing journal " << g_conf->osd_journal
	   << " for object store " << g_conf->osd_data
	   << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      exit(1);
    }
    store->umount();
    derr << "flushed journal " << g_conf->osd_journal
	 << " for object store " << g_conf->osd_data
	 << dendl;
    exit(0);
  }
  if (dump_journal) {
    common_init_finish(g_ceph_context);
    int err = store->dump_journal(cout);
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error dumping journal " << g_conf->osd_journal
	   << " for object store " << g_conf->osd_data
	   << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      exit(1);
    }
    derr << "dumped journal " << g_conf->osd_journal
	 << " for object store " << g_conf->osd_data
	 << dendl;
    exit(0);

  }


  if (convertfilestore) {
    int err = store->mount();
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error mounting store " << g_conf->osd_data
	   << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      exit(1);
    }
    err = store->upgrade();
    store->umount();
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error converting store " << g_conf->osd_data
	   << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      exit(1);
    }
    exit(0);
  }
  
  string magic;
  uuid_d cluster_fsid, osd_fsid;
  int w;
  int r = OSD::peek_meta(store, magic, cluster_fsid, osd_fsid, w);
  if (r < 0) {
    derr << TEXT_RED << " ** ERROR: unable to open OSD superblock on "
	 << g_conf->osd_data << ": " << cpp_strerror(-r)
	 << TEXT_NORMAL << dendl;
    if (r == -ENOTSUP) {
      derr << TEXT_RED << " **        please verify that underlying storage "
	   << "supports xattrs" << TEXT_NORMAL << dendl;
    }
    exit(1);
  }
  if (w != whoami) {
    derr << "OSD id " << w << " != my id " << whoami << dendl;
    exit(1);
  }
  if (strcmp(magic.c_str(), CEPH_OSD_ONDISK_MAGIC)) {
    derr << "OSD magic " << magic << " != my " << CEPH_OSD_ONDISK_MAGIC
	 << dendl;
    exit(1);
  }

  if (get_cluster_fsid) {
    cout << cluster_fsid << std::endl;
    exit(0);
  }
  if (get_osd_fsid) {
    cout << osd_fsid << std::endl;
    exit(0);
  }

  pick_addresses(g_ceph_context, CEPH_PICK_ADDRESS_PUBLIC
                                |CEPH_PICK_ADDRESS_CLUSTER);

  if (g_conf->public_addr.is_blank_ip() && !g_conf->cluster_addr.is_blank_ip()) {
    derr << TEXT_YELLOW
	 << " ** WARNING: specified cluster addr but not public addr; we recommend **\n"
	 << " **          you specify neither or both.                             **"
	 << TEXT_NORMAL << dendl;
  }

  Messenger *ms_public = Messenger::create(g_ceph_context, g_conf->ms_type,
					   entity_name_t::OSD(whoami), "client",
					   getpid());
  Messenger *ms_cluster = Messenger::create(g_ceph_context, g_conf->ms_type,
					    entity_name_t::OSD(whoami), "cluster",
					    getpid(), CEPH_FEATURES_ALL);
  Messenger *ms_hbclient = Messenger::create(g_ceph_context, g_conf->ms_type,
					     entity_name_t::OSD(whoami), "hbclient",
					     getpid());
  Messenger *ms_hb_back_server = Messenger::create(g_ceph_context, g_conf->ms_type,
						   entity_name_t::OSD(whoami), "hb_back_server",
						   getpid());
  Messenger *ms_hb_front_server = Messenger::create(g_ceph_context, g_conf->ms_type,
						    entity_name_t::OSD(whoami), "hb_front_server",
						    getpid());
  Messenger *ms_objecter = Messenger::create(g_ceph_context, g_conf->ms_type,
					     entity_name_t::OSD(whoami), "ms_objecter",
					     getpid());
  if (!ms_public || !ms_cluster || !ms_hbclient || !ms_hb_back_server || !ms_hb_front_server || !ms_objecter)
    exit(1);
  ms_cluster->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  ms_hbclient->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  ms_hb_back_server->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  ms_hb_front_server->set_cluster_protocol(CEPH_OSD_PROTOCOL);

  cout << "starting osd." << whoami
       << " at " << ms_public->get_myaddr()
       << " osd_data " << g_conf->osd_data
       << " " << ((g_conf->osd_journal.empty()) ?
		  "(no journal)" : g_conf->osd_journal)
       << std::endl;

  boost::scoped_ptr<Throttle> client_byte_throttler(
    new Throttle(g_ceph_context, "osd_client_bytes",
		 g_conf->osd_client_message_size_cap));
  boost::scoped_ptr<Throttle> client_msg_throttler(
    new Throttle(g_ceph_context, "osd_client_messages",
		 g_conf->osd_client_message_cap));

  uint64_t supported =
    CEPH_FEATURE_UID | 
    CEPH_FEATURE_NOSRCADDR |
    CEPH_FEATURE_PGID64 |
    CEPH_FEATURE_MSG_AUTH |
    CEPH_FEATURE_OSD_ERASURE_CODES;

  // All feature bits 0 - 34 should be present from dumpling v0.67 forward
  uint64_t osd_required =
    CEPH_FEATURE_UID |
    CEPH_FEATURE_PGID64 |
    CEPH_FEATURE_OSDENC |
    CEPH_FEATURE_OSD_SNAPMAPPER |
    CEPH_FEATURE_INDEP_PG_MAP |
    CEPH_FEATURE_OSD_PACKED_RECOVERY |
    CEPH_FEATURE_RECOVERY_RESERVATION |
    CEPH_FEATURE_BACKFILL_RESERVATION |
    CEPH_FEATURE_CHUNKY_SCRUB;

  ms_public->set_default_policy(Messenger::Policy::stateless_server(supported, 0));
  ms_public->set_policy_throttlers(entity_name_t::TYPE_CLIENT,
				   client_byte_throttler.get(),
				   client_msg_throttler.get());
  ms_public->set_policy(entity_name_t::TYPE_MON,
                               Messenger::Policy::lossy_client(supported,
							       CEPH_FEATURE_UID |
							       CEPH_FEATURE_PGID64 |
							       CEPH_FEATURE_OSDENC));
  //try to poison pill any OSD connections on the wrong address
  ms_public->set_policy(entity_name_t::TYPE_OSD,
			Messenger::Policy::stateless_server(0,0));

  ms_cluster->set_default_policy(Messenger::Policy::stateless_server(0, 0));
  ms_cluster->set_policy(entity_name_t::TYPE_MON, Messenger::Policy::lossy_client(0,0));
  ms_cluster->set_policy(entity_name_t::TYPE_OSD,
			 Messenger::Policy::lossless_peer(supported,
							  osd_required));
  ms_cluster->set_policy(entity_name_t::TYPE_CLIENT,
			 Messenger::Policy::stateless_server(0, 0));

  ms_hbclient->set_policy(entity_name_t::TYPE_OSD,
			  Messenger::Policy::lossy_client(0, 0));
  ms_hb_back_server->set_policy(entity_name_t::TYPE_OSD,
				Messenger::Policy::stateless_server(0, 0));
  ms_hb_front_server->set_policy(entity_name_t::TYPE_OSD,
				 Messenger::Policy::stateless_server(0, 0));

  ms_objecter->set_default_policy(Messenger::Policy::lossy_client(0, CEPH_FEATURE_OSDREPLYMUX));

  r = ms_public->bind(g_conf->public_addr);
  if (r < 0)
    exit(1);
  r = ms_cluster->bind(g_conf->cluster_addr);
  if (r < 0)
    exit(1);

  if (g_conf->osd_heartbeat_use_min_delay_socket) {
    ms_hbclient->set_socket_priority(SOCKET_PRIORITY_MIN_DELAY);
    ms_hb_back_server->set_socket_priority(SOCKET_PRIORITY_MIN_DELAY);
    ms_hb_front_server->set_socket_priority(SOCKET_PRIORITY_MIN_DELAY);
  }

  // hb back should bind to same ip as cluster_addr (if specified)
  entity_addr_t hb_back_addr = g_conf->osd_heartbeat_addr;
  if (hb_back_addr.is_blank_ip()) {
    hb_back_addr = g_conf->cluster_addr;
    if (hb_back_addr.is_ip())
      hb_back_addr.set_port(0);
  }
  r = ms_hb_back_server->bind(hb_back_addr);
  if (r < 0)
    exit(1);

  // hb front should bind to same ip as public_addr
  entity_addr_t hb_front_addr = g_conf->public_addr;
  if (hb_front_addr.is_ip())
    hb_front_addr.set_port(0);
  r = ms_hb_front_server->bind(hb_front_addr);
  if (r < 0)
    exit(1);

  // Set up crypto, daemonize, etc.
  global_init_daemonize(g_ceph_context);
  common_init_finish(g_ceph_context);

  TracepointProvider::initialize<osd_tracepoint_traits>(g_ceph_context);
  TracepointProvider::initialize<os_tracepoint_traits>(g_ceph_context);

  MonClient mc(g_ceph_context);
  if (mc.build_initial_monmap() < 0)
    return -1;
  global_init_chdir(g_ceph_context);

  if (preload_erasure_code() < 0)
    return -1;

  osd = new OSD(g_ceph_context,
                store,
                whoami,
                ms_cluster,
                ms_public,
                ms_hbclient,
                ms_hb_front_server,
                ms_hb_back_server,
                ms_objecter,
                &mc,
                g_conf->osd_data,
                g_conf->osd_journal);

  int err = osd->pre_init();
  if (err < 0) {
    derr << TEXT_RED << " ** ERROR: osd pre_init failed: " << cpp_strerror(-err)
	 << TEXT_NORMAL << dendl;
    return 1;
  }

  ms_public->start();
  ms_hbclient->start();
  ms_hb_front_server->start();
  ms_hb_back_server->start();
  ms_cluster->start();
  ms_objecter->start();

  // start osd
  err = osd->init();
  if (err < 0) {
    derr << TEXT_RED << " ** ERROR: osd init failed: " << cpp_strerror(-err)
         << TEXT_NORMAL << dendl;
    return 1;
  }

  // install signal handlers
  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);
  register_async_signal_handler_oneshot(SIGINT, handle_osd_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_osd_signal);

  osd->final_init();

  if (g_conf->inject_early_sigterm)
    kill(getpid(), SIGTERM);

  ms_public->wait();
  ms_hbclient->wait();
  ms_hb_front_server->wait();
  ms_hb_back_server->wait();
  ms_cluster->wait();
  ms_objecter->wait();

  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGINT, handle_osd_signal);
  unregister_async_signal_handler(SIGTERM, handle_osd_signal);
  shutdown_async_signal_handler();

  // done
  delete osd;
  delete ms_public;
  delete ms_hbclient;
  delete ms_hb_front_server;
  delete ms_hb_back_server;
  delete ms_cluster;
  delete ms_objecter;

  client_byte_throttler.reset();
  client_msg_throttler.reset();
  g_ceph_context->put();

  // cd on exit, so that gmon.out (if any) goes into a separate directory for each node.
  char s[20];
  snprintf(s, sizeof(s), "gmon/%d", getpid());
  if ((mkdir(s, 0755) == 0) && (chdir(s) == 0)) {
    dout(0) << "ceph-osd: gmon.out should be in " << s << dendl;
  }

  return 0;
}
