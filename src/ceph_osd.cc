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
#include <uuid/uuid.h>
#include <boost/scoped_ptr.hpp>

#include <iostream>
#include <string>
using namespace std;

#include "osd/OSD.h"
#include "os/FileStore.h"
#include "mon/MonClient.h"
#include "include/ceph_features.h"

#include "common/config.h"

#include "mon/MonMap.h"


#include "msg/Messenger.h"

#include "common/Timer.h"
#include "common/ceph_argparse.h"

#include "global/global_init.h"
#include "global/signal_handler.h"

#include "include/color.h"
#include "common/errno.h"
#include "common/pick_address.h"

#include "perfglue/heap_profiler.h"

#include "include/assert.h"

#define dout_subsys ceph_subsys_osd

OSD *osd = NULL;

void handle_osd_signal(int signum)
{
  if (osd)
    osd->handle_signal(signum);
}

void usage() 
{
  derr << "usage: ceph-osd -i osdid [--osd-data=path] [--osd-journal=path] "
       << "[--mkfs] [--mkjournal] [--convert-filestore]" << dendl;
  derr << "   --debug_osd N   set debug level (e.g. 10)" << dendl;
  generic_server_usage();
}

int main(int argc, const char **argv) 
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_OSD, CODE_ENVIRONMENT_DAEMON, 0);
  ceph_heap_profiler_init();

  // osd specific args
  bool mkfs = false;
  bool mkjournal = false;
  bool mkkey = false;
  bool flushjournal = false;
  bool dump_journal = false;
  bool convertfilestore = false;
  bool get_journal_fsid = false;
  bool get_osd_fsid = false;
  bool get_cluster_fsid = false;
  std::string dump_pg_log;

  std::string val;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      exit(0);
    } else if (ceph_argparse_flag(args, i, "--mkfs", (char*)NULL)) {
      mkfs = true;
    } else if (ceph_argparse_flag(args, i, "--mkjournal", (char*)NULL)) {
      mkjournal = true;
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
    } else {
      ++i;
    }
  }
  if (!args.empty()) {
    derr << "unrecognized arg " << args[0] << dendl;
    usage();
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

  if (mkfs) {
    common_init_finish(g_ceph_context);
    MonClient mc(g_ceph_context);
    if (mc.build_initial_monmap() < 0)
      return -1;
    if (mc.get_monmap_privately() < 0)
      return -1;

    int err = OSD::mkfs(g_conf->osd_data, g_conf->osd_journal, mc.monmap.fsid, whoami);
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error creating empty object store in "
	   << g_conf->osd_data << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      exit(1);
    }
    derr << "created object store " << g_conf->osd_data;
    if (!g_conf->osd_journal.empty())
      *_dout << " journal " << g_conf->osd_journal;
    *_dout << " for osd." << whoami << " fsid " << mc.monmap.fsid << dendl;
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
    int err = OSD::mkjournal(g_conf->osd_data, g_conf->osd_journal);
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
  if (flushjournal) {
    common_init_finish(g_ceph_context);
    int err = OSD::flushjournal(g_conf->osd_data, g_conf->osd_journal);
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error flushing journal " << g_conf->osd_journal
	   << " for object store " << g_conf->osd_data
	   << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      exit(1);
    }
    derr << "flushed journal " << g_conf->osd_journal
	 << " for object store " << g_conf->osd_data
	 << dendl;
    exit(0);
  }
  if (dump_journal) {
    common_init_finish(g_ceph_context);
    int err = OSD::dump_journal(g_conf->osd_data, g_conf->osd_journal, cout);
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
    int err = OSD::convertfs(g_conf->osd_data, g_conf->osd_journal);
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error converting store " << g_conf->osd_data
	   << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      exit(1);
    }
    exit(0);
  }
  
  if (get_journal_fsid) {
    uuid_d fsid;
    int r = OSD::peek_journal_fsid(g_conf->osd_journal, fsid);
    if (r == 0)
      cout << fsid << std::endl;
    exit(r);
  }

  string magic;
  uuid_d cluster_fsid, osd_fsid;
  int w;
  int r = OSD::peek_meta(g_conf->osd_data, magic, cluster_fsid, osd_fsid, w);
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

  pick_addresses(g_ceph_context);

  if (g_conf->public_addr.is_blank_ip() && !g_conf->cluster_addr.is_blank_ip()) {
    derr << TEXT_YELLOW
	 << " ** WARNING: specified cluster addr but not public addr; we recommend **\n"
	 << " **          you specify neither or both.                             **"
	 << TEXT_NORMAL << dendl;
  }

  Messenger *client_messenger = Messenger::create(g_ceph_context,
						  entity_name_t::OSD(whoami), "client",
						  getpid());
  Messenger *cluster_messenger = Messenger::create(g_ceph_context,
						   entity_name_t::OSD(whoami), "cluster",
						   getpid());
  Messenger *messenger_hbclient = Messenger::create(g_ceph_context,
						    entity_name_t::OSD(whoami), "hbclient",
						    getpid());
  Messenger *messenger_hbserver = Messenger::create(g_ceph_context,
						    entity_name_t::OSD(whoami), "hbserver",
						    getpid());
  cluster_messenger->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  messenger_hbclient->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  messenger_hbserver->set_cluster_protocol(CEPH_OSD_PROTOCOL);

  cout << "starting osd." << whoami
       << " at " << client_messenger->get_myaddr()
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
    CEPH_FEATURE_MSG_AUTH;

  client_messenger->set_default_policy(Messenger::Policy::stateless_server(supported, 0));
  client_messenger->set_policy_throttlers(entity_name_t::TYPE_CLIENT,
					  client_byte_throttler.get(),
					  client_msg_throttler.get());
  client_messenger->set_policy(entity_name_t::TYPE_MON,
                               Messenger::Policy::lossy_client(supported,
							       CEPH_FEATURE_UID |
							       CEPH_FEATURE_PGID64 |
							       CEPH_FEATURE_OSDENC));
  //try to poison pill any OSD connections on the wrong address
  client_messenger->set_policy(entity_name_t::TYPE_OSD,
			       Messenger::Policy::stateless_server(0,0));
  
  cluster_messenger->set_default_policy(Messenger::Policy::stateless_server(0, 0));
  cluster_messenger->set_policy(entity_name_t::TYPE_MON, Messenger::Policy::lossy_client(0,0));
  cluster_messenger->set_policy(entity_name_t::TYPE_OSD,
				Messenger::Policy::lossless_peer(supported,
								 CEPH_FEATURE_UID |
								 CEPH_FEATURE_PGID64 |
								 CEPH_FEATURE_OSDENC));
  cluster_messenger->set_policy(entity_name_t::TYPE_CLIENT,
				Messenger::Policy::stateless_server(0, 0));

  messenger_hbclient->set_policy(entity_name_t::TYPE_OSD,
			     Messenger::Policy::lossy_client(0, 0));
  messenger_hbserver->set_policy(entity_name_t::TYPE_OSD,
			     Messenger::Policy::stateless_server(0, 0));

  r = client_messenger->bind(g_conf->public_addr);
  if (r < 0)
    exit(1);
  r = cluster_messenger->bind(g_conf->cluster_addr);
  if (r < 0)
    exit(1);

  // hb should bind to same ip as cluster_addr (if specified)
  entity_addr_t hb_addr = g_conf->osd_heartbeat_addr;
  if (hb_addr.is_blank_ip()) {
    hb_addr = g_conf->cluster_addr;
    if (hb_addr.is_ip())
      hb_addr.set_port(0);
  }
  r = messenger_hbserver->bind(hb_addr);
  if (r < 0)
    exit(1);


  // Set up crypto, daemonize, etc.
  global_init_daemonize(g_ceph_context, 0);
  common_init_finish(g_ceph_context);

  if (g_conf->filestore_update_to >= (int)FileStore::on_disk_version) {
    int err = OSD::convertfs(g_conf->osd_data, g_conf->osd_journal);
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error converting store " << g_conf->osd_data
	   << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      exit(1);
    }
  }

  MonClient mc(g_ceph_context);
  if (mc.build_initial_monmap() < 0)
    return -1;
  global_init_chdir(g_ceph_context);

  osd = new OSD(whoami, cluster_messenger, client_messenger,
		messenger_hbclient, messenger_hbserver,
		&mc,
		g_conf->osd_data, g_conf->osd_journal);

  int err = osd->pre_init();
  if (err < 0) {
    derr << TEXT_RED << " ** ERROR: osd pre_init failed: " << cpp_strerror(-err)
	 << TEXT_NORMAL << dendl;
    return 1;
  }

  // Now close the standard file descriptors
  global_init_shutdown_stderr(g_ceph_context);

  client_messenger->start();
  messenger_hbclient->start();
  messenger_hbserver->start();
  cluster_messenger->start();

  // install signal handlers
  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);
  register_async_signal_handler_oneshot(SIGINT, handle_osd_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_osd_signal);

  // start osd
  err = osd->init();
  if (err < 0) {
    derr << TEXT_RED << " ** ERROR: osd init failed: " << cpp_strerror(-err)
         << TEXT_NORMAL << dendl;
    return 1;
  }

  client_messenger->wait();
  messenger_hbclient->wait();
  messenger_hbserver->wait();
  cluster_messenger->wait();

  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGINT, handle_osd_signal);
  unregister_async_signal_handler(SIGTERM, handle_osd_signal);

  // done
  delete osd;
  delete client_messenger;
  delete messenger_hbclient;
  delete messenger_hbserver;
  delete cluster_messenger;
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
