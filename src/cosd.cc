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

#include <sys/stat.h>
#include <iostream>
#include <string>
using namespace std;

#include "osd/OSD.h"

#include "common/config.h"

#include "mon/MonMap.h"
#include "mon/MonClient.h"


#include "msg/SimpleMessenger.h"

#include "common/Timer.h"
#include "common/common_init.h"
#include "common/ceph_argparse.h"

#include "include/color.h"
#include "common/errno.h"

#include "perfglue/heap_profiler.h"

void usage() 
{
  derr << "usage: cosd -i osdid [--osd-data=path] [--osd-journal=path] "
       << "[--mkfs] [--mkjournal]" << dendl;
  derr << "   --debug_osd N   set debug level (e.g. 10)" << dendl;
  generic_server_usage();
}

int main(int argc, const char **argv) 
{
  DEFINE_CONF_VARS(usage);
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  vector<const char *>::iterator args_iter;

  common_init(args, CEPH_ENTITY_TYPE_OSD, CODE_ENVIRONMENT_DAEMON, 0);
  ceph_heap_profiler_init();

  // osd specific args
  bool mkfs = false;
  bool mkjournal = false;
  bool mkkey = false;
  bool flushjournal = false;
  char *dump_pg_log = 0;
  FOR_EACH_ARG(args) {
    if (CEPH_ARGPARSE_EQ("mkfs", '\0')) {
      mkfs = true;
    } else if (CEPH_ARGPARSE_EQ("mkjournal", '\0')) {
      mkjournal = true;
    } else if (CEPH_ARGPARSE_EQ("mkkey", '\0')) {
      mkkey = true;
    } else if (CEPH_ARGPARSE_EQ("flush-journal", '\0')) {
      flushjournal = true;
    } else if (CEPH_ARGPARSE_EQ("dump-pg-log", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&dump_pg_log, OPT_STR);
    } else {
      derr << "unrecognized arg " << args[i] << dendl;
      usage();
    }
  }

  if (dump_pg_log) {
    common_init_finish(&g_ceph_context);
    bufferlist bl;
    int r = bl.read_file(dump_pg_log);
    if (r >= 0) {
      PG::Log::Entry e;
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
      derr << "unable to open " << dump_pg_log << ": " << cpp_strerror(r) << dendl;
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
    common_init_finish(&g_ceph_context);
    MonClient mc(&g_ceph_context);
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
    *_dout << " for osd" << whoami << " fsid " << mc.monmap.fsid << dendl;
  }
  if (mkkey) {
    common_init_finish(&g_ceph_context);
    KeyRing *keyring = KeyRing::create_empty();
    if (!keyring) {
      derr << "Unable to get a Ceph keyring." << dendl;
      return 1;
    }
    EntityName ename(g_conf->name);
    EntityAuth eauth;
    eauth.key.create(CEPH_CRYPTO_AES);
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
  if (mkfs || mkkey)
    exit(0);
  if (mkjournal) {
    common_init_finish(&g_ceph_context);
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
    common_init_finish(&g_ceph_context);
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
  
  string magic;
  ceph_fsid_t fsid;
  int w;
  int r = OSD::peek_meta(g_conf->osd_data, magic, fsid, w);
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

  bool client_addr_set = !g_conf->public_addr.is_blank_addr();
  bool cluster_addr_set = !g_conf->cluster_addr.is_blank_addr();

  if (cluster_addr_set && !client_addr_set) {
    derr << TEXT_RED << " ** "
         << "WARNING: set cluster address but not client address!" << " **\n"
         << "using cluster address for clients" << TEXT_NORMAL << dendl;
    g_conf->public_addr = g_conf->cluster_addr;
    client_addr_set = true;
    cluster_addr_set = false;
  }

  SimpleMessenger *client_messenger = new SimpleMessenger(&g_ceph_context);
  SimpleMessenger *cluster_messenger = new SimpleMessenger(&g_ceph_context);
  SimpleMessenger *messenger_hb = new SimpleMessenger(&g_ceph_context);

  client_messenger->bind(g_conf->public_addr, getpid());
  cluster_messenger->bind(g_conf->cluster_addr, getpid());

  // hb should bind to same ip as cluster_addr (if specified)
  entity_addr_t hb_addr = g_conf->cluster_addr;
  if (!hb_addr.is_blank_addr())
    hb_addr.set_port(0);
  messenger_hb->bind(hb_addr, getpid());

  cout << "starting osd" << whoami
       << " at " << client_messenger->get_ms_addr() 
       << " osd_data " << g_conf->osd_data
       << " " << ((g_conf->osd_journal.empty()) ?
		    "(no journal)" : g_conf->osd_journal)
       << std::endl;

  client_messenger->register_entity(entity_name_t::OSD(whoami));
  cluster_messenger->register_entity(entity_name_t::OSD(whoami));
  messenger_hb->register_entity(entity_name_t::OSD(whoami));

  Throttle client_throttler(g_conf->osd_client_message_size_cap);

  uint64_t supported =
    CEPH_FEATURE_UID | 
    CEPH_FEATURE_NOSRCADDR;

  client_messenger->set_default_policy(SimpleMessenger::Policy::stateless_server(supported, 0));
  client_messenger->set_policy(entity_name_t::TYPE_CLIENT,
			       SimpleMessenger::Policy::stateless_server(supported, 0));
  client_messenger->set_policy_throttler(entity_name_t::TYPE_CLIENT, &client_throttler);
  client_messenger->set_policy(entity_name_t::TYPE_MON,
                               SimpleMessenger::Policy::client(supported,
                                                               CEPH_FEATURE_UID));
  //try to poison pill any OSD connections on the wrong address
  client_messenger->set_policy(entity_name_t::TYPE_OSD,
			       SimpleMessenger::Policy::stateless_server(0,0));

  cluster_messenger->set_default_policy(SimpleMessenger::Policy::stateless_server(0, 0));
  cluster_messenger->set_policy(entity_name_t::TYPE_MON, SimpleMessenger::Policy::client(0,0));
  cluster_messenger->set_policy(entity_name_t::TYPE_OSD,
				SimpleMessenger::Policy::lossless_peer(supported, CEPH_FEATURE_UID));
  cluster_messenger->set_policy(entity_name_t::TYPE_CLIENT,
				SimpleMessenger::Policy::stateless_server(0, 0));

  // Set up crypto, daemonize, etc.
  // Leave stderr open in case we need to report errors.
  common_init_daemonize(&g_ceph_context, CINIT_FLAG_NO_CLOSE_STDERR);
  common_init_finish(&g_ceph_context);
  MonClient mc(&g_ceph_context);
  if (mc.build_initial_monmap() < 0)
    return -1;
  common_init_chdir(&g_ceph_context);

  OSD *osd = new OSD(whoami, cluster_messenger, client_messenger, messenger_hb, &mc,
		     g_conf->osd_data, g_conf->osd_journal);
  int err = osd->pre_init();
  if (err < 0) {
    derr << TEXT_RED << " ** ERROR: initializing osd failed: " << cpp_strerror(-err)
	 << TEXT_NORMAL << dendl;
    return 1;
  }

  // Now close the standard file descriptors
  common_init_shutdown_stderr();

  client_messenger->start();
  messenger_hb->start();
  cluster_messenger->start();

  // start osd
  err = osd->init();
  if (err < 0) {
    derr << TEXT_RED << " ** ERROR: initializing osd failed: " << cpp_strerror(-err)
         << TEXT_NORMAL << dendl;
    return 1;
  }

  client_messenger->wait();
  messenger_hb->wait();
  cluster_messenger->wait();

  // done
  delete osd;
  client_messenger->destroy();
  messenger_hb->destroy();
  cluster_messenger->destroy();

  // cd on exit, so that gmon.out (if any) goes into a separate directory for each node.
  char s[20];
  snprintf(s, sizeof(s), "gmon/%d", getpid());
  if ((mkdir(s, 0755) == 0) && (chdir(s) == 0)) {
    dout(0) << "cosd: gmon.out should be in " << s << dendl;
  }

  return 0;
}
