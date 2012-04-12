// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2012 New Dream Network
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <sstream>
#include <assert.h>
#include <time.h>
#include <stdlib.h>
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/debug.h"
#include "os/FileStore.h"

#include "DeterministicOpSequence.h"
#include "VerifyFileStore.h"

#include "common/config.h"

#define dout_subsys ceph_subsys_
#undef dout_prefix
#define dout_prefix *_dout << "test_idempotent_sequence "

void usage(const char *name) {
  if (name)
    cout << "usage: " << name << "[options]" << std::endl;

  cout << "\
\n\
Global Options:\n\
  -c FILE                             Read configuration from FILE\n\
  --osd-data PATH                     Set OSD Data path\n\
  --osd-journal PATH                  Set OSD Journal path\n\
  --osd-journal-size VAL              Set Journal size\n\
  --help                              This message\n\
\n\
Test-specific Options:\n\
    " << std::endl;
}

int main(int argc, const char *argv[]) {
  int err;
  vector<const char*> def_args;
  vector<const char*> args;
  const char *our_name = argv[0];

  def_args.push_back("--osd-journal-size");
  def_args.push_back("400");
  def_args.push_back("--osd-data");
  def_args.push_back("test_idempotent_data");
  def_args.push_back("--osd-journal");
  def_args.push_back("test_idempotent_journal");
  argv_to_vec(argc, argv, args);

  global_init(&def_args, args,
      CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);

  int seed = 0, num_txs = 100, num_colls = 30, num_objs = 0;
  bool is_seed_set = false;
  int verify_at = 0;
  std::string status_file;

  for (std::vector<const char*>::iterator i = args.begin(); i != args.end();) {
    string val;

    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-seed", (char*) NULL)) {
      seed = strtoll(val.c_str(), NULL, 10);
      is_seed_set = true;
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-num-txs", (char*) NULL)) {
      num_txs = strtoll(val.c_str(), NULL, 10);
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-num-colls", (char*) NULL)) {
      num_colls = strtoll(val.c_str(), NULL, 10);
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-num-objs", (char*) NULL)) {
      num_objs = strtoll(val.c_str(), NULL, 10);
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-verify-at", (char*) NULL)) {
      verify_at = strtoll(val.c_str(), NULL, 10);
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-status-file", (char*) NULL)) {
      status_file = val;
    } else if (ceph_argparse_flag(args, i, "--help", (char*) NULL)) {
      usage(our_name);
      exit(0);
    }
  }

  FileStore *store = new FileStore(g_conf->osd_data, g_conf->osd_journal);

  if (!is_seed_set)
    seed = (int) time(NULL);

  dout(0) << "running with "
      << " --test-seed " << seed
      << " --test-num-txs" << num_txs
      << " --test-num-colls " << num_colls
      << " --test-num-objs " << num_objs
      << " --filestore-kill-at " << (g_conf->filestore_kill_at)
      << " --test-verify-at " << verify_at << dendl;

  if (verify_at > 0) {
    std::ostringstream ss;
    ss << g_conf->osd_data << ".verify";
    std::string verify_osd_data = ss.str();
    ss.str("");
    ss << g_conf->osd_journal << ".verify";
    std::string verify_osd_journal = ss.str();

    FileStore *verify_store =
        new FileStore(verify_osd_data, verify_osd_journal);

    ::mkdir(verify_osd_data.c_str(), 0755);
    int err;
    err = verify_store->mkfs();
    ceph_assert(err == 0);
    err = verify_store->mount();
    ceph_assert(err == 0);

    VerifyFileStore verify(store, verify_store, status_file);
    verify.init(num_colls, num_objs);
    verify.generate(seed, verify_at);

    verify_store->umount();

  } else {
    ::mkdir(g_conf->osd_data.c_str(), 0755);
    err = store->mkfs();
    ceph_assert(err == 0);
    err = store->mount();
    ceph_assert(err == 0);

    DeterministicOpSequence op_sequence(store, status_file);
    op_sequence.init(num_colls, num_objs);
    op_sequence.generate(seed, num_txs);
    store->umount();
  }
  return 0;
}
