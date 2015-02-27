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
#include <time.h>
#include <stdlib.h>
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/debug.h"
#include "os/FileStore.h"

#include "DeterministicOpSequence.h"
#include "FileStoreDiff.h"

#include "common/config.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_
#undef dout_prefix
#define dout_prefix *_dout << "test_idempotent_sequence "

void usage(const char *name, std::string command = "") {
  assert(name != NULL);

  std::string more = "cmd <args...>";
  std::string diff = "diff <filestoreA> <journalA> <filestoreB> <journalB>";
  std::string get_last_op = "get-last-op <filestore> <journal>";
  std::string run_seq_to = "run-sequence-to <num-ops> <filestore> <journal>";

  if (!command.empty()) {
    if (command == "diff")
      more = diff;
    else if (command == "get-last-op")
      more = get_last_op;
    else if (command == "run-sequence-to")
      more = run_seq_to;
  }
  std::cout << "usage: " << name << " " << more << " [options]" << std::endl;

  std::cout << "\n\
Commands:\n\
  " << diff << "\n\
  " << get_last_op << "\n\
  " << run_seq_to << "\n\
\n\
Global Options:\n\
  -c FILE                             Read configuration from FILE\n\
  --osd-data PATH                     Set OSD Data path\n\
  --osd-journal PATH                  Set OSD Journal path\n\
  --osd-journal-size VAL              Set Journal size\n\
  --help                              This message\n\
\n\
Test-specific Options:\n\
  --test-seed VAL                     Seed to run the test\n\
  --test-status-file PATH             Path to keep the status file\n\
  --test-num-colls VAL                Number of collections to create on init\n\
  --test-num-objs VAL                 Number of objects to create on init\n\
" << std::endl;
}

const char *our_name = NULL;
int seed = 0, num_txs = 100, num_colls = 30, num_objs = 0;
bool is_seed_set = false;
int verify_at = 0;
std::string status_file;

int run_diff(std::string& a_path, std::string& a_journal,
	      std::string& b_path, std::string& b_journal)
{
  FileStore *a = new FileStore(a_path, a_journal, 0, "a");
  FileStore *b = new FileStore(b_path, b_journal, 0, "b");

  int ret = 0;
  {
    FileStoreDiff fsd(a, b);
    if (fsd.diff()) {
      dout(0) << "diff found an difference" << dendl;
      ret = -1;
    } else {
      dout(0) << "no diff" << dendl;
    }
  }

  delete a;
  delete b;
  return ret;
}

int run_get_last_op(std::string& filestore_path, std::string& journal_path)
{
  FileStore *store = new FileStore(filestore_path, journal_path);

  int err = store->mount();
  if (err) {
    store->umount();
    delete store;
    return err;
  }

  coll_t txn_coll("meta");
  hobject_t txn_object(sobject_t("txn", CEPH_NOSNAP));
  bufferlist bl;
  store->read(txn_coll, txn_object, 0, 100, bl);
  int32_t txn = 0;
  if (bl.length()) {
    bufferlist::iterator p = bl.begin();
    ::decode(txn, p);
  }

  store->umount();
  delete store;

  cout << txn << std::endl;
  return 0;
}

int run_sequence_to(int val, std::string& filestore_path,
		     std::string& journal_path)
{
  num_txs = val;

  if (!is_seed_set)
    seed = (int) time(NULL);

  FileStore *store = new FileStore(filestore_path, journal_path);

  int err;

  // mkfs iff directory dne
  err = ::mkdir(filestore_path.c_str(), 0755);
  if (err) {
    cerr << filestore_path << " already exists" << std::endl;
    store->umount();
    delete store;
    return err;
  }
  
  err = store->mkfs();
  ceph_assert(err == 0);

  err = store->mount();
  ceph_assert(err == 0);

  DeterministicOpSequence op_sequence(store, status_file);
  op_sequence.init(num_colls, num_objs);
  op_sequence.generate(seed, num_txs);
  store->umount();
  return 0;
}

int run_command(std::string& command, std::vector<std::string>& args)
{
  if (command.empty()) {
    usage(our_name);
    exit(0);
  }

  /* We'll have a class that will handle the options, the command
   * and its arguments. For the time being, and so we can move on, let's
   * tolerate this big, ugly code.
   */
  if (command == "diff") {
    /* expect 4 arguments: (filestore path + journal path)*2 */
    if (args.size() == 4) {
      return run_diff(args[0], args[1], args[2], args[3]);
    }
  } else if (command == "get-last-op") {
    /* expect 2 arguments: a filestore path + journal */
    if (args.size() == 2) {
      return run_get_last_op(args[0], args[1]);
    }
  } else if (command == "run-sequence-to") {
    /* expect 3 arguments: # of operations and a filestore path + journal. */
    if (args.size() == 3) {
      return run_sequence_to(strtoll(args[0].c_str(), NULL, 10), args[1], args[2]);
    }
  } else {
    std::cout << "unknown command " << command << std::endl;
    usage(our_name);
    exit(1);
  }

  usage(our_name, command);
  exit(1);
}

int main(int argc, const char *argv[])
{
  vector<const char*> def_args;
  vector<const char*> args;
  our_name = argv[0];
  argv_to_vec(argc, argv, args);

  global_init(&def_args, args,
	      CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
	      CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);

  std::string command;
  std::vector<std::string> command_args;

  for (std::vector<const char*>::iterator i = args.begin(); i != args.end();) {
    string val;

    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-seed", (char*) NULL)) {
      seed = strtoll(val.c_str(), NULL, 10);
      is_seed_set = true;
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-num-colls", (char*) NULL)) {
      num_colls = strtoll(val.c_str(), NULL, 10);
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-num-objs", (char*) NULL)) {
      num_objs = strtoll(val.c_str(), NULL, 10);
    } else if (ceph_argparse_witharg(args, i, &val,
        "--test-status-file", (char*) NULL)) {
      status_file = val;
    } else if (ceph_argparse_flag(args, i, "--help", (char*) NULL)) {
      usage(our_name);
      exit(0);
    } else {
      if (command.empty())
        command = *i++;
      else
        command_args.push_back(string(*i++));
    }
  }

  int ret = run_command(command, command_args);

  return ret;
}
