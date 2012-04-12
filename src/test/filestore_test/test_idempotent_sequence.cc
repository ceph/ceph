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

#include "common/config.h"

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

  std::cout << "\
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
" << std::endl;
}

const char *our_name = NULL;
int seed = 0, num_txs = 100, num_colls = 30, num_objs = 0;
bool is_seed_set = false;
int verify_at = 0;
std::string status_file;

bool diff_attrs(std::map<std::string,bufferptr>& verify,
    std::map<std::string,bufferptr>& store)
{
  std::map<std::string, bufferptr>::iterator v_it = verify.begin();
  std::map<std::string, bufferptr>::iterator s_it = store.begin();
  for (; v_it != verify.end(); ++v_it, ++s_it) {
    if (v_it->first != s_it->first) {
      dout(0) << "diff_attrs name mismatch (verify: " << v_it->first
          << ", store: " << s_it->first << ")" << dendl;
      return false;
    }

    if (!v_it->second.cmp(s_it->second)) {
      dout(0) << "diff_attrs contents mismatch on attr " << v_it->first << dendl;
      return false;
    }
  }
  return true;
}

bool diff_objects_stat(struct stat& a, struct stat& b)
{
  if (a.st_uid != b.st_uid) {
    dout(0) << "diff_objects_stat uid mismatch ("
        << a.st_uid << " != " << b.st_uid << ")" << dendl;
    return false;
  }

  if (a.st_gid != b.st_gid) {
    dout(0) << "diff_objects_stat gid mismatch ("
        << a.st_gid << " != " << b.st_gid << ")" << dendl;
    return false;
  }

  if (a.st_mode != b.st_mode) {
    dout(0) << "diff_objects_stat mode mismatch ("
        << a.st_mode << " != " << b.st_mode << ")" << dendl;
    return false;
  }

  if (a.st_nlink != b.st_nlink) {
    dout(0) << "diff_objects_stat nlink mismatch ("
        << a.st_nlink << " != " << b.st_nlink << ")" << dendl;
    return false;
  }

  if (a.st_size != b.st_size) {
    dout(0) << "diff_objects_stat size mismatch ("
        << a.st_size << " != " << b.st_size << ")" << dendl;
    return false;
  }
  return true;
}

bool diff_objects(FileStore *store, FileStore *verify, coll_t coll)
{
  int err;
  std::vector<hobject_t> verify_objects, store_objects;
  err = verify->collection_list(coll, verify_objects);
  if (err < 0) {
    dout(0) << "diff_objects list on verify coll " << coll.to_str()
        << " returns " << err << dendl;
    return false;
  }
  err = store->collection_list(coll, store_objects);
  if (err < 0) {
    dout(0) << "diff_objects list on store coll " << coll.to_str()
              << " returns " << err << dendl;
    return false;
  }

  if (verify_objects.size() != store_objects.size()) {
    dout(0) << "diff_objects size mismatch (verify: " << verify_objects.size()
        << ", store: " << store_objects.size() << ")" << dendl;
    return false;
  }

  std::vector<hobject_t>::iterator v_it = verify_objects.begin();
  std::vector<hobject_t>::iterator s_it = verify_objects.begin();
  for (; v_it != verify_objects.end(); ++v_it, ++s_it) {
    hobject_t v_obj = *v_it, s_obj = *s_it;
    if (v_obj.oid.name != s_obj.oid.name) {
      dout(0) << "diff_objects name mismatch (verify: "<< v_obj.oid.name
          << ", store: " << s_obj.oid.name << ")" << dendl;
      return false;
    }

    struct stat v_stat, s_stat;
    err = verify->stat(coll, v_obj, &v_stat);
    if (err < 0) {
      dout(0) << "diff_objects error stating B object " << v_obj.oid.name << dendl;
      return false;
    }
    err = store->stat(coll, s_obj, &s_stat);
    if (err < 0) {
      dout(0) << "diff_objects error stating A object " << s_obj.oid.name << dendl;
      return false;
    }

    if (diff_objects_stat(v_stat, s_stat)) {
      dout(0) << "diff_objects stat mismatch on " << coll << "/" << v_obj << dendl;
      return false;
    }

    bufferlist s_bl, v_bl;
    verify->read(coll, v_obj, 0, v_stat.st_size, v_bl);
    store->read(coll, s_obj, 0, s_stat.st_size, s_bl);

    if (!s_bl.contents_equal(v_bl)) {
      dout(0) << "diff_objects content mismatch on " << coll << "/" << v_obj << dendl;
      return false;
    }

    std::map<std::string, bufferptr> s_attrs_map, v_attrs_map;
    err = store->getattrs(coll, s_obj, s_attrs_map);
    if (err < 0) {
      dout(0) << "diff_objects getattrs on A object " << coll << "/" << s_obj
	      << "returns " << err << dendl;
      return false;
    }
    err = verify->getattrs(coll, v_obj, v_attrs_map);
    if (err < 0) {
      dout(0) << "diff_objects getattrs on B object " << coll << "/" << v_obj
	      << "returns " << err << dendl;
      return false;
    }

    if (!diff_attrs(v_attrs_map, s_attrs_map)) {
      dout(0) << "diff_objects attrs mismatch" << dendl;
      return false;
    }
  }

  return true;
}

bool diff_coll_attrs(FileStore *store, FileStore *verify, coll_t coll)
{
  int err;
  std::map<std::string, bufferptr> verify_coll_attrs, store_coll_attrs;
  err = verify->collection_getattrs(coll, verify_coll_attrs);
  if (err < 0) {
    dout(0) << "diff_attrs getattrs on verify coll " << coll.to_str()
        << "returns " << err << dendl;
    return false;
  }
  err = store->collection_getattrs(coll, store_coll_attrs);
  if (err < 0) {
    dout(0) << "diff_attrs getattrs on store coll " << coll.to_str()
              << "returns " << err << dendl;
    return false;
  }

  if (verify_coll_attrs.size() != store_coll_attrs.size()) {
    dout(0) << "diff_attrs size mismatch (verify: " << verify_coll_attrs.size()
        << ", store: " << store_coll_attrs.size() << ")" << dendl;
    return false;
  }

  return diff_attrs(verify_coll_attrs, store_coll_attrs);
}

bool diff(FileStore *store, FileStore *verify)
{
  bool ret = true;
  std::vector<coll_t> store_coll_list, verify_coll_list;;
  verify->list_collections(verify_coll_list);

  std::vector<coll_t>::iterator it = verify_coll_list.begin();
  for (; it != verify_coll_list.end(); ++it) {
    coll_t verify_coll = *it;
    if (!store->collection_exists(verify_coll)) {
      dout(0) << "diff collection " << verify_coll.to_str() << " DNE" << dendl;
      return false;
    }

    ret = diff_coll_attrs(store, verify, verify_coll);
    if (!ret)
      break;

    ret = diff_objects(store, verify, verify_coll);
    if (!ret)
      break;
  }
  return ret;
}

int run_diff(std::string& a_path, std::string& a_journal,
	      std::string& b_path, std::string& b_journal)
{
  if (!is_seed_set)
    seed = (int) time(NULL);

  FileStore *a = new FileStore(a_path, a_journal, "a");
  FileStore *b = new FileStore(b_path, b_journal, "b");

  int err;
  err = a->mount();
  ceph_assert(err == 0);
  err = b->mount();
  ceph_assert(err == 0);

  int ret = 0;
  if (diff(a, b)) {
    dout(0) << "diff looks okay!" << dendl;
  } else {
    dout(0) << "diff found an error." << dendl;
    ret = -1;
  }

  a->umount();
  b->umount();

  return ret;
}

int run_get_last_op(std::string& filestore_path, std::string& journal_path)
{
  FileStore *store = new FileStore(filestore_path, journal_path);

  int err = store->mount();
  if (err)
    return err;

  coll_t txn_coll("meta");
  hobject_t txn_object(sobject_t("txn", CEPH_NOSNAP));
  bufferlist bl;
  store->read(txn_coll, txn_object, 0, 100, bl);
  bufferlist::iterator p = bl.begin();
  int32_t txn;
  ::decode(txn, p);

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
      CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
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
