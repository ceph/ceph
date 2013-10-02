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

#include "os/LevelDBStore.h"

#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/config.h"
#include "common/strtol.h"

using namespace std;

class StoreTool
{
  boost::scoped_ptr<KeyValueDB> db;

  public:
  StoreTool(const string &path) {
    LevelDBStore *db_ptr = new LevelDBStore(g_ceph_context, path);
    assert(!db_ptr->open(std::cerr));
    db.reset(db_ptr);
  }

  void list(const string &prefix, const bool do_crc) {
    KeyValueDB::WholeSpaceIterator iter = db->get_iterator();

    if (prefix.empty())
      iter->seek_to_first();
    else
      iter->seek_to_first(prefix);

    while (iter->valid()) {
      pair<string,string> rk = iter->raw_key();
      if (!prefix.empty() && (rk.first != prefix))
	break;

      std::cout << rk.first << ":" << rk.second;
      if (do_crc) {
        std::cout << " (" << iter->value().crc32c(0) << ")";
      }
      std::cout << std::endl;
      iter->next();
    }
  }

  bool exists(const string &prefix) {
    assert(!prefix.empty());
    KeyValueDB::WholeSpaceIterator iter = db->get_iterator();
    iter->seek_to_first(prefix);
    return (iter->valid() && (iter->raw_key().first == prefix));
  }

  bool exists(const string &prefix, const string &key) {
    assert(!prefix.empty());

    if (key.empty()) {
      return exists(prefix);
    }

    bool exists = false;
    get(prefix, key, exists);
    return exists;
  }

  bufferlist get(const string &prefix, const string &key, bool &exists) {
    assert(!prefix.empty() && !key.empty());

    map<string,bufferlist> result;
    std::set<std::string> keys;
    keys.insert(key);
    db->get(prefix, keys, &result);

    if (result.count(key) > 0) {
      exists = true;
      return result[key];
    }
    exists = false;
    return bufferlist();
  }

  uint64_t get_size() {
    map<string,uint64_t> extras;
    uint64_t s = db->get_estimated_size(extras);
    for (map<string,uint64_t>::iterator p = extras.begin();
         p != extras.end(); ++p) {
      std::cout << p->first << " - " << p->second << std::endl;
    }
    std::cout << "total: " << s << std::endl;
    return s;
  }

  bool set(const string &prefix, const string &key, bufferlist &val) {
    assert(!prefix.empty());
    assert(!key.empty());
    assert(val.length() > 0);

    KeyValueDB::Transaction tx = db->get_transaction();
    tx->set(prefix, key, val);
    int ret = db->submit_transaction_sync(tx);

    return (ret == 0);
  }
};

void usage(const char *pname)
{
  std::cerr << "Usage: " << pname << " <store path> command [args...]\n"
    << "\n"
    << "Commands:\n"
    << "  list [prefix]\n"
    << "  list-crc [prefix]\n"
    << "  exists <prefix> [key]\n"
    << "  get <prefix> <key>\n"
    << "  crc <prefix> <key>\n"
    << "  get-size\n"
    << "  set <prefix> <key> [ver <N>|in <file>]\n"
    << std::endl;
}

int main(int argc, const char *argv[])
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(
      NULL, args,
      CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);


  if (args.size() < 2) {
    usage(argv[0]);
    return 1;
  }

  string path(args[0]);
  string cmd(args[1]);

  std::cout << "path: " << path << " cmd " << cmd << std::endl;

  StoreTool st(path);

  if (cmd == "list" || cmd == "list-crc") {
    string prefix;
    if (argc > 3)
      prefix = argv[3];

    bool do_crc = (cmd == "list-crc");

    st.list(prefix, do_crc);

  } else if (cmd == "exists") {
    string key;
    if (argc < 4) {
      usage(argv[0]);
      return 1;
    }
    string prefix(argv[3]);
    if (argc > 4)
      key = argv[4];

    bool ret = st.exists(prefix, key);
    std::cout << "(" << prefix << ", " << key << ") "
      << (ret ? "exists" : "does not exist")
      << std::endl;
    return (ret ? 0 : 1);

  } else if (cmd == "get") {
    if (argc < 5) {
      usage(argv[0]);
      return 1;
    }
    string prefix(argv[3]);
    string key(argv[4]);

    bool exists = false;
    bufferlist bl = st.get(prefix, key, exists);
    std::cout << "(" << prefix << ", " << key << ")";
    if (!exists) {
      std::cout << " does not exist" << std::endl;
      return 1;
    }
    std::cout << std::endl;
    ostringstream os;
    bl.hexdump(os);
    std::cout << os.str() << std::endl;

  } else if (cmd == "crc") {
    if (argc < 5) {
      usage(argv[0]);
      return 1;
    }
    string prefix(argv[3]);
    string key(argv[4]);

    bool exists = false;
    bufferlist bl = st.get(prefix, key, exists);
    std::cout << "(" << prefix << ", " << key << ") ";
    if (!exists) {
      std::cout << " does not exist" << std::endl;
      return 1;
    }
    std::cout << " crc " << bl.crc32c(0) << std::endl;

  } else if (cmd == "get-size") {
    std::cout << "estimated store size: " << st.get_size() << std::endl;

  } else if (cmd == "set") {
    if (argc < 7) {
      usage(argv[0]);
      return 1;
    }
    string prefix(argv[3]);
    string key(argv[4]);
    string subcmd(argv[5]);

    bufferlist val;
    string errstr;
    if (subcmd == "ver") {
      version_t v = (version_t) strict_strtoll(argv[6], 10, &errstr);
      if (!errstr.empty()) {
        std::cerr << "error reading version: " << errstr << std::endl;
        return 1;
      }
      ::encode(v, val);
    } else if (subcmd == "in") {
      int ret = val.read_file(argv[6], &errstr);
      if (ret < 0 || !errstr.empty()) {
        std::cerr << "error reading file: " << errstr << std::endl;
        return 1;
      }
    } else {
      std::cerr << "unrecognized subcommand '" << subcmd << "'" << std::endl;
      usage(argv[0]);
      return 1;
    }

    bool ret = st.set(prefix, key, val);
    if (!ret) {
      std::cerr << "error setting ("
                << prefix << "," << key << ")" << std::endl;
      return 1;
    }

  } else {
    std::cerr << "Unrecognized command: " << cmd << std::endl;
    return 1;
  }

  return 0;
}
