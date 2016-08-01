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
#include <map>
#include <set>
#include <string>

#include <boost/scoped_ptr.hpp>

#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/strtol.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "include/stringify.h"
#include "include/utime.h"
#include "common/Clock.h"
#include "kv/KeyValueDB.h"

using namespace std;

class StoreTool
{
  boost::scoped_ptr<KeyValueDB> db;
  string store_path;

  public:
  StoreTool(string type, const string &path) : store_path(path) {
    KeyValueDB *db_ptr = KeyValueDB::create(g_ceph_context, type, path);
    assert(!db_ptr->open(std::cerr));
    db.reset(db_ptr);
  }

  uint32_t traverse(const string &prefix,
                    const bool do_crc,
                    ostream *out) {
    KeyValueDB::WholeSpaceIterator iter = db->get_iterator();

    if (prefix.empty())
      iter->seek_to_first();
    else
      iter->seek_to_first(prefix);

    uint32_t crc = -1;

    while (iter->valid()) {
      pair<string,string> rk = iter->raw_key();
      if (!prefix.empty() && (rk.first != prefix))
        break;

      if (out)
        *out << rk.first << ":" << rk.second;
      if (do_crc) {
        bufferlist bl;
        bl.append(rk.first);
        bl.append(rk.second);
        bl.append(iter->value());

        crc = bl.crc32c(crc);
        if (out) {
          *out << " (" << bl.crc32c(0) << ")";
        }
      }
      if (out)
        *out << std::endl;
      iter->next();
    }

    return crc;
  }

  void list(const string &prefix, const bool do_crc) {
    traverse(prefix, do_crc, &std::cout);
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

  int copy_store_to(string type, const string &other_path,
		    const int num_keys_per_tx) {

    if (num_keys_per_tx <= 0) {
      std::cerr << "must specify a number of keys/tx > 0" << std::endl;
      return -EINVAL;
    }

    // open or create a leveldb store at @p other_path
    KeyValueDB *other = KeyValueDB::create(g_ceph_context, type, other_path);
    int err = other->create_and_open(std::cerr);
    if (err < 0)
      return err;

    KeyValueDB::WholeSpaceIterator it = db->get_iterator();
    it->seek_to_first();
    uint64_t total_keys = 0;
    uint64_t total_size = 0;
    uint64_t total_txs = 0;

    utime_t started_at = ceph_clock_now(g_ceph_context);

    do {
      int num_keys = 0;

      KeyValueDB::Transaction tx = other->get_transaction();


      while (it->valid() && num_keys < num_keys_per_tx) {
        pair<string,string> k = it->raw_key();
        bufferlist v = it->value();
        tx->set(k.first, k.second, v);

        num_keys ++;
        total_size += v.length();

        it->next();
      }

      total_txs ++;
      total_keys += num_keys;

      if (num_keys > 0)
        other->submit_transaction_sync(tx);

      utime_t cur_duration = ceph_clock_now(g_ceph_context) - started_at;
      std::cout << "ts = " << cur_duration << "s, copied " << total_keys
                << " keys so far (" << stringify(si_t(total_size)) << ")"
                << std::endl;

    } while (it->valid());

    utime_t time_taken = ceph_clock_now(g_ceph_context) - started_at;

    std::cout << "summary:" << std::endl;
    std::cout << "  copied " << total_keys << " keys" << std::endl;
    std::cout << "  used " << total_txs << " transactions" << std::endl;
    std::cout << "  total size " << stringify(si_t(total_size)) << std::endl;
    std::cout << "  from '" << store_path << "' to '" << other_path << "'"
              << std::endl;
    std::cout << "  duration " << time_taken << " seconds" << std::endl;

    return 0;
  }
};

void usage(const char *pname)
{
  std::cerr << "Usage: " << pname << " <leveldb|rocksdb|...> <store path> command [args...]\n"
    << "\n"
    << "Commands:\n"
    << "  list [prefix]\n"
    << "  list-crc [prefix]\n"
    << "  exists <prefix> [key]\n"
    << "  get <prefix> <key> [out <file>]\n"
    << "  crc <prefix> <key>\n"
    << "  get-size [<prefix> <key>]\n"
    << "  set <prefix> <key> [ver <N>|in <file>]\n"
    << "  store-copy <path> [num-keys-per-tx]\n"
    << "  store-crc <path>\n"
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


  if (args.size() < 3) {
    usage(argv[0]);
    return 1;
  }

  string type(args[0]);
  string path(args[1]);
  string cmd(args[2]);

  StoreTool st(type, path);

  if (cmd == "list" || cmd == "list-crc") {
    string prefix;
    if (argc > 4)
      prefix = argv[4];

    bool do_crc = (cmd == "list-crc");

    st.list(prefix, do_crc);

  } else if (cmd == "exists") {
    string key;
    if (argc < 5) {
      usage(argv[0]);
      return 1;
    }
    string prefix(argv[4]);
    if (argc > 5)
      key = argv[5];

    bool ret = st.exists(prefix, key);
    std::cout << "(" << prefix << ", " << key << ") "
      << (ret ? "exists" : "does not exist")
      << std::endl;
    return (ret ? 0 : 1);

  } else if (cmd == "get") {
    if (argc < 6) {
      usage(argv[0]);
      return 1;
    }
    string prefix(argv[4]);
    string key(argv[5]);

    bool exists = false;
    bufferlist bl = st.get(prefix, key, exists);
    std::cout << "(" << prefix << ", " << key << ")";
    if (!exists) {
      std::cout << " does not exist" << std::endl;
      return 1;
    }
    std::cout << std::endl;

    if (argc >= 7) {
      string subcmd(argv[6]);
      if (subcmd != "out") {
        std::cerr << "unrecognized subcmd '" << subcmd << "'"
                  << std::endl;
        return 1;
      }
      if (argc < 8) {
        std::cerr << "output path not specified" << std::endl;
        return 1;
      }
      string out(argv[7]);

      if (out.empty()) {
        std::cerr << "unspecified out file" << std::endl;
        return 1;
      }

      int err = bl.write_file(argv[7], 0644);
      if (err < 0) {
        std::cerr << "error writing value to '" << out << "': "
                  << cpp_strerror(err) << std::endl;
        return 1;
      }
    } else {
      ostringstream os;
      bl.hexdump(os);
      std::cout << os.str() << std::endl;
    }

  } else if (cmd == "crc") {
    if (argc < 6) {
      usage(argv[0]);
      return 1;
    }
    string prefix(argv[4]);
    string key(argv[5]);

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

    if (argc < 5)
      return 0;

    if (argc < 6) {
      usage(argv[0]);
      return 1;
    }
    string prefix(argv[4]);
    string key(argv[5]);

    bool exists = false;
    bufferlist bl = st.get(prefix, key, exists);
    if (!exists) {
      std::cerr << "(" << prefix << "," << key
                << ") does not exist" << std::endl;
      return 1;
    }
    std::cout << "(" << prefix << "," << key
              << ") size " << si_t(bl.length()) << std::endl;

  } else if (cmd == "set") {
    if (argc < 8) {
      usage(argv[0]);
      return 1;
    }
    string prefix(argv[4]);
    string key(argv[5]);
    string subcmd(argv[6]);

    bufferlist val;
    string errstr;
    if (subcmd == "ver") {
      version_t v = (version_t) strict_strtoll(argv[7], 10, &errstr);
      if (!errstr.empty()) {
        std::cerr << "error reading version: " << errstr << std::endl;
        return 1;
      }
      ::encode(v, val);
    } else if (subcmd == "in") {
      int ret = val.read_file(argv[7], &errstr);
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
  } else if (cmd == "store-copy") {
    int num_keys_per_tx = 128; // magic number that just feels right.
    if (argc < 5) {
      usage(argv[0]);
      return 1;
    } else if (argc > 5) {
      string err;
      num_keys_per_tx = strict_strtol(argv[5], 10, &err);
      if (!err.empty()) {
        std::cerr << "invalid num_keys_per_tx: " << err << std::endl;
        return 1;
      }
    }

    int ret = st.copy_store_to(argv[1], argv[4], num_keys_per_tx);
    if (ret < 0) {
      std::cerr << "error copying store to path '" << argv[4]
                << "': " << cpp_strerror(ret) << std::endl;
      return 1;
    }

  } else if (cmd == "store-crc") {
    uint32_t crc = st.traverse(string(), true, NULL);
    std::cout << "store at '" << path << "' crc " << crc << std::endl;

  } else {
    std::cerr << "Unrecognized command: " << cmd << std::endl;
    return 1;
  }

  return 0;
}
