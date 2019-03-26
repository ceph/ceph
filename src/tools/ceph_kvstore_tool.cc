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
#include <fstream>

#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/strtol.h"
#include "common/url_escape.h"

#include "global/global_context.h"
#include "global/global_init.h"

#include "kvstore_tool.h"

void usage(const char *pname)
{
  std::cout << "Usage: " << pname << " <leveldb|rocksdb|bluestore-kv> <store path> command [args...]\n"
    << "\n"
    << "Commands:\n"
    << "  list [prefix]\n"
    << "  list-crc [prefix]\n"
    << "  dump [prefix]\n"
    << "  exists <prefix> [key]\n"
    << "  get <prefix> <key> [out <file>]\n"
    << "  crc <prefix> <key>\n"
    << "  get-size [<prefix> <key>]\n"
    << "  set <prefix> <key> [ver <N>|in <file>]\n"
    << "  rm <prefix> <key>\n"
    << "  rm-prefix <prefix>\n"
    << "  store-copy <path> [num-keys-per-tx] [leveldb|rocksdb|...] \n"
    << "  store-crc <path>\n"
    << "  compact\n"
    << "  compact-prefix <prefix>\n"
    << "  compact-range <prefix> <start> <end>\n"
    << "  destructive-repair  (use only as last resort! may corrupt healthy data)\n"
    << "  stats\n"
    << std::endl;
}

int main(int argc, const char *argv[])
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage(argv[0]);
    exit(0);
  }

  map<string,string> defaults = {
    { "debug_rocksdb", "2" }
  };

  auto cct = global_init(
    &defaults, args,
    CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
    CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  ceph_assert((int)args.size() < argc);
  for(size_t i=0; i<args.size(); i++)
    argv[i+1] = args[i];
  argc = args.size() + 1;

  if (args.size() < 3) {
    usage(argv[0]);
    return 1;
  }

  string type(args[0]);
  string path(args[1]);
  string cmd(args[2]);

  if (type != "leveldb" &&
      type != "rocksdb" &&
      type != "bluestore-kv")  {

    std::cerr << "Unrecognized type: " << args[0] << std::endl;
    usage(argv[0]);
    return 1;
  }

  bool need_open_db = (cmd != "destructive-repair");
  bool need_stats = (cmd == "stats");
  StoreTool st(type, path, need_open_db, need_stats);

  if (cmd == "destructive-repair") {
    int ret = st.destructive_repair();
    if (!ret) {
      std::cout << "destructive-repair completed without reporting an error"
		<< std::endl;
    } else {
      std::cout << "destructive-repair failed with " << cpp_strerror(ret)
		<< std::endl;
    }
    return ret;
  } else if (cmd == "list" || cmd == "list-crc") {
    string prefix;
    if (argc > 4)
      prefix = url_unescape(argv[4]);

    bool do_crc = (cmd == "list-crc");
    st.list(prefix, do_crc, false);

  } else if (cmd == "dump") {
    string prefix;
    if (argc > 4)
      prefix = url_unescape(argv[4]);
    st.list(prefix, false, true);

  } else if (cmd == "exists") {
    string key;
    if (argc < 5) {
      usage(argv[0]);
      return 1;
    }
    string prefix(url_unescape(argv[4]));
    if (argc > 5)
      key = url_unescape(argv[5]);

    bool ret = st.exists(prefix, key);
    std::cout << "(" << url_escape(prefix) << ", " << url_escape(key) << ") "
      << (ret ? "exists" : "does not exist")
      << std::endl;
    return (ret ? 0 : 1);

  } else if (cmd == "get") {
    if (argc < 6) {
      usage(argv[0]);
      return 1;
    }
    string prefix(url_unescape(argv[4]));
    string key(url_unescape(argv[5]));

    bool exists = false;
    bufferlist bl = st.get(prefix, key, exists);
    std::cout << "(" << url_escape(prefix) << ", " << url_escape(key) << ")";
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
    string prefix(url_unescape(argv[4]));
    string key(url_unescape(argv[5]));

    bool exists = false;
    bufferlist bl = st.get(prefix, key, exists);
    std::cout << "(" << url_escape(prefix) << ", " << url_escape(key) << ") ";
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
    string prefix(url_unescape(argv[4]));
    string key(url_unescape(argv[5]));

    bool exists = false;
    bufferlist bl = st.get(prefix, key, exists);
    if (!exists) {
      std::cerr << "(" << url_escape(prefix) << "," << url_escape(key)
                << ") does not exist" << std::endl;
      return 1;
    }
    std::cout << "(" << url_escape(prefix) << "," << url_escape(key)
              << ") size " << byte_u_t(bl.length()) << std::endl;

  } else if (cmd == "set") {
    if (argc < 8) {
      usage(argv[0]);
      return 1;
    }
    string prefix(url_unescape(argv[4]));
    string key(url_unescape(argv[5]));
    string subcmd(argv[6]);

    bufferlist val;
    string errstr;
    if (subcmd == "ver") {
      version_t v = (version_t) strict_strtoll(argv[7], 10, &errstr);
      if (!errstr.empty()) {
        std::cerr << "error reading version: " << errstr << std::endl;
        return 1;
      }
      encode(v, val);
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
                << url_escape(prefix) << "," << url_escape(key) << ")" << std::endl;
      return 1;
    }
  } else if (cmd == "rm") {
    if (argc < 6) {
      usage(argv[0]);
      return 1;
    }
    string prefix(url_unescape(argv[4]));
    string key(url_unescape(argv[5]));

    bool ret = st.rm(prefix, key);
    if (!ret) {
      std::cerr << "error removing ("
                << url_escape(prefix) << "," << url_escape(key) << ")"
		<< std::endl;
      return 1;
    }
  } else if (cmd == "rm-prefix") {
    if (argc < 5) {
      usage(argv[0]);
      return 1;
    }
    string prefix(url_unescape(argv[4]));

    bool ret = st.rm_prefix(prefix);
    if (!ret) {
      std::cerr << "error removing prefix ("
                << url_escape(prefix) << ")"
		<< std::endl;
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
    string other_store_type = argv[1];
    if (argc > 6) {
      other_store_type = argv[6];
    }

    int ret = st.copy_store_to(argv[1], argv[4], num_keys_per_tx, other_store_type);
    if (ret < 0) {
      std::cerr << "error copying store to path '" << argv[4]
                << "': " << cpp_strerror(ret) << std::endl;
      return 1;
    }

  } else if (cmd == "store-crc") {
    if (argc < 4) {
      usage(argv[0]);
      return 1;
    }
    std::ofstream fs(argv[4]);
    uint32_t crc = st.traverse(string(), true, false, &fs);
    std::cout << "store at '" << argv[4] << "' crc " << crc << std::endl;

  } else if (cmd == "compact") {
    st.compact();
  } else if (cmd == "compact-prefix") {
    if (argc < 5) {
      usage(argv[0]);
      return 1;
    }
    string prefix(url_unescape(argv[4]));
    st.compact_prefix(prefix);
  } else if (cmd == "compact-range") {
    if (argc < 7) {
      usage(argv[0]);
      return 1;
    }
    string prefix(url_unescape(argv[4]));
    string start(url_unescape(argv[5]));
    string end(url_unescape(argv[6]));
    st.compact_range(prefix, start, end);
  } else if (cmd == "stats") {
    st.print_stats();
  } else {
    std::cerr << "Unrecognized command: " << cmd << std::endl;
    return 1;
  }

  return 0;
}
