// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

using namespace std;

#include "config.h"

#include "common/common_init.h"
#include "auth/Crypto.h"
#include "auth/KeysServer.h"

void usage()
{
  cout << " usage: [--gen-key] [--name] [--list] <filename>" << std::endl;
  exit(1);
}

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  DEFINE_CONF_VARS(usage);
  common_init(args, "osdmaptool", false);

  const char *me = argv[0];

  const char *fn = 0;
  bool gen_key = false;
  bool list = false;
  const char *name = "";

  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("gen-key", 'g')) {
      CONF_SAFE_SET_ARG_VAL(&gen_key, OPT_BOOL);
    } else if (CONF_ARG_EQ("name", 'n')) {
      CONF_SAFE_SET_ARG_VAL(&name, OPT_STR);
    } else if (CONF_ARG_EQ("list", 'l')) {
      CONF_SAFE_SET_ARG_VAL(&list, OPT_BOOL);
    } else if (!fn) {
      fn = args[i];
    } else 
      usage();
  }
  if (!fn) {
    cerr << me << ": must specify filename" << std::endl;
    usage();
  }

  map<string, CryptoKey> keys_map;
  string s = name;

  CryptoKey key;
  key.create(CEPH_SECRET_AES);

  bufferlist bl;
  int r = bl.read_file(fn);
  if (r >= 0) {
    try {
      bufferlist::iterator iter = bl.begin();
      ::decode(keys_map, iter);
    } catch (buffer::error *err) {
      cerr << "error reading file " << fn << std::endl;
      exit(1);
    }
  }

  if (gen_key) {
    keys_map[s] = key;
  }

  if (list) {
    map<string, CryptoKey>::iterator iter = keys_map.begin();
    for (; iter != keys_map.end(); ++iter) {
      string n = iter->first;
      if (n.empty()) {
        cout << "<default key>" << std::endl;
      } else {
        cout << n << std::endl;
      }
    }
  }

  if (gen_key) {
    bufferlist bl2;
    ::encode(keys_map, bl2);
    r = bl2.write_file(fn);

    if (r < 0) {
      cerr << "could not write " << fn << std::endl;
    }
  }

  return 0;
}
