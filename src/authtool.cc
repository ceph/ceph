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

#include "common/ConfUtils.h"
#include "common/common_init.h"
#include "auth/Crypto.h"
#include "auth/KeysServer.h"
#include "auth/Auth.h"

void usage()
{
  cout << " usage: [--gen-key] [--name=<name>] [--caps=<filename>] [--list] <filename>" << std::endl;
  exit(1);
}

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  DEFINE_CONF_VARS(usage);
  common_init(args, "osdmaptool", false, false);

  const char *me = argv[0];

  const char *fn = 0;
  bool gen_key = false;
  bool list = false;
  const char *name = "";
  const char *caps_fn = NULL;

  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("gen-key", 'g')) {
      CONF_SAFE_SET_ARG_VAL(&gen_key, OPT_BOOL);
    } else if (CONF_ARG_EQ("name", 'n')) {
      CONF_SAFE_SET_ARG_VAL(&name, OPT_STR);
    } else if (CONF_ARG_EQ("list", 'l')) {
      CONF_SAFE_SET_ARG_VAL(&list, OPT_BOOL);
    } else if (CONF_ARG_EQ("caps", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&caps_fn, OPT_STR);
    } else if (!fn) {
      fn = args[i];
    } else 
      usage();
  }
  if (!fn) {
    cerr << me << ": must specify filename" << std::endl;
    usage();
  }

  map<string, EntityAuth> keys_map;
  string s = name;


  if (caps_fn) {
    if (!name || !(*name)) {
      cerr << "can't specify caps without name" << std::endl;
      exit(1);
    }
  }

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
    keys_map[s].key = key;
  }

  if (list) {
    map<string, EntityAuth>::iterator iter = keys_map.begin();
    for (; iter != keys_map.end(); ++iter) {
      string n = iter->first;
      if (n.empty()) {
        cout << "<default key>" << std::endl;
      } else {
        cout << n << std::endl;
      }
      cout << "\tkey " << iter->second.key << std::endl;
      map<string, bufferlist>::iterator capsiter = iter->second.caps.begin();
      for (; capsiter != iter->second.caps.end(); ++capsiter) {
        bufferlist::iterator dataiter = capsiter->second.begin();
        string caps;
        ::decode(caps, dataiter);
	cout << "\tcaps: [" << capsiter->first << "] " << caps << std::endl;
      }
    }
  }

  if (caps_fn) {
    map<string, bufferlist>& caps = keys_map[s].caps;
    ConfFile *cf = new ConfFile(caps_fn);
    if (!cf->parse()) {
      cerr << "could not parse caps file " << caps_fn << std::endl;
      exit(1);
    }
    const char *key_names[] = { "mon", "osd", "mds", NULL };
    for (int i=0; key_names[i]; i++) {
      char *val;
      cf->read("global", key_names[i], &val, NULL);
      if (val) {
        bufferlist bl;
        ::encode(val, bl);
        string s(key_names[i]);
        caps[s] = bl; 
        free(val);
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
