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

void usage()
{
  cout << " usage: [--gen-key] <filename>" << std::endl;
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

  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("gen-key", 'g')) {
      CONF_SAFE_SET_ARG_VAL(&gen_key, OPT_BOOL);
    } else if (!fn) {
      fn = args[i];
    } else 
      usage();
  }
  if (!fn) {
    cerr << me << ": must specify filename" << std::endl;
    usage();
  }

  CryptoKey key;
  key.create(CEPH_SECRET_AES);

  bufferlist bl;
  ::encode(key, bl);
  int r = bl.write_file(fn);

  if (r < 0) {
    cerr << "could not write " << fn << std::endl;
  }

  return 0;
}
