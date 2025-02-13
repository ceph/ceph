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

#include "common/ConfUtils.h"
#include "common/ceph_argparse.h"
#include "common/config_proxy.h"
#include "common/strtol.h"
#include "global/global_context.h"
#include "global/global_init.h"

#include "auth/Crypto.h"
#include "auth/Auth.h"
#include "auth/KeyRing.h"

using std::map;
using std::string;
using std::vector;
using std::cerr;
using std::cout;

void usage()
{
  cout << "usage: ceph-authtool keyringfile [OPTIONS]...\n"
       << "where the options are:\n"
       << "  -l, --list                    will list all keys and capabilities present in\n"
       << "                                the keyring\n"
       << "  -p, --print-key               will print an encoded key for the specified\n"
       << "                                entityname. This is suitable for the\n"
       << "                                'mount -o secret=..' argument\n"
       << "  -C, --create-keyring          will create a new keyring, overwriting any\n"
       << "                                existing keyringfile\n"
       << "  -g, --gen-key                 will generate a new secret key for the\n"
       << "                                specified entityname\n"
       << "  --gen-print-key               will generate a new secret key without set it\n"
       << "                                to the keyringfile, prints the secret to stdout\n"
       << "  --import-keyring FILE         will import the content of a given keyring\n"
       << "                                into the keyringfile\n"
       << "  -n NAME, --name NAME          specify entityname to operate on\n"
       << "  -a BASE64, --add-key BASE64   will add an encoded key to the keyring\n"
       << "  --cap SUBSYSTEM CAPABILITY    will set the capability for given subsystem\n"
       << "  --caps CAPSFILE               will set all of capabilities associated with a\n"
       << "                                given key, for all subsystems\n"
       << "  --mode MODE                   will set the desired file mode to the keyring\n"
       << "                                e.g: '0644', defaults to '0600'"
       << std::endl;
  exit(1);
}

int main(int argc, const char **argv)
{
  auto args = argv_to_vec(argc, argv);
  std::string add_key;
  std::string caps_fn;
  std::string import_keyring;
  map<string,bufferlist> caps;
  std::string fn;

  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage();
    exit(0);
  }

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);

  bool gen_key = false;
  bool gen_print_key = false;
  bool list = false;
  bool print_key = false;
  bool create_keyring = false;
  int mode = 0600; // keyring file mode
  std::vector<const char*>::iterator i;

  /* Handle options unique to ceph-authtool
   * -n NAME, --name NAME is handled by global_init
   * */
  for (i = args.begin(); i != args.end(); ) {
    std::string val;
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, i, "-g", "--gen-key", (char*)NULL)) {
      gen_key = true;
    } else if (ceph_argparse_flag(args, i, "--gen-print-key", (char*)NULL)) {
      gen_print_key = true;
    } else if (ceph_argparse_witharg(args, i, &val, "-a", "--add-key", (char*)NULL)) {
      if (val.empty()) {
        cerr << "Option --add-key requires an argument" << std::endl;
        exit(1);
      }
      add_key = val;
    } else if (ceph_argparse_flag(args, i, "-l", "--list", (char*)NULL)) {
      list = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--caps", (char*)NULL)) {
      caps_fn = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--cap", (char*)NULL)) {
      std::string my_key = val;
      if (i == args.end()) {
	cerr << "must give two arguments to --cap: key and val." << std::endl;
	exit(1);
      }
      std::string my_val = *i;
      ++i;
      encode(my_val, caps[my_key]);
    } else if (ceph_argparse_flag(args, i, "-p", "--print-key", (char*)NULL)) {
      print_key = true;
    } else if (ceph_argparse_flag(args, i, "-C", "--create-keyring", (char*)NULL)) {
      create_keyring = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--import-keyring", (char*)NULL)) {
      import_keyring = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--mode", (char*)NULL)) {
      std::string err;
      mode = strict_strtoll(val.c_str(), 8, &err);
      if (!err.empty()) {
        cerr << "Option --mode requires an argument" << std::endl;
        exit(1);
      }
    } else if (fn.empty()) {
      fn = *i++;
    } else {
      cerr << argv[0] << ": unexpected '" << *i << "'" << std::endl;
      usage();
    }
  }

  if (fn.empty() && !gen_print_key) {
    cerr << argv[0] << ": must specify filename" << std::endl;
    usage();
  }
  if (!(gen_key ||
	gen_print_key ||
	!add_key.empty() ||
	list ||
	!caps_fn.empty() ||
	!caps.empty() ||
	print_key ||
	create_keyring ||
	!import_keyring.empty())) {
    cerr << "no command specified" << std::endl;
    usage();
  }
  if (gen_key && (!add_key.empty())) {
    cerr << "can't both gen-key and add-key" << std::endl;
    usage();
  }

  common_init_finish(g_ceph_context);
  EntityName ename(g_conf()->name);

  // Enforce the use of gen-key or add-key when creating to avoid ending up
  // with an "empty" key (key = AAAAAAAAAAAAAAAA)
  if (create_keyring && !gen_key && add_key.empty() && !caps.empty()) {
    cerr << "must specify either gen-key or add-key when creating" << std::endl;
    usage();
  }

  if (gen_print_key) {
    CryptoKey key;
    key.create(g_ceph_context, CEPH_CRYPTO_AES);
    cout << key << std::endl;
    return 0;
  }

  // keyring --------
  bool modified = false;
  bool added_entity = false;
  KeyRing keyring;

  bufferlist bl;
  int r = 0;
  if (create_keyring) {
    cout << "creating " << fn << std::endl;
    modified = true;
  } else {
    std::string err;
    r = bl.read_file(fn.c_str(), &err);
    if (r >= 0) {
      try {
	auto iter = bl.cbegin();
	decode(keyring, iter);
      } catch (const buffer::error &err) {
	cerr << "error reading file " << fn << std::endl;
	exit(1);
      }
    } else {
      cerr << "can't open " << fn << ": " << err << std::endl;
      exit(1);
    }
  }

  // Validate that "name" actually has an existing key in this keyring if we
  // have not given gen-key or add-key options
  if (!gen_key && add_key.empty() && !caps.empty()) {
    CryptoKey key;
    if (!keyring.get_secret(ename, key)) {
      cerr << "can't find existing key for " << ename 
           << " and neither gen-key nor add-key specified" << std::endl;
      exit(1);
    }
  }

  // write commands
  if (!import_keyring.empty()) {
    KeyRing other;
    bufferlist obl;
    std::string err;
    int r = obl.read_file(import_keyring.c_str(), &err);
    if (r >= 0) {
      try {
	auto iter = obl.cbegin();
	decode(other, iter);
      } catch (const buffer::error &err) {
	cerr << "error reading file " << import_keyring << std::endl;
	exit(1);
      }

      cout << "importing contents of " << import_keyring << " into " << fn << std::endl;
      //other.print(cout);
      keyring.import(g_ceph_context, other);
      modified = true;
    } else {
      cerr << "can't open " << import_keyring << ": " << err << std::endl;
      exit(1);
    }
  }
  if (gen_key) {
    EntityAuth eauth;
    eauth.key.create(g_ceph_context, CEPH_CRYPTO_AES);
    keyring.add(ename, eauth);
    modified = true;
  }
  if (!add_key.empty()) {
    EntityAuth eauth;
    try {
      eauth.key.decode_base64(add_key);
    } catch (const buffer::error &err) {
      cerr << "can't decode key '" << add_key << "'" << std::endl;
      exit(1);
    }
    keyring.add(ename, eauth);
    modified = true;
    cout << "added entity " << ename << " " << eauth << std::endl;
    added_entity = true;
  }
  if (!caps_fn.empty()) {
    ConfFile cf;
    if (cf.parse_file(caps_fn, &cerr) != 0) {
      cerr << "could not parse caps file " << caps_fn << std::endl;
      exit(1);
    }
    map<string, bufferlist> caps;
    const char *key_names[] = { "mon", "osd", "mds", "mgr", NULL };
    for (int i=0; key_names[i]; i++) {
      std::string val;
      if (cf.read("global", key_names[i], val) == 0) {
	bufferlist bl;
	encode(val, bl);
	string s(key_names[i]);
	caps[s] = bl;
      }
    }
    keyring.set_caps(ename, caps);
    modified = true;
  }
  if (!caps.empty()) {
    keyring.set_caps(ename, caps);
    modified = true;
  }
  if (added_entity && caps.size() > 0) {
    cout << "added " << caps.size() << " caps to entity " << ename << std::endl;
  }

  // read commands
  if (list) {
    try {
      keyring.print(cout);
    } catch (ceph::buffer::end_of_buffer &eob) {
      cout << "Exception (end_of_buffer) in print(), exit." << std::endl;
      exit(1);
    }
  }
  if (print_key) {
    CryptoKey key;
    if (keyring.get_secret(ename, key)) {
      cout << key << std::endl;
    } else {
      cerr << "entity " << ename << " not found" << std::endl;
      exit(1);
    }
  }

  // write result?
  if (modified) {
    bufferlist bl;
    keyring.encode_plaintext(bl);
    r = bl.write_file(fn.c_str(), mode);
    if (r < 0) {
      cerr << "could not write " << fn << std::endl;
      exit(1);
    }
    //cout << "wrote " << bl.length() << " bytes to " << fn << std::endl;
  }
  return 0;
}
