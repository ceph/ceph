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

#include "common/config.h"

#include "common/ConfUtils.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "auth/Crypto.h"
#include "auth/Auth.h"
#include "auth/KeyRing.h"

#include <sstream>

void usage()
{
  cout << "usage: cauthtool keyringfile [OPTIONS]...\n"
       << "where the options are:\n"
       << "  -l, --list                    will list all keys and capabilities present in\n"
       << "                                the keyring\n"
       << "  -p, --print                   will print an encoded key for the specified\n"
       << "                                entityname. This is suitable for the\n"
       << "                                'mount -o secret=..' argument\n"
       << "  -C, --create-keyring          will create a new keyring, overwriting any\n"
       << "                                existing keyringfile\n"
       << "  --gen-key                     will generate a new secret key for the\n"
       << "                                specified entityname\n"
       << "  --add-key                     will add an encoded key to the keyring\n"
       << "  --cap subsystem capability    will set the capability for given subsystem\n"
       << "  --caps capsfile               will set all of capabilities associated with a\n"
       << "                                given key, for all subsystems\n"
       << "  -b, --bin                     will create a binary formatted keyring" << std::endl;
  exit(1);
}

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  DEFINE_CONF_VARS(usage);

  common_init(args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
	      CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  EntityName ename(*g_conf.name);

  const char *me = argv[0];

  const char *fn = 0;
  bool gen_key = false;
  bool gen_print_key = false;
  const char *add_key = 0;
  bool list = false;
  bool print_key = false;
  bool create_keyring = false;
  const char *caps_fn = NULL;
  const char *import_keyring = NULL;
  bool set_auid = false;
  uint64_t auid = CEPH_AUTH_UID_DEFAULT;
  map<string,bufferlist> caps;
  bool bin_keyring = false;

  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("gen-key", 'g')) {
      CONF_SAFE_SET_ARG_VAL(&gen_key, OPT_BOOL);
    } else if (CONF_ARG_EQ("gen-print-key", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&gen_print_key, OPT_BOOL);
    } else if (CONF_ARG_EQ("add-key", 'a')) {
      CONF_SAFE_SET_ARG_VAL(&add_key, OPT_STR);
    } else if (CONF_ARG_EQ("list", 'l')) {
      CONF_SAFE_SET_ARG_VAL(&list, OPT_BOOL);
    } else if (CONF_ARG_EQ("caps", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&caps_fn, OPT_STR);
    } else if (CONF_ARG_EQ("cap", '\0')) {
      const char *key, *val;
      CONF_SAFE_SET_ARG_VAL(&key, OPT_STR);
      CONF_SAFE_SET_ARG_VAL(&val, OPT_STR);
      ::encode(val, caps[key]);
    } else if (CONF_ARG_EQ("print-key", 'p')) {
      CONF_SAFE_SET_ARG_VAL(&print_key, OPT_BOOL);
    } else if (CONF_ARG_EQ("create-keyring", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&create_keyring, OPT_BOOL);
    } else if (CONF_ARG_EQ("import-keyring", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&import_keyring, OPT_STR);
    } else if (CONF_ARG_EQ("set-uid", 'u')) {
      CONF_SAFE_SET_ARG_VAL(&auid, OPT_LONGLONG);
      set_auid = true;
    } else if (CONF_ARG_EQ("bin", 'b')) {
      CONF_SAFE_SET_ARG_VAL(&bin_keyring, OPT_BOOL);
    } else if (!fn) {
      fn = args[i];
    } else 
      usage();
  }
  if (!fn && !gen_print_key) {
    cerr << me << ": must specify filename" << std::endl;
    usage();
  }
  if (!(gen_key ||
	gen_print_key ||
	add_key ||
	list ||
	caps_fn ||
	caps.size() ||
	set_auid ||
	print_key ||
	create_keyring ||
	import_keyring)) {
    cerr << "no command specified" << std::endl;
    usage();
  }
  if (gen_key && add_key) {
    cerr << "can't both gen_key and add_key" << std::endl;
    usage();
  }	

  if (gen_print_key) {
    CryptoKey key;
    key.create(CEPH_CRYPTO_AES);
    cout << key << std::endl;    
    return 0;
  }

  // keyring --------
  bool modified = false;
  KeyRing keyring;

  bufferlist bl;
  int r = 0;
  if (create_keyring) {
    cout << "creating " << fn << std::endl;
    modified = true;
  } else {
    r = bl.read_file(fn, true);
    if (r >= 0) {
      try {
	bufferlist::iterator iter = bl.begin();
	::decode(keyring, iter);
      } catch (const buffer::error &err) {
	cerr << "error reading file " << fn << std::endl;
	exit(1);
      }
    } else {
      cerr << "can't open " << fn << ": " << strerror(-r) << std::endl;
      exit(1);
    }
  }

  // write commands
  if (import_keyring) {
    KeyRing other;
    bufferlist obl;
    int r = obl.read_file(import_keyring);
    if (r >= 0) {
      try {
	bufferlist::iterator iter = obl.begin();
	::decode(other, iter);
      } catch (const buffer::error &err) {
	cerr << "error reading file " << import_keyring << std::endl;
	exit(1);
      }
      
      cout << "importing contents of " << import_keyring << " into " << fn << std::endl;
      //other.print(cout);
      keyring.import(other);
      modified = true;
    } else {
      cerr << "can't open " << import_keyring << ": " << strerror(-r) << std::endl;
      exit(1);
    }
  }
  if (gen_key) {
    EntityAuth eauth;
    eauth.key.create(CEPH_CRYPTO_AES);
    keyring.add(ename, eauth);
    modified = true;
  }
  if (add_key) {
    EntityAuth eauth;
    string ekey(add_key);
    try {
      eauth.key.decode_base64(ekey);
    } catch (const buffer::error &err) {
      cerr << "can't decode key '" << add_key << "'" << std::endl;
      exit(1);
    }
    keyring.add(ename, eauth);
    modified = true;
    cout << "added entity " << ename << " auth " << eauth << std::endl;
  }
  if (caps_fn) {
    ConfFile *cf = new ConfFile(caps_fn);
    if (cf->parse() != 0) {
      cerr << "could not parse caps file " << caps_fn << std::endl;
      exit(1);
    }
    map<string, bufferlist> caps;
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
    keyring.set_caps(ename, caps);
    modified = true;
  }
  if (caps.size()) {
    keyring.set_caps(ename, caps);
    modified = true;
  }
  if (set_auid) {
    keyring.set_uid(ename, auid);
    modified = true;
  }

  // read commands
  if (list) {
    keyring.print(cout);
  }
  if (print_key) {
    CryptoKey key;
    if (keyring.get_secret(ename, key)) {
      cout << key << std::endl;
    } else {
      cerr << "entity " << ename << " not found" << std::endl;
    }
  }

  // write result?
  if (modified) {
    bufferlist bl;
    if (bin_keyring) {
      ::encode(keyring, bl);
    } else {
      std::ostringstream os;
      keyring.print(os);
      string str = os.str();
      bl.append(str);
    }
    r = bl.write_file(fn, 0600);
    if (r < 0) {
      cerr << "could not write " << fn << std::endl;
    }
    //cout << "wrote " << bl.length() << " bytes to " << fn << std::endl;
  }

  return 0;
}
