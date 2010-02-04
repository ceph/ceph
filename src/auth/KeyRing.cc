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

#include <errno.h>
#include <map>

#include "config.h"
#include "include/str_list.h"

#include "Crypto.h"
#include "auth/KeyRing.h"

#define DOUT_SUBSYS auth
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "auth: "


using namespace std;

KeyRing g_keyring;

bool KeyRing::load(const char *filename_list)
{
  string k = filename_list;
  string filename;
  list<string> ls;
  get_str_list(k, ls);
  bufferlist bl;
  bool loaded = false;
  for (list<string>::iterator p = ls.begin(); p != ls.end(); p++) {
    // subst in home dir?
    size_t pos = p->find("~/");
    if (pos != string::npos)
      p->replace(pos, 1, getenv("HOME"));

    if (bl.read_file(p->c_str(), true) == 0) {
      loaded = true;
      filename = *p;
      break;
    }
  }
  if (!loaded) {
    dout(0) << "can't open key file(s) " << filename_list << dendl;
    return false;
  }

  bufferlist::iterator p = bl.begin();
  decode(p);

  dout(1) << "loaded key file " << filename << dendl;
  return true;
}

void KeyRing::print(ostream& out)
{
  for (map<string, EntityAuth>::iterator p = keys.begin();
       p != keys.end();
       ++p) {
    string n = p->first;
    if (n.empty()) {
      out << "<default key>" << std::endl;
    } else {
      out << n << std::endl;
    }
    out << "\tkey: " << p->second.key << std::endl;

    for (map<string, bufferlist>::iterator q = p->second.caps.begin();
	 q != p->second.caps.end();
	 ++q) {
      bufferlist::iterator dataiter = q->second.begin();
      string caps;
      ::decode(caps, dataiter);
      out << "\tcaps: [" << q->first << "] " << caps << std::endl;
    }
  }
}

void KeyRing::import(KeyRing& other)
{
  for (map<string, EntityAuth>::iterator p = other.keys.begin();
       p != other.keys.end();
       ++p) {
    dout(10) << " importing " << p->first << " " << p->second << dendl;
    keys[p->first] = p->second;
  }
}

// ----------------
// rotating crap

void KeyRing::set_rotating(RotatingSecrets& secrets)
{
  Mutex::Locker l(lock);

  rotating_secrets = secrets;

  dout(0) << "KeyRing::set_rotating max_ver=" << secrets.max_ver << dendl;

  map<uint64_t, ExpiringCryptoKey>::iterator iter = secrets.secrets.begin();

  for (; iter != secrets.secrets.end(); ++iter) {
    ExpiringCryptoKey& key = iter->second;

    dout(0) << "id: " << iter->first << dendl;
    dout(0) << "key.expiration: " << key.expiration << dendl;
    bufferptr& bp = key.key.get_secret();
    bufferlist bl;
    bl.append(bp);
    hexdump(" key", bl.c_str(), bl.length());
  }
}

bool KeyRing::need_rotating_secrets()
{
  Mutex::Locker l(lock);

  if (rotating_secrets.secrets.size() < KEY_ROTATE_NUM)
    return true;

  map<uint64_t, ExpiringCryptoKey>::iterator iter = rotating_secrets.secrets.lower_bound(0);
  ExpiringCryptoKey& key = iter->second;
  if (key.expiration < g_clock.now()) {
    dout(0) << "key.expiration=" << key.expiration << " now=" << g_clock.now() << dendl;
    return true;
  }

  return false;
}

bool KeyRing::get_service_secret(uint32_t service_id, uint64_t secret_id, CryptoKey& secret)
{
  Mutex::Locker l(lock);
  /* we ignore the service id, there's only one service id that we're handling */

{
  map<uint64_t, ExpiringCryptoKey>::iterator iter = rotating_secrets.secrets.begin();
  dout(0) << "dumping rotating secrets: this=" << dendl;

  for (; iter != rotating_secrets.secrets.end(); ++iter) {
    ExpiringCryptoKey& key = iter->second;

    dout(0) << "id: " << iter->first << dendl;
    dout(0) << "key.expiration: " << key.expiration << dendl;
    bufferptr& bp = key.key.get_secret();
    bufferlist bl;
    bl.append(bp);
    hexdump(" key", bl.c_str(), bl.length());
  }
}

  map<uint64_t, ExpiringCryptoKey>::iterator iter = rotating_secrets.secrets.find(secret_id);
  if (iter == rotating_secrets.secrets.end()) {
    dout(0) << "could not find secret_id=" << secret_id << dendl;
    return false;
  }

  ExpiringCryptoKey& key = iter->second;
  if (key.expiration > g_clock.now()) {
    secret = key.key;
    return true;
  }
  dout(0) << "secret expired!" << dendl;
  return false;
}


