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

#include "Crypto.h"
#include "auth/KeyRing.h"
#include "auth/KeysServer.h"

using namespace std;

KeyRing g_keyring;

bool KeyRing::load_master(const char *filename)
{
  int fd = open(filename, O_RDONLY);
  if (fd < 0) {
    dout(0) << "can't open key file " << filename << dendl;
    return false;
  }

  // get size
  struct stat st;
  int rc = fstat(fd, &st);
  if (rc != 0) {
    dout(0) << "error stat'ing key file " << filename << dendl;
    return false;
  }
  __int32_t len = st.st_size;
 
  bufferlist bl;

  bufferptr bp(len);
  int off = 0;
  while (off < len) {
    int r = read(fd, bp.c_str()+off, len-off);
    if (r < 0) {
      derr(0) << "errno on read " << strerror(errno) << dendl;
      return false;
    }
    off += r;
  }
  bl.append(bp);
  close(fd);

  bufferlist::iterator iter = bl.begin();

  map<string, CryptoKey> m;
  map<string, CryptoKey>::iterator miter;

  ::decode(m, iter);

  string name = g_conf.entity_name->to_str();

  dout(0) << "looking for key entry name=" << name << dendl;

  miter = m.find(name);
  if (miter == m.end()) {
    miter = m.find("");
    if (miter == m.end())
      return false; 
  }
  master = miter->second;

  return true;
}

void KeyRing::set_rotating(RotatingSecrets& secrets)
{
  Mutex::Locker l(lock);

  rotating_secrets = secrets;

  dout(0) << "KeyRing::set_rotating max_ver=" << secrets.max_ver << dendl;

  map<uint64_t, ExpiringCryptoKey>::iterator iter = secrets.secrets.begin();

  for (; iter != secrets.secrets.end(); ++iter) {
    ExpiringCryptoKey& key = iter->second;

    dout(0) << "key.expiration: " << key.expiration << dendl;
    bufferptr& bp = key.key.get_secret();
    bufferlist bl;
    bl.append(bp);
    hexdump(" key", bl.c_str(), bl.length());
  }
}

void KeyRing::get_master(CryptoKey& dest)
{
  Mutex::Locker l(lock);

  dest = master;
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

  map<uint64_t, ExpiringCryptoKey>::iterator iter = rotating_secrets.secrets.find(secret_id);
  if (iter == rotating_secrets.secrets.end())
    return false;

  ExpiringCryptoKey& key = iter->second;
  if (key.expiration < g_clock.now()) {
    secret = key.key;
    return true;
  }
  return false;
}

