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

#include "config.h"

#include "KeysServer.h"

#include "Crypto.h"
#include "common/Timer.h"
#include "Auth.h"
#include "AuthProtocol.h"

static void hexdump(string msg, const char *s, int len)
{
  int buf_len = len*4;
  char buf[buf_len];
  int pos = 0;
  for (int i=0; i<len && pos<buf_len - 8; i++) {
    if (i && !(i%8))
      pos += snprintf(&buf[pos], buf_len-pos, " ");
    if (i && !(i%16))
      pos += snprintf(&buf[pos], buf_len-pos, "\n");
    pos += snprintf(&buf[pos], buf_len-pos, "%.2x ", (int)(unsigned char)s[i]);
  }
  dout(0) << msg << ":\n" << buf << dendl;
}



KeysServer::KeysServer() : rotating_lock("KeysServer::rotating_lock"),
                 secrets_lock("KeysServer::secrets_lock"), timer(rotating_lock)
{
  Mutex::Locker l(rotating_lock);

  generate_all_rotating_secrets();

  rotate_event = new C_RotateTimeout(this, KEY_ROTATE_TIME);
  timer.add_event_after(KEY_ROTATE_TIME, rotate_event);
}

void KeysServer::generate_all_rotating_secrets()
{
  dout(0) << "generate_all_rotating_secrets()" << dendl;
  _rotate_secret(CEPHX_PRINCIPAL_MON);
  _rotate_secret(CEPHX_PRINCIPAL_OSD);
  _rotate_secret(CEPHX_PRINCIPAL_MDS);

  dout(0) << "generated: " << dendl;
  
  map<uint32_t, RotatingSecret>::iterator iter = rotating_secrets.begin();

  for (; iter != rotating_secrets.end(); ++iter) {
    dout(0) << "service id: " << iter->first << dendl;
    RotatingSecret& key = iter->second;
    bufferptr bp = key.secret.get_secret();
    hexdump("key", bp.c_str(), bp.length());
    dout(0) << "expiration: " << key.expiration << dendl;
  }
}

void KeysServer::_rotate_secret(uint32_t service_id)
{
  RotatingSecret secret;
  generate_secret(secret.secret);
  secret.expiration = g_clock.now();
  secret.expiration += (KEY_ROTATE_TIME * 3);

  rotating_secrets[service_id] = secret;
}

void KeysServer::rotate_timeout(double timeout)
{
  generate_all_rotating_secrets();

  rotate_event = new C_RotateTimeout(this, timeout);
  timer.add_event_after(timeout, rotate_event);
}

bool KeysServer::get_secret(EntityName& name, CryptoKey& secret)
{
  Mutex::Locker l(secrets_lock);

  map<EntityName, CryptoKey>::iterator iter = secrets.find(name);
  if (iter == secrets.end())
    return false;

  secret = iter->second;
  return true;
}

bool KeysServer::get_service_secret(uint32_t service_id, RotatingSecret& secret)
{
  Mutex::Locker l(rotating_lock);

  map<uint32_t, RotatingSecret>::iterator iter = rotating_secrets.find(service_id);
  if (iter == rotating_secrets.end())
    return false;

  secret = iter->second;
  return true;
}

bool KeysServer::generate_secret(CryptoKey& secret)
{
  bufferptr bp;
  CryptoHandler *crypto = ceph_crypto_mgr.get_crypto(CEPH_SECRET_AES);
  if (!crypto)
    return false;

  if (crypto->create(bp) < 0)
    return false;

  secret.set_secret(CEPH_SECRET_AES, bp);

  return true;
}

bool KeysServer::generate_secret(EntityName& name, CryptoKey& secret)
{
  if (!generate_secret(secret))
    return false;

  Mutex::Locker l(secrets_lock);

  secrets[name] = secret;

  return true;
}

