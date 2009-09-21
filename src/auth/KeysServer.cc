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

#include <sstream>

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

void RotatingSecrets::add(ExpiringCryptoKey& key)
{
  secrets[++max_ver] = key;

  while (secrets.size() > KEY_ROTATE_NUM) {
    map<uint64_t, ExpiringCryptoKey>::iterator iter = secrets.lower_bound(0);
    secrets.erase(iter);
  }
}

bool KeysServerData::get_service_secret(uint32_t service_id, RotatingSecrets& secret)
{
  map<uint32_t, RotatingSecrets>::iterator iter = rotating_secrets.find(service_id);
  if (iter == rotating_secrets.end())
    return false;

  secret = iter->second;
  return true;
}

bool KeysServerData::get_secret(EntityName& name, CryptoKey& secret)
{
  map<EntityName, CryptoKey>::iterator iter = secrets.find(name);
  if (iter == secrets.end())
    return false;

  secret = iter->second;
  return true;
}

KeysServer::KeysServer() : lock("KeysServer::lock"), timer(lock)
{
}

int KeysServer::start_server(bool init)
{
  Mutex::Locker l(lock);

  if (init) {
    _generate_all_rotating_secrets();
  }
  rotate_event = new C_RotateTimeout(this, KEY_ROTATE_TIME);
  if (!rotate_event)
    return -ENOMEM;
  timer.add_event_after(KEY_ROTATE_TIME, rotate_event);
  return 0;
}

void KeysServer::_generate_all_rotating_secrets()
{
  data.rotating_ver++;
  dout(0) << "generate_all_rotating_secrets()" << dendl;
  _rotate_secret(CEPHX_PRINCIPAL_MON);
  _rotate_secret(CEPHX_PRINCIPAL_OSD);
  _rotate_secret(CEPHX_PRINCIPAL_MDS);

  dout(0) << "generated: " << dendl;
  
  map<uint32_t, RotatingSecrets>::iterator iter = data.rotating_secrets.begin();

  for (; iter != data.rotating_secrets.end(); ++iter) {
    dout(0) << "service id: " << iter->first << dendl;
    RotatingSecrets& key = iter->second;

    map<uint64_t, ExpiringCryptoKey>::iterator mapiter = key.secrets.begin();
    for (; mapiter != key.secrets.end(); ++mapiter) {
      dout(0) << "  id: " << mapiter->first << dendl;
      bufferptr bp = mapiter->second.key.get_secret();
      hexdump("    key", bp.c_str(), bp.length());
      dout(0) << "    expiration: " << mapiter->second.expiration << dendl;
    }
  }
}

void KeysServer::_rotate_secret(uint32_t service_id)
{
  ExpiringCryptoKey ek;
  generate_secret(ek.key);
  ek.expiration = g_clock.now();
  ek.expiration += (KEY_ROTATE_TIME * 3);
  
  data.add_rotating_secret(service_id, ek);
}

void KeysServer::rotate_timeout(double timeout)
{
  dout(0) << "KeysServer::rotate_timeout" << dendl;
  _generate_all_rotating_secrets();

  rotate_event = new C_RotateTimeout(this, timeout);
  timer.add_event_after(timeout, rotate_event);
}

bool KeysServer::get_secret(EntityName& name, CryptoKey& secret)
{
  Mutex::Locker l(lock);

  return data.get_secret(name, secret);
}

bool KeysServer::get_service_secret(uint32_t service_id, RotatingSecrets& secret)
{
  Mutex::Locker l(lock);

  return data.get_service_secret(service_id, secret);
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

  Mutex::Locker l(lock);

  data.add_secret(name, secret);

  return true;
}

bool KeysServer::contains(EntityName& name)
{
  Mutex::Locker l(lock);

  return data.contains(name);
}

void KeysServer::list_secrets(stringstream& ss)
{
  map<EntityName, CryptoKey>::iterator mapiter = data.secrets_begin();
  if (mapiter != data.secrets_end()) {
    ss << "installed auth entries: " << std::endl;      

    while (mapiter != data.secrets_end()) {
      const EntityName& name = mapiter->first;
      ss << name.to_str() << std::endl;
      
      ++mapiter;
    }
  } else {
    ss << "no installed auth entries!";
  }
}

bool KeysServer::updated_rotating(bufferlist& rotating_bl, version_t& rotating_ver)
{
  Mutex::Locker l(lock);

  if (data.rotating_ver <= rotating_ver)
    return false;

  ::encode(data.rotating_ver, rotating_bl);
  ::encode(data.rotating_secrets, rotating_bl);

  rotating_ver = data.rotating_ver;

  return true;
}

void KeysServer::decode_rotating(bufferlist& rotating_bl)
{
  Mutex::Locker l(lock);

  bufferlist::iterator iter = rotating_bl.begin();

  ::decode(data.rotating_ver, iter);
  ::decode(data.rotating_secrets, iter);
}

bool KeysServer::get_rotating_encrypted(EntityName& name, bufferlist& enc_bl)
{
  Mutex::Locker l(lock);

  map<EntityName, CryptoKey>::iterator mapiter = data.find_name(name);
  if (mapiter == data.secrets_end())
    return false;

  CryptoKey& specific_key = mapiter->second;

  map<uint32_t, RotatingSecrets>::iterator rotate_iter = data.rotating_secrets.find(name.entity_type);
  if (rotate_iter == data.rotating_secrets.end())
    return false;

  RotatingSecrets secrets = rotate_iter->second;

  encode_encrypt(secrets, specific_key, enc_bl);
}

