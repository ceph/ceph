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


void RotatingSecrets::add(ExpiringCryptoKey& key)
{
  secrets[++max_ver] = key;

  while (secrets.size() > KEY_ROTATE_NUM) {
    map<uint64_t, ExpiringCryptoKey>::iterator iter = secrets.lower_bound(0);
    secrets.erase(iter);
  }
}

bool KeysServerData::get_service_secret(uint32_t service_id, ExpiringCryptoKey& secret, uint64_t& secret_id)
{
  map<uint32_t, RotatingSecrets>::iterator iter = rotating_secrets.find(service_id);
  if (iter == rotating_secrets.end())
    return false;

  RotatingSecrets& secrets = iter->second;
  map<uint64_t, ExpiringCryptoKey>::iterator riter = secrets.secrets.lower_bound(0);
  if (secrets.secrets.size() > 1)
    ++riter;


  secret_id = riter->first;
  secret = riter->second;

  return true;
}

bool KeysServerData::get_service_secret(uint32_t service_id, CryptoKey& secret, uint64_t& secret_id)
{
  ExpiringCryptoKey e;

  if (!get_service_secret(service_id, e, secret_id))
    return false;

  secret = e.key;

  return true;
}

bool KeysServerData::get_service_secret(uint32_t service_id, uint64_t secret_id, CryptoKey& secret)
{
  map<uint32_t, RotatingSecrets>::iterator iter = rotating_secrets.find(service_id);
  if (iter == rotating_secrets.end())
    return false;

  RotatingSecrets& secrets = iter->second;
  map<uint64_t, ExpiringCryptoKey>::iterator riter = secrets.secrets.find(secret_id);

  if (riter == secrets.secrets.end())
    return false;

  secret = riter->second.key;

  return true;
}

bool KeysServerData::get_secret(EntityName& name, CryptoKey& secret, map<string,bufferlist>& caps)
{
  map<EntityName, CryptoKey>::iterator iter = secrets.find(name);
  if (iter == secrets.end())
    return false;

  secret = iter->second;

  return true;
}

KeysServer::KeysServer() : lock("KeysServer::lock")
{
}

int KeysServer::start_server(bool init)
{
  Mutex::Locker l(lock);

  if (init) {
    _generate_all_rotating_secrets(init);
  }
  return 0;
}

void KeysServer::_generate_all_rotating_secrets(bool init)
{
  data.rotating_ver++;
  data.next_rotating_time = g_clock.now();
  data.next_rotating_time += KEY_ROTATE_TIME;
  dout(0) << "generate_all_rotating_secrets()" << dendl;

  int i = KEY_ROTATE_NUM;

  if (init)
    i = 1;

  for (; i <= KEY_ROTATE_NUM; i++) {
    _rotate_secret(CEPHX_PRINCIPAL_AUTH, i);
    _rotate_secret(CEPHX_PRINCIPAL_MON, i);
    _rotate_secret(CEPHX_PRINCIPAL_OSD, i);
    _rotate_secret(CEPHX_PRINCIPAL_MDS, i);
  }

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

void KeysServer::_rotate_secret(uint32_t service_id, int factor)
{
  ExpiringCryptoKey ek;
  generate_secret(ek.key);
  ek.expiration = g_clock.now();
  ek.expiration += (KEY_ROTATE_TIME * factor);
  
  data.add_rotating_secret(service_id, ek);
}

bool KeysServer::_check_rotate()
{
  if (g_clock.now() > data.next_rotating_time) {
    dout(0) << "KeysServer::check_rotate: need to rotate keys" << dendl;
    _generate_all_rotating_secrets(false);
    return true;
  }
  return false;
}

bool KeysServer::get_secret(EntityName& name, CryptoKey& secret, map<string,bufferlist>& caps)
{
  Mutex::Locker l(lock);

  return data.get_secret(name, secret, caps);
}

bool KeysServer::get_service_secret(uint32_t service_id, ExpiringCryptoKey& secret, uint64_t& secret_id)
{
  Mutex::Locker l(lock);

  return data.get_service_secret(service_id, secret, secret_id);
}

bool KeysServer::get_service_secret(uint32_t service_id, CryptoKey& secret, uint64_t& secret_id)
{
  Mutex::Locker l(lock);

  return data.get_service_secret(service_id, secret, secret_id);
}

bool KeysServer::get_service_secret(uint32_t service_id, uint64_t secret_id, CryptoKey& secret)
{
  Mutex::Locker l(lock);

  return data.get_service_secret(service_id, secret_id, secret);
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
  Mutex::Locker l(lock);

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

  _check_rotate(); 

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

  return true;
}

int KeysServer::build_session_auth_info(uint32_t service_id, AuthServiceTicketInfo& auth_ticket_info, SessionAuthInfo& info)
{
  if (get_service_secret(service_id, info.service_secret, info.secret_id) < 0) {
    return -EPERM;
  }

  info.ticket.name = auth_ticket_info.ticket.name;
  info.ticket.addr = auth_ticket_info.ticket.addr;
  info.ticket.init_timestamps(g_clock.now(), g_conf.auth_service_ticket_ttl);

  generate_secret(info.session_key);

  info.service_id = service_id;
	  
  info.ticket.caps = auth_ticket_info.ticket.caps;

  return 0;
}

