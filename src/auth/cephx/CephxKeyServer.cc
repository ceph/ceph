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

#include "CephxKeyServer.h"
#include "common/Timer.h"

#include <sstream>


void RotatingSecrets::add(ExpiringCryptoKey& key)
{
  secrets[++max_ver] = key;

  while (secrets.size() > KEY_ROTATE_NUM) {
    map<uint64_t, ExpiringCryptoKey>::iterator iter = secrets.lower_bound(0);
    secrets.erase(iter);
  }
}

bool KeyServerData::get_service_secret(uint32_t service_id, ExpiringCryptoKey& secret, uint64_t& secret_id)
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

bool KeyServerData::get_service_secret(uint32_t service_id, CryptoKey& secret, uint64_t& secret_id)
{
  ExpiringCryptoKey e;

  if (!get_service_secret(service_id, e, secret_id))
    return false;

  secret = e.key;

  return true;
}

bool KeyServerData::get_service_secret(uint32_t service_id, uint64_t secret_id, CryptoKey& secret)
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

bool KeyServerData::get_secret(EntityName& name, CryptoKey& secret)
{
  map<EntityName, EntityAuth>::iterator iter = secrets.find(name);
  if (iter == secrets.end())
    return false;

  secret = iter->second.key;

  return true;
}

bool KeyServerData::get_caps(EntityName& name, string& type, AuthCapsInfo& caps_info)
{
  caps_info.allow_all = false;

  dout(0) << "get_caps: name=" << name.to_str() << dendl;
  map<EntityName, EntityAuth>::iterator iter = secrets.find(name);
  if (iter == secrets.end())
    return false;

  dout(0) << "get_secret: num of caps=" << iter->second.caps.size() << dendl;
  map<string, bufferlist>::iterator capsiter = iter->second.caps.find(type);
  if (capsiter != iter->second.caps.end()) {
    caps_info.caps = capsiter->second;
  }

  return true;
}

KeyServer::KeyServer() : lock("KeyServer::lock")
{
}

int KeyServer::start_server(bool init)
{
  Mutex::Locker l(lock);

  if (init) {
    _generate_all_rotating_secrets(init);
  }
  return 0;
}

void KeyServer::_generate_all_rotating_secrets(bool init)
{
  data.rotating_ver++;
  data.next_rotating_time = g_clock.now();
  data.next_rotating_time += g_conf.auth_mon_ticket_ttl;
  dout(0) << "generate_all_rotating_secrets()" << dendl;

  int i = KEY_ROTATE_NUM;

  if (init)
    i = 1;

  for (; i <= KEY_ROTATE_NUM; i++) {
    _rotate_secret(CEPH_ENTITY_TYPE_AUTH, i);
    _rotate_secret(CEPH_ENTITY_TYPE_MON, i);
    _rotate_secret(CEPH_ENTITY_TYPE_OSD, i);
    _rotate_secret(CEPH_ENTITY_TYPE_MDS, i);
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

void KeyServer::_rotate_secret(uint32_t service_id, int factor)
{
  ExpiringCryptoKey ek;
  generate_secret(ek.key);
  ek.expiration = g_clock.now();
  ek.expiration += (g_conf.auth_mon_ticket_ttl * factor);
  
  data.add_rotating_secret(service_id, ek);
}

bool KeyServer::_check_rotate()
{
  if (g_clock.now() > data.next_rotating_time) {
    dout(0) << "KeyServer::check_rotate: need to rotate keys" << dendl;
    _generate_all_rotating_secrets(false);
    return true;
  }
  return false;
}

bool KeyServer::get_secret(EntityName& name, CryptoKey& secret)
{
  Mutex::Locker l(lock);

  return data.get_secret(name, secret);
}

bool KeyServer::get_caps(EntityName& name, string& type, AuthCapsInfo& caps_info)
{
  Mutex::Locker l(lock);

  return data.get_caps(name, type, caps_info);
}

bool KeyServer::get_service_secret(uint32_t service_id, ExpiringCryptoKey& secret, uint64_t& secret_id)
{
  Mutex::Locker l(lock);

  return data.get_service_secret(service_id, secret, secret_id);
}

bool KeyServer::get_service_secret(uint32_t service_id, CryptoKey& secret, uint64_t& secret_id)
{
  Mutex::Locker l(lock);

  return data.get_service_secret(service_id, secret, secret_id);
}

bool KeyServer::get_service_secret(uint32_t service_id, uint64_t secret_id, CryptoKey& secret)
{
  Mutex::Locker l(lock);

  return data.get_service_secret(service_id, secret_id, secret);
}

bool KeyServer::generate_secret(CryptoKey& secret)
{
  bufferptr bp;
  CryptoHandler *crypto = ceph_crypto_mgr.get_crypto(CEPH_CRYPTO_AES);
  if (!crypto)
    return false;

  if (crypto->create(bp) < 0)
    return false;

  secret.set_secret(CEPH_CRYPTO_AES, bp);

  return true;
}

bool KeyServer::generate_secret(EntityName& name, CryptoKey& secret)
{
  if (!generate_secret(secret))
    return false;

  Mutex::Locker l(lock);

  EntityAuth auth;
  auth.key = secret;

  data.add_auth(name, auth);

  return true;
}

bool KeyServer::contains(EntityName& name)
{
  Mutex::Locker l(lock);

  return data.contains(name);
}

void KeyServer::list_secrets(stringstream& ss)
{
  Mutex::Locker l(lock);

  map<EntityName, EntityAuth>::iterator mapiter = data.secrets_begin();
  if (mapiter != data.secrets_end()) {
    ss << "installed auth entries: " << std::endl;      

    while (mapiter != data.secrets_end()) {
      const EntityName& name = mapiter->first;
      ss << name.to_str() << std::endl;

      ss << "\tkey: " << mapiter->second.key << std::endl;

      map<string, bufferlist>::iterator capsiter = mapiter->second.caps.begin();
      for (; capsiter != mapiter->second.caps.end(); ++capsiter) {
        bufferlist::iterator dataiter = capsiter->second.begin();
        string caps;
        ::decode(caps, dataiter);
	ss << "\tcaps: [" << capsiter->first << "] " << caps << std::endl;
      }
     
      ++mapiter;
    }
  } else {
    ss << "no installed auth entries!";
  }
}

bool KeyServer::updated_rotating(bufferlist& rotating_bl, version_t& rotating_ver)
{
  Mutex::Locker l(lock);

  _check_rotate(); 

  if (data.rotating_ver <= rotating_ver)
    return false;
 
  data.encode_rotating(rotating_bl);

  rotating_ver = data.rotating_ver;

  return true;
}

bool KeyServer::get_rotating_encrypted(EntityName& name, bufferlist& enc_bl)
{
  Mutex::Locker l(lock);

  map<EntityName, EntityAuth>::iterator mapiter = data.find_name(name);
  if (mapiter == data.secrets_end())
    return false;

  CryptoKey& specific_key = mapiter->second.key;

  map<uint32_t, RotatingSecrets>::iterator rotate_iter = data.rotating_secrets.find(name.entity_type);
  if (rotate_iter == data.rotating_secrets.end())
    return false;

  RotatingSecrets secrets = rotate_iter->second;

  encode_encrypt(secrets, specific_key, enc_bl);

  return true;
}

bool KeyServer::_get_service_caps(EntityName& name, uint32_t service_id, AuthCapsInfo& caps_info)
{
  string s = ceph_entity_type_name(service_id);

  return data.get_caps(name, s, caps_info);
}

bool KeyServer::get_service_caps(EntityName& name, uint32_t service_id, AuthCapsInfo& caps_info)
{
  Mutex::Locker l(lock);
  return _get_service_caps(name, service_id, caps_info);
}


int KeyServer::_build_session_auth_info(uint32_t service_id, CephXServiceTicketInfo& auth_ticket_info, CephXSessionAuthInfo& info)
{
  info.ticket.name = auth_ticket_info.ticket.name;
  info.ticket.global_id = auth_ticket_info.ticket.global_id;
  info.ticket.init_timestamps(g_clock.now(), g_conf.auth_service_ticket_ttl);

  generate_secret(info.session_key);

  info.service_id = service_id;

  string s = ceph_entity_type_name(service_id);

  if (!data.get_caps(info.ticket.name, s, info.ticket.caps)) {
    return -EINVAL;
  }

  return 0;
}

int KeyServer::build_session_auth_info(uint32_t service_id, CephXServiceTicketInfo& auth_ticket_info, CephXSessionAuthInfo& info)
{
  if (get_service_secret(service_id, info.service_secret, info.secret_id) < 0) {
    return -EPERM;
  }

  Mutex::Locker l(lock);

  return _build_session_auth_info(service_id, auth_ticket_info, info);
}

int KeyServer::build_session_auth_info(uint32_t service_id, CephXServiceTicketInfo& auth_ticket_info, CephXSessionAuthInfo& info,
                                        CryptoKey& service_secret, uint64_t secret_id)
{
  info.service_secret = service_secret;
  info.secret_id = secret_id;

  return _build_session_auth_info(service_id, auth_ticket_info, info);
}

