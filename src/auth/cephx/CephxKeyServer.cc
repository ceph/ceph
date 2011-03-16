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

#include "common/config.h"

#include "CephxKeyServer.h"
#include "common/Timer.h"

#include <sstream>

#define DOUT_SUBSYS auth
#undef dout_prefix
#define dout_prefix *_dout << "cephx keyserverdata: "

bool KeyServerData::get_service_secret(uint32_t service_id, ExpiringCryptoKey& secret, uint64_t& secret_id)
{
  map<uint32_t, RotatingSecrets>::iterator iter = rotating_secrets.find(service_id);
  if (iter == rotating_secrets.end()) { 
    dout(10) << "get_service_secret service " << ceph_entity_type_name(service_id) << " not found " << dendl;
    return false;
  }

  RotatingSecrets& secrets = iter->second;

  // second to oldest, unless it's expired
  map<uint64_t, ExpiringCryptoKey>::iterator riter = secrets.secrets.begin();
  if (secrets.secrets.size() > 1)
    ++riter;

  if (riter->second.expiration < g_clock.now())
    ++riter;   // "current" key has expired, use "next" key instead

  secret_id = riter->first;
  secret = riter->second;
  dout(10) << "get_service_secret service " << ceph_entity_type_name(service_id)
	   << " id " << secret_id << " " << secret << dendl;
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

  if (riter == secrets.secrets.end()) {
    dout(10) << "get_service_secret service " << ceph_entity_type_name(service_id) << " secret " << secret_id
            << " not found; i have:" << dendl;
    for (map<uint64_t, ExpiringCryptoKey>::iterator iter = secrets.secrets.begin();
        iter != secrets.secrets.end();
        ++iter)
      dout(10) << " id " << iter->first << " " << iter->second << dendl;
    return false;
  }

  secret = riter->second.key;

  return true;
}
bool KeyServerData::get_auth(EntityName& name, EntityAuth& auth) {
  map<EntityName, EntityAuth>::iterator iter = secrets.find(name);
  if (iter == secrets.end())
    return false;
  auth = iter->second;
  return true;
}

bool KeyServerData::get_secret(EntityName& name, CryptoKey& secret) {
  map<EntityName, EntityAuth>::iterator iter = secrets.find(name);
  if (iter == secrets.end())
    return false;
  secret = iter->second.key;
  return true;
}

bool KeyServerData::get_caps(EntityName& name, string& type, AuthCapsInfo& caps_info)
{
  caps_info.allow_all = false;

  dout(10) << "get_caps: name=" << name.to_str() << dendl;
  map<EntityName, EntityAuth>::iterator iter = secrets.find(name);
  if (iter == secrets.end())
    return false;

  dout(10) << "get_secret: num of caps=" << iter->second.caps.size() << dendl;
  map<string, bufferlist>::iterator capsiter = iter->second.caps.find(type);
  if (capsiter != iter->second.caps.end()) {
    caps_info.caps = capsiter->second;
  }

  return true;
}


#undef dout_prefix
#define dout_prefix *_dout << "cephx keyserver: "


KeyServer::KeyServer() : lock("KeyServer::lock")
{
}

int KeyServer::start_server()
{
  Mutex::Locker l(lock);

  _check_rotating_secrets();
  _dump_rotating_secrets();
  return 0;
}

bool KeyServer::_check_rotating_secrets()
{
  dout(10) << "_check_rotating_secrets" << dendl;

  int added = 0;
  added += _rotate_secret(CEPH_ENTITY_TYPE_AUTH);
  added += _rotate_secret(CEPH_ENTITY_TYPE_MON);
  added += _rotate_secret(CEPH_ENTITY_TYPE_OSD);
  added += _rotate_secret(CEPH_ENTITY_TYPE_MDS);

  if (added) {
    data.rotating_ver++;
    //data.next_rotating_time = g_clock.now();
    //data.next_rotating_time += MIN(g_conf.auth_mon_ticket_ttl, g_conf.auth_service_ticket_ttl);
    _dump_rotating_secrets();
    return true;
  }
  return false;
}

void KeyServer::_dump_rotating_secrets()
{
  dout(10) << "_dump_rotating_secrets" << dendl;
  for (map<uint32_t, RotatingSecrets>::iterator iter = data.rotating_secrets.begin();
       iter != data.rotating_secrets.end();
       ++iter) {
    RotatingSecrets& key = iter->second;
    for (map<uint64_t, ExpiringCryptoKey>::iterator mapiter = key.secrets.begin();
	 mapiter != key.secrets.end();
	 ++mapiter)
      dout(10) << "service " << ceph_entity_type_name(iter->first)
	       << " id " << mapiter->first
	       << " key " << mapiter->second
	       << dendl;
  }
}

int KeyServer::_rotate_secret(uint32_t service_id)
{
  RotatingSecrets& r = data.rotating_secrets[service_id];
  int added = 0;
  utime_t now = g_clock.now();
  double ttl = service_id == CEPH_ENTITY_TYPE_AUTH ? g_conf.auth_mon_ticket_ttl : g_conf.auth_service_ticket_ttl;

  while (r.need_new_secrets(now)) {
    ExpiringCryptoKey ek;
    generate_secret(ek.key);
    if (r.empty()) {
      ek.expiration = now;
    } else {
      utime_t next_ttl = now;
      next_ttl += ttl;
      ek.expiration = MAX(next_ttl, r.next().expiration);
    }
    ek.expiration += ttl;
    uint64_t secret_id = r.add(ek);
    dout(10) << "_rotate_secret adding " << ceph_entity_type_name(service_id)
	     << " id " << secret_id << " " << ek
	     << dendl;
    added++;
  }
  return added;
}

bool KeyServer::get_secret(EntityName& name, CryptoKey& secret)
{
  Mutex::Locker l(lock);
  return data.get_secret(name, secret);
}

bool KeyServer::get_auth(EntityName& name, EntityAuth& auth)
{
  Mutex::Locker l(lock);
  return data.get_auth(name, auth);
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

  _check_rotating_secrets(); 

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

  map<uint32_t, RotatingSecrets>::iterator rotate_iter =
    data.rotating_secrets.find(name.get_type());
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


int KeyServer::_build_session_auth_info(uint32_t service_id, CephXServiceTicketInfo& auth_ticket_info,
					CephXSessionAuthInfo& info)
{
  info.service_id = service_id;
  info.ticket = auth_ticket_info.ticket;
  info.ticket.init_timestamps(g_clock.now(), g_conf.auth_service_ticket_ttl);

  generate_secret(info.session_key);

  string s = ceph_entity_type_name(service_id);
  if (!data.get_caps(info.ticket.name, s, info.ticket.caps)) {
    return -EINVAL;
  }

  return 0;
}

int KeyServer::build_session_auth_info(uint32_t service_id, CephXServiceTicketInfo& auth_ticket_info,
				       CephXSessionAuthInfo& info)
{
  if (!get_service_secret(service_id, info.service_secret, info.secret_id)) {
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

