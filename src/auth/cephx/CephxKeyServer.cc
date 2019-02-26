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
#include "common/dout.h"
#include <sstream>

#define dout_subsys ceph_subsys_auth
#undef dout_prefix
#define dout_prefix *_dout << "cephx keyserverdata: "

bool KeyServerData::get_service_secret(CephContext *cct, uint32_t service_id,
			    ExpiringCryptoKey& secret, uint64_t& secret_id) const
{
  map<uint32_t, RotatingSecrets>::const_iterator iter =
	rotating_secrets.find(service_id);
  if (iter == rotating_secrets.end()) { 
    ldout(cct, 10) << "get_service_secret service " << ceph_entity_type_name(service_id) << " not found " << dendl;
    return false;
  }

  const RotatingSecrets& secrets = iter->second;

  // second to oldest, unless it's expired
  map<uint64_t, ExpiringCryptoKey>::const_iterator riter =
	secrets.secrets.begin();
  if (secrets.secrets.size() > 1)
    ++riter;

  if (riter->second.expiration < ceph_clock_now())
    ++riter;   // "current" key has expired, use "next" key instead

  secret_id = riter->first;
  secret = riter->second;
  ldout(cct, 30) << "get_service_secret service " << ceph_entity_type_name(service_id)
	   << " id " << secret_id << " " << secret << dendl;
  return true;
}

bool KeyServerData::get_service_secret(CephContext *cct, uint32_t service_id,
				CryptoKey& secret, uint64_t& secret_id) const
{
  ExpiringCryptoKey e;

  if (!get_service_secret(cct, service_id, e, secret_id))
    return false;

  secret = e.key;
  return true;
}

bool KeyServerData::get_service_secret(CephContext *cct, uint32_t service_id,
				uint64_t secret_id, CryptoKey& secret) const
{
  map<uint32_t, RotatingSecrets>::const_iterator iter =
      rotating_secrets.find(service_id);
  if (iter == rotating_secrets.end())
    return false;

  const RotatingSecrets& secrets = iter->second;
  map<uint64_t, ExpiringCryptoKey>::const_iterator riter = 
      secrets.secrets.find(secret_id);

  if (riter == secrets.secrets.end()) {
    ldout(cct, 10) << "get_service_secret service " << ceph_entity_type_name(service_id)
	     << " secret " << secret_id << " not found" << dendl;
    ldout(cct, 30) << " I have:" << dendl;
    for (map<uint64_t, ExpiringCryptoKey>::const_iterator iter =
	     secrets.secrets.begin();
        iter != secrets.secrets.end();
        ++iter)
      ldout(cct, 30) << " id " << iter->first << " " << iter->second << dendl;
    return false;
  }

  secret = riter->second.key;

  return true;
}
bool KeyServerData::get_auth(const EntityName& name, EntityAuth& auth) const {
  map<EntityName, EntityAuth>::const_iterator iter = secrets.find(name);
  if (iter != secrets.end()) {
    auth = iter->second;
    return true;
  }
  return extra_secrets->get_auth(name, auth);
}

bool KeyServerData::get_secret(const EntityName& name, CryptoKey& secret) const {
  map<EntityName, EntityAuth>::const_iterator iter = secrets.find(name);
  if (iter != secrets.end()) {
    secret = iter->second.key;
    return true;
  }
  return extra_secrets->get_secret(name, secret);
}

bool KeyServerData::get_caps(CephContext *cct, const EntityName& name,
			     const string& type, AuthCapsInfo& caps_info) const
{
  caps_info.allow_all = false;

  ldout(cct, 10) << "get_caps: name=" << name.to_str() << dendl;
  map<EntityName, EntityAuth>::const_iterator iter = secrets.find(name);
  if (iter != secrets.end()) {
    ldout(cct, 10) << "get_secret: num of caps=" << iter->second.caps.size() << dendl;
    map<string, bufferlist>::const_iterator capsiter = iter->second.caps.find(type);
    if (capsiter != iter->second.caps.end()) {
      caps_info.caps = capsiter->second;
    }
    return true;
  }

  return extra_secrets->get_caps(name, type, caps_info);
}


#undef dout_prefix
#define dout_prefix *_dout << "cephx keyserver: "


KeyServer::KeyServer(CephContext *cct_, KeyRing *extra_secrets)
  : cct(cct_),
    data(extra_secrets),
    lock{ceph::make_mutex("KeyServer::lock")}
{
}

int KeyServer::start_server()
{
  std::scoped_lock l{lock};

  _check_rotating_secrets();
  _dump_rotating_secrets();
  return 0;
}

bool KeyServer::_check_rotating_secrets()
{
  ldout(cct, 10) << "_check_rotating_secrets" << dendl;

  int added = 0;
  added += _rotate_secret(CEPH_ENTITY_TYPE_AUTH);
  added += _rotate_secret(CEPH_ENTITY_TYPE_MON);
  added += _rotate_secret(CEPH_ENTITY_TYPE_OSD);
  added += _rotate_secret(CEPH_ENTITY_TYPE_MDS);
  added += _rotate_secret(CEPH_ENTITY_TYPE_MGR);

  if (added) {
    ldout(cct, 10) << __func__ << " added " << added << dendl;
    data.rotating_ver++;
    //data.next_rotating_time = ceph_clock_now(cct);
    //data.next_rotating_time += std::min(cct->_conf->auth_mon_ticket_ttl, cct->_conf->auth_service_ticket_ttl);
    _dump_rotating_secrets();
    return true;
  }
  return false;
}

void KeyServer::_dump_rotating_secrets()
{
  ldout(cct, 30) << "_dump_rotating_secrets" << dendl;
  for (map<uint32_t, RotatingSecrets>::iterator iter = data.rotating_secrets.begin();
       iter != data.rotating_secrets.end();
       ++iter) {
    RotatingSecrets& key = iter->second;
    for (map<uint64_t, ExpiringCryptoKey>::iterator mapiter = key.secrets.begin();
	 mapiter != key.secrets.end();
	 ++mapiter)
      ldout(cct, 30) << "service " << ceph_entity_type_name(iter->first)
	             << " id " << mapiter->first
	             << " key " << mapiter->second << dendl;
  }
}

int KeyServer::_rotate_secret(uint32_t service_id)
{
  RotatingSecrets& r = data.rotating_secrets[service_id];
  int added = 0;
  utime_t now = ceph_clock_now();
  double ttl = service_id == CEPH_ENTITY_TYPE_AUTH ? cct->_conf->auth_mon_ticket_ttl : cct->_conf->auth_service_ticket_ttl;

  while (r.need_new_secrets(now)) {
    ExpiringCryptoKey ek;
    generate_secret(ek.key);
    if (r.empty()) {
      ek.expiration = now;
    } else {
      utime_t next_ttl = now;
      next_ttl += ttl;
      ek.expiration = std::max(next_ttl, r.next().expiration);
    }
    ek.expiration += ttl;
    uint64_t secret_id = r.add(ek);
    ldout(cct, 10) << "_rotate_secret adding " << ceph_entity_type_name(service_id) << dendl;
    ldout(cct, 30) << "_rotate_secret adding " << ceph_entity_type_name(service_id)
	           << " id " << secret_id << " " << ek
	           << dendl;
    added++;
  }
  return added;
}

bool KeyServer::get_secret(const EntityName& name, CryptoKey& secret) const
{
  std::scoped_lock l{lock};
  return data.get_secret(name, secret);
}

bool KeyServer::get_auth(const EntityName& name, EntityAuth& auth) const
{
  std::scoped_lock l{lock};
  return data.get_auth(name, auth);
}

bool KeyServer::get_caps(const EntityName& name, const string& type,
	      AuthCapsInfo& caps_info) const
{
  std::scoped_lock l{lock};

  return data.get_caps(cct, name, type, caps_info);
}

bool KeyServer::get_service_secret(uint32_t service_id,
		CryptoKey& secret, uint64_t& secret_id) const
{
  std::scoped_lock l{lock};

  return data.get_service_secret(cct, service_id, secret, secret_id);
}

bool KeyServer::get_service_secret(uint32_t service_id,
		uint64_t secret_id, CryptoKey& secret) const
{
  std::scoped_lock l{lock};

  return data.get_service_secret(cct, service_id, secret_id, secret);
}

bool KeyServer::generate_secret(CryptoKey& secret)
{
  bufferptr bp;
  CryptoHandler *crypto = cct->get_crypto_handler(CEPH_CRYPTO_AES);
  if (!crypto)
    return false;

  if (crypto->create(cct->random(), bp) < 0)
    return false;

  secret.set_secret(CEPH_CRYPTO_AES, bp, ceph_clock_now());

  return true;
}

bool KeyServer::generate_secret(EntityName& name, CryptoKey& secret)
{
  if (!generate_secret(secret))
    return false;

  std::scoped_lock l{lock};

  EntityAuth auth;
  auth.key = secret;

  data.add_auth(name, auth);

  return true;
}

bool KeyServer::contains(const EntityName& name) const
{
  std::scoped_lock l{lock};

  return data.contains(name);
}

int KeyServer::encode_secrets(Formatter *f, stringstream *ds) const
{
  std::scoped_lock l{lock};
  map<EntityName, EntityAuth>::const_iterator mapiter = data.secrets_begin();

  if (mapiter == data.secrets_end())
    return -ENOENT;

  if (f)
    f->open_array_section("auth_dump");

  while (mapiter != data.secrets_end()) {
    const EntityName& name = mapiter->first;
    if (ds) {
      *ds << name.to_str() << std::endl;
      *ds << "\tkey: " << mapiter->second.key << std::endl;
    }
    if (f) {
      f->open_object_section("auth_entities");
      f->dump_string("entity", name.to_str());
      f->dump_stream("key") << mapiter->second.key;
      f->open_object_section("caps");
    }

    map<string, bufferlist>::const_iterator capsiter =
        mapiter->second.caps.begin();
    for (; capsiter != mapiter->second.caps.end(); ++capsiter) {
      // FIXME: need a const_iterator for bufferlist, but it doesn't exist yet.
      bufferlist *bl = const_cast<bufferlist*>(&capsiter->second);
      auto dataiter = bl->cbegin();
      string caps;
      using ceph::decode;
      decode(caps, dataiter);
      if (ds)
        *ds << "\tcaps: [" << capsiter->first << "] " << caps << std::endl;
      if (f)
        f->dump_string(capsiter->first.c_str(), caps);
    }
    if (f) {
      f->close_section(); // caps
      f->close_section(); // auth_entities
    }

    ++mapiter;
  }

  if (f)
    f->close_section(); // auth_dump
  return 0;
}

void KeyServer::encode_formatted(string label, Formatter *f, bufferlist &bl)
{
  ceph_assert(f != NULL);
  f->open_object_section(label.c_str());
  encode_secrets(f, NULL);
  f->close_section();
  f->flush(bl);
}

void KeyServer::encode_plaintext(bufferlist &bl)
{
  stringstream os;
  encode_secrets(NULL, &os);
  bl.append(os.str());
}

bool KeyServer::updated_rotating(bufferlist& rotating_bl, version_t& rotating_ver)
{
  std::scoped_lock l{lock};

  _check_rotating_secrets(); 

  if (data.rotating_ver <= rotating_ver)
    return false;
 
  data.encode_rotating(rotating_bl);

  rotating_ver = data.rotating_ver;

  return true;
}

bool KeyServer::get_rotating_encrypted(const EntityName& name,
	bufferlist& enc_bl) const
{
  std::scoped_lock l{lock};

  map<EntityName, EntityAuth>::const_iterator mapiter = data.find_name(name);
  if (mapiter == data.secrets_end())
    return false;

  const CryptoKey& specific_key = mapiter->second.key;

  map<uint32_t, RotatingSecrets>::const_iterator rotate_iter =
    data.rotating_secrets.find(name.get_type());
  if (rotate_iter == data.rotating_secrets.end())
    return false;

  RotatingSecrets secrets = rotate_iter->second;

  std::string error;
  if (encode_encrypt(cct, secrets, specific_key, enc_bl, error))
    return false;

  return true;
}

bool KeyServer::_get_service_caps(const EntityName& name, uint32_t service_id,
				  AuthCapsInfo& caps_info) const
{
  string s = ceph_entity_type_name(service_id);

  return data.get_caps(cct, name, s, caps_info);
}

bool KeyServer::get_service_caps(const EntityName& name, uint32_t service_id,
				 AuthCapsInfo& caps_info) const
{
  std::scoped_lock l{lock};
  return _get_service_caps(name, service_id, caps_info);
}


int KeyServer::_build_session_auth_info(uint32_t service_id,
					const AuthTicket& parent_ticket,
					CephXSessionAuthInfo& info)
{
  info.service_id = service_id;
  info.ticket = parent_ticket;
  info.ticket.init_timestamps(ceph_clock_now(),
			      cct->_conf->auth_service_ticket_ttl);

  generate_secret(info.session_key);

  // mon keys are stored externally.  and the caps are blank anyway.
  if (service_id != CEPH_ENTITY_TYPE_MON) {
    string s = ceph_entity_type_name(service_id);
    if (!data.get_caps(cct, info.ticket.name, s, info.ticket.caps)) {
      return -EINVAL;
    }
  }
  return 0;
}

int KeyServer::build_session_auth_info(uint32_t service_id,
				       const AuthTicket& parent_ticket,
				       CephXSessionAuthInfo& info)
{
  if (!get_service_secret(service_id, info.service_secret, info.secret_id)) {
    return -EPERM;
  }

  std::scoped_lock l{lock};

  return _build_session_auth_info(service_id, parent_ticket, info);
}

int KeyServer::build_session_auth_info(uint32_t service_id,
				       const AuthTicket& parent_ticket,
				       CephXSessionAuthInfo& info,
				       CryptoKey& service_secret,
				       uint64_t secret_id)
{
  info.service_secret = service_secret;
  info.secret_id = secret_id;

  std::scoped_lock l{lock};
  return _build_session_auth_info(service_id, parent_ticket, info);
}

