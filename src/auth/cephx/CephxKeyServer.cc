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

using std::ostringstream;
using std::string;
using std::stringstream;

using ceph::bufferptr;
using ceph::bufferlist;
using ceph::Formatter;

bool KeyServerData::get_service_secret(CephContext *cct, uint32_t service_id,
				       CryptoKey& secret, uint64_t& secret_id,
				       double& ttl) const
{
  auto iter = rotating_secrets.find(service_id);
  if (iter == rotating_secrets.end()) { 
    ldout(cct, 10) << "get_service_secret service " << ceph_entity_type_name(service_id) << " not found " << dendl;
    return false;
  }

  const RotatingSecrets& secrets = iter->second;

  // second to oldest, unless it's expired
  auto riter = secrets.secrets.begin();
  if (secrets.secrets.size() > 1)
    ++riter;

  utime_t now = ceph_clock_now();
  if (riter->second.expiration < now)
    ++riter;   // "current" key has expired, use "next" key instead

  secret_id = riter->first;
  secret = riter->second.key;

  // ttl may have just been increased by the user
  // cap it by expiration of "next" key to prevent handing out a ticket
  // with a bogus, possibly way into the future, validity
  ttl = service_id == CEPH_ENTITY_TYPE_AUTH ?
      cct->_conf->auth_mon_ticket_ttl : cct->_conf->auth_service_ticket_ttl;
  ttl = std::min(ttl, static_cast<double>(
		     secrets.secrets.rbegin()->second.expiration - now));

  ldout(cct, 30) << __func__ << " service "
		 << ceph_entity_type_name(service_id) << " secret_id "
		 << secret_id << " " << riter->second << " ttl " << ttl
		 << dendl;
  return true;
}

bool KeyServerData::get_service_secret(CephContext *cct, uint32_t service_id,
				uint64_t secret_id, CryptoKey& secret) const
{
  auto iter = rotating_secrets.find(service_id);
  if (iter == rotating_secrets.end()) {
    ldout(cct, 10) << __func__ << " no rotating_secrets for service " << service_id
		   << " " << ceph_entity_type_name(service_id) << dendl;
    return false;
  }

  const RotatingSecrets& secrets = iter->second;
  auto riter = secrets.secrets.find(secret_id);

  if (riter == secrets.secrets.end()) {
    ldout(cct, 10) << "get_service_secret service " << ceph_entity_type_name(service_id)
	     << " secret " << secret_id << " not found" << dendl;
    ldout(cct, 30) << " I have:" << dendl;
    for (auto iter = secrets.secrets.begin();
	 iter != secrets.secrets.end();
	 ++iter)
      ldout(cct, 30) << " id " << iter->first << " " << iter->second << dendl;
    return false;
  }

  secret = riter->second.key;

  return true;
}
bool KeyServerData::get_auth(const EntityName& name, EntityAuth& auth) const {
  auto iter = secrets.find(name);
  if (iter != secrets.end()) {
    auth = iter->second;
    return true;
  }
  return extra_secrets->get_auth(name, auth);
}

bool KeyServerData::get_secret(const EntityName& name, CryptoKey& secret) const {
  auto iter = secrets.find(name);
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
  auto iter = secrets.find(name);
  if (iter != secrets.end()) {
    ldout(cct, 10) << "get_caps: num of caps=" << iter->second.caps.size() << dendl;
    auto capsiter = iter->second.caps.find(type);
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
  _dump_rotating_secrets();
  return 0;
}

void KeyServer::dump()
{
  _dump_rotating_secrets();
}

void KeyServer::_dump_rotating_secrets()
{
  ldout(cct, 30) << "_dump_rotating_secrets" << dendl;
  for (auto iter = data.rotating_secrets.begin();
       iter != data.rotating_secrets.end();
       ++iter) {
    RotatingSecrets& key = iter->second;
    for (auto mapiter = key.secrets.begin();
	 mapiter != key.secrets.end();
	 ++mapiter)
      ldout(cct, 30) << "service " << ceph_entity_type_name(iter->first)
	             << " id " << mapiter->first
	             << " key " << mapiter->second << dendl;
  }
}

int KeyServer::_rotate_secret(uint32_t service_id, KeyServerData &pending_data)
{
  RotatingSecrets& r = pending_data.rotating_secrets[service_id];
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

bool KeyServer::get_service_secret(uint32_t service_id, CryptoKey& secret,
				   uint64_t& secret_id, double& ttl) const
{
  std::scoped_lock l{lock};

  return data.get_service_secret(cct, service_id, secret, secret_id, ttl);
}

bool KeyServer::get_service_secret(uint32_t service_id,
		uint64_t secret_id, CryptoKey& secret) const
{
  std::scoped_lock l{lock};

  return data.get_service_secret(cct, service_id, secret_id, secret);
}

void KeyServer::note_used_pending_key(const EntityName& name, const CryptoKey& key)
{
  std::scoped_lock l(lock);
  used_pending_keys[name] = key;
}

void KeyServer::clear_used_pending_keys()
{
  std::scoped_lock l(lock);
  used_pending_keys.clear();
}

std::map<EntityName,CryptoKey> KeyServer::get_used_pending_keys()
{
  std::map<EntityName,CryptoKey> ret;
  std::scoped_lock l(lock);
  ret.swap(used_pending_keys);
  return ret;
}

void KeyServer::dump(Formatter *f) const
{
  f->dump_object("data", data);
}

void KeyServer::generate_test_instances(std::list<KeyServer*>& ls)
{
  ls.push_back(new KeyServer(nullptr, nullptr));
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
  auto mapiter = data.secrets_begin();

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

    auto capsiter = mapiter->second.caps.begin();
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

bool KeyServer::prepare_rotating_update(bufferlist& rotating_bl)
{
  std::scoped_lock l{lock};
  ldout(cct, 20) << __func__ << " before: data.rotating_ver=" << data.rotating_ver
		 << dendl;

  KeyServerData pending_data(nullptr);
  pending_data.rotating_ver = data.rotating_ver + 1;
  pending_data.rotating_secrets = data.rotating_secrets;

  int added = 0;
  added += _rotate_secret(CEPH_ENTITY_TYPE_AUTH, pending_data);
  added += _rotate_secret(CEPH_ENTITY_TYPE_MON, pending_data);
  added += _rotate_secret(CEPH_ENTITY_TYPE_OSD, pending_data);
  added += _rotate_secret(CEPH_ENTITY_TYPE_MDS, pending_data);
  added += _rotate_secret(CEPH_ENTITY_TYPE_MGR, pending_data);
  if (!added) {
    return false;
  }

  ldout(cct, 20) << __func__ << " after: pending_data.rotating_ver="
		 << pending_data.rotating_ver
		 << dendl;
  pending_data.encode_rotating(rotating_bl);
  return true;
}

bool KeyServer::get_rotating_encrypted(const EntityName& name,
	bufferlist& enc_bl) const
{
  std::scoped_lock l{lock};

  auto mapiter = data.find_name(name);
  if (mapiter == data.secrets_end())
    return false;

  const CryptoKey& specific_key = mapiter->second.key;

  auto rotate_iter = data.rotating_secrets.find(name.get_type());
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
					CephXSessionAuthInfo& info,
					double ttl)
{
  info.service_id = service_id;
  info.ticket = parent_ticket;
  info.ticket.init_timestamps(ceph_clock_now(), ttl);
  info.validity.set_from_double(ttl);

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
  double ttl;
  if (!get_service_secret(service_id, info.service_secret, info.secret_id,
			  ttl)) {
    return -EACCES;
  }

  std::scoped_lock l{lock};
  return _build_session_auth_info(service_id, parent_ticket, info, ttl);
}

int KeyServer::build_session_auth_info(uint32_t service_id,
				       const AuthTicket& parent_ticket,
				       const CryptoKey& service_secret,
				       uint64_t secret_id,
				       CephXSessionAuthInfo& info)
{
  info.service_secret = service_secret;
  info.secret_id = secret_id;

  std::scoped_lock l{lock};
  return _build_session_auth_info(service_id, parent_ticket, info,
				  cct->_conf->auth_service_ticket_ttl);
}

