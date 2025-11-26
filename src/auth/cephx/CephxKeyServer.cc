// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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

#include "CephxKeyServer.h"
#include "common/ceph_json.h"
#include "common/Clock.h" // for ceph_clock_now()
#include "common/config.h"
#include "common/dout.h"
#include "common/Formatter.h"

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

void KeyServerData::encode(ceph::buffer::list& bl) const {
  __u8 struct_v = 1;
  using ceph::encode;
  encode(struct_v, bl);
  encode(version, bl);
  encode(rotating_ver, bl);
  encode(secrets, bl);
  encode(rotating_secrets, bl);
}

void KeyServerData::decode(ceph::buffer::list::const_iterator& bl) {
  using ceph::decode;
  __u8 struct_v;
  decode(struct_v, bl);
  decode(version, bl);
  decode(rotating_ver, bl);
  decode(secrets, bl);
  decode(rotating_secrets, bl);
}

void KeyServerData::encode_rotating(ceph::buffer::list& bl) const {
  using ceph::encode;
  __u8 struct_v = 1;
  encode(struct_v, bl);
  encode(rotating_ver, bl);
  encode(rotating_secrets, bl);
}

void KeyServerData::decode_rotating(ceph::buffer::list& rotating_bl) {
  using ceph::decode;
  auto iter = rotating_bl.cbegin();
  __u8 struct_v;
  decode(struct_v, iter);
  decode(rotating_ver, iter);
  decode(rotating_secrets, iter);
}

void KeyServerData::dump(ceph::Formatter *f) const {
  f->dump_unsigned("version", version);
  f->dump_unsigned("rotating_version", rotating_ver);
  f->open_array_section("secrets");
  for (auto const& [name, auth] : secrets) {
    f->open_object_section("secret");
    f->dump_object("entity", name);
    f->dump_object("auth", auth);
    f->close_section();
  }
  f->close_section();
  f->open_array_section("rotating_secrets");
  for (auto const& [entity_type, secrets] : rotating_secrets) {
    f->open_object_section("rotating_secret");
    auto name = EntityName(entity_type);
    f->dump_object("entity", name);
    f->dump_object("secrets", secrets);
    f->close_section();
  }
  f->close_section();
}

bool KeyServerData::get_service_secret(CephContext *cct, uint32_t service_id,
				       CryptoKey& secret, uint64_t& secret_id,
				       double& ttl) const
{
  ldout(cct,30) << __func__ << ": " << service_id << dendl;
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

  double const auth_mon_ticket_ttl = cct->_conf.get_val<double>("auth_mon_ticket_ttl");
  double const auth_service_ticket_ttl= cct->_conf.get_val<double>("auth_service_ticket_ttl");

  // ttl may have just been increased by the user
  // cap it by expiration of "next" key to prevent handing out a ticket
  // with a bogus, possibly way into the future, validity
  ttl = service_id == CEPH_ENTITY_TYPE_AUTH ? auth_mon_ticket_ttl : auth_service_ticket_ttl;
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
bool KeyServerData::get_auth(CephContext *cct, const EntityName& name, EntityAuth& auth) const {
  ldout(cct, 20) << __func__ << ": " << name << dendl;
  auto iter = secrets.find(name);
  if (iter != secrets.end()) {
    auth = iter->second;
    ldout(cct, 30) << __func__ << ": found " << auth << dendl;
    return true;
  }
  ldout(cct, 30) << __func__ << ": searching extra secrets" << dendl;
  return extra_secrets->get_auth(name, auth);
}

bool KeyServerData::get_secret(CephContext *cct, const EntityName& name, CryptoKey& secret) const {
  ldout(cct, 20) << __func__ << ": " << name << dendl;
  auto iter = secrets.find(name);
  if (iter != secrets.end()) {
    secret = iter->second.key;
    ldout(cct, 30) << __func__ << ": found " << secret << dendl;
    return true;
  }

  ldout(cct, 30) << __func__ << ": searching extra secrets" << dendl;
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

void KeyServerData::Incremental::encode(ceph::buffer::list& bl) const {
  using ceph::encode;
  __u8 struct_v = 1;
  encode(struct_v, bl);
  __u32 _op = (__u32)op;
  encode(_op, bl);
  if (op == AUTH_INC_SET_ROTATING) {
    encode(rotating_bl, bl);
  } else {
    encode(name, bl);
    encode(auth, bl);
  }
}

void KeyServerData::Incremental::decode(ceph::buffer::list::const_iterator& bl) {
  using ceph::decode;
  __u8 struct_v;
  decode(struct_v, bl);
  __u32 _op;
  decode(_op, bl);
  op = (IncrementalOp)_op;
  ceph_assert(op >= AUTH_INC_NOP && op <= AUTH_INC_SET_ROTATING);
  if (op == AUTH_INC_SET_ROTATING) {
    decode(rotating_bl, bl);
  } else {
    decode(name, bl);
    decode(auth, bl);
  }
}

void KeyServerData::Incremental::dump(ceph::Formatter *f) const {
  f->dump_unsigned("op", op);
  f->dump_object("name", name);
  f->dump_object("auth", auth);
}

#undef dout_prefix
#define dout_prefix *_dout << "cephx keyserver: "
#define cct kscct.get()

KeyServer::KeyServer(CephContext *cct_, KeyRing *extra_secrets)
  : kscct(cct_),
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

  double const auth_mon_ticket_ttl = cct->_conf.get_val<double>("auth_mon_ticket_ttl");
  double const auth_service_ticket_ttl= cct->_conf.get_val<double>("auth_service_ticket_ttl");
  auto const auth_service_cipher_key_type = get_service_cipher();
  auto auth_service_cipher = CryptoManager::get_key_type_name(auth_service_cipher_key_type);
  //const int auth_service_cipher_key_type = CryptoManager::get_key_type(auth_service_cipher);

  ldout(cct, 10) << __func__
          << ": auth_mon_ticket_ttl=" << auth_mon_ticket_ttl
          << ", auth_service_ticket_ttl=" << auth_service_ticket_ttl
          << ", auth_service_cipher=" << auth_service_cipher
          << ", service_id=" << service_id
          << dendl;;

  double ttl = service_id == CEPH_ENTITY_TYPE_AUTH ? auth_mon_ticket_ttl : auth_service_ticket_ttl;

  while (r.need_new_secrets(now)) {
    ExpiringCryptoKey ek;

    int key_type = auth_service_cipher_key_type;
    if (key_type < 0 || key_type == CEPH_CRYPTO_NONE) {
      key_type = CEPH_CRYPTO_AES256KRB5;
    }

    generate_secret(ek.key, key_type);
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
  return data.get_secret(cct, name, secret);
}

bool KeyServer::get_auth(const EntityName& name, EntityAuth& auth) const
{
  std::scoped_lock l{lock};
  return data.get_auth(cct, name, auth);
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

std::list<KeyServer> KeyServer::generate_test_instances()
{
  std::list<KeyServer> ls;
  ls.emplace_back(nullptr, nullptr);
  return ls;
}

bool KeyServer::generate_secret(CryptoKey& secret, std::optional<int> key_type)
{
  int type = key_type.value_or(CEPH_CRYPTO_AES256KRB5);
  bufferptr bp;
  auto crypto = cct->get_crypto_manager()->get_handler(type);
  if (!crypto)
    return false;

  ldout(cct, 20) << __func__ << ": generating key type " << type << dendl;

  if (crypto->create(cct->random(), bp) < 0)
    return false;

  secret.set_secret(type, bp, ceph_clock_now());

  return true;
}

void KeyServer::encode(ceph::buffer::list& bl) const {
  using ceph::encode;
  encode(data, bl);
}

void KeyServer::decode(ceph::buffer::list::const_iterator& bl) {
  std::scoped_lock l{lock};
  using ceph::decode;
  decode(data, bl);
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

bool KeyServer::prepare_rotating_update(bufferlist& rotating_bl, bool wipe)
{
  std::scoped_lock l{lock};
  ldout(cct, 20) << __func__ << " before: data.rotating_ver=" << data.rotating_ver
		 << dendl;

  KeyServerData pending_data(nullptr);
  pending_data.rotating_ver = data.rotating_ver + 1;
  if (wipe) {
    /* Always keep CEPH_ENTITY_TYPE_AUTH: existing auth service keys are needed
     * to renew tickets by daemons/clients and the only information in an old
     * ticket used is the global_id. Forging tickets is not a significant
     * concern. A stolen auth service key is not worthwhile since you would
     * be incaapable of generating useful service tickets with the associated
     * service key (e.g. "osd").
     */
    RotatingSecrets& r = data.rotating_secrets[CEPH_ENTITY_TYPE_AUTH];
    pending_data.rotating_secrets[CEPH_ENTITY_TYPE_AUTH] = r;
  } else {
    pending_data.rotating_secrets = data.rotating_secrets;
  }

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
  if (encode_encrypt(cct, secrets, specific_key, CEPHX_KEY_USAGE_ROTATING_SECRET, enc_bl, error))
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
                                        std::optional<int> key_type,
					CephXSessionAuthInfo& info,
					double ttl)
{
  info.service_id = service_id;
  info.ticket = parent_ticket;
  info.ticket.init_timestamps(ceph_clock_now(), ttl);
  info.validity.set_from_double(ttl);

  generate_secret(info.session_key, key_type);

  /* N.B.: the Monitor special cases cap retrieval via a call to
   * CephxServiceHandler::handle_request which fills in the
   * Connection::peer_caps_info. This lets the Monitor always use the latest
   * up-to-date mon caps for the entity but it's an unfortunate divergence in
   * behavior.
   */
  string s = ceph_entity_type_name(service_id);
  if (!data.get_caps(cct, info.ticket.name, s, info.ticket.caps)) {
    return -EINVAL;
  }
  return 0;
}

int KeyServer::build_session_auth_info(uint32_t service_id,
				       const AuthTicket& parent_ticket,
                                       std::optional<int> key_type,
				       CephXSessionAuthInfo& info)
{
  double ttl;
  if (!get_service_secret(service_id, info.service_secret, info.secret_id,
			  ttl)) {
    return -EACCES;
  }

  /* either use the provided key type, or the one that the service
   * is using. As things are, there are two different cases:
   * one that this is being called as a result of a client call
   * and in which case we'll be provided with the client's key type.
   * The second case is when the monitor generates tickets to
   * connects to the manager, in which case we want to use
   * the manager's key type. In any case, we assume that services
   * are upgraded first before clients, so we prioritize client's
   * key type over the service key type.
   */
  int ktype = key_type.value_or(info.service_secret.get_type());

  std::scoped_lock l{lock};
  return _build_session_auth_info(service_id, parent_ticket,
                                  ktype, info, ttl);
}

int KeyServer::build_session_auth_info(uint32_t service_id,
				       const AuthTicket& parent_ticket,
				       const CryptoKey& service_secret,
				       uint64_t secret_id,
                                       std::optional<int> key_type,
				       CephXSessionAuthInfo& info)
{
  info.service_secret = service_secret;
  info.secret_id = secret_id;

  std::scoped_lock l{lock};
  double const auth_service_ticket_ttl= cct->_conf.get_val<double>("auth_service_ticket_ttl");
  return _build_session_auth_info(service_id, parent_ticket, key_type, info, auth_service_ticket_ttl);
}

