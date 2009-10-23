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

#ifndef __KEYSSERVER_H
#define __KEYSSERVER_H

#include "config.h"

#include "CephxProtocol.h"

#include "common/Timer.h"

struct KeyServerData {
  version_t version;
  version_t rotating_ver;
  utime_t next_rotating_time;

  /* for each entity */
  map<EntityName, EntityAuth> secrets;

  /* for each service type */
  map<uint32_t, RotatingSecrets> rotating_secrets;

  KeyServerData() : version(0), rotating_ver(0) {}

  void encode(bufferlist& bl) const {
    ::encode(version, bl);
    ::encode(rotating_ver, bl);
    ::encode(next_rotating_time, bl);
    ::encode(secrets, bl);
    ::encode(rotating_secrets, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(version, bl);
    ::decode(rotating_ver, bl);
    ::decode(next_rotating_time, bl);
    ::decode(secrets, bl);
    ::decode(rotating_secrets, bl);
  }

  void decode_rotating(bufferlist& rotating_bl) {
    bufferlist::iterator iter = rotating_bl.begin();
    ::decode(rotating_ver, iter);
    ::decode(rotating_secrets, iter);
  }

  bool contains(EntityName& name) {
    return (secrets.find(name) != secrets.end());
  }

  void add_auth(const EntityName& name, EntityAuth& auth) {
    secrets[name] = auth;
  }

  void remove_secret(const EntityName& name) {
    map<EntityName, EntityAuth>::iterator iter = secrets.find(name);
    if (iter == secrets.end())
      return;
    secrets.erase(iter);
  }

  void add_rotating_secret(uint32_t service_id, ExpiringCryptoKey& key) {
    rotating_secrets[service_id].add(key);
  }

  bool get_service_secret(uint32_t service_id, ExpiringCryptoKey& secret, uint64_t& secret_id);
  bool get_service_secret(uint32_t service_id, CryptoKey& secret, uint64_t& secret_id);
  bool get_service_secret(uint32_t service_id, uint64_t secret_id, CryptoKey& secret);
  bool get_secret(EntityName& name, CryptoKey& secret);
  bool get_caps(EntityName& name, string& type, bufferlist& caps);

  map<EntityName, EntityAuth>::iterator secrets_begin() { return secrets.begin(); }
  map<EntityName, EntityAuth>::iterator secrets_end() { return secrets.end(); }
  map<EntityName, EntityAuth>::iterator find_name(EntityName& name) { return secrets.find(name); }


  // -- incremental updates --
  typedef enum {
    AUTH_INC_NOP,
    AUTH_INC_ADD,
    AUTH_INC_DEL,
    AUTH_INC_SET_ROTATING,
  } IncrementalOp;

  struct Incremental {
    IncrementalOp op;
    bufferlist rotating_bl;  // if SET_ROTATING.  otherwise,
    EntityName name;
    EntityAuth auth;
    
    void encode(bufferlist& bl) const {
      __u32 _op = (__u32)op;
      ::encode(_op, bl);
      if (op == AUTH_INC_SET_ROTATING) {
	::encode(rotating_bl, bl);
      } else {
	::encode(name, bl);
	::encode(auth, bl);
      }
    }
    void decode(bufferlist::iterator& bl) {
      __u32 _op;
      ::decode(_op, bl);
      op = (IncrementalOp)_op;
      assert(op >= AUTH_INC_NOP && op <= AUTH_INC_SET_ROTATING);
      if (op == AUTH_INC_SET_ROTATING) {
	::decode(rotating_bl, bl);
      } else {
	::decode(name, bl);
	::decode(auth, bl);
      }
    }
  };

  void apply_incremental(Incremental& inc) {
    switch (inc.op) {
    case AUTH_INC_ADD:
      add_auth(inc.name, inc.auth);
      break;
      
    case AUTH_INC_DEL:
      remove_secret(inc.name);
      break;

    case AUTH_INC_SET_ROTATING:
      decode_rotating(inc.rotating_bl);
      break;

    case AUTH_INC_NOP:
      break;

    default:
      assert(0);
    }
  }

};
WRITE_CLASS_ENCODER(KeyServerData);
WRITE_CLASS_ENCODER(KeyServerData::Incremental);




class KeyServer : public KeyStore {
  KeyServerData data;

  Mutex lock;

  void _rotate_secret(uint32_t service_id, int factor);
  void _generate_all_rotating_secrets(bool init);
  bool _check_rotate();
  int _build_session_auth_info(uint32_t service_id, CephXServiceTicketInfo& auth_ticket_info, CephXSessionAuthInfo& info);
public:
  KeyServer();

  bool generate_secret(CryptoKey& secret);

  bool get_secret(EntityName& name, CryptoKey& secret);
  bool get_caps(EntityName& name, string& type, bufferlist& caps);
  bool get_active_rotating_secret(EntityName& name, CryptoKey& secret);
  int start_server(bool init);
  void rotate_timeout(double timeout);

  int build_session_auth_info(uint32_t service_id, CephXServiceTicketInfo& auth_ticket_info, CephXSessionAuthInfo& info);
  int build_session_auth_info(uint32_t service_id, CephXServiceTicketInfo& auth_ticket_info, CephXSessionAuthInfo& info,
                                        CryptoKey& service_secret, uint64_t secret_id);

  /* get current secret for specific service type */
  bool get_service_secret(uint32_t service_id, ExpiringCryptoKey& service_key, uint64_t& secret_id);
  bool get_service_secret(uint32_t service_id, CryptoKey& service_key, uint64_t& secret_id);
  bool get_service_secret(uint32_t service_id, uint64_t secret_id, CryptoKey& secret);

  bool generate_secret(EntityName& name, CryptoKey& secret);

  void encode(bufferlist& bl) const {
    ::encode(data, bl);
  }
  void decode(bufferlist::iterator& bl) {
    Mutex::Locker l(lock);
    ::decode(data, bl);
  }
  bool contains(EntityName& name);
  void list_secrets(stringstream& ss);
  version_t get_ver() {
    Mutex::Locker l(lock);
    return data.version;    
  }

  void apply_data_incremental(KeyServerData::Incremental& inc) {
    data.apply_incremental(inc);
  }
  void set_ver(version_t ver) {
    Mutex::Locker l(lock);
    data.version = ver;
  }

  void add_auth(const EntityName& name, EntityAuth& auth) {
    Mutex::Locker l(lock);
    data.add_auth(name, auth);
  }

  void remove_secret(const EntityName& name) {
    Mutex::Locker l(lock);
    data.remove_secret(name);
  }

  void add_rotating_secret(uint32_t service_id, ExpiringCryptoKey& key) {
    Mutex::Locker l(lock);
    data.add_rotating_secret(service_id, key);
  }
  void clone_to(KeyServerData& dst) {
    Mutex::Locker l(lock);
    dst = data;
  }

  bool updated_rotating(bufferlist& rotating_bl, version_t& rotating_ver);

  bool get_rotating_encrypted(EntityName& name, bufferlist& enc_bl);

  Mutex& get_lock() { return lock; }
};
WRITE_CLASS_ENCODER(KeyServer);





#endif
