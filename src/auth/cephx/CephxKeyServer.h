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

#ifndef CEPH_KEYSSERVER_H
#define CEPH_KEYSSERVER_H

#include "auth/KeyRing.h"
#include "CephxProtocol.h"
#include "CephxKeyServer.h"
#include "common/ceph_mutex.h"

class CephContext;

struct KeyServerData {
  version_t version;

  /* for each entity */
  map<EntityName, EntityAuth> secrets;
  KeyRing *extra_secrets;

  /* for each service type */
  version_t rotating_ver;
  map<uint32_t, RotatingSecrets> rotating_secrets;

  explicit KeyServerData(KeyRing *extra)
    : version(0),
      extra_secrets(extra),
      rotating_ver(0) {}

  void encode(bufferlist& bl) const {
     __u8 struct_v = 1;
    using ceph::encode;
    encode(struct_v, bl);
    encode(version, bl);
    encode(rotating_ver, bl);
    encode(secrets, bl);
    encode(rotating_secrets, bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    using ceph::decode;
    __u8 struct_v;
    decode(struct_v, bl);
    decode(version, bl);
    decode(rotating_ver, bl);
    decode(secrets, bl);
    decode(rotating_secrets, bl);
  }

  void encode_rotating(bufferlist& bl) const {
    using ceph::encode;
     __u8 struct_v = 1;
    encode(struct_v, bl);
    encode(rotating_ver, bl);
    encode(rotating_secrets, bl);
  }
  void decode_rotating(bufferlist& rotating_bl) {
    using ceph::decode;
    auto iter = rotating_bl.cbegin();
    __u8 struct_v;
    decode(struct_v, iter);
    decode(rotating_ver, iter);
    decode(rotating_secrets, iter);
  }

  bool contains(const EntityName& name) const {
    return (secrets.find(name) != secrets.end());
  }

  void clear_secrets() {
    secrets.clear();
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

  bool get_service_secret(CephContext *cct, uint32_t service_id,
			  ExpiringCryptoKey& secret, uint64_t& secret_id) const;
  bool get_service_secret(CephContext *cct, uint32_t service_id,
			  CryptoKey& secret, uint64_t& secret_id) const;
  bool get_service_secret(CephContext *cct, uint32_t service_id,
			  uint64_t secret_id, CryptoKey& secret) const;
  bool get_auth(const EntityName& name, EntityAuth& auth) const;
  bool get_secret(const EntityName& name, CryptoKey& secret) const;
  bool get_caps(CephContext *cct, const EntityName& name,
		const std::string& type, AuthCapsInfo& caps) const;

  map<EntityName, EntityAuth>::iterator secrets_begin()
  { return secrets.begin(); }
  map<EntityName, EntityAuth>::const_iterator secrets_begin() const 
  { return secrets.begin(); }
  map<EntityName, EntityAuth>::iterator secrets_end()
  { return secrets.end(); }
  map<EntityName, EntityAuth>::const_iterator secrets_end() const
  { return secrets.end(); }
  map<EntityName, EntityAuth>::iterator find_name(const EntityName& name)
  { return secrets.find(name); }
  map<EntityName, EntityAuth>::const_iterator find_name(const EntityName& name) const
  { return secrets.find(name); }


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
    void decode(bufferlist::const_iterator& bl) {
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
      ceph_abort();
    }
  }

};
WRITE_CLASS_ENCODER(KeyServerData)
WRITE_CLASS_ENCODER(KeyServerData::Incremental)




class KeyServer : public KeyStore {
  CephContext *cct;
  KeyServerData data;
  mutable ceph::mutex lock;

  int _rotate_secret(uint32_t service_id);
  bool _check_rotating_secrets();
  void _dump_rotating_secrets();
  int _build_session_auth_info(uint32_t service_id, 
			       const AuthTicket& parent_ticket,
			       CephXSessionAuthInfo& info);
  bool _get_service_caps(const EntityName& name, uint32_t service_id,
	AuthCapsInfo& caps) const;
public:
  KeyServer(CephContext *cct_, KeyRing *extra_secrets);
  bool generate_secret(CryptoKey& secret);

  bool get_secret(const EntityName& name, CryptoKey& secret) const override;
  bool get_auth(const EntityName& name, EntityAuth& auth) const;
  bool get_caps(const EntityName& name, const string& type, AuthCapsInfo& caps) const;
  bool get_active_rotating_secret(const EntityName& name, CryptoKey& secret) const;
  int start_server();
  void rotate_timeout(double timeout);

  int build_session_auth_info(uint32_t service_id,
			      const AuthTicket& parent_ticket,
			      CephXSessionAuthInfo& info);
  int build_session_auth_info(uint32_t service_id,
			      const AuthTicket& parent_ticket,
			      CephXSessionAuthInfo& info,
			      CryptoKey& service_secret,
			      uint64_t secret_id);

  /* get current secret for specific service type */
  bool get_service_secret(uint32_t service_id, CryptoKey& service_key, 
			  uint64_t& secret_id) const;
  bool get_service_secret(uint32_t service_id, uint64_t secret_id,
			  CryptoKey& secret) const override;

  bool generate_secret(EntityName& name, CryptoKey& secret);

  void encode(bufferlist& bl) const {
    using ceph::encode;
    encode(data, bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    std::scoped_lock l{lock};
    using ceph::decode;
    decode(data, bl);
  }
  bool contains(const EntityName& name) const;
  int encode_secrets(Formatter *f, stringstream *ds) const;
  void encode_formatted(string label, Formatter *f, bufferlist &bl);
  void encode_plaintext(bufferlist &bl);
  int list_secrets(stringstream& ds) const {
    return encode_secrets(NULL, &ds);
  }
  version_t get_ver() const {
    std::scoped_lock l{lock};
    return data.version;    
  }

  void clear_secrets() {
    std::scoped_lock l{lock};
    data.clear_secrets();
  }

  void apply_data_incremental(KeyServerData::Incremental& inc) {
    std::scoped_lock l{lock};
    data.apply_incremental(inc);
  }
  void set_ver(version_t ver) {
    std::scoped_lock l{lock};
    data.version = ver;
  }

  void add_auth(const EntityName& name, EntityAuth& auth) {
    std::scoped_lock l{lock};
    data.add_auth(name, auth);
  }

  void remove_secret(const EntityName& name) {
    std::scoped_lock l{lock};
    data.remove_secret(name);
  }

  bool has_secrets() {
    map<EntityName, EntityAuth>::const_iterator b = data.secrets_begin();
    return (b != data.secrets_end());
  }
  int get_num_secrets() {
    std::scoped_lock l{lock};
    return data.secrets.size();
  }

  void clone_to(KeyServerData& dst) const {
    std::scoped_lock l{lock};
    dst = data;
  }
  void export_keyring(KeyRing& keyring) {
    std::scoped_lock l{lock};
    for (map<EntityName, EntityAuth>::iterator p = data.secrets.begin();
	 p != data.secrets.end();
	 ++p) {
      keyring.add(p->first, p->second);
    }
  }

  bool updated_rotating(bufferlist& rotating_bl, version_t& rotating_ver);

  bool get_rotating_encrypted(const EntityName& name, bufferlist& enc_bl) const;

  ceph::mutex& get_lock() const { return lock; }
  bool get_service_caps(const EntityName& name, uint32_t service_id,
			AuthCapsInfo& caps) const;

  map<EntityName, EntityAuth>::iterator secrets_begin()
  { return data.secrets_begin(); }
  map<EntityName, EntityAuth>::iterator secrets_end()
  { return data.secrets_end(); }
};
WRITE_CLASS_ENCODER(KeyServer)


#endif
