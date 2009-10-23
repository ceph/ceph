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

#ifndef __AUTHTYPES_H
#define __AUTHTYPES_H

#include "Crypto.h"
#include "msg/msg_types.h"

#include "config.h"

#include <errno.h>

class Cond;

struct EntityName {
  uint32_t entity_type;
  string name;

  void encode(bufferlist& bl) const {
    ::encode(entity_type, bl);
    ::encode(name, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(entity_type, bl);
    ::decode(name, bl);
  }

  void to_str(string& str) const {
    str.append(ceph_entity_type_name(entity_type));
    str.append(".");
    str.append(name);
  }
  string to_str() const {
    string s;
    to_str(s);
    return s;
  }

  bool from_str(string& s) {
    int pos = s.find('.');

    if (pos < 0)
      return false;
   
    string pre = s.substr(0, pos);
    const char *pres = pre.c_str();

    set_type(pres);

    name = s.substr(pos + 1);

    return true;
  }

  void set_type(const char *type) {
    if (strcmp(type, "auth") == 0) {
      entity_type = CEPH_ENTITY_TYPE_AUTH;
    } else if (strcmp(type, "mon") == 0) {
      entity_type = CEPH_ENTITY_TYPE_MON;
    } else if (strcmp(type, "osd") == 0) {
      entity_type = CEPH_ENTITY_TYPE_OSD;
    } else if (strcmp(type, "mds") == 0) {
      entity_type = CEPH_ENTITY_TYPE_MDS;
    } else {
      entity_type = CEPH_ENTITY_TYPE_CLIENT;
    }
  }
  void from_type_id(const char *type, const char *id) {
    set_type(type);
    name = id;
  }

  void get_type_str(string& s) {
    s = ceph_entity_type_name(entity_type);
  }
};
WRITE_CLASS_ENCODER(EntityName);

inline bool operator<(const EntityName& a, const EntityName& b) {
  return (a.entity_type < b.entity_type) || (a.entity_type == b.entity_type && a.name < b.name);
}

static inline ostream& operator<<(ostream& out, const EntityName& n) {
  return out << n.to_str();
}



struct EntityAuth {
  CryptoKey key;
  map<string, bufferlist> caps;

  void encode(bufferlist& bl) const {
    ::encode(key, bl);
    ::encode(caps, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(key, bl);
    ::decode(caps, bl);
  }
};
WRITE_CLASS_ENCODER(EntityAuth)

static inline ostream& operator<<(ostream& out, const EntityAuth& a) {
  return out << "auth(key=" << a.key << " with " << a.caps.size() << " caps)";
}

/*
 * The ticket (if properly validated) authorizes the principal use
 * services as described by 'caps' during the specified validity
 * period.
 */
struct AuthTicket {
  EntityName name;
  utime_t created, renew_after, expires;
  bufferlist caps;
  __u32 flags;

  AuthTicket() : flags(0) {}

  void init_timestamps(utime_t now, double ttl) {
    created = now;
    expires = now;
    expires += ttl;
    renew_after = now;
    renew_after += ttl / 2.0;
  }

  void encode(bufferlist& bl) const {
    __u8 v = 1;
    ::encode(v, bl);
    ::encode(name, bl);
    ::encode(created, bl);
    ::encode(expires, bl);
    ::encode(caps, bl);
    ::encode(flags, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 v;
    ::decode(v, bl);
    ::decode(name, bl);
    ::decode(created, bl);
    ::decode(expires, bl);
    ::decode(caps, bl);
    ::decode(flags, bl);
  }
};
WRITE_CLASS_ENCODER(AuthTicket)

struct AuthBlob {
  uint64_t secret_id;
  bufferlist blob;

  void encode(bufferlist& bl) const {
    ::encode(secret_id, bl);
    ::encode(blob, bl);
  }

  void decode(bufferlist::iterator& bl) {
    ::decode(secret_id, bl);
    ::decode(blob, bl);
  }
};
WRITE_CLASS_ENCODER(AuthBlob);

struct SessionAuthInfo {
  uint32_t service_id;
  uint64_t secret_id;
  AuthTicket ticket;
  CryptoKey session_key;
  CryptoKey service_secret;
};


/*
 * Authentication
 */
extern void build_authenticate_request(EntityName& principal_name, bufferlist& request);


extern bool build_service_ticket(SessionAuthInfo& ticket_info, bufferlist& reply);

extern void build_service_ticket_request(uint32_t keys,
					 bufferlist& request);

extern bool build_service_ticket_reply(CryptoKey& principal_secret,
				       vector<SessionAuthInfo> ticket_info,
				       bufferlist& reply);

struct AuthAuthenticateRequest {
  EntityName name;

  AuthAuthenticateRequest() {}
  AuthAuthenticateRequest(EntityName& principal_name) :
    name(principal_name) {}

  void encode(bufferlist& bl) const {
    ::encode(name, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(name, bl);
  }
};
WRITE_CLASS_ENCODER(AuthAuthenticateRequest)

struct AuthServiceTicketRequest {
  uint32_t keys;

  void encode(bufferlist& bl) const {
    ::encode(keys, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(keys, bl);
  }
};
WRITE_CLASS_ENCODER(AuthServiceTicketRequest);


struct AuthAuthorizeReply {
  // uint32_t trans_id;
  utime_t timestamp;
  void encode(bufferlist& bl) const {
    // ::encode(trans_id, bl);
    ::encode(timestamp, bl);
  }
  void decode(bufferlist::iterator& bl) {
    // ::decode(trans_id, bl);
    ::decode(timestamp, bl);
  }
};
WRITE_CLASS_ENCODER(AuthAuthorizeReply);


struct AuthAuthorizer {
  CryptoKey session_key;
  utime_t timestamp;

  bufferlist bl;

  bool build_authorizer();
  bool verify_reply(bufferlist::iterator& reply);
  void clear() { bl.clear(); }
};

/*
 * AuthTicketHandler
 */
struct AuthTicketHandler {
  uint32_t service_id;
  CryptoKey session_key;
  AuthBlob ticket;        // opaque to us
  utime_t renew_after, expires;
  bool has_key_flag;

  AuthTicketHandler() : has_key_flag(false) {}

  // to build our ServiceTicket
  bool verify_service_ticket_reply(CryptoKey& principal_secret,
				 bufferlist::iterator& indata);
  // to access the service
  bool build_authorizer(AuthAuthorizer& authorizer);

  bool has_key() { return has_key_flag; }
};

struct AuthTicketManager {
  map<uint32_t, AuthTicketHandler> tickets_map;

  bool verify_service_ticket_reply(CryptoKey& principal_secret,
				 bufferlist::iterator& indata);

  AuthTicketHandler& get_handler(uint32_t type) {
    AuthTicketHandler& handler = tickets_map[type];
    handler.service_id = type;
    return handler;
  }
  bool build_authorizer(uint32_t service_id, AuthAuthorizer& authorizer);
  bool has_key(uint32_t service_id);
};


/* A */
struct AuthServiceTicket {
  CryptoKey session_key;
  utime_t validity;

  void encode(bufferlist& bl) const {
    ::encode(session_key, bl);
    ::encode(validity, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(session_key, bl);
    ::decode(validity, bl);
  }
};
WRITE_CLASS_ENCODER(AuthServiceTicket);

/* B */
struct AuthServiceTicketInfo {
  AuthTicket ticket;
  CryptoKey session_key;

  void encode(bufferlist& bl) const {
    ::encode(ticket, bl);
    ::encode(session_key, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(ticket, bl);
    ::decode(session_key, bl);
  }
};
WRITE_CLASS_ENCODER(AuthServiceTicketInfo);

struct AuthAuthorize {
  // uint32_t trans_id;
  utime_t now;
  void encode(bufferlist& bl) const {
    // ::encode(trans_id, bl);
    ::encode(now, bl);
  }
  void decode(bufferlist::iterator& bl) {
    // ::decode(trans_id, bl);
    ::decode(now, bl);
  }
};
WRITE_CLASS_ENCODER(AuthAuthorize);

struct ExpiringCryptoKey {
  CryptoKey key;
  utime_t expiration;

  void encode(bufferlist& bl) const {
    ::encode(key, bl);
    ::encode(expiration, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(key, bl);
    ::decode(expiration, bl);
  }
};
WRITE_CLASS_ENCODER(ExpiringCryptoKey);

struct RotatingSecrets {
  map<uint64_t, ExpiringCryptoKey> secrets;
  version_t max_ver;

  void encode(bufferlist& bl) const {
    ::encode(secrets, bl);
    ::encode(max_ver, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(secrets, bl);
    ::decode(max_ver, bl);
  }

  void add(ExpiringCryptoKey& key);
};
WRITE_CLASS_ENCODER(RotatingSecrets);



/*
 * Key management
 */ 
#define KEY_ROTATE_TIME 20
#define KEY_ROTATE_NUM 3

class KeyStore {
public:
  virtual bool get_secret(EntityName& name, CryptoKey& secret) = 0;
  virtual bool get_service_secret(uint32_t service_id, uint64_t secret_id, CryptoKey& secret) = 0;
};

static inline bool auth_principal_needs_rotating_keys(EntityName& name)
{
  return ((name.entity_type == CEPH_ENTITY_TYPE_OSD) ||
          (name.entity_type == CEPH_ENTITY_TYPE_MDS));
}


/*
 * encode+encrypt macros
 */
#define AUTH_ENC_MAGIC 0xff009cad8826aa55

template <class T>
int decode_decrypt(T& t, CryptoKey key, bufferlist::iterator& iter) {
  uint64_t magic;
  bufferlist bl_enc, bl;
  ::decode(bl_enc, iter);

  int ret = key.decrypt(bl_enc, bl);
  if (ret < 0) {
    generic_dout(0) << "error from decrypt " << ret << dendl;
    return ret;
  }

  bufferlist::iterator iter2 = bl.begin();
  ::decode(magic, iter2);
  if (magic != AUTH_ENC_MAGIC) {
    generic_dout(0) << "bad magic in decode_decrypt, " << magic << " != " << AUTH_ENC_MAGIC << dendl;
    return -EPERM;
  }

  ::decode(t, iter2);

  return 0;
}

template <class T>
int encode_encrypt(const T& t, CryptoKey& key, bufferlist& out) {
  uint64_t magic = AUTH_ENC_MAGIC;
  bufferlist bl;
  ::encode(magic, bl);
  ::encode(t, bl);

  bufferlist bl_enc;
  int ret = key.encrypt(bl, bl_enc);
  if (ret < 0)
    return ret;

  ::encode(bl_enc, out);
  return 0;
}



/*
 * Verify authorizer and generate reply authorizer
 */
extern bool verify_service_ticket_request(AuthServiceTicketRequest& ticket_req,
					  bufferlist::iterator& indata);
extern bool verify_authorizer(KeyStore& keys, bufferlist::iterator& indata,
                       AuthServiceTicketInfo& ticket_info, bufferlist& reply_bl);

#endif
