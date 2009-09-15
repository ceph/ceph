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

class Cond;

struct AuthContext {
  int status;
  int id;
  utime_t timestamp;
  Cond *cond;
};

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
};
WRITE_CLASS_ENCODER(EntityName);

/*
 * The ticket (if properly validated) authorizes the principal use
 * services as described by 'caps' during the specified validity
 * period.
 */
struct AuthTicket {
  entity_addr_t addr;
  utime_t created, renew_after, expires;
  string nonce;
  map<string, bufferlist> caps;
  __u32 flags;

  void encode(bufferlist& bl) const {
    __u8 v = 1;
    ::encode(v, bl);
    ::encode(addr, bl);
    ::encode(created, bl);
    ::encode(expires, bl);
    ::encode(nonce, bl);
    ::encode(caps, bl);
    ::encode(flags, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 v;
    ::decode(v, bl);
    ::decode(addr, bl);
    ::decode(created, bl);
    ::decode(expires, bl);
    ::decode(nonce, bl);
    ::decode(caps, bl);
    ::decode(flags, bl);
  }
};
WRITE_CLASS_ENCODER(AuthTicket)

struct AuthBlob {
  bufferlist blob;

  void encode(bufferlist& bl) const {
    ::encode(blob, bl);
  }

  void decode(bufferlist::iterator& bl) {
    ::decode(blob, bl);
  }
};
WRITE_CLASS_ENCODER(AuthBlob);

struct SessionAuthInfo {
  uint32_t service_id;
  AuthTicket ticket;
  CryptoKey session_key;
  CryptoKey service_secret;
};


/*
 * Authentication
 */
extern void build_service_ticket_request(EntityName& principal_name, entity_addr_t& principal_addr,
                                uint32_t keys,
                                bool encrypt,
                                CryptoKey& session_key,
                                AuthBlob& ticket_info,
				bufferlist& request);

extern bool build_service_ticket_reply(
                     CryptoKey& principal_secret,
                     vector<SessionAuthInfo> ticket_info,
                     bufferlist& reply);

class AuthenticateRequest {
  EntityName name;
  entity_addr_t addr;
public:
  AuthenticateRequest(EntityName& principal_name, entity_addr_t principal_addr) : name(principal_name), addr(principal_addr) {}

  void encode(bufferlist& bl) const {
    ::encode(name, bl);
    ::encode(addr, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(name, bl);
    ::decode(addr, bl);
  }
};
WRITE_CLASS_ENCODER(AuthenticateRequest)

struct AuthAuthorizeReply {
  uint32_t trans_id;
  utime_t timestamp;
  void encode(bufferlist& bl) const {
    ::encode(trans_id, bl);
    ::encode(timestamp, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(trans_id, bl);
    ::decode(timestamp, bl);
  }
};
WRITE_CLASS_ENCODER(AuthAuthorizeReply);


/*
 * AuthTicketHandler
 */
struct AuthTicketHandler {
  CryptoKey session_key;
  AuthBlob ticket;        // opaque to us
  string nonce;
  utime_t renew_after, expires;
  bool has_key_flag;

  AuthTicketHandler() : has_key_flag(false) {}

  bool build_authenticate_request();
  bool verify_authenticate_response();

  // to build our ServiceTicket
  bool verify_service_ticket_reply(CryptoKey& principal_secret,
				 bufferlist::iterator& indata);
#if 0
  // to build a new ServiceTicket, to access different service
  bool get_session_keys(uint32_t keys, entity_addr_t& principal_addr, bufferlist& bl);
#endif
  // to access the service
  bool build_authorizer(bufferlist& bl, AuthContext& ctx);
  bool decode_reply_authorizer(bufferlist::iterator& indata, AuthAuthorizeReply& reply);
  bool verify_reply_authorizer(AuthContext& ctx, AuthAuthorizeReply& reply);

  bool has_key() { return has_key_flag; }
};

struct AuthTicketsManager {
  map<uint32_t, AuthTicketHandler> tickets_map;

  bool verify_service_ticket_reply(CryptoKey& principal_secret,
				 bufferlist::iterator& indata);

  bool get_session_keys(uint32_t keys, entity_addr_t& principal_addr, bufferlist& bl);

  AuthTicketHandler& get_handler(uint32_t type) { return tickets_map[type]; }
  bool build_authorizer(uint32_t service_id, bufferlist& bl, AuthContext& context);
  bool has_key(uint32_t service_id);
};

struct AuthServiceTicketRequest {
  entity_addr_t addr;
  utime_t timestamp;
  uint32_t keys;

  void encode(bufferlist& bl) const {
    ::encode(addr, bl);
    ::encode(timestamp, bl);
    ::encode(keys, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(addr, bl);
    ::decode(timestamp, bl);
    ::decode(keys, bl);
  }
};
WRITE_CLASS_ENCODER(AuthServiceTicketRequest);

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
    ::encode(ticket.renew_after, bl);
    ::encode(ticket.expires, bl);
    ::encode(ticket.nonce, bl);
    ::encode(session_key, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(ticket.renew_after, bl);
    ::decode(ticket.expires, bl);
    ::decode(ticket.nonce, bl);
    ::decode(session_key, bl);
  }
};
WRITE_CLASS_ENCODER(AuthServiceTicketInfo);

struct AuthAuthorize {
  uint32_t trans_id;
  utime_t now;
  string nonce;
  void encode(bufferlist& bl) const {
    ::encode(trans_id, bl);
    ::encode(now, bl);
    ::encode(nonce, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(trans_id, bl);
    ::decode(now, bl);
    ::decode(nonce, bl);
  }
};
WRITE_CLASS_ENCODER(AuthAuthorize);

template <class T>
int decode_decrypt(T& t, CryptoKey key, bufferlist::iterator& iter) {
   bufferlist bl_enc, bl;
   ::decode(bl_enc, iter);

   int ret = key.decrypt(bl_enc, bl);
   if (ret < 0)
     return ret;

   bufferlist::iterator iter2 = bl.begin();
   ::decode(t, iter2); 
   return 0;
}

template <class T>
int encode_encrypt(const T& t, CryptoKey& key, bufferlist& out) {
  bufferlist bl;
  ::encode(t, bl);

  bufferlist bl_enc;
  int ret = key.encrypt(bl, bl_enc);
  if (ret < 0)
    return ret;

  ::encode(bl_enc, out);
  return 0;
}

extern void build_authenticate_request(EntityName& principal_name, entity_addr_t& principal_addr,
                                uint32_t keys,
                                bool encrypt,
                                CryptoKey& session_key,
                                AuthBlob& ticket_info,
				bufferlist& request);

extern bool build_authenticate_reply(AuthTicketHandler ticket_handler,
                        CryptoKey session_key,
                        CryptoKey auth_session_key,
                        CryptoKey& service_secret,
			bufferlist& reply);
/*
 * Verify authorizer and generate reply authorizer
 */

extern bool verify_service_ticket_request(bool encrypted,
                                   CryptoKey& service_secret,
                                   CryptoKey& session_key,
                                   uint32_t& keys,
                                   bufferlist::iterator& indata);

extern bool verify_authorizer(CryptoKey& service_secret, bufferlist::iterator& bl,
				 bufferlist& enc_reply);

#endif
