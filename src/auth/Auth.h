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
 * The principal ticket (if properly validated) authorizes the principal use
 * services as described by 'caps' during the specified validity
 * period.
 */
struct PrincipalTicket {
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
WRITE_CLASS_ENCODER(PrincipalTicket)


/*
 * Authentication
 */
extern void build_get_tgt_request(EntityName& principal_name, entity_addr_t principal_addr,
				       bufferlist& request);
extern bool build_get_tgt_reply(PrincipalTicket& principal_ticket, CryptoKey& principal_secret,
				     CryptoKey& session_key, CryptoKey& service_secret,
				     bufferlist& reply);


/*
 * ServiceTicket gives a principal access to some service
 * (monitor, osd, mds).
 */
struct ServiceTicket {
  CryptoKey session_key;
  bufferlist enc_ticket;        // opaque to us
  string nonce;
  utime_t renew_after, expires;
  bool has_key_flag;

  ServiceTicket() : has_key_flag(false) {}

  // to build our ServiceTicket
  bool verify_service_ticket_reply(CryptoKey& principal_secret,
				 bufferlist::iterator& indata);

  // to build a new ServiceTicket, to access different service
  bool get_session_keys(uint32_t keys, entity_addr_t& principal_addr, bufferlist& bl);

  // to access the service
  utime_t build_authenticator(bufferlist& bl);
  bool verify_reply_authenticator(utime_t then, bufferlist& enc_reply);

  bool has_key() { return has_key_flag; }
  void encode(bufferlist& bl) const {
    __u8 v = 1;
    ::encode(v, bl);
    ::encode(session_key, bl);
    ::encode(enc_ticket, bl);
    ::encode(nonce, bl);
    ::encode(renew_after, bl);
    ::encode(expires, bl);
    __u8 f = has_key_flag;
    ::encode(f, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 v;
    ::decode(v, bl);
    ::decode(session_key, bl);
    ::decode(enc_ticket, bl);
    ::decode(nonce, bl);
    ::decode(renew_after, bl);
    ::decode(expires, bl);
    __u8 f;
    ::decode(f, bl);
    has_key_flag = f;
  }
};
WRITE_CLASS_ENCODER(ServiceTicket)

extern bool verify_get_session_keys_request(CryptoKey& service_secret,
                                     CryptoKey& session_key, uint32_t& keys, bufferlist::iterator& indata);

extern bool build_ticket_reply(ServiceTicket service_ticket, CryptoKey auth_session_key, CryptoKey& service_secret,
			bufferlist& reply);
/*
 * Verify authenticator and generate reply authenticator
 */
extern bool verify_authenticator(CryptoKey& service_secret, bufferlist::iterator& bl,
				 bufferlist& enc_reply);

#endif
