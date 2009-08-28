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
 * The client ticket (if properly validated) authorizes the client use
 * services as described by 'caps' during the specified validity
 * period.
 */
struct ClientTicket {
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
WRITE_CLASS_ENCODER(ClientTicket)


/*
 * Authentication
 */
extern void build_authenticate_request(EntityName& client_name, entity_addr_t client_addr,
				       bufferlist& request);
extern bool build_authenticate_reply(ClientTicket& client_ticket, CryptoKey& client_secret,
				     CryptoKey& session_key, CryptoKey& service_secret,
				     bufferlist& reply);


/*
 * ServiceTicket gives a client access to some service
 * (monitor, osd, mds).
 */
struct ServiceTicket {
  CryptoKey session_key;
  bufferlist enc_ticket;        // opaque to us
  string nonce;
  utime_t renew_after, expires;

  // to build our ServiceTicket
  bool verify_authenticate_reply(CryptoKey& client_secret,
				 bufferlist::iterator& indata);

  // to access the service
  utime_t build_authenticator(bufferlist& bl);
  bool verify_reply_authenticator(utime_t then, bufferlist& enc_reply);
};


/*
 * Verify authenticator and generate reply authenticator
 */
extern bool verify_authenticator(CryptoKey& service_secret, bufferlist& bl,
				 bufferlist& enc_reply);

#endif
