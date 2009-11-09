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

#ifndef __CEPHXPROTOCOL_H
#define __CEPHXPROTOCOL_H

/*
  Ceph X protocol

  First, the principal has to authenticate with the authenticator. A
  shared-secret mechanism is being used, and the negotitaion goes like this:

  A = Authenticator
  P = Principle
  S = Service

  1. Obtaining principal/auth session key

  (Authenticate Request)
  p->a : principal, principal_addr.  authenticate me!

 ...authenticator does lookup in database...

  a->p : A= {principal/auth session key, validity}^principal_secret (*)
         B= {principal ticket, validity, principal/auth session key}^authsecret

  
  [principal/auth session key, validity] = service ticket
  [principal ticket, validity, principal/auth session key] = service ticket info

  (*) annotation: ^ signifies 'encrypted by'

  At this point, if is genuine, the principal should have the principal/auth
  session key at hand. The next step would be to request an authorization to
  use some other service:

  2. Obtaining principal/service session key

  p->a : B, {principal_addr, timestamp}^principal/auth session key.  authorize
         me!
  a->p : E= {service ticket}^svcsecret
         F= {principal/service session key, validity}^principal/auth session key

  principal_addr, timestamp = authenticator

  service ticket = principal name, client network address, validity, principal/service session key

  Note that steps 1 and 2 are pretty much the same thing; contacting the
  authenticator and requesting for a key.

  Following this the principal should have a principal/service session key that
  could be used later on for creating a session:

  3. Opening a session to a service

  p->s : E + {principal_addr, timestamp}^principal/service session key
  s->p : {timestamp+1}^principal/service/session key

  timestamp+1 = reply authenticator

  Now, the principal is fully authenticated with the service. So, logically we
  have 2 main actions here. The first one would be to obtain a session key to
  the service (steps 1 and 2), and the second one would be to authenticate with
  the service, using that ticket.
*/

/* authenticate requests */
#define CEPHX_GET_AUTH_SESSION_KEY      0x0100
#define CEPHX_GET_PRINCIPAL_SESSION_KEY 0x0200
#define CEPHX_GET_ROTATING_KEY          0x0400

#define CEPHX_REQUEST_TYPE_MASK            0x0F00

#include "../Auth.h"


/*
 * Authentication
 */

// initial server -> client challenge
struct CephXServerChallenge {
  __u64 server_challenge;

  void encode(bufferlist& bl) const {
    ::encode(server_challenge, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(server_challenge, bl);
  }
};
WRITE_CLASS_ENCODER(CephXServerChallenge);


// request/reply headers, for subsequent exchanges.

struct CephXRequestHeader {
  __u16 request_type;

  void encode(bufferlist& bl) const {
    ::encode(request_type, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(request_type, bl);
  }
};
WRITE_CLASS_ENCODER(CephXRequestHeader);

struct CephXResponseHeader {
  uint16_t request_type;
  int32_t status;

  void encode(bufferlist& bl) const {
    ::encode(request_type, bl);
    ::encode(status, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(request_type, bl);
    ::decode(status, bl);
  }
};
WRITE_CLASS_ENCODER(CephXResponseHeader);

// client -> server response to challenge
struct CephXAuthenticate {
  __u64 client_challenge;
  __u64 key;

  void encode(bufferlist& bl) const {
    ::encode(client_challenge, bl);
    ::encode(key, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(client_challenge, bl);
    ::decode(key, bl);
 }
};
WRITE_CLASS_ENCODER(CephXAuthenticate)

struct CephXChallengeBlob {
  __u64 server_challenge, client_challenge;
  
  void encode(bufferlist& bl) const {
    ::encode(server_challenge, bl);
    ::encode(client_challenge, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(server_challenge, bl);
    ::decode(client_challenge, bl);
  }
};
WRITE_CLASS_ENCODER(CephXChallengeBlob)

int cephx_calc_client_server_challenge(CryptoKey& secret, __u64 server_challenge, __u64 client_challenge,
				       __u64 *key);


/*
 * getting service tickets
 */
struct CephXSessionAuthInfo {
  uint32_t service_id;
  uint64_t secret_id;
  AuthTicket ticket;
  CryptoKey session_key;
  CryptoKey service_secret;
  utime_t validity;
};


extern bool cephx_build_service_ticket(CephXSessionAuthInfo& ticket_info, bufferlist& reply);

extern void cephx_build_service_ticket_request(uint32_t keys,
					 bufferlist& request);

extern bool cephx_build_service_ticket_reply(CryptoKey& principal_secret,
					     vector<CephXSessionAuthInfo> ticket_info,
					     bufferlist& reply);

struct CephXServiceTicketRequest {
  uint32_t keys;

  void encode(bufferlist& bl) const {
    ::encode(keys, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(keys, bl);
  }
};
WRITE_CLASS_ENCODER(CephXServiceTicketRequest);


/*
 * Authorize
 */

struct CephXAuthorizeReply {
  __u64 nonce_plus_one;
  void encode(bufferlist& bl) const {
    ::encode(nonce_plus_one, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(nonce_plus_one, bl);
  }
};
WRITE_CLASS_ENCODER(CephXAuthorizeReply);


struct CephXAuthorizer : public AuthAuthorizer {
  __u64 nonce;
  CryptoKey session_key;

  CephXAuthorizer() : AuthAuthorizer(CEPH_AUTH_CEPHX) {}

  bool build_authorizer();
  bool verify_reply(bufferlist::iterator& reply);
};


struct CephXTicketBlob {
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
WRITE_CLASS_ENCODER(CephXTicketBlob);


/*
 * TicketHandler
 */
struct CephXTicketHandler {
  uint32_t service_id;
  CryptoKey session_key;
  CephXTicketBlob ticket;        // opaque to us
  utime_t renew_after, expires;
  bool have_key_flag;

  CephXTicketHandler() : service_id(0), have_key_flag(false) {}

  // to build our ServiceTicket
  bool verify_service_ticket_reply(CryptoKey& principal_secret,
				 bufferlist::iterator& indata);
  // to access the service
  CephXAuthorizer *build_authorizer(uint64_t global_id);

  bool have_key();
  bool need_key();
};

struct CephXTicketManager {
  map<uint32_t, CephXTicketHandler> tickets_map;
  uint64_t global_id;

  CephXTicketManager() : global_id(0) {}

  bool verify_service_ticket_reply(CryptoKey& principal_secret,
				 bufferlist::iterator& indata);

  CephXTicketHandler& get_handler(uint32_t type) {
    CephXTicketHandler& handler = tickets_map[type];
    handler.service_id = type;
    return handler;
  }
  CephXAuthorizer *build_authorizer(uint32_t service_id);
  bool have_key(uint32_t service_id);
  bool need_key(uint32_t service_id);
  void set_have_need_key(uint32_t service_id, uint32_t& have, uint32_t& need);
  void validate_tickets(uint32_t mask, uint32_t& have, uint32_t& need);
};


/* A */
struct CephXServiceTicket {
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
WRITE_CLASS_ENCODER(CephXServiceTicket);

/* B */
struct CephXServiceTicketInfo {
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
WRITE_CLASS_ENCODER(CephXServiceTicketInfo);

struct CephXAuthorize {
  __u64 nonce;
  void encode(bufferlist& bl) const {
    ::encode(nonce, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(nonce, bl);
  }
};
WRITE_CLASS_ENCODER(CephXAuthorize);

/*
 * Verify authorizer and generate reply authorizer
 */
extern bool cephx_verify_authorizer(KeyStore& keys, bufferlist::iterator& indata,
				    CephXServiceTicketInfo& ticket_info, bufferlist& reply_bl);






/*
 * encode+encrypt macros
 */
#define AUTH_ENC_MAGIC 0xff009cad8826aa55

template <typename T>
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

template <typename T>
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


#endif
