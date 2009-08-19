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

#include "config.h"
#include "ClientTicket.h"

/*
 * match encoding of struct ceph_secret
 */
class EntitySecret {
protected:
  __u16 type;
  bufferlist secret;

public:
  EntitySecret(bufferlist& bl) { secret = bl; }

  void encode(bufferlist& bl) const {
    ::encode(type, bl);
    __u16 len = secret.length();
    ::encode(len, bl);
    bl.append(secret);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(type, bl);
    __u16 len;
    ::decode(len, bl);
    bl.copy(len, secret);
  }

  bufferlist& get_secret() { return secret; }
};
WRITE_CLASS_ENCODER(EntitySecret);



/*
 * ServiceTicket gives a client access to some service
 * (monitor, osd, mds).
 */
struct ServiceTicket {
  EntitySecret session_key;
  bufferlist enc_ticket;        // opaque to us
  string nonce;
  utime_t renew_after, expires;

  utime_t build_authenticator(bufferlist& bl) {
    ::encode(enc_ticket, bl);

    bufferlist info;
    utime_t now = g_clock.now();
    ::encode(now, info);
    ::encode(nonce, info);

    //encrypt(info);....
    ::encode(info, bl);

    return now;
  }

  bool verify_reply_authenticator(utime_t then, bufferlist& enc_reply) {
    bufferlist reply;
    //decrypt
    bufferlist::iterator p = reply.begin();
    utime_t later;
    ::decode(later, p);
    if (then + 1 == later)
      return true;
    return false;
  }
};


bool verify_authenticator(bufferlist& bl)
{
  bufferlist::iterator p = bl.begin();
  bufferlist enc_ticket, enc_info;
  ::decode(enc_ticket, p);
  ::decode(info, p);

  bufferlist ticket;
  // decrypt with my key

  // decrypt info with session key


  if (info.nonce != ticket.nonce)
    return false;
  if (old)
    return false;
}


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



struct AuthReply {
  bufferlist enc_info;      // with client's secret
  bufferlist enc_ticket;    // with service's secret

  void encode(bufferlist& bl) const {
    ::encode(enc_info, bl);
    ::encode(enc_ticket, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(enc_info, bl);
    ::decode(enc_ticket, bl);
  }
};

struct AuthRequest {
  EntityName client_name;
  entity_addr_t client_addr;

  void encode(bufferlist& bl) const {
    ::encode(client_name, bl);
    ::encode(client_addr, bl);    
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(client_name, bl);
    ::decode(client_addr, bl);    
  }

  bool validate_reply(AuthReply& reply, ServiceTicket& ticket) {
    // validate info
    bufferlist::iterator p = reply.enc_info.begin();
    try {
      ::decode(ticket.session_key, p);
      ::decode(ticket.renew_after, p);
      ::decode(ticket.expires, p);
      ::decode(ticket.nonce, p);
    }
    catch (buffer::error *e) {
      delete e;
      return false;
    }
    if (!p.end())
      return false;

    // yay!
    ticket.enc_ticket = reply.enc_ticket;
    return true;
  }
};




class ServiceSecret : public EntitySecret {
  utime_t created;

public:
  void encode(bufferlist& bl) const {
    ::encode(secret, bl);
    ::encode(created, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(secret, bl);
    ::decode(created, bl);
  }
};
WRITE_CLASS_ENCODER(ServiceSecret);

struct SessionKey {
  bufferlist key;

  void encode(bufferlist& bl) const {
    ::encode(key, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(key, bl);
  }
};
WRITE_CLASS_ENCODER(SessionKey);

#endif
