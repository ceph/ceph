// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef __MCLIENTAUTHUSERACK_H
#define __MCLIENTAUTHUSERACK_H

#include "msg/Message.h"
#include "crypto/Ticket.h"

class MClientAuthUserAck : public Message {
  Ticket myTicket;

 public:
  MClientAuthUserAck() : Message(MSG_CLIENT_AUTH_USER_ACK) { 
  }
  MClientAuthUserAck(Ticket *ticket) : Message(MSG_CLIENT_AUTH_USER_ACK) { 
    myTicket = (*ticket);

  }

  char *get_type_name() { return "client_auth_user_ack"; }

  uid_t get_uid() { return myTicket.get_uid(); }  // fixme

  Ticket *getTicket() {
    return &myTicket;
  }

  void decode_payload() {
    int off = 0;
    myTicket.decode(payload, off);
  }
  void encode_payload() {
    myTicket.encode(payload);
  }
};

#endif
