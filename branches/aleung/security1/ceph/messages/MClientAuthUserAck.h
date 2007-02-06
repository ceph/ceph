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
  //bufferlist ticketBL;
  Ticket myTicket;
 public:
  MClientAuthUserAck() : Message(MSG_CLIENT_AUTH_USER_ACK) { 
  }
  MClientAuthUserAck(Ticket *ticket) : Message(MSG_CLIENT_AUTH_USER_ACK) { 
    //ticket->encode(ticketBL);
    myTicket = (*ticket);
  }

  char *get_type_name() { return "client_auth_user_ack"; }

  uid_t get_uid() { return 0; }  // fixme

  Ticket *getTicket() {
    return &myTicket;
  }

  void decode_payload() {
    cout << "Trying decode payload ACK" << endl;
    int off = 0;
    //::_decode(myTicket, payload, off);
    myTicket.decode(payload, off);
    cout << "ACK Decoded OK" << endl;
  }
  void encode_payload() {
    cout << "Trying encode payload ACK" << endl;
    //::_encode(myTicket, payload);
    myTicket.encode(payload);
    cout << "ACK Encoded OK" << endl;
  }
};

#endif
