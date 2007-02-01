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


#ifndef __MMONELECTIONACK_H
#define __MMONELECTIONACK_H

#include "msg/Message.h"


class MMonElectionAck : public Message {
 public:
  MMonElectionAck() : Message(MSG_MON_ELECTION_ACK) {}
  
  virtual char *get_type_name() { return "election_ack"; }

  void encode_payload() {}
  void decode_payload() {}
};

#endif
