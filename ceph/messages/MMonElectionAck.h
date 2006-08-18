// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
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
  int q;
  int refresh_num;

  MMonElectionAck() {}
  MMonElectionAck(int _q, int _n) :
	Message(MSG_MON_ELECTION_ACK),
	q(_q), refresh_num(_n) {}
 
  void decode_payload() {
	int off = 0;
	payload.copy(off, sizeof(q), (char*)&q);
	off += sizeof(q);
	payload.copy(off, sizeof(refresh_num), (char*)&refresh_num);
	off += sizeof(refresh_num);
  }
  void encode_payload() {
	payload.append((char*)&q, sizeof(q));
	payload.append((char*)&refresh_num, sizeof(refresh_num));
  }

  virtual char *get_type_name() { return "MonElAck"; }
};

#endif
