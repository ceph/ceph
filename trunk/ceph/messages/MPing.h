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



#ifndef __MPING_H
#define __MPING_H

#include "msg/Message.h"


class MPing : public Message {
 public:
  int seq;
  MPing(int s) : Message(MSG_PING) {
    seq = s;
  }
  MPing() : Message(MSG_PING) {}

  virtual void decode_payload(crope& s, int& off) {
    s.copy(0, sizeof(seq), (char*)&seq);
    off += sizeof(seq);
  }
  virtual void encode_payload(crope& s) {
    s.append((char*)&seq, sizeof(seq));
  }

  virtual char *get_type_name() { return "ping"; }
};

#endif
