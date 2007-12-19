// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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


#ifndef __MPINGACK_H
#define __MPINGACK_H

#include "MPing.h"


class MPingAck : public Message {
 public:
  int seq;
  MPingAck() {}
  MPingAck(MPing *p) : Message(CEPH_MSG_PING_ACK) {
    this->seq = p->seq;
  }

  virtual void decode_payload() {
    int off = 0;
    payload.copy(0, sizeof(seq), (char*)&seq);
    off += sizeof(seq);
  }
  virtual void encode_payload() {
    payload.append((char*)&seq, sizeof(seq));
  }

  virtual char *get_type_name() { return "pinga"; }
};

#endif
