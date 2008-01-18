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
  __u64 seq;
  utime_t sender_stamp;
  utime_t reply_stamp;

  MPingAck() {}
  MPingAck(MPing *p, utime_t w) : Message(CEPH_MSG_PING_ACK) {
    seq = p->seq;
    sender_stamp = p->stamp;
    reply_stamp = w;
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::_decode_simple(seq, p);
    ::_decode_simple(sender_stamp, p);
    ::_decode_simple(reply_stamp, p);
  }
  void encode_payload() {
    ::_encode_simple(seq, payload);
    ::_encode_simple(sender_stamp, payload);
    ::_encode_simple(reply_stamp, payload);
  }

  const char *get_type_name() { return "pinga"; }
};

#endif
