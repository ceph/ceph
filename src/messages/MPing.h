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



#ifndef __MPING_H
#define __MPING_H

#include "msg/Message.h"
#include "include/encodable.h"

class MPing : public Message {
 public:
  __u64 seq;
  utime_t stamp;
  MPing(int s, utime_t w) : Message(CEPH_MSG_PING) {
    seq = s;
    stamp = w;
  }
  MPing() : Message(CEPH_MSG_PING) {}

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::_decode_simple(seq, p);
    ::_decode_simple(stamp, p);
  }
  void encode_payload() {
    ::_encode_simple(seq, payload);
    ::_encode_simple(stamp, payload);
  }

  const char *get_type_name() { return "ping"; }
};

#endif
