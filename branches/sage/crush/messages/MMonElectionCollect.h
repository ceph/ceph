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


#ifndef __MMONELECTIONCOLLECT_H
#define __MMONELECTIONCOLLECT_H

#include "msg/Message.h"


class MMonElectionCollect : public Message {
 public:
  int read_num;

  MMonElectionCollect() {}
  MMonElectionCollect(int n) :
    Message(MSG_MON_ELECTION_COLLECT),
    read_num(n) {}
 
  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(read_num), (char*)&read_num);
    off += sizeof(read_num);
  }
  void encode_payload() {
    payload.append((char*)&read_num, sizeof(read_num));
  }

  virtual char *get_type_name() { return "MonElCollect"; }
};

#endif
