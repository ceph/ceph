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


#ifndef CEPH_MOSDPGREMOVE_H
#define CEPH_MOSDPGREMOVE_H

#include "msg/Message.h"


class MOSDPGRemove : public Message {
  epoch_t epoch;

 public:
  vector<pg_t> pg_list;

  epoch_t get_epoch() { return epoch; }

  MOSDPGRemove() : Message(MSG_OSD_PG_REMOVE) {}
  MOSDPGRemove(epoch_t e, vector<pg_t>& l) :
    Message(MSG_OSD_PG_REMOVE) {
    this->epoch = e;
    pg_list.swap(l);
  }
private:
  ~MOSDPGRemove() {}

public:  
  const char *get_type_name() const { return "PGrm"; }

  void encode_payload(uint64_t features) {
    ::encode(epoch, payload);
    ::encode(pg_list, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(epoch, p);
    ::decode(pg_list, p);
  }
  void print(ostream& out) const {
    out << "osd pg remove(" << "epoch " << epoch << "; ";
    for (vector<pg_t>::const_iterator i = pg_list.begin();
         i != pg_list.end();
         ++i) {
      out << "pg" << *i << "; ";
    }
    out << ")";
  }
};

#endif
