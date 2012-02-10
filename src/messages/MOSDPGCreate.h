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


#ifndef CEPH_MOSDPGCREATE_H
#define CEPH_MOSDPGCREATE_H

#include "msg/Message.h"
#include "osd/osd_types.h"

/*
 * PGCreate - instruct an OSD to create a pg, if it doesn't already exist
 */

struct MOSDPGCreate : public Message {
  version_t          epoch;
  map<pg_t,pg_create_t> mkpg;

  MOSDPGCreate() : Message(MSG_OSD_PG_CREATE) {}
  MOSDPGCreate(epoch_t e) :
    Message(MSG_OSD_PG_CREATE),
    epoch(e) { }
private:
  ~MOSDPGCreate() {}

public:  
  const char *get_type_name() const { return "pg_create"; }

  void encode_payload(uint64_t features) {
    ::encode(epoch, payload);
    ::encode(mkpg, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(epoch, p);
    ::decode(mkpg, p);
  }

  void print(ostream& out) const {
    out << "osd pg create(";
    for (map<pg_t,pg_create_t>::const_iterator i = mkpg.begin();
         i != mkpg.end();
         ++i) {
      out << "pg" << i->first << "," << i->second.created << "; ";
    }
    out << ")";
  }
};

#endif
