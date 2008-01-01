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


#ifndef __MOSDPGQUERY_H
#define __MOSDPGQUERY_H

#include "msg/Message.h"

/*
 * PGQuery - query another OSD as to the contents of their PGs
 */

class MOSDPGQuery : public Message {
  version_t       epoch;

 public:
  version_t get_epoch() { return epoch; }
  map<pg_t,PG::Query>  pg_list;

  MOSDPGQuery() {}
  MOSDPGQuery(epoch_t e, map<pg_t,PG::Query>& ls) :
    Message(MSG_OSD_PG_QUERY),
    epoch(e), pg_list(ls) {
  }
  
  const char *get_type_name() { return "PGq"; }

  void encode_payload() {
    ::_encode(epoch, payload);
    ::_encode(pg_list, payload);
  }
  void decode_payload() {
    int off = 0;
    ::_decode(epoch, payload, off);
    ::_decode(pg_list, payload, off);
  }
};

#endif
