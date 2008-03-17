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


#ifndef __MOSDPGCREATE_H
#define __MOSDPGCREATE_H

#include "msg/Message.h"

/*
 * PGCreate - instruct an OSD to create a pg, if it doesn't already exist
 */

struct MOSDPGCreate : public Message {
  version_t          epoch;
  struct create_rec {
    epoch_t created;   // epoch pg created
    pg_t parent;       // split from parent (if != pg_t())
  };
  map<pg_t,create_rec> mkpg;

  MOSDPGCreate() {}
  MOSDPGCreate(epoch_t e) :
    Message(MSG_OSD_PG_CREATE),
    epoch(e) { }
  
  const char *get_type_name() { return "pg_create"; }

  void encode_payload() {
    ::_encode(epoch, payload);
    ::_encode(mkpg, payload);
  }
  void decode_payload() {
    int off = 0;
    ::_decode(epoch, payload, off);
    ::_decode(mkpg, payload, off);
  }
};

#endif
