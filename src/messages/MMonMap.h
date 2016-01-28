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

#ifndef CEPH_MMONMAP_H
#define CEPH_MMONMAP_H

#include "include/ceph_features.h"
#include "msg/Message.h"
#include "mon/MonMap.h"

class MMonMap : public Message {
public:
  bufferlist monmapbl;

  MMonMap() : Message(CEPH_MSG_MON_MAP) { }
  explicit MMonMap(bufferlist &bl) : Message(CEPH_MSG_MON_MAP) { 
    monmapbl.claim(bl);
  }
private:
  ~MMonMap() {}

public:
  const char *get_type_name() const { return "mon_map"; }

  void encode_payload(uint64_t features) { 
    if (monmapbl.length() && (features & CEPH_FEATURE_MONENC) == 0) {
      // reencode old-format monmap
      MonMap t;
      t.decode(monmapbl);
      monmapbl.clear();
      t.encode(monmapbl, features);
    }

    ::encode(monmapbl, payload);
  }
  void decode_payload() { 
    bufferlist::iterator p = payload.begin();
    ::decode(monmapbl, p);
  }
};

#endif
