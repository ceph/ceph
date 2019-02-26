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

class MMonMap : public MessageInstance<MMonMap> {
public:
  friend factory;

  bufferlist monmapbl;

  MMonMap() : MessageInstance(CEPH_MSG_MON_MAP) { }
  explicit MMonMap(bufferlist &bl) : MessageInstance(CEPH_MSG_MON_MAP) { 
    monmapbl.claim(bl);
  }
private:
  ~MMonMap() override {}

public:
  std::string_view get_type_name() const override { return "mon_map"; }

  void encode_payload(uint64_t features) override { 
    if (monmapbl.length() &&
	((features & CEPH_FEATURE_MONENC) == 0 ||
	 (features & CEPH_FEATURE_MSG_ADDR2) == 0)) {
      // reencode old-format monmap
      MonMap t;
      t.decode(monmapbl);
      monmapbl.clear();
      t.encode(monmapbl, features);
    }

    using ceph::encode;
    encode(monmapbl, payload);
  }
  void decode_payload() override { 
    auto p = payload.cbegin();
    decode(monmapbl, p);
  }
};

#endif
