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

#ifndef __MOSDBOOT_H
#define __MOSDBOOT_H

#include "msg/Message.h"

#include "include/types.h"
#include "osd/osd_types.h"

class MOSDBoot : public Message {
 public:
  entity_inst_t inst;
  OSDSuperblock sb;

  MOSDBoot() {}
  MOSDBoot(entity_inst_t i, OSDSuperblock& s) : 
    Message(MSG_OSD_BOOT),
    inst(i),
    sb(s) {
  }

  const char *get_type_name() { return "osd_boot"; }
  void print(ostream& out) {
    out << "osd_boot(" << inst << ")";
  }
  
  void encode_payload() {
    ::encode(inst, payload);
    ::encode(sb, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(inst, p);
    ::decode(sb, p);
  }
};

#endif
