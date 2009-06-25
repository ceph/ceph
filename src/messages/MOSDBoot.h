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

#include "messages/PaxosServiceMessage.h"

#include "include/types.h"
#include "osd/osd_types.h"

class MOSDBoot : public PaxosServiceMessage {
 public:
  OSDSuperblock sb;

  MOSDBoot() : PaxosServiceMessage(){}
  MOSDBoot(OSDSuperblock& s) : 
    PaxosServiceMessage(MSG_OSD_BOOT, s.current_epoch), sb(s) {
  }

  const char *get_type_name() { return "osd_boot"; }
  void print(ostream& out) {
    out << "osd_boot(osd" << sb.whoami << "v " << version << ")";
  }
  
  void encode_payload() {
    paxos_encode();
    ::encode(sb, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(sb, p);
  }
};

#endif
