// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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
  OSDSuperblock sb;
  string public_key_str;

  MOSDBoot() {}
  MOSDBoot(OSDSuperblock& s) : 
    Message(MSG_OSD_BOOT),
    sb(s) {
  }
  MOSDBoot(OSDSuperblock& s, string pks) :
    Message(MSG_OSD_BOOT), sb(s), public_key_str(pks) {
  }

  char *get_type_name() { return "oboot"; }

  string get_public_key() { return public_key_str; }
  
  void encode_payload() {
    payload.append((char*)&sb, sizeof(sb));
    // pass the key
    _encode(public_key_str, payload);
  }
  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(sb), (char*)&sb);
    off += sizeof(sb);

    _decode(public_key_str, payload, off);
  }
};

#endif
