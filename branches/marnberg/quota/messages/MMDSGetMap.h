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

#ifndef __MMDSGETMAP_H
#define __MMDSGETMAP_H

#include "msg/Message.h"

#include "include/types.h"

class MMDSGetMap : public Message {
 public:
  MMDSGetMap() : Message(MSG_MDS_GETMAP) {
  }

  char *get_type_name() { return "mdsgetmap"; }
  
  void encode_payload() {
    //payload.append((char*)&sb, sizeof(sb));
  }
  void decode_payload() {
    //int off = 0;
    //payload.copy(off, sizeof(sb), (char*)&sb);
    //off += sizeof(sb);
  }
};

#endif
