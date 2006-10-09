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

#ifndef __MOSDGETMAP_H
#define __MOSDGETMAP_H

#include "msg/Message.h"

#include "include/types.h"

class MOSDGetMap : public Message {
 public:
  epoch_t since;

  //MOSDGetMap() : since(0) {}
  MOSDGetMap(epoch_t s=0) : 
    Message(MSG_OSD_GETMAP),
    since(s) {
  }

  epoch_t get_since() { return since; }

  char *get_type_name() { return "getomap"; }
  
  void encode_payload() {
    payload.append((char*)&since, sizeof(since));
  }
  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(since), (char*)&since);
    off += sizeof(since);
  }
};

#endif
