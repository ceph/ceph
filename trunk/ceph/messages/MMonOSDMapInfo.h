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

#ifndef __MMONOSDMAPINFO_H
#define __MMONOSDMAPINFO_H

#include "msg/Message.h"

#include "include/types.h"

class MMonOSDMapInfo : public Message {
 public:
  epoch_t epoch;
  epoch_t mon_epoch;

  epoch_t get_epoch() { return epoch; }
  epoch_t get_mon_epoch() { return mon_epoch; }
  
  MMonOSDMapInfo(epoch_t e, epoch_t me) :
    Message(MSG_MON_OSDMAP_UPDATE_PREPARE),
    epoch(e), mon_epoch(me) {
  }
  
  char *get_type_name() { return "omap_info"; }
  
  void encode_payload() {
    payload.append((char*)&epoch, sizeof(epoch));
    payload.append((char*)&mon_epoch, sizeof(mon_epoch));
  }
  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(epoch), (char*)&epoch);
    off += sizeof(epoch);
    payload.copy(off, sizeof(mon_epoch), (char*)&mon_epoch);
    off += sizeof(mon_epoch);
  }
};

#endif
