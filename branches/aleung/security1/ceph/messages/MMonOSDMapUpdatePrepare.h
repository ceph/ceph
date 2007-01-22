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

#ifndef __MMONOSDMAPUPDATEPREPARE_H
#define __MMONOSDMAPUPDATEPREPARE_H

#include "msg/Message.h"

#include "include/types.h"

class MMonOSDMapUpdatePrepare : public Message {
 public:
  epoch_t epoch;
  bufferlist map_bl;
  bufferlist inc_map_bl;

  epoch_t get_epoch() { return epoch; }

  MMonOSDMapUpdatePrepare(epoch_t e, 
			  bufferlist& mbl, bufferlist& incmbl) : 
    Message(MSG_MON_OSDMAP_UPDATE_PREPARE),
    epoch(e), 
    map_bl(mbl), inc_map_bl(incmbl) {
  }
  
  char *get_type_name() { return "omap_update_prepare"; }
  
  void encode_payload() {
    payload.append((char*)&epoch, sizeof(epoch));
    ::_encode(map_bl, payload);
    ::_encode(inc_map_bl, payload);
  }
  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(epoch), (char*)&epoch);
    off += sizeof(epoch);
    ::_decode(map_bl, payload, off);
    ::_decode(inc_map_bl, payload, off);
  }
};

#endif
