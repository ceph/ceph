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

#ifndef __MMONOSDMAPUPDATECOMMIT_H
#define __MMONOSDMAPUPDATECOMMIT_H

#include "msg/Message.h"

#include "include/types.h"

class MMonOSDMapUpdateCommit : public Message {
 public:
  epoch_t epoch;

  MMonOSDMapUpdateCommit(epoch_t e) :
    Message(MSG_MON_OSDMAP_UPDATE_COMMIT),
    epoch(e) {
  }
  
  char *get_type_name() { return "omap_update_commit"; }
  
  void encode_payload() {
    payload.append((char*)&epoch, sizeof(epoch));
  }
  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(epoch), (char*)&epoch);
    off += sizeof(epoch);
  }
};

#endif
