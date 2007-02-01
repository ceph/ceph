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

#ifndef __MMONOSDMAPLEASEACK_H
#define __MMONOSDMAPLEASEACK_H

#include "msg/Message.h"

#include "include/types.h"

class MMonOSDMapLeaseAck : public Message {
  epoch_t epoch;

public:
  epoch_t get_epoch() { return epoch; }
  
  MMonOSDMapLeaseAck(epoch_t e) :
    Message(MSG_MON_OSDMAP_LEASE_ACK),
    epoch(e) {
  }
  
  char *get_type_name() { return "omap_lease_ack"; }
  
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
