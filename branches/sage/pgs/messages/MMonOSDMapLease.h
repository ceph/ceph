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

#ifndef __MMONOSDMAPLEASE_H
#define __MMONOSDMAPLEASE_H

#include "msg/Message.h"

#include "include/types.h"

class MMonOSDMapLease : public Message {
  epoch_t epoch;
  utime_t lease_expire;

 public:
  epoch_t get_epoch() { return epoch; }
  const utime_t& get_lease_expire() { return lease_expire; }

  MMonOSDMapLease(epoch_t e, utime_t le) :
    Message(MSG_MON_OSDMAP_LEASE),
    epoch(e), lease_expire(le) {
  }
  
  char *get_type_name() { return "omap_lease"; }
  
  void encode_payload() {
    payload.append((char*)&epoch, sizeof(epoch));
    payload.append((char*)&lease_expire, sizeof(lease_expire));
  }
  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(epoch), (char*)&epoch);
    off += sizeof(epoch);
    payload.copy(off, sizeof(lease_expire), (char*)&lease_expire);
    off += sizeof(lease_expire);
  }
};

#endif
