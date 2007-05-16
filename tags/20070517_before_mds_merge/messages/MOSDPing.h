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

#ifndef __MOSDPING_H
#define __MOSDPING_H

#include "msg/Message.h"


class MOSDPing : public Message {
 public:
  epoch_t map_epoch;
  bool ack;
  float avg_qlen;

  MOSDPing(epoch_t e, 
	   float aq,
	   bool a=false) : Message(MSG_OSD_PING), map_epoch(e), ack(a), avg_qlen(aq) {
  }
  MOSDPing() {}

  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(map_epoch), (char*)&map_epoch);
    off += sizeof(map_epoch);
    payload.copy(off, sizeof(ack), (char*)&ack);
    off += sizeof(ack);
    payload.copy(off, sizeof(avg_qlen), (char*)&avg_qlen);
    off += sizeof(avg_qlen);
  }
  virtual void encode_payload() {
    payload.append((char*)&map_epoch, sizeof(map_epoch));
    payload.append((char*)&ack, sizeof(ack));
    payload.append((char*)&avg_qlen, sizeof(avg_qlen));
  }

  virtual char *get_type_name() { return "oping"; }
};

#endif
