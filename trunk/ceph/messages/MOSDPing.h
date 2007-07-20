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

#ifndef __MOSDPING_H
#define __MOSDPING_H

#include "common/Clock.h"

#include "msg/Message.h"


class MOSDPing : public Message {
 public:
  epoch_t map_epoch;
  bool ack;
  float avg_qlen;
  double read_mean_time;

  MOSDPing(epoch_t e, 
	   float aq,
	   double _read_mean_time,
	   bool a=false) : Message(MSG_OSD_PING), map_epoch(e), ack(a), avg_qlen(aq), read_mean_time(_read_mean_time) {
  }
  MOSDPing() {}

  virtual void decode_payload() {
    int off = 0;
    ::_decode(map_epoch, payload, off);
    ::_decode(ack, payload, off);
    ::_decode(avg_qlen, payload, off);
    ::_decode(read_mean_time, payload, off);
  }
  virtual void encode_payload() {
    ::_encode(map_epoch, payload);
    ::_encode(ack, payload);
    ::_encode(avg_qlen, payload);
    ::_encode(read_mean_time, payload);
  }

  virtual char *get_type_name() { return "osd_ping"; }
};

#endif
