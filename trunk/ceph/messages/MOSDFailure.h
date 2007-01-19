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


#ifndef __MOSDFAILURE_H
#define __MOSDFAILURE_H

#include "msg/Message.h"


class MOSDFailure : public Message {
 public:
  msg_addr_t    failed;
  entity_inst_t inst;
  epoch_t       epoch;

  MOSDFailure() {}
  MOSDFailure(msg_addr_t f, const entity_inst_t& i, epoch_t e) : 
    Message(MSG_OSD_FAILURE),
    failed(f), inst(i), epoch(e) {}
 
  msg_addr_t get_failed() { return failed; }
  entity_inst_t& get_inst() { return inst; }
  epoch_t get_epoch() { return epoch; }

  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(failed), (char*)&failed);
    off += sizeof(failed);
    payload.copy(off, sizeof(inst), (char*)&inst);
    off += sizeof(inst);
    payload.copy(off, sizeof(epoch), (char*)&epoch);
    off += sizeof(epoch);
  }
  void encode_payload() {
    payload.append((char*)&failed, sizeof(failed));
    payload.append((char*)&inst, sizeof(inst));
    payload.append((char*)&epoch, sizeof(epoch));
  }

  virtual char *get_type_name() { return "osdfail"; }
};

#endif
