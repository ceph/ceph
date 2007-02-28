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


#ifndef __MFAILURE_H
#define __MFAILURE_H

#include "msg/Message.h"


class MFailure : public Message {
 public:
  entity_name_t failed;
  entity_inst_t inst;

  MFailure() {}
  MFailure(entity_name_t f, entity_inst_t& i) : 
    Message(MSG_FAILURE),
    failed(f), inst(i) {}
 
  entity_name_t get_failed() { return failed; }
  entity_inst_t& get_inst() { return inst; }

  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(failed), (char*)&failed);
    off += sizeof(failed);
    payload.copy(off, sizeof(inst), (char*)&inst);
    off += sizeof(inst);
  }
  void encode_payload() {
    payload.append((char*)&failed, sizeof(failed));
    payload.append((char*)&inst, sizeof(inst));
  }

  virtual char *get_type_name() { return "fail"; }
};

#endif
