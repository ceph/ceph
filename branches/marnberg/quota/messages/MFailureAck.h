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


#ifndef __MFAILUREACK_H
#define __MFAILUREACK_H

#include "MFailure.h"


class MFailureAck : public Message {
 public:
  entity_name_t failed;
  MFailureAck(MFailure *m) : Message(MSG_FAILURE_ACK) {
    this->failed = m->get_failed();
  }
  MFailureAck() {}
 
  entity_name_t get_failed() { return failed; }

  virtual void decode_payload(crope& s, int& off) {
    s.copy(0, sizeof(failed), (char*)&failed);
    off += sizeof(failed);
  }
  virtual void encode_payload(crope& s) {
    s.append((char*)&failed, sizeof(failed));
  }

  virtual char *get_type_name() { return "faila"; }
};

#endif
