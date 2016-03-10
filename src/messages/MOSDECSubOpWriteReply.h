// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef MOSDECSUBOPWRITEREPLY_H
#define MOSDECSUBOPWRITEREPLY_H

#include "msg/Message.h"
#include "osd/osd_types.h"
#include "osd/ECMsgTypes.h"

class MOSDECSubOpWriteReply : public Message {
  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;

public:
  spg_t pgid;
  epoch_t map_epoch;
  ECSubWriteReply op;

  int get_cost() const {
    return 0;
  }

  MOSDECSubOpWriteReply() :
    Message(MSG_OSD_EC_WRITE_REPLY, HEAD_VERSION, COMPAT_VERSION)
    {}

  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(pgid, p);
    ::decode(map_epoch, p);
    ::decode(op, p);
  }

  virtual void encode_payload(uint64_t features) {
    ::encode(pgid, payload);
    ::encode(map_epoch, payload);
    ::encode(op, payload);
  }

  const char *get_type_name() const { return "MOSDECSubOpWriteReply"; }

  void print(ostream& out) const {
    out << "MOSDECSubOpWriteReply(" << pgid
	<< " " << map_epoch
	<< " " << op;
    out << ")";
  }
};
REGISTER_MESSAGE(MOSDECSubOpWriteReply, MSG_OSD_EC_WRITE_REPLY);
#endif
