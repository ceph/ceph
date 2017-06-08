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

#ifndef MOSDECSUBOPREAD_H
#define MOSDECSUBOPREAD_H

#include "msg/Message.h"
#include "osd/osd_types.h"
#include "osd/ECMsgTypes.h"

class MOSDECSubOpRead : public Message {
  static const int HEAD_VERSION = 2;
  static const int COMPAT_VERSION = 1;

public:
  spg_t pgid;
  epoch_t map_epoch;
  ECSubRead op;

  int get_cost() const {
    return 0;
  }

  MOSDECSubOpRead() :
    Message(MSG_OSD_EC_READ, HEAD_VERSION, COMPAT_VERSION)
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
    ::encode(op, payload, features);
  }

  const char *get_type_name() const { return "MOSDECSubOpRead"; }

  void print(ostream& out) const {
    out << "MOSDECSubOpRead(" << pgid
	<< " " << map_epoch
	<< " " << op;
    out << ")";
  }
};

#endif
