// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#ifndef CEPH_MOSDBACKOFF_H
#define CEPH_MOSDBACKOFF_H

#include "msg/Message.h"
#include "osd/osd_types.h"

class MOSDBackoff : public Message {
public:
  uint8_t op = 0;           ///< CEPH_OSD_BACKOFF_OP_*
  uint64_t id = 0;          ///< unique id within this session
  hobject_t begin, end;     ///< [) range to block, unless ==, block single obj
  epoch_t osd_epoch = 0;

  MOSDBackoff() : Message(CEPH_MSG_OSD_BACKOFF) {}
  MOSDBackoff(uint8_t op_, uint64_t id_,
	      hobject_t begin_, hobject_t end_, epoch_t ep)
    : Message(CEPH_MSG_OSD_BACKOFF),
      op(op_),
      id(id_),
      begin(begin_),
      end(end_),
      osd_epoch(ep) { }

  void encode_payload(uint64_t features) override {
    ::encode(op, payload);
    ::encode(id, payload);
    ::encode(begin, payload);
    ::encode(end, payload);
    ::encode(osd_epoch, payload);
  }

  void decode_payload() override {
    auto p = payload.begin();
    ::decode(op, p);
    ::decode(id, p);
    ::decode(begin, p);
    ::decode(end, p);
    ::decode(osd_epoch, p);
  }

  const char *get_type_name() const override { return "osd_backoff"; }

  void print(ostream& out) const override {
    out << "osd_backoff(" << ceph_osd_backoff_op_name(op)
	<< " id " << id
	<< " [" << begin << "," << end << ")"
	<< " epoch " << osd_epoch << ")";
  }
};

#endif
