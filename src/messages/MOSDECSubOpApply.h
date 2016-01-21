// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef MOSDECSUBOPAPPLY_H
#define MOSDECSUBOPAPPLY_H

#include "msg/Message.h"
#include "osd/osd_types.h"
#include "osd/ECMsgTypes.h"

class MOSDECSubOpApply : public Message {
  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;
  
public:
  spg_t pgid;
  epoch_t map_epoch;
  ECSubApply op;
  
  int get_cost() const {
    return 0;
  }
  
  MOSDECSubOpApply()
    : Message(MSG_OSD_EC_APPLY, HEAD_VERSION, COMPAT_VERSION)
    {}
  MOSDECSubOpApply(ECSubApply &in_op)
    : Message(MSG_OSD_EC_APPLY, HEAD_VERSION, COMPAT_VERSION) {
    op.claim(in_op);
  }

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

  const char *get_type_name() const { return "MOSDECSubOpApply"; }

  void print(ostream& out) const {
    out << "MOSDECSubOpApply(" << pgid
	<< " " << map_epoch
	<< " " << op;
    out << ")";
  }
};

#endif
