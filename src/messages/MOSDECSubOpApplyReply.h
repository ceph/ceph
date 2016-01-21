// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef MOSDECSUBOPAPPLYREPLY_H
#define	MOSDECSUBOPAPPLYREPLY_H

#include "msg/Message.h"
#include "osd/osd_types.h"
#include "osd/ECMsgTypes.h"

class MOSDECSubOpApplyReply : public Message {
  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;

public:
  spg_t pgid;
  epoch_t map_epoch;
  ECSubApplyReply op;

  int get_cost() const {
    return 0;
  }

  MOSDECSubOpApplyReply() :
    Message(MSG_OSD_EC_APPLY_REPLY, HEAD_VERSION, COMPAT_VERSION)
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

  const char *get_type_name() const { return "MOSDECSubOpApplyReply"; }

  void print(ostream& out) const {
    out << "MOSDECSubOpApplyReply(" << pgid
	<< " " << map_epoch
	<< " " << op;
    out << ")";
  }
};

#endif

