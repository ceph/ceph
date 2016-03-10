// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 Inktank, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */
#ifndef CEPH_MTIMECHECK_H
#define CEPH_MTIMECHECK_H

struct MTimeCheck : public Message
{
  static const int HEAD_VERSION = 1;

  enum {
    OP_PING = 1,
    OP_PONG = 2,
    OP_REPORT = 3,
  };

  int op;
  version_t epoch;
  version_t round;

  utime_t timestamp;
  map<entity_inst_t, double> skews;
  map<entity_inst_t, double> latencies;

  MTimeCheck() : Message(MSG_TIMECHECK, HEAD_VERSION) { }
  MTimeCheck(int op) :
    Message(MSG_TIMECHECK, HEAD_VERSION),
    op(op)
  { }

private:
  ~MTimeCheck() { }

public:
  const char *get_type_name() const { return "time_check"; }
  const char *get_op_name() const {
    switch (op) {
    case OP_PING: return "ping";
    case OP_PONG: return "pong";
    case OP_REPORT: return "report";
    }
    return "???";
  }
  void print(ostream &o) const {
    o << "time_check( " << get_op_name()
      << " e " << epoch << " r " << round;
    if (op == OP_PONG) {
      o << " ts " << timestamp;
    } else if (op == OP_REPORT) {
      o << " #skews " << skews.size()
        << " #latencies " << latencies.size();
    }
    o << " )";
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(op, p);
    ::decode(epoch, p);
    ::decode(round, p);
    ::decode(timestamp, p);
    ::decode(skews, p);
    ::decode(latencies, p);
  }

  void encode_payload(uint64_t features) {
    ::encode(op, payload);
    ::encode(epoch, payload);
    ::encode(round, payload);
    ::encode(timestamp, payload);
    ::encode(skews, payload);
    ::encode(latencies, payload);
  }

};
REGISTER_MESSAGE(MTimeCheck, MSG_TIMECHECK);
#endif /* CEPH_MTIMECHECK_H */
