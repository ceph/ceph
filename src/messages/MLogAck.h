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

#ifndef CEPH_MLOGACK_H
#define CEPH_MLOGACK_H

class MLogAck : public Message {
public:
  uuid_d fsid;
  version_t last;
  std::string channel;

  MLogAck() : Message(MSG_LOGACK) {}
  MLogAck(uuid_d& f, version_t l) : Message(MSG_LOGACK), fsid(f), last(l) {}
private:
  ~MLogAck() {}

public:
  const char *get_type_name() const { return "log_ack"; }
  void print(ostream& out) const {
    out << "log(last " << last << ")";
  }

  void encode_payload(uint64_t features) {
    ::encode(fsid, payload);
    ::encode(last, payload);
    ::encode(channel, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(fsid, p);
    ::decode(last, p);
    if (!p.end())
      ::decode(channel, p);
  }
};
REGISTER_MESSAGE(MLogAck, MSG_LOGACK);
#endif
