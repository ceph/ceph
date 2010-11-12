// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2010 Dreamhost
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#ifndef CEPH_MOSDPGMISSING_H
#define CEPH_MOSDPGMISSING_H

#include "msg/Message.h"

class MOSDPGMissing : public Message {
  epoch_t epoch;

public:
  PG::Info info;
  PG::Missing missing;

  epoch_t get_epoch() { return epoch; }

  MOSDPGMissing() {}
  MOSDPGMissing(version_t mv, const PG::Info &info_,
		const PG::Missing &missing_)
    : Message(MSG_OSD_PG_MISSING), epoch(mv), info(info_),
      missing(missing_) { }
private:
  ~MOSDPGMissing() {}

public:
  const char *get_type_name() { return "pg_missing"; }
  void print(ostream& out) {
    out << "pg_missing(" << info.pgid << " e" << epoch << ")";
  }

  void encode_payload() {
    ::encode(epoch, payload);
    ::encode(info, payload);
    ::encode(missing, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(epoch, p);
    ::decode(info, p);
    ::decode(missing, p);
  }
};

#endif
