// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 OVH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#ifndef CEPH_MOSDFORCERECOVERY_H
#define CEPH_MOSDFORCERECOVERY_H

#include "msg/Message.h"

/*
 * instruct an OSD to boost/unboost recovery/backfill priority of some or all pg(s)
 */

// boost priority of recovery
static const int OFR_RECOVERY = 1;

// boost priority of backfill
static const int OFR_BACKFILL = 2;

// cancel priority boost, requeue if necessary
static const int OFR_CANCEL = 4;

struct MOSDForceRecovery : public Message {

  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;

  uuid_d fsid;
  vector<pg_t> forced_pgs;
  uint8_t options = 0;

  MOSDForceRecovery() : Message(MSG_OSD_FORCE_RECOVERY, HEAD_VERSION, COMPAT_VERSION) {}
  MOSDForceRecovery(const uuid_d& f, char opts) :
    Message(MSG_OSD_FORCE_RECOVERY, HEAD_VERSION, COMPAT_VERSION),
    fsid(f), options(opts) {}
  MOSDForceRecovery(const uuid_d& f, vector<pg_t>& pgs, char opts) :
    Message(MSG_OSD_FORCE_RECOVERY, HEAD_VERSION, COMPAT_VERSION),
    fsid(f), forced_pgs(pgs), options(opts) {}
private:
  ~MOSDForceRecovery() {}

public:
  const char *get_type_name() const { return "force_recovery"; }
  void print(ostream& out) const {
    out << "force_recovery(";
    if (forced_pgs.empty())
      out << "osd";
    else
      out << forced_pgs;
    if (options & OFR_RECOVERY)
      out << " recovery";
    if (options & OFR_BACKFILL)
       out << " backfill";
    if (options & OFR_CANCEL)
       out << " cancel";
     out << ")";
  }

  void encode_payload(uint64_t features) {
    using ceph::encode;
    encode(fsid, payload);
    encode(forced_pgs, payload);
    encode(options, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    decode(fsid, p);
    decode(forced_pgs, p);
    decode(options, p);
  }
};

#endif /* CEPH_MOSDFORCERECOVERY_H_ */
