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

class MOSDForceRecovery : public MessageInstance<MOSDForceRecovery> {
public:
  friend factory;

  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 2;

  uuid_d fsid;
  vector<spg_t> forced_pgs;
  uint8_t options = 0;

  MOSDForceRecovery() : MessageInstance(MSG_OSD_FORCE_RECOVERY, HEAD_VERSION, COMPAT_VERSION) {}
  MOSDForceRecovery(const uuid_d& f, char opts) :
    MessageInstance(MSG_OSD_FORCE_RECOVERY, HEAD_VERSION, COMPAT_VERSION),
    fsid(f), options(opts) {}
  MOSDForceRecovery(const uuid_d& f, vector<spg_t>& pgs, char opts) :
    MessageInstance(MSG_OSD_FORCE_RECOVERY, HEAD_VERSION, COMPAT_VERSION),
    fsid(f), forced_pgs(pgs), options(opts) {}
private:
  ~MOSDForceRecovery() {}

public:
  std::string_view get_type_name() const { return "force_recovery"; }
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
    if (!HAVE_FEATURE(features, SERVER_MIMIC)) {
      header.version = 1;
      header.compat_version = 1;
      vector<pg_t> pgs;
      for (auto pgid : forced_pgs) {
	pgs.push_back(pgid.pgid);
      }
      encode(fsid, payload);
      encode(pgs, payload);
      encode(options, payload);
      return;
    }
    header.version = HEAD_VERSION;
    header.compat_version = COMPAT_VERSION;
    encode(fsid, payload);
    encode(forced_pgs, payload);
    encode(options, payload);
  }
  void decode_payload() {
    auto p = payload.cbegin();
    if (header.version == 1) {
      vector<pg_t> pgs;
      decode(fsid, p);
      decode(pgs, p);
      decode(options, p);
      for (auto pg : pgs) {
	// note: this only works with replicated pools.  if a pre-mimic mon
	// tries to force a mimic+ osd on an ec pool it will not work.
	forced_pgs.push_back(spg_t(pg));
      }
      return;
    }
    decode(fsid, p);
    decode(forced_pgs, p);
    decode(options, p);
  }
};

#endif /* CEPH_MOSDFORCERECOVERY_H_ */
