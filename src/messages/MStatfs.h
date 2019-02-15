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


#ifndef CEPH_MSTATFS_H
#define CEPH_MSTATFS_H

#include <sys/statvfs.h>    /* or <sys/statfs.h> */
#include "messages/PaxosServiceMessage.h"

class MStatfs : public MessageInstance<MStatfs, PaxosServiceMessage> {
public:
  friend factory;
private:
  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 1;

public:
  uuid_d fsid;
  boost::optional<int64_t> data_pool;

  MStatfs() : MessageInstance(CEPH_MSG_STATFS, 0, HEAD_VERSION, COMPAT_VERSION) {}
  MStatfs(const uuid_d& f, ceph_tid_t t, boost::optional<int64_t> _data_pool,
	      version_t v) : MessageInstance(CEPH_MSG_STATFS, v,
                                            HEAD_VERSION, COMPAT_VERSION),
					         fsid(f), data_pool(_data_pool) {
    set_tid(t);
  }

private:
  ~MStatfs() override {}

public:
  std::string_view get_type_name() const override { return "statfs"; }
  void print(ostream& out) const override {
    out << "statfs(" << get_tid() << " pool "
        << (data_pool ? *data_pool : -1) << " v" << version << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(fsid, payload);
    encode(data_pool, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(fsid, p);
    if (header.version >= 2) {
      decode(data_pool, p);
    } else {
      data_pool = boost::optional<int64_t> ();
    }
  }
};

#endif
