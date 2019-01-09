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

#ifndef CEPH_MCLIENTSESSION_H
#define CEPH_MCLIENTSESSION_H

#include "msg/Message.h"
#include "mds/mdstypes.h"

class MClientSession : public MessageInstance<MClientSession> {
public:
  friend factory;
private:
  static constexpr int HEAD_VERSION = 3;
  static constexpr int COMPAT_VERSION = 1;

public:
  ceph_mds_session_head head;

  std::map<std::string, std::string> metadata;
  feature_bitset_t supported_features;

  int get_op() const { return head.op; }
  version_t get_seq() const { return head.seq; }
  utime_t get_stamp() const { return utime_t(head.stamp); }
  int get_max_caps() const { return head.max_caps; }
  int get_max_leases() const { return head.max_leases; }

protected:
  MClientSession() : MessageInstance(CEPH_MSG_CLIENT_SESSION, HEAD_VERSION, COMPAT_VERSION) { }
  MClientSession(int o, version_t s=0) : 
    MessageInstance(CEPH_MSG_CLIENT_SESSION, HEAD_VERSION, COMPAT_VERSION) {
    memset(&head, 0, sizeof(head));
    head.op = o;
    head.seq = s;
  }
  MClientSession(int o, utime_t st) : 
    MessageInstance(CEPH_MSG_CLIENT_SESSION, HEAD_VERSION, COMPAT_VERSION) {
    memset(&head, 0, sizeof(head));
    head.op = o;
    head.seq = 0;
    st.encode_timeval(&head.stamp);
  }
  ~MClientSession() override {}

public:
  std::string_view get_type_name() const override { return "client_session"; }
  void print(ostream& out) const override {
    out << "client_session(" << ceph_session_op_name(get_op());
    if (get_seq())
      out << " seq " << get_seq();
    if (get_op() == CEPH_SESSION_RECALL_STATE)
      out << " max_caps " << head.max_caps << " max_leases " << head.max_leases;
    out << ")";
  }

  void decode_payload() override { 
    auto p = payload.cbegin();
    decode(head, p);
    if (header.version >= 2)
      decode(metadata, p);
    if (header.version >= 3)
      decode(supported_features, p);
  }
  void encode_payload(uint64_t features) override { 
    using ceph::encode;
    encode(head, payload);
    if (metadata.empty() && supported_features.empty()) {
      // If we're not trying to send any metadata (always the case if
      // we are a server) then send older-format message to avoid upsetting
      // old kernel clients.
      header.version = 1;
    } else {
      header.version = HEAD_VERSION;
      encode(metadata, payload);
      encode(supported_features, payload);
    }
  }
};

#endif
