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


#ifndef CEPH_MCLIENTREQUESTFORWARD_H
#define CEPH_MCLIENTREQUESTFORWARD_H

#include "msg/Message.h"

class MClientRequestForward : public MessageInstance<MClientRequestForward> {
public:
  friend factory;
private:
  int32_t dest_mds;
  int32_t num_fwd;
  bool client_must_resend;

protected:
  MClientRequestForward()
    : MessageInstance(CEPH_MSG_CLIENT_REQUEST_FORWARD),
      dest_mds(-1), num_fwd(-1), client_must_resend(false) {}
  MClientRequestForward(ceph_tid_t t, int dm, int nf, bool cmr) :
    MessageInstance(CEPH_MSG_CLIENT_REQUEST_FORWARD),
    dest_mds(dm), num_fwd(nf), client_must_resend(cmr) {
    ceph_assert(client_must_resend);
    header.tid = t;
  }
  ~MClientRequestForward() override {}

public:
  int get_dest_mds() const { return dest_mds; }
  int get_num_fwd() const { return num_fwd; }
  bool must_resend() const { return client_must_resend; }

  std::string_view get_type_name() const override { return "client_request_forward"; }
  void print(ostream& o) const override {
    o << "client_request_forward(" << get_tid()
      << " to mds." << dest_mds
      << " num_fwd=" << num_fwd
      << (client_must_resend ? " client_must_resend":"")
      << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(dest_mds, payload);
    encode(num_fwd, payload);
    encode(client_must_resend, payload);
  }

  void decode_payload() override {
    auto p = payload.cbegin();
    decode(dest_mds, p);
    decode(num_fwd, p);
    decode(client_must_resend, p);
  }
};

#endif
