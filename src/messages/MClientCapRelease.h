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

#ifndef CEPH_MCLIENTCAPRELEASE_H
#define CEPH_MCLIENTCAPRELEASE_H

#include "msg/Message.h"


class MClientCapRelease : public MessageInstance<MClientCapRelease> {
public:
  friend factory;

private:
  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 1;
 public:

  struct ceph_mds_cap_release head;
  vector<ceph_mds_cap_item> caps;

  // The message receiver must wait for this OSD epoch
  // before actioning this cap release.
  epoch_t osd_epoch_barrier;

  MClientCapRelease() : 
    MessageInstance(CEPH_MSG_CLIENT_CAPRELEASE, HEAD_VERSION, COMPAT_VERSION),
    osd_epoch_barrier(0)
  {
    memset(&head, 0, sizeof(head));
  }
private:
  ~MClientCapRelease() override {}

public:
  std::string_view get_type_name() const override { return "client_cap_release";}
  void print(ostream& out) const override {
    out << "client_cap_release(" << caps.size() << ")";
  }
  
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(head, p);
    decode_nohead(head.num, caps, p);
    if (header.version >= 2) {
      decode(osd_epoch_barrier, p);
    }
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    head.num = caps.size();
    encode(head, payload);
    encode_nohead(caps, payload);
    encode(osd_epoch_barrier, payload);
  }
};

#endif
