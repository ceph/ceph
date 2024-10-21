// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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

#include "MClientSnap.h"

#include <ostream>

#include "msg/Message.h"

#include "include/buffer.h"
#include "include/ceph_fs_encoder.h"
#include "include/encoding_vector.h"

void MClientSnap::print(std::ostream& out) const {
  out << "client_snap(" << ceph_snap_op_name(head.op);
  if (head.split)
    out << " split=" << inodeno_t(head.split);
  out << " tracelen=" << bl.length();
  out << ")";
}

void MClientSnap::encode_payload(uint64_t features) {
  using ceph::encode;
  using ceph::encode_nohead;
  head.num_split_inos = split_inos.size();
  head.num_split_realms = split_realms.size();
  head.trace_len = bl.length();
  encode(head, payload);
  encode_nohead(split_inos, payload);
  encode_nohead(split_realms, payload);
  encode_nohead(bl, payload);
}

void MClientSnap::decode_payload() {
  using ceph::decode;
  using ceph::decode_nohead;
  auto p = payload.cbegin();
  decode(head, p);
  decode_nohead(head.num_split_inos, split_inos, p);
  decode_nohead(head.num_split_realms, split_realms, p);
  decode_nohead(head.trace_len, bl, p);
  ceph_assert(p.end());
}
