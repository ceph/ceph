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

#ifndef CEPH_MCLIENTSNAP_H
#define CEPH_MCLIENTSNAP_H

#include <iosfwd>
#include <vector>

#include "msg/Message.h"

#include "include/buffer.h"
#include "include/ceph_fs.h" // for ceph_mds_snap_head
#include "include/fs_types.h" // for inodeno_t

class MClientSnap final : public SafeMessage {
public:
  ceph_mds_snap_head head;
  ceph::buffer::list bl;
  
  // (for split only)
  std::vector<inodeno_t> split_inos;
  std::vector<inodeno_t> split_realms;

protected:
  MClientSnap(int o=0) : 
    SafeMessage{CEPH_MSG_CLIENT_SNAP} {
    memset(&head, 0, sizeof(head));
    head.op = o;
  }
  ~MClientSnap() final {}

public:  
  std::string_view get_type_name() const override { return "client_snap"; }
  void print(std::ostream& out) const override;

  void encode_payload(uint64_t features) override;
  void decode_payload() override;

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

#endif
