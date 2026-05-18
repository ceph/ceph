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

#ifndef CEPH_MCLIENTSESSION_H
#define CEPH_MCLIENTSESSION_H

#include <limits.h> // for UINT64_MAX

#include <cstdint>
#include <iosfwd>
#include <map>
#include <string>
#include <vector>

#include "include/ceph_fs.h" // for ceph_mds_session_head
#include "include/types.h" // for ceph_tid_t, version_t
#include "include/utime.h"
#include "mds/MDSAuthCaps.h" // for MDSCapAuth
#include "msg/Message.h"
#include "mds/mdstypes.h" // for feature_bitset_t, metric_spec_t

class MClientSession final : public SafeMessage {
private:
  static constexpr int HEAD_VERSION = 7;
  static constexpr int COMPAT_VERSION = 1;

public:
  ceph_mds_session_head head;
  static constexpr unsigned SESSION_BLOCKLISTED = (1<<0);

  uint32_t flags = 0;
  std::map<std::string, std::string> metadata;
  feature_bitset_t supported_features;
  metric_spec_t metric_spec;
  std::vector<MDSCapAuth> cap_auths;
  ceph_tid_t oldest_client_tid = UINT64_MAX;

  int get_op() const { return head.op; }
  version_t get_seq() const { return head.seq; }
  utime_t get_stamp() const { return utime_t(head.stamp); }
  int get_max_caps() const { return head.max_caps; }
  int get_max_leases() const { return head.max_leases; }

protected:
  MClientSession() : SafeMessage{CEPH_MSG_CLIENT_SESSION, HEAD_VERSION, COMPAT_VERSION} { }
  MClientSession(int o, version_t s=0, unsigned msg_flags=0);
  MClientSession(int o, utime_t st);
  ~MClientSession() final {}

public:
  std::string_view get_type_name() const override { return "client_session"; }
  void print(std::ostream& out) const override;

  void decode_payload() override;
  void encode_payload(uint64_t features) override;

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

#endif
