// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MOSDMARKMEDOWN_H
#define CEPH_MOSDMARKMEDOWN_H

#include "messages/PaxosServiceMessage.h"

class MOSDMarkMeDown final : public PaxosServiceMessage {
private:
  static constexpr int HEAD_VERSION = 4;
  static constexpr int COMPAT_VERSION = 3;

 public:
  uuid_d fsid;
  int32_t target_osd;
  entity_addrvec_t target_addrs;
  epoch_t epoch = 0;
  bool request_ack = false;          // ack requested
  bool down_and_dead = false;        // mark down and dead

  MOSDMarkMeDown()
    : PaxosServiceMessage{MSG_OSD_MARK_ME_DOWN, 0,
			  HEAD_VERSION, COMPAT_VERSION} { }
  MOSDMarkMeDown(const uuid_d &fs, int osd, const entity_addrvec_t& av,
		 epoch_t e, bool request_ack)
    : PaxosServiceMessage{MSG_OSD_MARK_ME_DOWN, e,
			  HEAD_VERSION, COMPAT_VERSION},
      fsid(fs), target_osd(osd), target_addrs(av),
      epoch(e), request_ack(request_ack) {}
  MOSDMarkMeDown(const uuid_d &fs, int osd, const entity_addrvec_t& av,
		 epoch_t e, bool request_ack, bool down_and_dead)
    : PaxosServiceMessage{MSG_OSD_MARK_ME_DOWN, e,
			  HEAD_VERSION, COMPAT_VERSION},
      fsid(fs), target_osd(osd), target_addrs(av),
      epoch(e), request_ack(request_ack), down_and_dead(down_and_dead) {}
 private:
  ~MOSDMarkMeDown() final {}

public: 
  epoch_t get_epoch() const { return epoch; }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    paxos_decode(p);
    assert(header.version >= 3);
    decode(fsid, p);
    decode(target_osd, p);
    decode(target_addrs, p);
    decode(epoch, p);
    decode(request_ack, p);
    if(header.version >= 4)
      decode(down_and_dead, p);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    assert(HAVE_FEATURE(features, SERVER_NAUTILUS));
    header.version = HEAD_VERSION;
    header.compat_version = COMPAT_VERSION;
    encode(fsid, payload);
    encode(target_osd, payload, features);
    encode(target_addrs, payload, features);
    encode(epoch, payload);
    encode(request_ack, payload);
    encode(down_and_dead, payload);
  }

  std::string_view get_type_name() const override { return "MOSDMarkMeDown"; }
  void print(std::ostream& out) const override {
    out << "MOSDMarkMeDown("
	<< "request_ack=" << request_ack
	<< ", down_and_dead=" << down_and_dead
	<< ", osd." << target_osd
	<< ", " << target_addrs
	<< ", fsid=" << fsid
	<< ")";
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
