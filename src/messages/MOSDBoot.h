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

#ifndef CEPH_MOSDBOOT_H
#define CEPH_MOSDBOOT_H

#include "messages/PaxosServiceMessage.h"

#include "include/types.h"
#include "osd/osd_types.h"

class MOSDBoot : public PaxosServiceMessage {

  static const int HEAD_VERSION = 6;
  static const int COMPAT_VERSION = 6;

 public:
  OSDSuperblock sb;
  entity_addr_t hb_back_addr, hb_front_addr;
  entity_addr_t cluster_addr;
  epoch_t boot_epoch;  // last epoch this daemon was added to the map (if any)
  map<string,string> metadata; ///< misc metadata about this osd
  uint64_t osd_features;

  MOSDBoot()
    : PaxosServiceMessage(MSG_OSD_BOOT, 0, HEAD_VERSION, COMPAT_VERSION),
      boot_epoch(0), osd_features(0)
  { }
  MOSDBoot(OSDSuperblock& s, epoch_t e, epoch_t be,
	   const entity_addr_t& hb_back_addr_ref,
	   const entity_addr_t& hb_front_addr_ref,
           const entity_addr_t& cluster_addr_ref,
	   uint64_t feat)
    : PaxosServiceMessage(MSG_OSD_BOOT, e, HEAD_VERSION, COMPAT_VERSION),
      sb(s),
      hb_back_addr(hb_back_addr_ref),
      hb_front_addr(hb_front_addr_ref),
      cluster_addr(cluster_addr_ref),
      boot_epoch(be),
      osd_features(feat)
  { }
  
private:
  ~MOSDBoot() override { }

public:
  const char *get_type_name() const override { return "osd_boot"; }
  void print(ostream& out) const override {
    out << "osd_boot(osd." << sb.whoami << " booted " << boot_epoch
	<< " features " << osd_features
	<< " v" << version << ")";
  }
  
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(sb, payload);
    encode(hb_back_addr, payload, features);
    encode(cluster_addr, payload, features);
    encode(boot_epoch, payload);
    encode(hb_front_addr, payload, features);
    encode(metadata, payload);
    encode(osd_features, payload);
  }
  void decode_payload() override {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    decode(sb, p);
    decode(hb_back_addr, p);
    decode(cluster_addr, p);
    decode(boot_epoch, p);
    decode(hb_front_addr, p);
    decode(metadata, p);
    decode(osd_features, p);
  }
};

#endif
