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

#include "include/ceph_features.h"
#include "include/types.h"
#include "osd/osd_types.h"

class MOSDBoot : public MessageInstance<MOSDBoot, PaxosServiceMessage> {
public:
  friend factory;
private:
  static constexpr int HEAD_VERSION = 7;
  static constexpr int COMPAT_VERSION = 7;

 public:
  OSDSuperblock sb;
  entity_addrvec_t hb_back_addrs, hb_front_addrs;
  entity_addrvec_t cluster_addrs;
  epoch_t boot_epoch;  // last epoch this daemon was added to the map (if any)
  map<string,string> metadata; ///< misc metadata about this osd
  uint64_t osd_features;

  MOSDBoot()
    : MessageInstance(MSG_OSD_BOOT, 0, HEAD_VERSION, COMPAT_VERSION),
      boot_epoch(0), osd_features(0)
  { }
  MOSDBoot(OSDSuperblock& s, epoch_t e, epoch_t be,
	   const entity_addrvec_t& hb_back_addr_ref,
	   const entity_addrvec_t& hb_front_addr_ref,
           const entity_addrvec_t& cluster_addr_ref,
	   uint64_t feat)
    : MessageInstance(MSG_OSD_BOOT, e, HEAD_VERSION, COMPAT_VERSION),
      sb(s),
      hb_back_addrs(hb_back_addr_ref),
      hb_front_addrs(hb_front_addr_ref),
      cluster_addrs(cluster_addr_ref),
      boot_epoch(be),
      osd_features(feat)
  { }
  
private:
  ~MOSDBoot() override { }

public:
  std::string_view get_type_name() const override { return "osd_boot"; }
  void print(ostream& out) const override {
    out << "osd_boot(osd." << sb.whoami << " booted " << boot_epoch
	<< " features " << osd_features
	<< " v" << version << ")";
  }
  
  void encode_payload(uint64_t features) override {
    header.version = HEAD_VERSION;
    header.compat_version = COMPAT_VERSION;
    using ceph::encode;
    paxos_encode();
    if (!HAVE_FEATURE(features, SERVER_NAUTILUS)) {
      header.version = 6;
      header.compat_version = 6;
      encode(sb, payload);
      hb_back_addrs.legacy_addr().encode(payload, features);
      cluster_addrs.legacy_addr().encode(payload, features);
      encode(boot_epoch, payload);
      hb_front_addrs.legacy_addr().encode(payload, features);
      encode(metadata, payload);
      encode(osd_features, payload);
      return;
    }
    encode(sb, payload);
    encode(hb_back_addrs, payload, features);
    encode(cluster_addrs, payload, features);
    encode(boot_epoch, payload);
    encode(hb_front_addrs, payload, features);
    encode(metadata, payload);
    encode(osd_features, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    paxos_decode(p);
    if (header.version < 7) {
      entity_addr_t a;
      decode(sb, p);
      decode(a, p);
      hb_back_addrs = entity_addrvec_t(a);
      decode(a, p);
      cluster_addrs = entity_addrvec_t(a);
      decode(boot_epoch, p);
      decode(a, p);
      hb_front_addrs = entity_addrvec_t(a);
      decode(metadata, p);
      decode(osd_features, p);
      return;
    }
    decode(sb, p);
    decode(hb_back_addrs, p);
    decode(cluster_addrs, p);
    decode(boot_epoch, p);
    decode(hb_front_addrs, p);
    decode(metadata, p);
    decode(osd_features, p);
  }
};

#endif
