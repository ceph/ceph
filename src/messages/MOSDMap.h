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


#ifndef CEPH_MOSDMAP_H
#define CEPH_MOSDMAP_H

#include "msg/Message.h"
#include "osd/OSDMap.h"
#include "include/ceph_features.h"

class MOSDMap : public MessageInstance<MOSDMap> {
public:
  friend factory;
private:
  static constexpr int HEAD_VERSION = 4;
  static constexpr int COMPAT_VERSION = 3;

 public:
  uuid_d fsid;
  uint64_t encode_features = 0;
  std::map<epoch_t, ceph::buffer::list> maps;
  std::map<epoch_t, ceph::buffer::list> incremental_maps;
  epoch_t oldest_map =0, newest_map = 0;

  // if we are fetching maps from the mon and have to jump a gap
  // (client's next needed std::map is older than mon's oldest) we can
  // share removed snaps from the gap here.
  mempool::osdmap::map<int64_t,OSDMap::snap_interval_set_t> gap_removed_snaps;

  epoch_t get_first() const {
    epoch_t e = 0;
    auto i = maps.cbegin();
    if (i != maps.cend())  e = i->first;
    i = incremental_maps.begin();    
    if (i != incremental_maps.end() &&
        (e == 0 || i->first < e)) e = i->first;
    return e;
  }
  epoch_t get_last() const {
    epoch_t e = 0;
    auto i = maps.crbegin();
    if (i != maps.crend())  e = i->first;
    i = incremental_maps.rbegin();    
    if (i != incremental_maps.rend() &&
        (e == 0 || i->first > e)) e = i->first;
    return e;
  }
  epoch_t get_oldest() {
    return oldest_map;
  }
  epoch_t get_newest() {
    return newest_map;
  }


  MOSDMap() : MessageInstance(CEPH_MSG_OSD_MAP, HEAD_VERSION, COMPAT_VERSION) { }
  MOSDMap(const uuid_d &f, const uint64_t features)
    : MessageInstance(CEPH_MSG_OSD_MAP, HEAD_VERSION, COMPAT_VERSION),
      fsid(f), encode_features(features),
      oldest_map(0), newest_map(0) { }
private:
  ~MOSDMap() override {}
public:
  // marshalling
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(fsid, p);
    decode(incremental_maps, p);
    decode(maps, p);
    if (header.version >= 2) {
      decode(oldest_map, p);
      decode(newest_map, p);
    } else {
      oldest_map = 0;
      newest_map = 0;
    }
    if (header.version >= 4) {
      decode(gap_removed_snaps, p);
    }
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    header.version = HEAD_VERSION;
    header.compat_version = COMPAT_VERSION;
    encode(fsid, payload);
    if (OSDMap::get_significant_features(encode_features) !=
         OSDMap::get_significant_features(features)) {
      if ((features & CEPH_FEATURE_PGID64) == 0 ||
	  (features & CEPH_FEATURE_PGPOOL3) == 0) {
	header.version = 1;  // old old_client version
	header.compat_version = 1;
      } else if ((features & CEPH_FEATURE_OSDENC) == 0) {
	header.version = 2;  // old pg_pool_t
	header.compat_version = 2;
      }

      // reencode maps using old format
      //
      // FIXME: this can probably be done more efficiently higher up
      // the stack, or maybe replaced with something that only
      // includes the pools the client cares about.
      for (auto p = incremental_maps.begin(); p != incremental_maps.end(); ++p) {
	OSDMap::Incremental inc;
	auto q = p->second.cbegin();
	inc.decode(q);
	// always encode with subset of osdmaps canonical features
	uint64_t f = inc.encode_features & features;
	p->second.clear();
	if (inc.fullmap.length()) {
	  // embedded full std::map?
	  OSDMap m;
	  m.decode(inc.fullmap);
	  inc.fullmap.clear();
	  m.encode(inc.fullmap, f | CEPH_FEATURE_RESERVED);
	}
	if (inc.crush.length()) {
	  // embedded crush std::map
	  CrushWrapper c;
	  auto p = inc.crush.cbegin();
	  c.decode(p);
	  inc.crush.clear();
	  c.encode(inc.crush, f);
	}
	inc.encode(p->second, f | CEPH_FEATURE_RESERVED);
      }
      for (auto p = maps.begin(); p != maps.end(); ++p) {
	OSDMap m;
	m.decode(p->second);
	// always encode with subset of osdmaps canonical features
	uint64_t f = m.get_encoding_features() & features;
	p->second.clear();
	m.encode(p->second, f | CEPH_FEATURE_RESERVED);
      }
    }
    encode(incremental_maps, payload);
    encode(maps, payload);
    if (header.version >= 2) {
      encode(oldest_map, payload);
      encode(newest_map, payload);
    }
    if (header.version >= 4) {
      encode(gap_removed_snaps, payload);
    }
  }

  std::string_view get_type_name() const override { return "osdmap"; }
  void print(std::ostream& out) const override {
    out << "osd_map(" << get_first() << ".." << get_last();
    if (oldest_map || newest_map)
      out << " src has " << oldest_map << ".." << newest_map;
    if (!gap_removed_snaps.empty())
      out << " +gap_removed_snaps";
    out << ")";
  }
};

#endif
