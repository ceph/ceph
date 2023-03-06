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

class MOSDMap final : public Message {
private:
  static constexpr int HEAD_VERSION = 4;
  static constexpr int COMPAT_VERSION = 3;

public:
  uuid_d fsid;
  uint64_t encode_features = 0;
  std::map<epoch_t, ceph::buffer::list> maps;
  std::map<epoch_t, ceph::buffer::list> incremental_maps;
  /**
   * cluster_osdmap_trim_lower_bound
   *
   * Encodes a lower bound on the monitor's osdmap trim bound.  Recipients
   * can safely trim up to this bound.  The sender stores maps back to
   * cluster_osdmap_trim_lower_bound.
   *
   * This field was formerly named oldest_map and encoded the oldest map
   * stored by the sender.  The primary usage of this field, however, was to
   * allow the recipient to trim.  The secondary usage was to inform the
   * recipient of how many maps the sender stored in case it needed to request
   * more.  For both purposes, it should be safe for an older OSD to interpret
   * this field as oldest_map, and it should be safe for a new osd to interpret
   * the oldest_map field sent by an older osd as
   * cluster_osdmap_trim_lower_bound.
   * See bug https://tracker.ceph.com/issues/49689
   */
  epoch_t cluster_osdmap_trim_lower_bound = 0;
  epoch_t newest_map = 0;

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

  MOSDMap() : Message{CEPH_MSG_OSD_MAP, HEAD_VERSION, COMPAT_VERSION} { }
  MOSDMap(const uuid_d &f, const uint64_t features)
    : Message{CEPH_MSG_OSD_MAP, HEAD_VERSION, COMPAT_VERSION},
      fsid(f), encode_features(features),
      cluster_osdmap_trim_lower_bound(0), newest_map(0) { }
private:
  ~MOSDMap() final {}
public:
  // marshalling
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(fsid, p);
    decode(incremental_maps, p);
    decode(maps, p);
    if (header.version >= 2) {
      decode(cluster_osdmap_trim_lower_bound, p);
      decode(newest_map, p);
    } else {
      cluster_osdmap_trim_lower_bound = 0;
      newest_map = 0;
    }
    if (header.version >= 4) {
      // removed in octopus
      mempool::osdmap::map<int64_t,snap_interval_set_t> gap_removed_snaps;
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
      encode(cluster_osdmap_trim_lower_bound, payload);
      encode(newest_map, payload);
    }
    if (header.version >= 4) {
      encode((uint32_t)0, payload);
    }
  }

  std::string_view get_type_name() const override { return "osdmap"; }
  void print(std::ostream& out) const override {
    out << "osd_map(" << get_first() << ".." << get_last();
    if (cluster_osdmap_trim_lower_bound || newest_map)
      out << " src has " << cluster_osdmap_trim_lower_bound
          << ".." << newest_map;
    out << ")";
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
