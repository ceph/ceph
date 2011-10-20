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


class MOSDMap : public Message {
 public:
  ceph_fsid_t fsid;
  map<epoch_t, bufferlist> maps;
  map<epoch_t, bufferlist> incremental_maps;
  epoch_t oldest_map, newest_map;

  epoch_t get_first() {
    epoch_t e = 0;
    map<epoch_t, bufferlist>::iterator i = maps.begin();
    if (i != maps.end())  e = i->first;
    i = incremental_maps.begin();    
    if (i != incremental_maps.end() &&
        (e == 0 || i->first < e)) e = i->first;
    return e;
  }
  epoch_t get_last() {
    epoch_t e = 0;
    map<epoch_t, bufferlist>::reverse_iterator i = maps.rbegin();
    if (i != maps.rend())  e = i->first;
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


  MOSDMap() : Message(CEPH_MSG_OSD_MAP) { }
  MOSDMap(const ceph_fsid_t &f, OSDMap *oc=0)
    : Message(CEPH_MSG_OSD_MAP), fsid(f),
      oldest_map(0), newest_map(0)
  {
    if (oc)
      oc->encode(maps[oc->get_epoch()]);
  }
private:
  ~MOSDMap() {}

public:
  // marshalling
  void decode_payload(CephContext *cct) {
    bufferlist::iterator p = payload.begin();
    ::decode(fsid, p);
    ::decode(incremental_maps, p);
    ::decode(maps, p);
    if (header.version > 1) {
      ::decode(oldest_map, p);
      ::decode(newest_map, p);
    }
  }
  void encode_payload(CephContext *cct) {
    ::encode(fsid, payload);
    header.version = 2;
    if (connection && (!connection->has_feature(CEPH_FEATURE_PGID64) ||
		       !connection->has_feature(CEPH_FEATURE_PGPOOL3))) {
      // reencode maps using old format
      //
      // FIXME: this can probably be done more efficiently higher up
      // the stack, or maybe replaced with something that only
      // includes the pools the client cares about.
      for (map<epoch_t,bufferlist>::iterator p = incremental_maps.begin();
	   p != incremental_maps.end();
	   ++p) {
	OSDMap::Incremental inc;
	bufferlist::iterator q = p->second.begin();
	inc.decode(q);
	p->second.clear();
	inc.encode(p->second, connection->get_features());
      }
      for (map<epoch_t,bufferlist>::iterator p = maps.begin();
	   p != maps.end();
	   ++p) {
	OSDMap m;
	m.decode(p->second);
	p->second.clear();
	m.encode(p->second, connection->get_features());
      }
      header.version = 1;
    }
    ::encode(incremental_maps, payload);
    ::encode(maps, payload);
    if (header.version >= 2) {
      ::encode(oldest_map, payload);
      ::encode(newest_map, payload);
    }
  }

  const char *get_type_name() { return "omap"; }
  void print(ostream& out) {
    out << "osd_map(" << get_first() << ".." << get_last();
    if (oldest_map || newest_map)
      out << " src has " << oldest_map << ".." << newest_map;
    out << ")";
  }
};

#endif
