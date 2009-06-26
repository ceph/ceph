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


#ifndef __MOSDGETMAPACK_H
#define __MOSDGETMAPACK_H

#include "msg/Message.h"
#include "osd/OSDMap.h"


class MOSDMap : public Message {
 public:
  ceph_fsid_t fsid;
  map<epoch_t, bufferlist> maps;
  map<epoch_t, bufferlist> incremental_maps;

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


  MOSDMap() : Message(CEPH_MSG_OSD_MAP) { }
  MOSDMap(const ceph_fsid_t &f, OSDMap *oc=0) : Message(CEPH_MSG_OSD_MAP),
						fsid(f) {
    if (oc)
      oc->encode(maps[oc->get_epoch()]);
  }


  // marshalling
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(fsid, p);
    ::decode(incremental_maps, p);
    ::decode(maps, p);
  }
  void encode_payload() {
    ::encode(fsid, payload);
    ::encode(incremental_maps, payload);
    ::encode(maps, payload);
  }

  const char *get_type_name() { return "omap"; }
  void print(ostream& out) {
    out << "osd_map(" << get_first() << "," << get_last() << ")";
  }
};

#endif
