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


#ifndef CEPH_MMDSMAP_H
#define CEPH_MMDSMAP_H

#include "msg/Message.h"
#include "mds/MDSMap.h"
#include "include/ceph_features.h"

class MMDSMap : public Message {
 public:
  /*
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
  */
  
  uuid_d fsid;
  epoch_t epoch;
  bufferlist encoded;

  version_t get_epoch() const { return epoch; }
  bufferlist& get_encoded() { return encoded; }

  MMDSMap() : 
    Message(CEPH_MSG_MDS_MAP) {}
  MMDSMap(const uuid_d &f, MDSMap *mm) :
    Message(CEPH_MSG_MDS_MAP),
    fsid(f) {
    epoch = mm->get_epoch();
    mm->encode(encoded, -1);  // we will reencode with fewer features as necessary
  }
private:
  ~MMDSMap() {}

public:
  const char *get_type_name() const { return "mdsmap"; }
  void print(ostream& out) const {
    out << "mdsmap(e " << epoch << ")";
  }

  // marshalling
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(fsid, p);
    ::decode(epoch, p);
    ::decode(encoded, p);
  }
  void encode_payload(uint64_t features) {
    ::encode(fsid, payload);
    ::encode(epoch, payload);
    if ((features & CEPH_FEATURE_PGID64) == 0 ||
	(features & CEPH_FEATURE_MDSENC) == 0) {
      // reencode for old clients.
      MDSMap m;
      m.decode(encoded);
      encoded.clear();
      m.encode(encoded, features);
    }
    ::encode(encoded, payload);
  }
};
REGISTER_MESSAGE(MMDSMap, CEPH_MSG_MDS_MAP);
#endif
