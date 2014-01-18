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

#ifndef CEPH_MMDSRESOLVE_H
#define CEPH_MMDSRESOLVE_H

#include "msg/Message.h"

#include "include/types.h"

class MMDSResolve : public Message {
 public:
  map<dirfrag_t, vector<dirfrag_t> > subtrees;
  map<dirfrag_t, vector<dirfrag_t> > ambiguous_imports;
  map<metareqid_t, bufferlist> slave_requests;

  MMDSResolve() : Message(MSG_MDS_RESOLVE) {}
private:
  ~MMDSResolve() {}

public:
  const char *get_type_name() const { return "mds_resolve"; }

  void print(ostream& out) const {
    out << "mds_resolve(" << subtrees.size()
	<< "+" << ambiguous_imports.size()
	<< " subtrees +" << slave_requests.size() << " slave requests)";
  }
  
  void add_subtree(dirfrag_t im) {
    subtrees[im].clear();
  }
  void add_subtree_bound(dirfrag_t im, dirfrag_t ex) {
    subtrees[im].push_back(ex);
  }

  void add_ambiguous_import(dirfrag_t im, const vector<dirfrag_t>& m) {
    ambiguous_imports[im] = m;
  }

  void add_slave_request(metareqid_t reqid) {
    slave_requests[reqid].clear();
  }

  void add_slave_request(metareqid_t reqid, bufferlist& bl) {
    slave_requests[reqid].claim(bl);
  }

  void encode_payload(uint64_t features) {
    ::encode(subtrees, payload);
    ::encode(ambiguous_imports, payload);
    ::encode(slave_requests, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(subtrees, p);
    ::decode(ambiguous_imports, p);
    ::decode(slave_requests, p);
  }
};

#endif
