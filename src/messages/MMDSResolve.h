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

#ifndef __MMDSRESOLVE_H
#define __MMDSRESOLVE_H

#include "msg/Message.h"

#include "include/types.h"

class MMDSResolve : public Message {
 public:
  map<dirfrag_t, list<dirfrag_t> > subtrees;
  map<dirfrag_t, list<dirfrag_t> > ambiguous_imports;
  list<metareqid_t> slave_requests;

  MMDSResolve() : Message(MSG_MDS_RESOLVE) {}

  const char *get_type_name() { return "mds_resolve"; }

  void print(ostream& out) {
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

  void add_ambiguous_import(dirfrag_t im, const list<dirfrag_t>& m) {
    ambiguous_imports[im] = m;
  }

  void add_slave_request(metareqid_t reqid) {
    slave_requests.push_back(reqid);
  }

  void encode_payload() {
    ::_encode(subtrees, payload);
    ::_encode(ambiguous_imports, payload);
    ::_encode(slave_requests, payload);
  }
  void decode_payload() {
    int off = 0;
    ::_decode(subtrees, payload, off);
    ::_decode(ambiguous_imports, payload, off);
    ::_decode(slave_requests, payload, off);
  }
};

#endif
