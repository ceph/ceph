// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#ifndef __MMDSIMPORTMAP_H
#define __MMDSIMPORTMAP_H

#include "msg/Message.h"

#include "include/types.h"


class MMDSImportMap : public Message {
 public:
  map<inodeno_t, set<inodeno_t> > imap;
  map<inodeno_t, set<inodeno_t> > ambiguous_imap;

  MMDSImportMap() : Message(MSG_MDS_IMPORTMAP) {}

  char *get_type_name() { return "mdsimportmap"; }

  void print(ostream& out) {
    out << "mdsimportmap(" << imap.size()
	<< "+" << ambiguous_imap.size()
	<< " imports)";
  }
  
  void add_import(inodeno_t im) {
    imap[im].clear();
  }
  void add_import_export(inodeno_t im, inodeno_t ex) {
    imap[im].insert(ex);
  }

  void add_ambiguous_import(inodeno_t im, const set<inodeno_t>& m) {
    ambiguous_imap[im] = m;
  }

  void encode_payload() {
    ::_encode(imap, payload);
    ::_encode(ambiguous_imap, payload);
  }
  void decode_payload() {
    int off = 0;
    ::_decode(imap, payload, off);
    ::_decode(ambiguous_imap, payload, off);
  }
};

#endif
