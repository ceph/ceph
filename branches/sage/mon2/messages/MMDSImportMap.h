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

#ifndef __MMDSIMPORTMAP_H
#define __MMDSIMPORTMAP_H

#include "msg/Message.h"

#include "include/types.h"


class MMDSImportMap : public Message {
 public:
  map<dirfrag_t, list<dirfrag_t> > imap;
  map<dirfrag_t, list<dirfrag_t> > ambiguous_imap;

  MMDSImportMap() : Message(MSG_MDS_IMPORTMAP) {}

  char *get_type_name() { return "mdsimportmap"; }

  void print(ostream& out) {
    out << "mdsimportmap(" << imap.size()
	<< "+" << ambiguous_imap.size()
	<< " imports)";
  }
  
  void add_import(dirfrag_t im) {
    imap[im].clear();
  }
  void add_import_export(dirfrag_t im, dirfrag_t ex) {
    imap[im].push_back(ex);
  }

  void add_ambiguous_import(dirfrag_t im, const list<dirfrag_t>& m) {
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
