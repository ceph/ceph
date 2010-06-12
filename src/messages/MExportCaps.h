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


#ifndef CEPH_MEXPORTCAPS_H
#define CEPH_MEXPORTCAPS_H

#include "msg/Message.h"


class MExportCaps : public Message {
 public:  
  inodeno_t ino;
  bufferlist cap_bl;
  map<client_t,entity_inst_t> client_map;

  MExportCaps() :
    Message(MSG_MDS_EXPORTCAPS) {}
private:
  ~MExportCaps() {}

public:
  const char *get_type_name() { return "export_caps"; }
  void print(ostream& o) {
    o << "export_caps(" << ino << ")";
  }

  void encode_payload() {
    ::encode(ino, payload);
    ::encode(cap_bl, payload);
    ::encode(client_map, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(ino, p);
    ::decode(cap_bl, p);
    ::decode(client_map, p);
  }

};

#endif
