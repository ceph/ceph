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


#ifndef __MEXPORTCAPS_H
#define __MEXPORTCAPS_H

#include "msg/Message.h"


class MExportCaps : public Message {
 public:  
  inodeno_t ino;
  bufferlist cap_bl;
  map<int,entity_inst_t> client_map;

  MExportCaps() :
    Message(MSG_MDS_EXPORTCAPS) {}

  const char *get_type_name() { return "export_caps"; }
  void print(ostream& o) {
    o << "export_caps(" << ino << ")";
  }

  virtual void decode_payload() {
    int off = 0;
    ::_decode(ino, payload, off);
    ::_decode(cap_bl, payload, off);
    ::_decode(client_map, payload, off);
  }
  virtual void encode_payload() {
    ::_encode(ino, payload);
    ::_encode(cap_bl, payload);
    ::_encode(client_map, payload);
  }

};

#endif
