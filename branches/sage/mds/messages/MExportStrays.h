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


#ifndef __MEXPORTSTRAYS_H
#define __MEXPORTSTRAYS_H

#include "msg/Message.h"


class MExportStrays : public Message {
 public:  
  bufferlist state;

  MExportStrays() :
    Message(MSG_MDS_EXPORTSTRAYS) {}

  virtual char *get_type_name() { return "SEx"; }
  void print(ostream& o) {
    o << "export_strays";
  }

  virtual void decode_payload() {
    state = payload;
  }
  virtual void encode_payload() {
    payload = state;
  }

};

#endif
